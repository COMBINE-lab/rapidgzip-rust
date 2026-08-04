use crate::backend::{
    LineCounter, Output, SequentialDecoder, SequentialItem, decode_source,
    decode_source_with_index, validate_initial_stream,
};
use crate::config::Config;
use crate::gzip::StreamCursor;
use crate::index::{DeflateIndex, IndexCollector, IndexOptions};
use crate::indexed_parallel::IndexedPlan;
use crate::runtime::{AuxiliaryKind, RuntimeState};
use crate::{
    DecodeError, DecodeReport, DecoderHandle, DecoderStats, IndexedDecodeReport, IndexingError,
    ReadAt, WorkerLimitError,
};
use std::io::{self, IoSliceMut, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

enum Message {
    Data(Vec<u8>),
    Finished(Completion),
    Failed(Failure),
}

const CONSUMER_SHAPE_UNKNOWN: u8 = 0;
const CONSUMER_SHAPE_SMALL_READS: u8 = 1;
const CONSUMER_SHAPE_BULK_READS: u8 = 2;

struct RecyclingControl {
    state: OnceLock<Arc<RecyclingState>>,
    consumer_shape: std::sync::atomic::AtomicU8,
    decoded_chunk_size: usize,
}

impl RecyclingControl {
    fn note_consumer_read(&self, read_size: usize) -> bool {
        let bulk_threshold = self.decoded_chunk_size.div_ceil(4).min(64 * 1024);
        if read_size < bulk_threshold {
            return false;
        }
        self.consumer_shape
            .store(CONSUMER_SHAPE_BULK_READS, Ordering::Relaxed);
        true
    }

    fn finish_small_read_probe(&self) {
        let _ = self.consumer_shape.compare_exchange(
            CONSUMER_SHAPE_UNKNOWN,
            CONSUMER_SHAPE_SMALL_READS,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }
}

struct RecyclingState {
    sender: SyncSender<Vec<u8>>,
    decoded_chunk_size: usize,
    active: AtomicBool,
    reusable_allocations: Mutex<Vec<usize>>,
}

impl RecyclingState {
    /// Tags one live allocation without changing the ordinary message shape.
    ///
    /// The address is never dereferenced. The vector remains owned by the
    /// in-flight message or reader until `try_recycle` removes this tag, so no
    /// other live allocation can acquire the same address in the meantime.
    fn register(&self, buffer: &[u8]) {
        self.reusable_allocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(buffer.as_ptr() as usize);
    }

    /// Returns whether `buffer` was registered and therefore consumed here.
    fn try_recycle(&self, buffer: &mut Vec<u8>) -> bool {
        if !self.active.load(Ordering::Relaxed) {
            return false;
        }
        let pointer = buffer.as_ptr() as usize;
        let mut registered = self
            .reusable_allocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(index) = registered.iter().position(|&value| value == pointer) else {
            return false;
        };
        registered.swap_remove(index);
        drop(registered);

        let mut returned = std::mem::take(buffer);
        returned.clear();
        let capacity = returned.capacity();
        let eligible = capacity >= self.decoded_chunk_size
            && capacity <= self.decoded_chunk_size.saturating_mul(2);
        if eligible {
            let _ = self.sender.try_send(returned);
        }
        true
    }
}

struct CoordinatorRecycling {
    state: Arc<RecyclingState>,
    recycled: Receiver<Vec<u8>>,
}

enum Completion {
    Decode(DecodeReport),
    Indexed(IndexedDecodeReport),
}

enum Failure {
    Decode(DecodeError),
    Indexing(IndexingError),
}

struct ChannelOutput {
    sender: SyncSender<Message>,
    cancelled: Arc<AtomicBool>,
    runtime: Arc<RuntimeState>,
    recycling_control: Option<Arc<RecyclingControl>>,
    recycling: Option<CoordinatorRecycling>,
    decoded_chunk_size: usize,
}

impl ChannelOutput {
    fn send(&self, mut message: Message) -> Result<(), DecodeError> {
        let mut observed_full = false;
        loop {
            if self.cancelled.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            match self.sender.try_send(message) {
                Ok(()) => {
                    if !observed_full {
                        self.runtime.set_consumer_blocked(false);
                    }
                    return Ok(());
                }
                Err(TrySendError::Disconnected(_)) => return Err(DecodeError::Cancelled),
                Err(TrySendError::Full(returned)) => {
                    observed_full = true;
                    self.runtime.set_consumer_blocked(true);
                    message = returned;
                    thread::park_timeout(Duration::from_millis(1));
                }
            }
        }
    }
}

impl Output for ChannelOutput {
    fn emit(&mut self, chunk: Vec<u8>) -> Result<(), DecodeError> {
        let byte_count = chunk.len();
        self.send(Message::Data(chunk))?;
        self.runtime.add_decompressed_bytes(byte_count);
        Ok(())
    }

    fn emit_reusable(&mut self, chunk: Vec<u8>) -> Result<Vec<u8>, DecodeError> {
        if self.recycling_control.is_none() {
            self.emit(chunk)?;
            return Ok(Vec::new());
        }
        self.emit_reusable_controlled(chunk)
    }
}

impl ChannelOutput {
    #[inline(never)]
    fn emit_reusable_controlled(&mut self, chunk: Vec<u8>) -> Result<Vec<u8>, DecodeError> {
        let Some(consumer_shape) = self
            .recycling_control
            .as_ref()
            .map(|control| control.consumer_shape.load(Ordering::Relaxed))
        else {
            self.emit(chunk)?;
            return Ok(Vec::new());
        };
        match consumer_shape {
            CONSUMER_SHAPE_UNKNOWN => {
                self.emit(chunk)?;
                return Ok(Vec::new());
            }
            CONSUMER_SHAPE_BULK_READS => {
                self.recycling = None;
                self.recycling_control = None;
                self.emit(chunk)?;
                return Ok(Vec::new());
            }
            CONSUMER_SHAPE_SMALL_READS => {}
            _ => unreachable!("consumer shape has a valid encoding"),
        }
        // Reusing a buffer across the producer/consumer boundary saves
        // allocations for large sequential members, but transfers ownership
        // of the same pages between threads. Dense multi-member streams have
        // enough independent handoffs that this cache traffic is slower than
        // fresh worker-local allocations. A zero count means no member footer
        // has completed yet, so this keeps the full single-member fast path
        // and automatically retires recycling when a second member begins.
        if self.runtime.member_count() != 0 {
            // Drop the receiver as part of retirement so returned first-member
            // capacity is released immediately instead of remaining live
            // until the coordinator itself exits.
            if let Some(recycling) = &self.recycling {
                recycling.state.active.store(false, Ordering::Relaxed);
            }
            self.recycling = None;
            self.recycling_control = None;
            self.emit(chunk)?;
            return Ok(Vec::new());
        }
        let recycling_inactive = self
            .recycling_control
            .as_ref()
            .and_then(|control| control.state.get())
            .is_some_and(|state| !state.active.load(Ordering::Relaxed));
        if recycling_inactive {
            self.recycling = None;
            self.recycling_control = None;
            self.emit(chunk)?;
            return Ok(Vec::new());
        }
        let capacity = chunk.capacity();
        if chunk.len() < self.decoded_chunk_size
            || capacity < self.decoded_chunk_size
            || capacity > self.decoded_chunk_size.saturating_mul(2)
        {
            self.emit(chunk)?;
            return Ok(Vec::new());
        }
        if self.recycling.is_none() {
            let (recycler, recycled) = mpsc::sync_channel(2);
            let state = Arc::new(RecyclingState {
                sender: recycler,
                decoded_chunk_size: self.decoded_chunk_size,
                active: AtomicBool::new(true),
                reusable_allocations: Mutex::new(Vec::new()),
            });
            self.recycling_control
                .as_ref()
                .expect("small-read recycling retains its control")
                .state
                .set(Arc::clone(&state))
                .unwrap_or_else(|_| unreachable!("one coordinator initializes recycling"));
            self.recycling = Some(CoordinatorRecycling { state, recycled });
        }
        let recycling = self
            .recycling
            .as_ref()
            .expect("recycling is initialized before use");
        recycling.state.register(&chunk);
        self.emit(chunk)?;
        match self
            .recycling
            .as_ref()
            .expect("recycling is initialized before polling")
            .recycled
            .try_recv()
        {
            Ok(buffer) => {
                debug_assert!(buffer.is_empty());
                debug_assert!(buffer.capacity() >= self.decoded_chunk_size);
                Ok(buffer)
            }
            Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => Ok(Vec::new()),
        }
    }
}

enum Terminal {
    Open,
    Finished(Completion),
    Failed(Failure),
}

/// Owned parallel decoder output implementing [`Read`] and [`Send`].
///
/// Reaching EOF means the selected container passed every available check and
/// makes the final [`DecodeReport`] available through
/// [`DecoderReader::report`]. A decoding
/// failure is returned as an [`io::Error`] whose source is a [`DecodeError`].
///
/// Dropping this value cancels the background pipeline. It does not verify
/// unread compressed data; use [`DecoderReader::finish`] to discard unread
/// output while still verifying the complete stream.
///
/// A positional source is decoded by a background pipeline that is cancelled
/// and joined on drop. A non-seekable source is decoded synchronously as this
/// reader is pulled, so dropping it immediately drops the source and never
/// leaves a blocked coordinator thread behind.
///
/// Reusable positional output allocations make a bounded ownership round trip
/// through the reader handoff. Capacity is returned at the next receive after
/// the reader has finished observing that chunk. The lazy, non-blocking channel is
/// enabled only after a one-worker reader consumes one full chunk with small
/// requests. It retires before output from a second gzip member. Bulk readers
/// never construct it, preserving ordinary bulk-read, dense multi-member, and
/// BGZF hot-path behavior. It never changes output, verification, cancellation,
/// or error semantics.
#[must_use]
pub struct DecoderReader {
    mode: ReaderMode,
    cancelled: Arc<AtomicBool>,
    handle: DecoderHandle,
    current: Vec<u8>,
    recycling: Option<Arc<RecyclingControl>>,
    consumer_shape_observed: bool,
    current_offset: usize,
    terminal: Terminal,
}

enum ReaderMode {
    Coordinator {
        receiver: Option<Receiver<Message>>,
        worker: Option<JoinHandle<()>>,
    },
    Streaming {
        decoder: Box<SequentialDecoder<StreamCursor<Box<dyn Read + Send>>>>,
        collector: Option<Arc<IndexCollector>>,
        line_counter: LineCounter,
    },
}

/// Launches a coordinator thread around `decode` and wires it to the reader.
///
/// `configured_workers` seeds the runtime's immutable worker maximum.
fn spawn_coordinator<F>(
    decode: F,
    in_flight_chunks: usize,
    configured_workers: usize,
    decoded_chunk_size: usize,
) -> Result<DecoderReader, DecodeError>
where
    F: FnOnce(
            &AtomicBool,
            &mut ChannelOutput,
            &Arc<RuntimeState>,
        ) -> Result<Completion, IndexingError>
        + Send
        + 'static,
{
    let (sender, receiver) = mpsc::sync_channel(in_flight_chunks);
    let recycling = (configured_workers == 1).then(|| {
        Arc::new(RecyclingControl {
            state: OnceLock::new(),
            consumer_shape: std::sync::atomic::AtomicU8::new(CONSUMER_SHAPE_UNKNOWN),
            decoded_chunk_size,
        })
    });
    let consumer_shape_observed = recycling.is_none();
    let coordinator_recycling = recycling.clone();
    let cancelled = Arc::new(AtomicBool::new(false));
    let runtime = RuntimeState::new(configured_workers);
    let handle = DecoderHandle::new(Arc::clone(&runtime));
    let worker_cancelled = Arc::clone(&cancelled);
    let worker_runtime = Arc::clone(&runtime);
    let worker = thread::Builder::new()
        .name("rapidgzip-coordinator".to_owned())
        .spawn(move || {
            let _registration = worker_runtime.register_auxiliary(AuxiliaryKind::Coordinator);
            let mut output = ChannelOutput {
                sender,
                cancelled: Arc::clone(&worker_cancelled),
                runtime: Arc::clone(&worker_runtime),
                recycling_control: coordinator_recycling,
                recycling: None,
                decoded_chunk_size,
            };
            let terminal = match decode(&worker_cancelled, &mut output, &worker_runtime) {
                Ok(completion) => {
                    worker_runtime.set_member_count(match &completion {
                        Completion::Decode(report) => report.member_count,
                        Completion::Indexed(report) => report.decode.member_count,
                    });
                    Message::Finished(completion)
                }
                Err(IndexingError::Decode(DecodeError::Cancelled))
                    if worker_cancelled.load(Ordering::Relaxed) =>
                {
                    return;
                }
                Err(IndexingError::Decode(error)) => Message::Failed(Failure::Decode(error)),
                Err(error) => Message::Failed(Failure::Indexing(error)),
            };
            let _ = output.send(terminal);
        })
        .map_err(DecodeError::output_io)?;

    Ok(DecoderReader {
        mode: ReaderMode::Coordinator {
            receiver: Some(receiver),
            worker: Some(worker),
        },
        cancelled,
        handle,
        current: Vec::new(),
        recycling,
        consumer_shape_observed,
        current_offset: 0,
        terminal: Terminal::Open,
    })
}

pub(crate) fn spawn<R>(source: R, config: Config) -> Result<DecoderReader, DecodeError>
where
    R: ReadAt + 'static,
{
    let in_flight_chunks = config.in_flight_chunks;
    let configured_workers = config.decoder_threads;
    let decoded_chunk_size = config.decoded_chunk_size;
    spawn_coordinator(
        move |cancelled, output, runtime| {
            decode_source(&source, &config, cancelled, output, runtime)
                .map(Completion::Decode)
                .map_err(IndexingError::from)
        },
        in_flight_chunks,
        configured_workers,
        decoded_chunk_size,
    )
}

pub(crate) fn spawn_indexed<R>(
    source: R,
    config: Config,
    options: IndexOptions,
) -> Result<IndexingDecoderReader, DecodeError>
where
    R: ReadAt + 'static,
{
    let in_flight_chunks = config.in_flight_chunks;
    let configured_workers = config.decoder_threads;
    let decoded_chunk_size = config.decoded_chunk_size;
    spawn_coordinator(
        move |cancelled, output, runtime| {
            decode_source_with_index(&source, &config, cancelled, output, runtime, options)
                .map(Completion::Indexed)
        },
        in_flight_chunks,
        configured_workers,
        decoded_chunk_size,
    )
    .map(|inner| IndexingDecoderReader { inner })
}

pub(crate) fn spawn_from_index<R>(
    source: R,
    config: Config,
    index: Arc<DeflateIndex>,
    plan: IndexedPlan,
) -> Result<DecoderReader, DecodeError>
where
    R: ReadAt + 'static,
{
    let in_flight_chunks = config.in_flight_chunks;
    let configured_workers = config.decoder_threads;
    let decoded_chunk_size = config.decoded_chunk_size;
    spawn_coordinator(
        move |cancelled, output, runtime| {
            crate::indexed_parallel::decode(
                &source, &config, cancelled, output, &index, &plan, runtime,
            )
            .map(Completion::Decode)
            .map_err(IndexingError::from)
        },
        in_flight_chunks,
        configured_workers,
        decoded_chunk_size,
    )
}

/// Creates a pull-driven decoder for a non-seekable source.
///
/// One initial read provides best-effort fail-fast header validation. The
/// resumable decoder then reads only from `DecoderReader::read`; no coordinator
/// or decoder-worker thread is created for this path.
pub(crate) fn spawn_stream<R>(source: R, config: Config) -> Result<DecoderReader, DecodeError>
where
    R: Read + Send + 'static,
{
    let source: Box<dyn Read + Send> = Box::new(source);
    let mut cursor = StreamCursor::new(source, config.input_page_size);
    validate_initial_stream(&mut cursor, &config)?;
    let runtime = RuntimeState::new(config.decoder_threads);
    let handle = DecoderHandle::new(Arc::clone(&runtime));
    let decoder = SequentialDecoder::new(
        cursor,
        &config,
        0,
        0,
        config.decoder_threads,
        &runtime,
        None,
    );
    Ok(DecoderReader {
        mode: ReaderMode::Streaming {
            decoder: Box::new(decoder),
            collector: None,
            line_counter: LineCounter::new(config.count_lines),
        },
        cancelled: Arc::new(AtomicBool::new(false)),
        handle,
        current: Vec::new(),
        recycling: None,
        consumer_shape_observed: true,
        current_offset: 0,
        terminal: Terminal::Open,
    })
}

pub(crate) fn spawn_stream_indexed<R>(
    source: R,
    config: Config,
    options: IndexOptions,
) -> Result<IndexingDecoderReader, DecodeError>
where
    R: Read + Send + 'static,
{
    let source: Box<dyn Read + Send> = Box::new(source);
    let mut cursor = StreamCursor::new(source, config.input_page_size);
    validate_initial_stream(&mut cursor, &config)?;
    let runtime = RuntimeState::new(config.decoder_threads);
    let handle = DecoderHandle::new(Arc::clone(&runtime));
    let collector = IndexCollector::new(options, config.count_lines);
    let decoder = SequentialDecoder::new(
        cursor,
        &config,
        0,
        0,
        config.decoder_threads,
        &runtime,
        Some(&collector),
    );
    Ok(IndexingDecoderReader {
        inner: DecoderReader {
            mode: ReaderMode::Streaming {
                decoder: Box::new(decoder),
                collector: Some(collector),
                line_counter: LineCounter::new(config.count_lines),
            },
            cancelled: Arc::new(AtomicBool::new(false)),
            handle,
            current: Vec::new(),
            recycling: None,
            consumer_shape_observed: true,
            current_offset: 0,
            terminal: Terminal::Open,
        },
    })
}

impl DecoderReader {
    /// Returns a cloneable telemetry and runtime-control handle.
    ///
    /// The handle can be retained after moving this reader into a parser or a
    /// `Box<dyn Read + Send>`.
    pub fn handle(&self) -> DecoderHandle {
        self.handle.clone()
    }

    /// Returns an approximate lock-free snapshot of decoder activity.
    pub fn stats(&self) -> DecoderStats {
        self.handle.stats()
    }

    /// Changes the maximum number of workers that may accept decoder tasks.
    ///
    /// This is a convenience forwarding method for
    /// [`DecoderHandle::set_worker_limit`]. Retain a handle when the reader
    /// will be moved into another component.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLimitError`] for zero or a value above the configured
    /// worker budget.
    pub fn set_worker_limit(&self, workers: usize) -> Result<(), WorkerLimitError> {
        self.handle.set_worker_limit(workers)
    }

    /// Returns the report after verified EOF has been observed.
    ///
    /// This is `None` while decoding is open and after a terminal failure.
    pub const fn report(&self) -> Option<&DecodeReport> {
        match &self.terminal {
            Terminal::Finished(Completion::Decode(report)) => Some(report),
            Terminal::Finished(Completion::Indexed(report)) => Some(&report.decode),
            Terminal::Open | Terminal::Failed(_) => None,
        }
    }

    fn join_worker(&mut self) -> Result<(), DecodeError> {
        let worker = match &mut self.mode {
            ReaderMode::Coordinator { worker, .. } => worker.take(),
            ReaderMode::Streaming { .. } => None,
        };
        if let Some(worker) = worker {
            if worker.join().is_err() {
                return Err(DecodeError::WorkerPanicked);
            }
        }
        Ok(())
    }

    fn release_current(&mut self) -> bool {
        let Some(recycling) = self
            .recycling
            .as_deref()
            .and_then(|control| control.state.get())
        else {
            return false;
        };
        if !recycling.try_recycle(&mut self.current) {
            return false;
        }
        self.current_offset = 0;
        true
    }

    fn receive(&mut self) {
        let coordinator = matches!(self.mode, ReaderMode::Coordinator { .. });
        let mut reusable = if coordinator {
            if self.recycling.is_none() {
                self.current = Vec::new();
                self.current_offset = 0;
            } else if self.release_current() {
                // Return eligible capacity before the coordinator produces
                // its next reusable handoff.
            } else {
                self.current = Vec::new();
                self.current_offset = 0;
            }
            Vec::new()
        } else {
            debug_assert!(self.recycling.is_none());
            std::mem::take(&mut self.current)
        };
        reusable.clear();
        let message = match &mut self.mode {
            ReaderMode::Coordinator { receiver, .. } => receiver
                .as_ref()
                .expect("receiver remains present until shutdown")
                .recv()
                .ok(),
            ReaderMode::Streaming {
                decoder,
                collector,
                line_counter,
            } => {
                let runtime = Arc::clone(&self.handle.state);
                let result = {
                    let _busy = runtime.begin_task();
                    decoder.next_chunk(&self.cancelled, reusable)
                };
                match result {
                    Ok(SequentialItem::Chunk(data)) => {
                        line_counter.note_output(&data, collector.as_deref());
                        runtime.add_decompressed_bytes(data.len());
                        Some(Message::Data(data))
                    }
                    Ok(SequentialItem::Finished(report)) => {
                        let report = line_counter.finish_report(report);
                        if let Some(collector) = collector {
                            match collector.finish(
                                report.compressed_bytes,
                                report.decompressed_bytes,
                                report.line_count,
                            ) {
                                Ok(index) => Some(Message::Finished(Completion::Indexed(
                                    IndexedDecodeReport {
                                        decode: report,
                                        index,
                                    },
                                ))),
                                Err(error) => Some(Message::Failed(Failure::Indexing(
                                    IndexingError::Index(error),
                                ))),
                            }
                        } else {
                            Some(Message::Finished(Completion::Decode(report)))
                        }
                    }
                    Err(error) => Some(Message::Failed(Failure::Decode(error))),
                }
            }
        };
        match message {
            Some(Message::Data(data)) => {
                self.current = data;
                self.current_offset = 0;
                if !self.consumer_shape_observed && self.handle.state.member_count() != 0 {
                    if let Some(recycling) = self
                        .recycling
                        .as_deref()
                        .and_then(|control| control.state.get())
                    {
                        recycling.active.store(false, Ordering::Relaxed);
                    }
                    self.recycling = None;
                    self.consumer_shape_observed = true;
                }
            }
            Some(Message::Finished(completion)) => {
                self.handle.state.mark_terminal();
                let terminal = match self.join_worker() {
                    Ok(()) => Terminal::Finished(completion),
                    Err(error) => Terminal::Failed(Failure::Decode(error)),
                };
                self.terminal = terminal;
            }
            Some(Message::Failed(error)) => {
                self.handle.state.mark_terminal();
                let terminal = match self.join_worker() {
                    Ok(()) => Terminal::Failed(error),
                    Err(join_error) => Terminal::Failed(Failure::Decode(join_error)),
                };
                self.terminal = terminal;
            }
            None => {
                self.handle.state.mark_terminal();
                let error = self
                    .join_worker()
                    .err()
                    .unwrap_or(DecodeError::WorkerPanicked);
                self.terminal = Terminal::Failed(Failure::Decode(error));
            }
        }
    }

    /// Discards unread output, verifies the remaining stream, and returns its
    /// final report.
    ///
    /// # Errors
    ///
    /// Returns the first decoding, verification, input, or worker failure.
    pub fn finish(mut self) -> Result<DecodeReport, DecodeError> {
        self.current.clear();
        loop {
            match &self.terminal {
                Terminal::Finished(Completion::Decode(report)) => return Ok(*report),
                Terminal::Finished(Completion::Indexed(report)) => return Ok(report.decode),
                Terminal::Failed(Failure::Decode(error)) => return Err(error.clone()),
                Terminal::Failed(Failure::Indexing(IndexingError::Decode(error))) => {
                    return Err(error.clone());
                }
                Terminal::Failed(Failure::Indexing(IndexingError::Index(_))) => {
                    return Err(DecodeError::WorkerPanicked);
                }
                Terminal::Open => self.receive(),
            }
            self.current.clear();
        }
    }

    fn finish_indexed(mut self) -> Result<IndexedDecodeReport, IndexingError> {
        self.current.clear();
        while matches!(self.terminal, Terminal::Open) {
            self.receive();
            self.current.clear();
        }
        match std::mem::replace(&mut self.terminal, Terminal::Open) {
            Terminal::Finished(Completion::Indexed(report)) => Ok(report),
            Terminal::Finished(Completion::Decode(_)) | Terminal::Open => {
                Err(IndexingError::Decode(DecodeError::WorkerPanicked))
            }
            Terminal::Failed(Failure::Decode(error)) => Err(IndexingError::Decode(error)),
            Terminal::Failed(Failure::Indexing(error)) => Err(error),
        }
    }
}

/// Owned decoded output that publishes a random-access index at verified EOF.
///
/// This reader has the same `Read + Send` behavior, runtime telemetry, dynamic
/// worker controls, backpressure, and cancellation semantics as
/// [`DecoderReader`]. Index construction is explicit in the type so the normal
/// reader does not pay for checkpoint windows or lose the small, [`Copy`]
/// [`DecodeReport`] result.
///
/// Reaching [`Read`] EOF means both the compressed stream and collected index
/// have been validated. [`Self::report`] then borrows the complete result, while
/// [`Self::finish`] consumes the reader and returns ownership of it.
#[must_use]
pub struct IndexingDecoderReader {
    inner: DecoderReader,
}

impl IndexingDecoderReader {
    /// Returns a cloneable telemetry and runtime-control handle.
    pub fn handle(&self) -> DecoderHandle {
        self.inner.handle()
    }

    /// Returns an approximate lock-free snapshot of decoder activity.
    pub fn stats(&self) -> DecoderStats {
        self.inner.stats()
    }

    /// Changes the maximum number of workers that may accept decoder tasks.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLimitError`] for zero or a value above the configured
    /// worker budget.
    pub fn set_worker_limit(&self, workers: usize) -> Result<(), WorkerLimitError> {
        self.inner.set_worker_limit(workers)
    }

    /// Returns the indexed result after verified EOF has been observed.
    ///
    /// This is `None` while decoding is open and after a terminal failure.
    #[must_use]
    pub const fn report(&self) -> Option<&IndexedDecodeReport> {
        match &self.inner.terminal {
            Terminal::Finished(Completion::Indexed(report)) => Some(report),
            Terminal::Open | Terminal::Finished(Completion::Decode(_)) | Terminal::Failed(_) => {
                None
            }
        }
    }

    /// Discards unread output, verifies the remaining stream, finalizes the
    /// index, and returns both the scalar report and index.
    ///
    /// # Errors
    ///
    /// Returns the first decoding, verification, input, worker, or index
    /// construction failure.
    pub fn finish(self) -> Result<IndexedDecodeReport, IndexingError> {
        self.inner.finish_indexed()
    }
}

impl Read for IndexingDecoderReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.inner.read(output)
    }

    fn read_vectored(&mut self, buffers: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.inner.read_vectored(buffers)
    }
}

impl DecoderReader {
    #[inline]
    fn read_plain(&mut self, output: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.current_offset < self.current.len() {
                let count = output
                    .len()
                    .min(self.current.len().saturating_sub(self.current_offset));
                output[..count].copy_from_slice(
                    &self.current[self.current_offset..self.current_offset + count],
                );
                self.current_offset += count;
                if self.current_offset == self.current.len() {
                    self.current.clear();
                    self.current_offset = 0;
                }
                self.handle.state.add_consumed_bytes(count);
                return Ok(count);
            }

            match &self.terminal {
                Terminal::Finished(_) => return Ok(0),
                Terminal::Failed(Failure::Decode(error)) => return Err(error.to_io_error()),
                Terminal::Failed(Failure::Indexing(error)) => return Err(error.to_io_error()),
                Terminal::Open => self.receive(),
            }
        }
    }

    #[inline(never)]
    fn read_recycling(&mut self, output: &mut [u8]) -> io::Result<usize> {
        loop {
            let current_length = self.current.len();
            if self.current_offset < current_length {
                let observed_bulk = !self.consumer_shape_observed
                    && self
                        .recycling
                        .as_deref()
                        .is_some_and(|control| control.note_consumer_read(output.len()));
                if observed_bulk {
                    self.recycling = None;
                    self.consumer_shape_observed = true;
                    // The probe has not copied any bytes yet. Tail-dispatch
                    // immediately so even this first bulk request uses the
                    // compact, original reader loop.
                    return self.read_plain(output);
                }
                let count = output
                    .len()
                    .min(current_length.saturating_sub(self.current_offset));
                output[..count].copy_from_slice(
                    &self.current[self.current_offset..self.current_offset + count],
                );
                self.current_offset += count;
                let exhausted = self.current_offset == current_length;
                let retain_for_recycling = self
                    .recycling
                    .as_deref()
                    .and_then(|control| control.state.get())
                    .is_some();
                if exhausted && !retain_for_recycling {
                    // Preserve the original reader path unless a published
                    // recycler may own this allocation tag. Streaming decode
                    // also reuses this cleared vector synchronously.
                    self.current_offset = 0;
                    self.current.clear();
                }
                if exhausted
                    && !self.consumer_shape_observed
                    && let Some(control) = self.recycling.as_deref()
                    && current_length >= control.decoded_chunk_size
                {
                    // Observe every request used for the first recyclable
                    // chunk so a one-byte format probe cannot hide a later
                    // bulk-read shape. Later chunks pay no sampling cost.
                    control.finish_small_read_probe();
                    self.consumer_shape_observed = true;
                }
                self.handle.state.add_consumed_bytes(count);
                return Ok(count);
            }

            match &self.terminal {
                Terminal::Finished(_) => return Ok(0),
                Terminal::Failed(Failure::Decode(error)) => return Err(error.to_io_error()),
                Terminal::Failed(Failure::Indexing(error)) => return Err(error.to_io_error()),
                Terminal::Open => self.receive(),
            }
        }
    }
}

impl Read for DecoderReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.recycling.is_none() {
            self.read_plain(output)
        } else {
            self.read_recycling(output)
        }
    }

    fn read_vectored(&mut self, buffers: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        let mut total = 0;
        for buffer in buffers {
            if buffer.is_empty() {
                continue;
            }
            match self.read(buffer) {
                Ok(0) => break,
                Ok(read) => {
                    total += read;
                    if read < buffer.len() {
                        break;
                    }
                }
                Err(_) if total > 0 => break,
                Err(error) => return Err(error),
            }
        }
        Ok(total)
    }
}

impl Drop for DecoderReader {
    fn drop(&mut self) {
        self.release_current();
        self.cancelled.store(true, Ordering::Relaxed);
        self.handle.state.mark_terminal();
        if let ReaderMode::Coordinator { receiver, .. } = &mut self.mode {
            receiver.take();
            let _ = self.join_worker();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChannelOutput, DecoderReader, IndexingDecoderReader, Message, ReaderMode, Terminal,
    };
    use crate::DecoderHandle;
    use crate::backend::Output;
    use crate::runtime::RuntimeState;
    use std::io::Read;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    fn assert_traits<T: Read + Send + Unpin>() {}

    #[test]
    fn decoder_reader_is_read_send_and_unpin() {
        assert_traits::<DecoderReader>();
        assert_traits::<IndexingDecoderReader>();
    }

    fn channel_output(
        sender: mpsc::SyncSender<Message>,
        recycling_enabled: bool,
    ) -> (ChannelOutput, Option<Arc<super::RecyclingControl>>) {
        let control = recycling_enabled.then(|| {
            Arc::new(super::RecyclingControl {
                state: std::sync::OnceLock::new(),
                consumer_shape: std::sync::atomic::AtomicU8::new(super::CONSUMER_SHAPE_SMALL_READS),
                decoded_chunk_size: 1024,
            })
        });
        (
            ChannelOutput {
                sender,
                cancelled: Arc::new(AtomicBool::new(false)),
                runtime: RuntimeState::new(1),
                recycling_control: control.clone(),
                recycling: None,
                decoded_chunk_size: 1024,
            },
            control,
        )
    }

    #[test]
    fn reusable_handoff_recycles_only_after_the_reader_releases_it() {
        let (sender, receiver) = mpsc::sync_channel(2);
        let (mut output, slot) = channel_output(sender, true);
        let mut bytes = Vec::with_capacity(1024);
        bytes.resize(1024, 7);

        let immediate = output.emit_reusable(bytes).unwrap();
        assert_eq!(immediate.capacity(), 0);
        let Message::Data(mut chunk) = receiver.recv().unwrap() else {
            panic!("expected data handoff");
        };
        assert_eq!(chunk, vec![7; 1024]);
        assert!(
            slot.unwrap()
                .state
                .get()
                .expect("emission initializes recycling")
                .try_recycle(&mut chunk)
        );

        let replacement = output.emit_reusable(vec![8; 1024]).unwrap();
        assert_eq!(replacement.capacity(), 1024);
    }

    #[test]
    fn disabled_recycling_preserves_the_plain_data_handoff() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (mut output, slot) = channel_output(sender, false);
        let mut bytes = Vec::with_capacity(1024);
        bytes.resize(1024, 3);

        assert_eq!(output.emit_reusable(bytes).unwrap().capacity(), 0);
        let Message::Data(bytes) = receiver.recv().unwrap() else {
            panic!("expected plain data handoff");
        };
        assert_eq!(bytes, vec![3; 1024]);
        assert!(slot.is_none());
    }

    #[test]
    fn short_reusable_handoff_does_not_initialize_recycling() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (mut output, slot) = channel_output(sender, true);

        assert_eq!(output.emit_reusable(vec![3; 1023]).unwrap().capacity(), 0);
        assert!(matches!(receiver.recv().unwrap(), Message::Data(_)));
        assert!(slot.unwrap().state.get().is_none());
    }

    #[test]
    fn completed_member_retires_recycling_before_the_next_handoff() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (mut output, slot) = channel_output(sender, true);
        output.runtime.set_member_count(1);
        let mut bytes = Vec::with_capacity(1024);
        bytes.resize(1024, 4);

        assert_eq!(output.emit_reusable(bytes).unwrap().capacity(), 0);
        assert!(matches!(receiver.recv().unwrap(), Message::Data(_)));
        assert!(slot.unwrap().state.get().is_none());
    }

    #[test]
    fn retirement_disables_reader_side_registry_checks() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (mut output, slot) = channel_output(sender, true);
        assert_eq!(output.emit_reusable(vec![1; 1024]).unwrap().capacity(), 0);
        let Message::Data(mut first) = receiver.recv().unwrap() else {
            panic!("expected first data handoff");
        };
        let state = Arc::clone(slot.unwrap().state.get().expect("initialized recycler"));

        output.runtime.set_member_count(1);
        assert_eq!(output.emit_reusable(vec![2; 1024]).unwrap().capacity(), 0);
        assert!(!state.try_recycle(&mut first));
    }

    #[test]
    fn bulk_consumer_shape_never_initializes_recycling() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (mut output, control) = channel_output(sender, true);
        let control = control.unwrap();
        control
            .consumer_shape
            .store(super::CONSUMER_SHAPE_UNKNOWN, Ordering::Relaxed);

        assert!(control.note_consumer_read(256));
        assert_eq!(output.emit_reusable(vec![1; 1024]).unwrap().capacity(), 0);
        assert!(matches!(receiver.recv().unwrap(), Message::Data(_)));
        assert!(control.state.get().is_none());
    }

    #[test]
    fn parser_sized_probe_enables_recycling_only_after_a_full_chunk() {
        let (_state, _recycled, control) = recycling_state();
        control
            .consumer_shape
            .store(super::CONSUMER_SHAPE_UNKNOWN, Ordering::Relaxed);

        assert!(!control.note_consumer_read(255));
        assert_eq!(
            control.consumer_shape.load(Ordering::Relaxed),
            super::CONSUMER_SHAPE_UNKNOWN
        );
        control.finish_small_read_probe();
        assert_eq!(
            control.consumer_shape.load(Ordering::Relaxed),
            super::CONSUMER_SHAPE_SMALL_READS
        );
    }

    fn positional_reader(
        bytes: Vec<u8>,
        recycling: Option<Arc<super::RecyclingControl>>,
        register: bool,
    ) -> DecoderReader {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(sender);
        let runtime = RuntimeState::new(1);
        if register {
            recycling
                .as_deref()
                .and_then(|control| control.state.get())
                .expect("test recycling state")
                .register(&bytes);
        }
        DecoderReader {
            mode: ReaderMode::Coordinator {
                receiver: Some(receiver),
                worker: None,
            },
            cancelled: Arc::new(AtomicBool::new(false)),
            handle: DecoderHandle::new(Arc::clone(&runtime)),
            current: bytes,
            recycling,
            consumer_shape_observed: false,
            current_offset: 0,
            terminal: Terminal::Open,
        }
    }

    fn recycling_state() -> (
        Arc<super::RecyclingState>,
        mpsc::Receiver<Vec<u8>>,
        Arc<super::RecyclingControl>,
    ) {
        let (sender, receiver) = mpsc::sync_channel(2);
        let state = Arc::new(super::RecyclingState {
            sender,
            decoded_chunk_size: 1024,
            active: AtomicBool::new(true),
            reusable_allocations: std::sync::Mutex::new(Vec::new()),
        });
        let control = Arc::new(super::RecyclingControl {
            state: std::sync::OnceLock::new(),
            consumer_shape: std::sync::atomic::AtomicU8::new(super::CONSUMER_SHAPE_SMALL_READS),
            decoded_chunk_size: 1024,
        });
        control
            .state
            .set(Arc::clone(&state))
            .unwrap_or_else(|_| unreachable!("new test slot is empty"));
        (state, receiver, control)
    }

    #[test]
    fn next_receive_recycles_an_exhausted_positional_chunk() {
        let mut bytes = Vec::with_capacity(1024);
        bytes.resize(1024, 9);
        let (_state, recycled, recycling) = recycling_state();
        let mut reader = positional_reader(bytes, Some(recycling), true);

        let mut output = [0_u8; 255];
        let mut decoded = Vec::new();
        while decoded.len() < 1024 {
            let count = reader.read(&mut output).unwrap();
            decoded.extend_from_slice(&output[..count]);
        }
        assert_eq!(decoded, vec![9; 1024]);
        assert!(recycled.try_recv().is_err());

        reader.receive();
        assert_eq!(recycled.recv().unwrap().capacity(), 1024);
    }

    #[test]
    fn returned_capacity_is_size_and_entry_bounded() {
        let (_state, recycled, recycling) = recycling_state();
        let mut reader = positional_reader(Vec::new(), Some(recycling), false);
        for capacity in [1023, 2049, 1024, 1024, 1024] {
            reader.current = Vec::with_capacity(capacity);
            reader
                .recycling
                .as_deref()
                .and_then(|control| control.state.get())
                .expect("test recycling state")
                .register(&reader.current);
            reader.release_current();
        }

        assert_eq!(recycled.try_iter().count(), 2);
    }

    #[test]
    fn exhausting_unpooled_positional_output_does_not_replay_it() {
        let bytes = vec![5_u8; 1024];
        let mut reader = positional_reader(bytes, None, false);

        let mut output = [0_u8; 1024];
        assert_eq!(reader.read(&mut output).unwrap(), output.len());
        assert_eq!(output, [5; 1024]);
        assert_eq!(reader.current_offset, reader.current.len());
    }

    #[test]
    fn plain_output_is_not_returned_on_a_recycling_reader() {
        let (_state, recycled, recycling) = recycling_state();
        let mut reader = positional_reader(vec![6_u8; 1024], Some(recycling), false);

        let mut output = [0_u8; 1024];
        assert_eq!(reader.read(&mut output).unwrap(), output.len());
        assert_eq!(reader.current_offset, reader.current.len());
        assert!(recycled.try_recv().is_err());
    }
}
