use crate::backend::{
    Output, SequentialDecoder, SequentialItem, decode_source, decode_source_with_index,
    validate_initial_stream,
};
use crate::config::Config;
use crate::gzip::StreamCursor;
use crate::index::{IndexCollector, IndexOptions};
use crate::runtime::{AuxiliaryKind, RuntimeState};
use crate::{
    DecodeError, DecodeReport, DecoderHandle, DecoderStats, IndexedDecodeReport, IndexingError,
    ReadAt, WorkerLimitError,
};
use std::io::{self, IoSliceMut, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

enum Message {
    Data(Vec<u8>),
    Finished(Completion),
    Failed(Failure),
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
#[must_use]
pub struct DecoderReader {
    mode: ReaderMode,
    cancelled: Arc<AtomicBool>,
    handle: DecoderHandle,
    current: Vec<u8>,
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
    },
}

/// Launches a coordinator thread around `decode` and wires it to the reader.
///
/// `configured_workers` seeds the runtime's immutable worker maximum.
fn spawn_coordinator<F>(
    decode: F,
    in_flight_chunks: usize,
    configured_workers: usize,
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
    spawn_coordinator(
        move |cancelled, output, runtime| {
            decode_source(&source, &config, cancelled, output, runtime)
                .map(Completion::Decode)
                .map_err(IndexingError::from)
        },
        in_flight_chunks,
        configured_workers,
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
    spawn_coordinator(
        move |cancelled, output, runtime| {
            decode_source_with_index(&source, &config, cancelled, output, runtime, options)
                .map(Completion::Indexed)
        },
        in_flight_chunks,
        configured_workers,
    )
    .map(|inner| IndexingDecoderReader { inner })
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
        },
        cancelled: Arc::new(AtomicBool::new(false)),
        handle,
        current: Vec::new(),
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
    let collector = IndexCollector::new(options);
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
            },
            cancelled: Arc::new(AtomicBool::new(false)),
            handle,
            current: Vec::new(),
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

    fn receive(&mut self) {
        let mut reusable = std::mem::take(&mut self.current);
        reusable.clear();
        let message = match &mut self.mode {
            ReaderMode::Coordinator { receiver, .. } => receiver
                .as_ref()
                .expect("receiver remains present until shutdown")
                .recv()
                .ok(),
            ReaderMode::Streaming { decoder, collector } => {
                let runtime = Arc::clone(&self.handle.state);
                let result = {
                    let _busy = runtime.begin_task();
                    decoder.next_chunk(&self.cancelled, reusable)
                };
                match result {
                    Ok(SequentialItem::Chunk(data)) => {
                        runtime.add_decompressed_bytes(data.len());
                        Some(Message::Data(data))
                    }
                    Ok(SequentialItem::Finished(report)) => {
                        if let Some(collector) = collector {
                            match collector
                                .finish(report.compressed_bytes, report.decompressed_bytes)
                            {
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

impl Read for DecoderReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }

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
    use super::{DecoderReader, IndexingDecoderReader};
    use std::io::Read;

    fn assert_traits<T: Read + Send + Unpin>() {}

    #[test]
    fn decoder_reader_is_read_send_and_unpin() {
        assert_traits::<DecoderReader>();
        assert_traits::<IndexingDecoderReader>();
    }
}
