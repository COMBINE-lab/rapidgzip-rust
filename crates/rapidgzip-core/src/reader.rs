use crate::backend::{Output, decode_source, decode_stream};
use crate::config::Config;
use crate::gzip::{StreamCursor, validate_initial_stream_header};
use crate::runtime::{AuxiliaryKind, RuntimeState};
use crate::{DecodeError, DecodeReport, DecoderHandle, DecoderStats, ReadAt, WorkerLimitError};
use std::io::{self, IoSliceMut, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

enum Message {
    Data(Vec<u8>),
    Finished(DecodeReport),
    Failed(DecodeError),
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
    Finished(DecodeReport),
    Failed(DecodeError),
}

/// Owned parallel decoder output implementing [`Read`] and [`Send`].
///
/// Reaching EOF means every gzip member was verified and makes the final
/// [`DecodeReport`] available through [`DecoderReader::report`]. A decoding
/// failure is returned as an [`io::Error`] whose source is a [`DecodeError`].
///
/// Dropping this value cancels the background pipeline. It does not verify
/// unread compressed data; use [`DecoderReader::finish`] to discard unread
/// output while still verifying the complete stream.
///
/// A reader over a positional source also joins the pipeline on drop. A reader
/// over a non-seekable source does not, because its coordinator can be blocked
/// in a read against a producer that never writes again, and a drop must not be
/// able to block forever. That coordinator observes the cancellation and exits
/// at its next read or send boundary.
#[must_use]
pub struct DecoderReader {
    receiver: Option<Receiver<Message>>,
    worker: Option<JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
    handle: DecoderHandle,
    current: Vec<u8>,
    current_offset: usize,
    terminal: Terminal,
    join_on_drop: bool,
}

/// Launches a coordinator thread around `decode` and wires it to the reader.
///
/// `configured_workers` seeds the runtime's immutable worker maximum, and
/// `join_on_drop` selects whether dropping the reader waits for the coordinator.
fn spawn_coordinator<F>(
    decode: F,
    in_flight_chunks: usize,
    configured_workers: usize,
    join_on_drop: bool,
) -> Result<DecoderReader, DecodeError>
where
    F: FnOnce(
            &AtomicBool,
            &mut ChannelOutput,
            &Arc<RuntimeState>,
        ) -> Result<DecodeReport, DecodeError>
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
                Ok(report) => {
                    worker_runtime.set_member_count(report.member_count);
                    Message::Finished(report)
                }
                Err(DecodeError::Cancelled) if worker_cancelled.load(Ordering::Relaxed) => return,
                Err(error) => Message::Failed(error),
            };
            let _ = output.send(terminal);
        })
        .map_err(DecodeError::output_io)?;

    Ok(DecoderReader {
        receiver: Some(receiver),
        worker: Some(worker),
        cancelled,
        handle,
        current: Vec::new(),
        current_offset: 0,
        terminal: Terminal::Open,
        join_on_drop,
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
        },
        in_flight_chunks,
        configured_workers,
        true,
    )
}

/// Spawns a coordinator that decodes a non-seekable source sequentially.
///
/// The initial header is validated on the calling thread, before anything is
/// spawned, so obviously wrong input is reported by the constructor rather than
/// by the first read.
///
/// The runtime is configured with a single worker because the sequential path
/// is the only one a forward-only source can reach, so the telemetry reports
/// the concurrency actually in use rather than the builder's thread count.
pub(crate) fn spawn_stream<R>(source: R, config: Config) -> Result<DecoderReader, DecodeError>
where
    R: Read + Send + 'static,
{
    let mut cursor = StreamCursor::new(source, config.input_page_size);
    // Only gzip has a header worth checking before the coordinator starts.
    // zlib framing is validated by the decode itself, and raw DEFLATE has no
    // header at all.
    if crate::format::detect(cursor.buffered_prefix()?) != Some(crate::Format::Zlib)
        && config.format != crate::Format::RawDeflate
        && config.format != crate::Format::Zlib
    {
        validate_initial_stream_header(&mut cursor)?;
    }
    let in_flight_chunks = config.in_flight_chunks;
    spawn_coordinator(
        move |cancelled, output, runtime| {
            decode_stream(&mut cursor, &config, cancelled, output, runtime)
        },
        in_flight_chunks,
        1,
        false,
    )
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
            Terminal::Finished(report) => Some(report),
            Terminal::Open | Terminal::Failed(_) => None,
        }
    }

    fn join_worker(&mut self) -> Result<(), DecodeError> {
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                return Err(DecodeError::WorkerPanicked);
            }
        }
        Ok(())
    }

    fn receive(&mut self) {
        let message = self
            .receiver
            .as_ref()
            .expect("receiver remains present until shutdown")
            .recv();
        match message {
            Ok(Message::Data(data)) => {
                self.current = data;
                self.current_offset = 0;
            }
            Ok(Message::Finished(report)) => {
                self.handle.state.mark_terminal();
                let terminal = match self.join_worker() {
                    Ok(()) => Terminal::Finished(report),
                    Err(error) => Terminal::Failed(error),
                };
                self.terminal = terminal;
            }
            Ok(Message::Failed(error)) => {
                self.handle.state.mark_terminal();
                let terminal = match self.join_worker() {
                    Ok(()) => Terminal::Failed(error),
                    Err(join_error) => Terminal::Failed(join_error),
                };
                self.terminal = terminal;
            }
            Err(_) => {
                self.handle.state.mark_terminal();
                let error = self
                    .join_worker()
                    .err()
                    .unwrap_or(DecodeError::WorkerPanicked);
                self.terminal = Terminal::Failed(error);
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
                Terminal::Finished(report) => return Ok((*report).clone()),
                Terminal::Failed(error) => return Err(error.clone()),
                Terminal::Open => self.receive(),
            }
            self.current.clear();
        }
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
                Terminal::Failed(error) => return Err(error.to_io_error()),
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
        self.receiver.take();
        if self.join_on_drop {
            let _ = self.join_worker();
        } else {
            // A non-seekable coordinator can be parked inside a read against a
            // producer that never writes again, so joining here could block
            // forever. Cancellation is already visible and the closed receiver
            // fails its next send, so it exits without further help.
            self.worker.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DecoderReader;
    use std::io::Read;

    fn assert_traits<T: Read + Send + Unpin>() {}

    #[test]
    fn decoder_reader_is_read_send_and_unpin() {
        assert_traits::<DecoderReader>();
    }
}
