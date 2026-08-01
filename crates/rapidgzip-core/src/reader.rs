use crate::backend::{Output, decode_source};
use crate::buffer_pool::ByteBufferFreeList;
use crate::config::Config;
use crate::{DecodeError, DecodeReport, ReadAt};
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
    /// Reader-local free list: consumer recycles fully-read chunks; this sink
    /// steals empty capacity for `emit_reusable` returns so sequential emit
    /// loops can reuse buffers without cold-allocating.
    free_list: Arc<ByteBufferFreeList>,
}

impl ChannelOutput {
    fn send(&self, mut message: Message) -> Result<(), DecodeError> {
        loop {
            if self.cancelled.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            match self.sender.try_send(message) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(_)) => return Err(DecodeError::Cancelled),
                Err(TrySendError::Full(returned)) => {
                    message = returned;
                    thread::park_timeout(Duration::from_millis(1));
                }
            }
        }
    }
}

impl Output for ChannelOutput {
    fn emit(&mut self, chunk: Vec<u8>) -> Result<(), DecodeError> {
        self.send(Message::Data(chunk))
    }

    fn emit_reusable(&mut self, chunk: Vec<u8>) -> Result<Vec<u8>, DecodeError> {
        // Ownership of the payload moves to the reader via the channel. Return
        // a *different* free-list buffer (if any) so the coordinator can reuse
        // capacity for the next fill. The sent buffer is recycled when the
        // consumer finishes reading that chunk (`DecoderReader::recycle_current`).
        self.send(Message::Data(chunk))?;
        Ok(self.free_list.try_steal().unwrap_or_default())
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
/// Dropping this value cancels and joins the background pipeline. It does not
/// verify unread compressed data; use [`DecoderReader::finish`] to discard
/// unread output while still verifying the complete stream.
#[must_use]
pub struct DecoderReader {
    receiver: Option<Receiver<Message>>,
    worker: Option<JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
    current: Vec<u8>,
    current_offset: usize,
    terminal: Terminal,
    /// Soft-capped pool of empty channel-chunk buffers. Shared with the
    /// background `ChannelOutput` so `emit_reusable` can steal capacity that
    /// this reader recycles after fully consuming a chunk. Separate from the
    /// estimated-path free list inside `decode_rapidgzip_estimated`.
    free_list: Arc<ByteBufferFreeList>,
}

pub(crate) fn spawn<R>(source: R, config: Config) -> Result<DecoderReader, DecodeError>
where
    R: ReadAt + 'static,
{
    let (sender, receiver) = mpsc::sync_channel(config.in_flight_chunks);
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = Arc::clone(&cancelled);
    // Soft-cap near channel depth: enough headroom for in-flight messages plus
    // the chunk currently being read / one extra emit_reusable steal without
    // unbounded RSS growth if the consumer is slow.
    let free_list = Arc::new(ByteBufferFreeList::new(
        config.in_flight_chunks.saturating_mul(2),
    ));
    let worker_free_list = Arc::clone(&free_list);
    let worker = thread::Builder::new()
        .name("rapidgzip-coordinator".to_owned())
        .spawn(move || {
            let mut output = ChannelOutput {
                sender,
                cancelled: Arc::clone(&worker_cancelled),
                free_list: worker_free_list,
            };
            let terminal = match decode_source(&source, &config, &worker_cancelled, &mut output) {
                Ok(report) => Message::Finished(report),
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
        current: Vec::new(),
        current_offset: 0,
        terminal: Terminal::Open,
        free_list,
    })
}

impl DecoderReader {
    /// Returns the report after verified EOF has been observed.
    ///
    /// This is `None` while decoding is open and after a terminal failure.
    pub const fn report(&self) -> Option<&DecodeReport> {
        match &self.terminal {
            Terminal::Finished(report) => Some(report),
            Terminal::Open | Terminal::Failed(_) => None,
        }
    }

    /// Recycle fully-consumed (or discarded) `current` capacity into the
    /// reader free list and reset the read cursor.
    fn recycle_current(&mut self) {
        self.free_list.recycle(std::mem::take(&mut self.current));
        self.current_offset = 0;
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
                let terminal = match self.join_worker() {
                    Ok(()) => Terminal::Finished(report),
                    Err(error) => Terminal::Failed(error),
                };
                self.terminal = terminal;
            }
            Ok(Message::Failed(error)) => {
                let terminal = match self.join_worker() {
                    Ok(()) => Terminal::Failed(error),
                    Err(join_error) => Terminal::Failed(join_error),
                };
                self.terminal = terminal;
            }
            Err(_) => {
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
        self.recycle_current();
        loop {
            if matches!(self.terminal, Terminal::Open) {
                self.receive();
                // Discard unread payload but retain capacity in the free list
                // so a concurrent emit_reusable can still steal.
                self.recycle_current();
                continue;
            }
            return match std::mem::replace(&mut self.terminal, Terminal::Open) {
                Terminal::Finished(report) => Ok(report),
                Terminal::Failed(error) => Err(error),
                Terminal::Open => unreachable!("Open is handled above"),
            };
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
                    self.recycle_current();
                }
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
        self.receiver.take();
        let _ = self.join_worker();
    }
}

#[cfg(test)]
mod tests {
    use super::DecoderReader;
    use crate::buffer_pool::ByteBufferFreeList;
    use std::io::Read;
    use std::sync::Arc;

    fn assert_traits<T: Read + Send + Unpin>() {}

    #[test]
    fn decoder_reader_is_read_send_and_unpin() {
        assert_traits::<DecoderReader>();
    }

    #[test]
    fn free_list_recycle_after_consume_is_stealable() {
        // Mirrors the DecoderReader / ChannelOutput hand-off without spinning
        // up a full coordinator: recycle a finished chunk, then steal it as
        // emit_reusable would.
        let free_list = Arc::new(ByteBufferFreeList::new(4));
        let mut current = Vec::with_capacity(4096);
        current.extend_from_slice(&[0u8; 100]);
        free_list.recycle(std::mem::take(&mut current));
        assert_eq!(free_list.len(), 1);

        let stolen = free_list.try_steal().expect("recycled buffer available");
        assert!(stolen.is_empty());
        assert!(stolen.capacity() >= 4096);
        assert_eq!(free_list.len(), 0);
    }
}
