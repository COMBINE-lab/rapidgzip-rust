use crate::backend::{Output, decode_source};
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
}

enum Terminal {
    Open,
    Finished(DecodeReport),
    Failed(DecodeError),
}

/// Owned parallel decoder output implementing [`Read`] and [`Send`].
///
/// Dropping this value cancels and joins the background pipeline.
#[must_use]
pub struct DecoderReader {
    receiver: Option<Receiver<Message>>,
    worker: Option<JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
    current: Vec<u8>,
    current_offset: usize,
    terminal: Terminal,
}

pub(crate) fn spawn<R>(source: R, config: Config) -> Result<DecoderReader, DecodeError>
where
    R: ReadAt + 'static,
{
    let (sender, receiver) = mpsc::sync_channel(config.in_flight_chunks);
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = Arc::clone(&cancelled);
    let worker = thread::Builder::new()
        .name("rapidgzip-coordinator".to_owned())
        .spawn(move || {
            let mut output = ChannelOutput {
                sender,
                cancelled: Arc::clone(&worker_cancelled),
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
    })
}

impl DecoderReader {
    /// Returns the report after verified EOF has been observed.
    pub const fn report(&self) -> Option<&DecodeReport> {
        match &self.terminal {
            Terminal::Finished(report) => Some(report),
            Terminal::Open | Terminal::Failed(_) => None,
        }
    }

    fn join_worker(&mut self) -> Result<(), DecodeError> {
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            return Err(DecodeError::WorkerPanicked);
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
    pub fn finish(mut self) -> Result<DecodeReport, DecodeError> {
        self.current.clear();
        loop {
            match &self.terminal {
                Terminal::Finished(report) => return Ok(*report),
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
    use std::io::Read;

    fn assert_traits<T: Read + Send + Unpin>() {}

    #[test]
    fn decoder_reader_is_read_send_and_unpin() {
        assert_traits::<DecoderReader>();
    }
}
