use crate::backend::{DirectOutput, decode_source};
use crate::gzip::validate_initial_header;
use crate::reader;
use crate::runtime::RuntimeState;
use crate::{DecodeError, DecodeReport, DecoderReader, ReadAt};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::AtomicBool;

const MIB: usize = 1024 * 1024;

/// Invalid decoder configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError(&'static str);

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for ConfigError {}

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) decoder_threads: usize,
    pub(crate) decoded_chunk_size: usize,
    pub(crate) input_page_size: usize,
    pub(crate) compressed_chunk_size: usize,
    pub(crate) in_flight_chunks: usize,
    pub(crate) output_limit: Option<u64>,
}

/// Builder for an immutable, reusable [`Decoder`].
///
/// Defaults use [`std::thread::available_parallelism`] as the maximum decoder
/// budget, 4 MiB decoded chunks, 1 MiB positional input pages and compressed
/// grid spacing, `decoder_threads + 2` in-flight chunks, and no output limit.
/// The defaults favor throughput; applications with tight memory budgets can
/// reduce the worker budget, decoded chunk size, or in-flight count.
#[derive(Clone, Debug)]
pub struct DecoderBuilder {
    config: Config,
}

impl Default for DecoderBuilder {
    fn default() -> Self {
        let decoder_threads = std::thread::available_parallelism()
            .map(NonZeroUsize::get)
            .unwrap_or(1);
        Self {
            config: Config {
                decoder_threads,
                decoded_chunk_size: 4 * MIB,
                input_page_size: MIB,
                compressed_chunk_size: MIB,
                in_flight_chunks: decoder_threads.saturating_add(2),
                output_limit: None,
            },
        }
    }
}

impl DecoderBuilder {
    /// Sets the maximum decoder-worker budget.
    ///
    /// Individual paths may use fewer active workers when the input exposes
    /// less parallelism or when a larger speculative window would reduce
    /// throughput through memory pressure.
    ///
    /// This also resets the in-flight chunk count to `threads + 2`. Call
    /// [`DecoderBuilder::in_flight_chunks`] afterward to override that value.
    pub const fn decoder_threads(mut self, threads: usize) -> Self {
        self.config.decoder_threads = threads;
        self.config.in_flight_chunks = threads.saturating_add(2);
        self
    }

    /// Sets the target decoded chunk size in bytes.
    ///
    /// Larger chunks reduce handoff overhead but can increase per-worker and
    /// queued memory. The value must be non-zero and fit in zlib's `uInt`.
    pub const fn decoded_chunk_size(mut self, bytes: usize) -> Self {
        self.config.decoded_chunk_size = bytes;
        self
    }

    /// Sets the positional input page size in bytes.
    ///
    /// The value must be non-zero and fit in zlib's `uInt`.
    pub const fn input_page_size(mut self, bytes: usize) -> Self {
        self.config.input_page_size = bytes;
        self
    }

    /// Sets the target spacing between speculative chunk starts.
    ///
    /// The current estimated-grid decoder requires at least 1 MiB to keep its
    /// independently discovered boundaries strongly validated.
    pub const fn compressed_chunk_size(mut self, bytes: usize) -> Self {
        self.config.compressed_chunk_size = bytes;
        self
    }

    /// Sets the maximum number of decoded chunks awaiting consumption.
    ///
    /// This bounds reader backpressure and ordered-result buffering at the
    /// final handoff. The value must be non-zero.
    pub const fn in_flight_chunks(mut self, count: usize) -> Self {
        self.config.in_flight_chunks = count;
        self
    }

    /// Sets or clears the total decoded-output limit in bytes.
    ///
    /// On overflow, decoding returns [`DecodeError::OutputLimitExceeded`]
    /// before emitting bytes beyond the limit. Output already emitted remains
    /// visible to the caller.
    pub const fn output_limit(mut self, bytes: Option<u64>) -> Self {
        self.config.output_limit = bytes;
        self
    }

    /// Validates the configuration and creates a reusable decoder.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a size or count violates the constraints
    /// documented on its setter.
    pub fn build(self) -> Result<Decoder, ConfigError> {
        if self.config.decoder_threads == 0 {
            return Err(ConfigError("decoder_threads must be non-zero"));
        }
        if self.config.decoded_chunk_size == 0 {
            return Err(ConfigError("decoded_chunk_size must be non-zero"));
        }
        if self.config.decoded_chunk_size > u32::MAX as usize {
            return Err(ConfigError("decoded_chunk_size must fit zlib's uInt"));
        }
        if self.config.input_page_size == 0 {
            return Err(ConfigError("input_page_size must be non-zero"));
        }
        if self.config.input_page_size > u32::MAX as usize {
            return Err(ConfigError("input_page_size must fit zlib's uInt"));
        }
        if self.config.compressed_chunk_size < MIB {
            return Err(ConfigError("compressed_chunk_size must be at least 1 MiB"));
        }
        if self.config.in_flight_chunks == 0 {
            return Err(ConfigError("in_flight_chunks must be non-zero"));
        }
        Ok(Decoder {
            config: self.config,
        })
    }
}

/// Immutable, reusable decompressor configuration.
#[derive(Clone, Debug)]
pub struct Decoder {
    pub(crate) config: Config,
}

impl Decoder {
    /// Creates a builder initialized with the [`DecoderBuilder`] defaults.
    pub fn builder() -> DecoderBuilder {
        DecoderBuilder::default()
    }

    /// Decodes and verifies all gzip members into `output`.
    ///
    /// The writer is used only by the calling thread and need not implement
    /// [`Send`].
    ///
    /// The compressed source must keep its length and contents stable for the
    /// duration of this call. On error, `output` can contain a verified prefix;
    /// writes are not rolled back.
    pub fn decode<R, W>(&self, source: &R, output: &mut W) -> Result<DecodeReport, DecodeError>
    where
        R: ReadAt + ?Sized,
        W: Write,
    {
        let cancelled = AtomicBool::new(false);
        let mut sink = DirectOutput::new(output);
        let runtime = RuntimeState::new(self.config.decoder_threads);
        decode_source(source, &self.config, &cancelled, &mut sink, &runtime)
    }

    /// Starts decoding an owned positional source and returns `Read + Send`
    /// decompressed output.
    ///
    /// Initial gzip framing is validated before the background coordinator is
    /// spawned. Later decoding failures are returned as [`std::io::Error`]
    /// values by [`std::io::Read`], or as [`DecodeError`] by
    /// [`DecoderReader::finish`].
    pub fn reader<R>(&self, source: R) -> Result<DecoderReader, DecodeError>
    where
        R: ReadAt + 'static,
    {
        validate_initial_header(&source, self.config.input_page_size)?;
        reader::spawn(source, self.config.clone())
    }

    /// Opens a compressed file and returns a `Read + Send` decompressed stream.
    ///
    /// The file is owned by the returned reader and accessed positionally.
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<DecoderReader, DecodeError> {
        let file = File::open(path).map_err(|error| DecodeError::input_io(0, error))?;
        self.reader(file)
    }
}

impl Default for Decoder {
    fn default() -> Self {
        DecoderBuilder::default()
            .build()
            .expect("the default decoder configuration is valid")
    }
}

impl From<ConfigError> for io::Error {
    fn from(error: ConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests {
    use super::{Decoder, MIB};

    #[test]
    fn rejects_speculative_grid_smaller_than_one_mibibyte() {
        let error = Decoder::builder()
            .compressed_chunk_size(MIB - 1)
            .build()
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "compressed_chunk_size must be at least 1 MiB"
        );
    }
}
