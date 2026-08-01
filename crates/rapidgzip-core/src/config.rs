use crate::backend::{DirectOutput, decode_source, decode_stream};
use crate::gzip::{StreamCursor, validate_initial_header, validate_initial_stream_header};
use crate::reader;
use crate::runtime::RuntimeState;
use crate::{DecodeError, DecodeReport, DecoderReader, Format, ReadAt};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::{self, Read, Seek, Write};
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
    pub(crate) build_index: bool,
    pub(crate) index_spacing: u64,
    pub(crate) compress_index_windows: bool,
    pub(crate) format: Format,
    pub(crate) expected_uncompressed_size: Option<u64>,
    pub(crate) count_lines: bool,
    pub(crate) index: Option<crate::GzipIndex>,
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
                build_index: false,
                index_spacing: 4 * MIB as u64,
                compress_index_windows: true,
                format: Format::Auto,
                expected_uncompressed_size: None,
                count_lines: false,
                index: None,
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

    /// Enables collecting a random-access index while decoding.
    ///
    /// The index arrives in [`DecodeReport::index`] once the decode completes
    /// and can then be persisted or handed to [`crate::IndexedReader`]. It
    /// costs one predecessor window per interior checkpoint in memory, which
    /// [`Self::compress_index_windows`] reduces.
    ///
    /// Indexing is off by default, so existing callers pay nothing.
    pub const fn build_index(mut self, enabled: bool) -> Self {
        self.config.build_index = enabled;
        self
    }

    /// Sets the target spacing between interior index checkpoints in
    /// decompressed bytes.
    ///
    /// Smaller spacing makes seeks cheaper and the index larger. Member and
    /// BGZF block boundaries are always recorded regardless of this value. The
    /// value must be non-zero and is ignored when indexing is disabled.
    pub const fn index_spacing(mut self, bytes: u64) -> Self {
        self.config.index_spacing = bytes;
        self
    }

    /// Sets whether index windows are held zlib-compressed in memory.
    ///
    /// Compression trades a small amount of time per checkpoint for a large
    /// reduction in resident memory on compressible data. Ignored when
    /// indexing is disabled.
    pub const fn compress_index_windows(mut self, enabled: bool) -> Self {
        self.config.compress_index_windows = enabled;
        self
    }

    /// Selects the container framing of the compressed input.
    ///
    /// The default, [`Format::Auto`], reads the first two bytes and accepts
    /// gzip or zlib. Raw DEFLATE has no header to recognize and must be
    /// requested with [`Format::RawDeflate`].
    pub const fn format(mut self, format: Format) -> Self {
        self.config.format = format;
        self
    }

    /// Sets the decompressed size the caller expects, in bytes.
    ///
    /// Raw DEFLATE carries neither a checksum nor a length, so this is the
    /// only end-to-end check available for it. Decoding fails with
    /// [`DecodeError::UnexpectedOutputSize`] when the sizes disagree.
    ///
    /// Supplying a size for any other format is a [`ConfigError`]: gzip and
    /// zlib verify their own trailers, and a second, weaker check would
    /// suggest a guarantee that is already stronger.
    pub const fn expected_uncompressed_size(mut self, bytes: Option<u64>) -> Self {
        self.config.expected_uncompressed_size = bytes;
        self
    }

    /// Enables counting newlines in the decompressed output.
    ///
    /// The count arrives in [`DecodeReport::line_count`]. When an index is
    /// also collected, each checkpoint records the line offset at its
    /// decompressed offset and the index records the total, which is what
    /// gztool's line-aware format stores.
    ///
    /// Counting happens once over output the decoder already holds, on the
    /// thread that emits it. It is off by default, so existing callers pay
    /// nothing.
    pub const fn count_lines(mut self, enabled: bool) -> Self {
        self.config.count_lines = enabled;
        self
    }

    /// Supplies an index so decoding can be split without speculation.
    ///
    /// Every checkpoint carries a compressed bit offset and the window needed
    /// to resume there, so each worker can run plain zlib over its own span
    /// instead of decoding with unknown history and resolving markers
    /// afterwards. The index is used only when it validates against the
    /// source and holds enough checkpoints to be worth splitting; otherwise
    /// decoding proceeds exactly as it would without one.
    ///
    /// Building an index in order to use it here is not worthwhile: that is
    /// two decodes where the speculative path needs one. This helps when an
    /// index already exists.
    pub fn index(mut self, index: Option<crate::GzipIndex>) -> Self {
        self.config.index = index;
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
        if self.config.index_spacing == 0 {
            return Err(ConfigError("index_spacing must be non-zero"));
        }
        if self.config.expected_uncompressed_size.is_some()
            && self.config.format != Format::RawDeflate
        {
            return Err(ConfigError(
                "expected_uncompressed_size applies to Format::RawDeflate only",
            ));
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
        let runtime = RuntimeState::new(self.config.decoder_threads);
        let mut sink = DirectOutput::new(output, &runtime);
        decode_source(source, &self.config, &cancelled, &mut sink, &runtime)
    }

    /// Walks every DEFLATE block, reporting the input's structure.
    ///
    /// Analysis decodes the whole input sequentially with the crate's native
    /// DEFLATE decoder, since zlib exposes none of the per-block detail. It is
    /// therefore slower than decoding and holds the decompressed output in
    /// memory. It is a diagnostic, not a decode path.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] for the same malformed input a decode would
    /// reject.
    pub fn analyze<R>(&self, source: &R) -> Result<crate::Analysis, DecodeError>
    where
        R: ReadAt + ?Sized,
    {
        crate::analyze::analyze_source(source, self.config.format)
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
        // Only gzip has a header worth checking before the coordinator
        // starts. zlib framing is validated by the decode itself, and raw
        // DEFLATE has no header at all.
        if self.config.format == Format::Gzip
            || (self.config.format == Format::Auto
                && crate::backend::looks_like_gzip(&source, self.config.input_page_size)?)
        {
            validate_initial_header(&source, self.config.input_page_size)?;
        }
        reader::spawn(source, self.config.clone())
    }

    /// Decodes and verifies all gzip members from a non-seekable source.
    ///
    /// This is the push interface for input that cannot be read positionally,
    /// such as standard input, a FIFO, a process substitution, or a socket. It
    /// mirrors [`Decoder::decode`], including the writer being used only by the
    /// calling thread.
    ///
    /// Verification is identical to [`Decoder::decode`]: a member is accepted
    /// only after a real final DEFLATE block whose CRC32 and ISIZE both match,
    /// trailing non-gzip bytes are an error, and [`DecoderBuilder::output_limit`]
    /// still fails before emitting bytes past the limit. The source is read once
    /// in order, so decoding is single-threaded regardless of
    /// [`DecoderBuilder::decoder_threads`], and the returned report's
    /// `decoder_threads` is `1`.
    ///
    /// Input memory is bounded by one [`DecoderBuilder::input_page_size`]
    /// window; nothing is spooled.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rapidgzip_core::Decoder;
    /// use std::io;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let decoder = Decoder::default();
    /// let report = decoder.decode_stream(io::stdin(), &mut io::sink())?;
    /// println!("verified {} gzip members", report.member_count);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the first framing, DEFLATE, verification, input, or output-limit
    /// failure. On error, `output` can contain a verified prefix; writes are not
    /// rolled back.
    pub fn decode_stream<R, W>(
        &self,
        source: R,
        output: &mut W,
    ) -> Result<DecodeReport, DecodeError>
    where
        R: Read,
        W: Write,
    {
        let cancelled = AtomicBool::new(false);
        let runtime = RuntimeState::new(1);
        let mut sink = DirectOutput::new(output, &runtime);
        let mut cursor = StreamCursor::new(source, self.config.input_page_size);
        if self.config.format != Format::Zlib
            && self.config.format != Format::RawDeflate
            && crate::format::detect(cursor.buffered_prefix()?) != Some(Format::Zlib)
        {
            validate_initial_stream_header(&mut cursor)?;
        }
        decode_stream(&mut cursor, &self.config, &cancelled, &mut sink, &runtime)
    }

    /// Starts decoding an owned non-seekable source and returns `Read + Send`
    /// decompressed output.
    ///
    /// This is the pull counterpart to [`Decoder::decode_stream`] and mirrors
    /// [`Decoder::reader`], returning the same [`DecoderReader`] so it can still
    /// be handed to a parser as `Box<dyn Read + Send>`.
    ///
    /// The gzip magic and as much of the first member header as the initial
    /// input window holds are validated before the background coordinator is
    /// spawned. Later failures are returned as [`std::io::Error`] values by
    /// [`std::io::Read`], or as [`DecodeError`] by [`DecoderReader::finish`].
    ///
    /// [`DecoderReader::stats`] reports [`crate::DecoderPath::Sequential`] with
    /// a single configured worker, because the four parallel paths all require
    /// positional reads. Dropping the reader before end of output cancels
    /// without waiting for the coordinator, so a stalled producer cannot block
    /// the drop.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rapidgzip_core::Decoder;
    /// use std::io::{self, Read};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let decoder = Decoder::default();
    /// let reader = decoder.stream_reader(io::stdin())?;
    ///
    /// // Still Read + Send, so a parser can own it.
    /// let mut parser_input: Box<dyn Read + Send> = Box::new(reader);
    /// io::copy(&mut parser_input, &mut io::sink())?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an input failure, or a framing failure detectable in the first
    /// input window.
    pub fn stream_reader<R>(&self, source: R) -> Result<DecoderReader, DecodeError>
    where
        R: Read + Send + 'static,
    {
        reader::spawn_stream(source, self.config.clone())
    }

    /// Opens a compressed file and returns a `Read + Send` decompressed stream.
    ///
    /// A regular file is owned by the returned reader and accessed positionally
    /// through every decode path. A path that cannot be read positionally, such
    /// as a FIFO, a character device, or a socket, is routed to
    /// [`Decoder::stream_reader`] instead and decoded sequentially with the same
    /// verification. Such a path previously failed, so no successful call
    /// changes behaviour.
    ///
    /// # Errors
    ///
    /// Returns an input failure, or a framing failure detected before the
    /// background coordinator is spawned.
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<DecoderReader, DecodeError> {
        let mut file = File::open(path).map_err(|error| DecodeError::input_io(0, error))?;
        if supports_positional_reads(&mut file) {
            self.reader(file)
        } else {
            self.stream_reader(file)
        }
    }
}

/// Reports whether an opened file can serve the positional decode paths.
///
/// A regular file always can. Anything else is probed with a seek, which keeps
/// block devices on the parallel paths while sending pipes, terminals, and
/// sockets to the sequential streaming path.
fn supports_positional_reads(file: &mut File) -> bool {
    match file.metadata() {
        Ok(metadata) if metadata.file_type().is_file() => true,
        Ok(_) => file.stream_position().is_ok(),
        Err(_) => false,
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
