use crate::backend::{
    DirectOutput, decode_source, decode_source_with_index, decode_stream, decode_stream_with_index,
};
use crate::gzip::{StreamCursor, validate_initial_header};
use crate::reader;
use crate::runtime::RuntimeState;
use crate::{
    DecodeError, DecodeReport, DecoderReader, IndexOptions, IndexedDecodeReport,
    IndexingDecoderReader, IndexingError, ReadAt,
};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::{self, Read, Write};
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

    /// Decodes and verifies every gzip member while collecting a random-access
    /// index.
    ///
    /// Index construction is explicit per operation. Ordinary [`Self::decode`]
    /// calls therefore retain their small [`Copy`] report and perform no
    /// checkpoint-window work. On error, `output` can contain a verified
    /// prefix; writes are not rolled back.
    ///
    /// # Errors
    ///
    /// Returns [`IndexingError::Decode`] for source, framing, DEFLATE,
    /// verification, output, or limit failures, and [`IndexingError::Index`]
    /// when a checkpoint window cannot be stored or the final index is invalid.
    pub fn decode_with_index<R, W>(
        &self,
        source: &R,
        output: &mut W,
        options: IndexOptions,
    ) -> Result<IndexedDecodeReport, IndexingError>
    where
        R: ReadAt + ?Sized,
        W: Write,
    {
        let cancelled = AtomicBool::new(false);
        let mut sink = DirectOutput::new(output);
        let runtime = RuntimeState::new(self.config.decoder_threads);
        decode_source_with_index(
            source,
            &self.config,
            &cancelled,
            &mut sink,
            &runtime,
            options,
        )
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

    /// Starts positional decoding with index construction and returns owned
    /// `Read + Send` decompressed output.
    ///
    /// The returned [`IndexingDecoderReader`] exposes the same telemetry and
    /// dynamic worker controls as [`DecoderReader`]. Its index becomes
    /// available only after verified EOF, either through
    /// [`IndexingDecoderReader::report`] or [`IndexingDecoderReader::finish`].
    ///
    /// # Errors
    ///
    /// Returns an initial source or gzip-framing failure. Later decode and
    /// index failures are reported by [`Read::read`] and preserved in typed
    /// form by [`IndexingDecoderReader::finish`].
    pub fn reader_with_index<R>(
        &self,
        source: R,
        options: IndexOptions,
    ) -> Result<IndexingDecoderReader, DecodeError>
    where
        R: ReadAt + 'static,
    {
        validate_initial_header(&source, self.config.input_page_size)?;
        reader::spawn_indexed(source, self.config.clone(), options)
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
    /// in order, so decoding uses one calling thread regardless of
    /// [`DecoderBuilder::decoder_threads`]. The returned report retains the
    /// configured worker budget, just like [`Decoder::decode`].
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
        let mut sink = DirectOutput::new(output);
        let runtime = RuntimeState::new(self.config.decoder_threads);
        let mut cursor = StreamCursor::new(source, self.config.input_page_size);
        decode_stream(&mut cursor, &self.config, &cancelled, &mut sink, &runtime)
    }

    /// Decodes a non-seekable gzip stream while collecting a coarse but valid
    /// member-boundary index.
    ///
    /// A forward-only source does not expose independently discoverable
    /// interior block boundaries, so the resulting index records member starts.
    /// It can later seek a stable positional copy of the same compressed bytes.
    ///
    /// # Errors
    ///
    /// Returns [`IndexingError`] for the same decode failures as
    /// [`Self::decode_stream`] or for index construction and validation errors.
    pub fn decode_stream_with_index<R, W>(
        &self,
        source: R,
        output: &mut W,
        options: IndexOptions,
    ) -> Result<IndexedDecodeReport, IndexingError>
    where
        R: Read,
        W: Write,
    {
        let cancelled = AtomicBool::new(false);
        let mut sink = DirectOutput::new(output);
        let runtime = RuntimeState::new(self.config.decoder_threads);
        let mut cursor = StreamCursor::new(source, self.config.input_page_size);
        decode_stream_with_index(
            &mut cursor,
            &self.config,
            &cancelled,
            &mut sink,
            &runtime,
            options,
        )
    }

    /// Starts decoding an owned non-seekable source and returns `Read + Send`
    /// decompressed output.
    ///
    /// This is the pull counterpart to [`Decoder::decode_stream`] and mirrors
    /// [`Decoder::reader`], returning the same [`DecoderReader`] so it can still
    /// be handed to a parser as `Box<dyn Read + Send>`.
    ///
    /// One initial source read is used for best-effort fail-fast header
    /// validation. A short read can defer validation until [`std::io::Read`];
    /// later failures are returned as [`std::io::Error`] values by that method,
    /// or as [`DecodeError`] by [`DecoderReader::finish`].
    ///
    /// [`DecoderReader::stats`] reports [`crate::DecoderPath::Sequential`], the
    /// builder-supplied configured worker budget, an effective target of one,
    /// and zero spawned decoder or auxiliary threads. Decoding occurs in the
    /// caller's `read`, so dropping the reader immediately drops the source and
    /// cannot strand a coordinator blocked on input.
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
    /// Returns an input failure, or a framing failure detectable from the
    /// best-effort initial read. A short read can defer a framing failure until
    /// the returned reader is consumed.
    pub fn stream_reader<R>(&self, source: R) -> Result<DecoderReader, DecodeError>
    where
        R: Read + Send + 'static,
    {
        reader::spawn_stream(source, self.config.clone())
    }

    /// Starts pull-driven decoding of a non-seekable source while collecting
    /// a member-boundary index.
    ///
    /// Like [`Self::stream_reader`], this runs synchronously in the caller's
    /// `read` calls and spawns no coordinator or decoder worker. The returned
    /// reader remains `Read + Send` and publishes the index only at verified
    /// EOF.
    ///
    /// # Errors
    ///
    /// Returns an input failure or an initial framing failure. Later failures
    /// are returned by [`Read::read`] or [`IndexingDecoderReader::finish`].
    pub fn stream_reader_with_index<R>(
        &self,
        source: R,
        options: IndexOptions,
    ) -> Result<IndexingDecoderReader, DecodeError>
    where
        R: Read + Send + 'static,
    {
        reader::spawn_stream_indexed(source, self.config.clone(), options)
    }

    /// Opens, decodes, and verifies every gzip member from a filesystem path.
    ///
    /// This is the push counterpart to [`Decoder::open`]. A regular file uses
    /// positional decoding; a non-regular path accepted by [`File::open`], such
    /// as a FIFO or character device, uses [`Decoder::decode_stream`]. The
    /// writer remains on the calling thread in both cases and need not implement
    /// [`Send`].
    ///
    /// # Errors
    ///
    /// Returns the first open, framing, DEFLATE, verification, input, output,
    /// or output-limit failure. On error, `output` can contain a verified
    /// prefix; writes are not rolled back.
    pub fn decode_path<P, W>(&self, path: P, output: &mut W) -> Result<DecodeReport, DecodeError>
    where
        P: AsRef<Path>,
        W: Write,
    {
        let file = File::open(path).map_err(|error| DecodeError::input_io(0, error))?;
        if supports_positional_reads(&file) {
            self.decode(&file, output)
        } else {
            self.decode_stream(file, output)
        }
    }

    /// Opens a compressed file and returns a `Read + Send` decompressed stream.
    ///
    /// A regular file is owned by the returned reader and accessed positionally
    /// through every decode path. A non-regular path accepted by [`File::open`],
    /// such as a FIFO or character device, is routed to
    /// [`Decoder::stream_reader`] instead and decoded sequentially with the same
    /// verification. Such a path previously failed, so no successful call
    /// changes behaviour.
    ///
    /// # Errors
    ///
    /// Returns an input failure, or a framing failure detected while opening
    /// the reader. Further decoding and verification failures are returned by
    /// [`std::io::Read`] or [`DecoderReader::finish`].
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<DecoderReader, DecodeError> {
        let file = File::open(path).map_err(|error| DecodeError::input_io(0, error))?;
        if supports_positional_reads(&file) {
            self.reader(file)
        } else {
            self.stream_reader(file)
        }
    }
}

/// Reports whether an opened file satisfies the stable-length positional-read
/// contract required by the parallel decoder.
fn supports_positional_reads(file: &File) -> bool {
    file.metadata()
        .is_ok_and(|metadata| metadata.file_type().is_file())
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
    use super::{Decoder, MIB, supports_positional_reads};

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

    #[test]
    fn regular_files_satisfy_the_positional_contract() {
        let file = std::fs::File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).unwrap();
        assert!(supports_positional_reads(&file));
    }

    #[cfg(unix)]
    #[test]
    fn seekable_non_regular_files_still_use_streaming() {
        let device = std::fs::File::open("/dev/null").unwrap();
        assert!(!supports_positional_reads(&device));
    }
}
