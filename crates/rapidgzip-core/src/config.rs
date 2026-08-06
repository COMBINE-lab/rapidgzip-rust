use crate::backend::{
    DirectOutput, decode_source, decode_source_with_index, decode_stream, decode_stream_with_index,
};
use crate::format::FormatSelection;
use crate::gzip::StreamCursor;
use crate::pool::DecoderPool;
use crate::reader;
use crate::runtime::RuntimeState;
use crate::{
    Analysis, AnalyzeOptions, DecodeError, DecodeReport, DecoderReader, DeflateIndex, Format,
    IndexDecodeError, IndexOptions, IndexedDecodeReport, IndexingDecoderReader, IndexingError,
    ReadAt,
};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
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
    pub(crate) expected_uncompressed_size: Option<u64>,
    pub(crate) count_lines: bool,
    pub(crate) format: FormatSelection,
    pub(crate) decoder_pool: Option<DecoderPool>,
}

impl Config {
    /// Checks a proposed decoded-output handoff against both configured bounds.
    pub(crate) fn checked_output_total(
        &self,
        current: u64,
        additional: usize,
    ) -> Result<u64, DecodeError> {
        let Some(actual) = current.checked_add(additional as u64) else {
            return match (self.expected_uncompressed_size, self.output_limit) {
                (Some(expected), Some(limit)) if limit < expected => {
                    Err(DecodeError::OutputLimitExceeded { limit })
                }
                (Some(expected), _) => Err(DecodeError::UnexpectedOutputSize {
                    expected,
                    actual: u64::MAX,
                }),
                (None, limit) => Err(DecodeError::OutputLimitExceeded {
                    limit: limit.unwrap_or(u64::MAX),
                }),
            };
        };
        let expectation_crossed = self
            .expected_uncompressed_size
            .is_some_and(|expected| actual > expected);
        let limit_crossed = self.output_limit.is_some_and(|limit| actual > limit);

        if expectation_crossed
            && (!limit_crossed
                || self.expected_uncompressed_size.expect("checked as some")
                    <= self.output_limit.expect("checked as some"))
        {
            return Err(DecodeError::UnexpectedOutputSize {
                expected: self.expected_uncompressed_size.expect("checked as some"),
                actual,
            });
        }
        if limit_crossed {
            return Err(DecodeError::OutputLimitExceeded {
                limit: self.output_limit.unwrap_or(u64::MAX),
            });
        }
        Ok(actual)
    }

    /// Confirms an exact output expectation after framing verification.
    pub(crate) fn verify_expected_output(&self, actual: u64) -> Result<(), DecodeError> {
        if let Some(expected) = self.expected_uncompressed_size {
            if expected != actual {
                return Err(DecodeError::UnexpectedOutputSize { expected, actual });
            }
        }
        Ok(())
    }
}

/// Builder for an immutable, reusable [`Decoder`].
///
/// Defaults use [`std::thread::available_parallelism`] as the maximum decoder
/// budget, 4 MiB decoded chunks, 1 MiB positional input pages and compressed
/// grid spacing, `decoder_threads + 2` in-flight chunks, strict gzip framing,
/// no output limit or exact-size expectation, no line counting, and no shared
/// decoder pool. The defaults favor throughput; applications with tight
/// memory budgets can reduce the worker budget, decoded chunk size, or
/// in-flight count.
///
/// A built [`Decoder`] is cheap to clone and can start multiple operations.
/// Each operation receives its own runtime telemetry and control handle. If a
/// [`DecoderPool`] is attached, those operations additionally share the pool's
/// aggregate execution budget.
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
                expected_uncompressed_size: None,
                count_lines: false,
                format: FormatSelection::default(),
                decoder_pool: None,
            },
        }
    }
}

impl DecoderBuilder {
    /// Sets the maximum decoder-worker budget.
    ///
    /// This is immutable headroom for each operation started by the built
    /// [`Decoder`], not an eager operating-system thread count or a requested
    /// steady-state width. Individual paths may use fewer active workers when
    /// the input exposes less parallelism, the consumer is backpressured, or
    /// empirical control finds that a wider speculative window would not
    /// improve throughput. Use [`crate::DecoderHandle::stats`] to observe the
    /// selected width, [`crate::DecoderHandle::set_worker_limit`] for a mutable
    /// hard ceiling, and [`crate::DecoderHandle::request_workers`] for an
    /// explicit growth floor.
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
    /// The value must be non-zero and fit in zlib's `uInt`. A streaming cursor
    /// retains at least two bytes so format detection can span short reads.
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

    /// Requires exactly `bytes` of decoded output, or clears the expectation.
    ///
    /// Unlike [`Self::output_limit`], this is both an upper and lower bound.
    /// An overrun is rejected before the offending chunk is emitted, and an
    /// underrun is rejected after the selected container is complete.
    pub const fn expected_uncompressed_size(mut self, bytes: Option<u64>) -> Self {
        self.config.expected_uncompressed_size = bytes;
        self
    }

    /// Enables or disables counting newline bytes in the decoded output.
    ///
    /// The final count is returned through [`DecodeReport::line_count`]. When
    /// an index is collected by the same operation, every retained checkpoint
    /// is also annotated with the number of preceding newlines and the index
    /// records the total. This metadata enables
    /// [`crate::IndexedReader::seek_to_line`] and gztool version 1 export.
    ///
    /// Counting is disabled by default. When disabled, output is not scanned
    /// and reports and newly built indexes carry no line metadata.
    pub const fn count_lines(mut self, enabled: bool) -> Self {
        self.config.count_lines = enabled;
        self
    }

    /// Selects the container framing explicitly.
    ///
    /// The default is [`Format::Gzip`], preserving strict gzip behavior.
    /// Select [`Format::RawDeflate`] explicitly because an unwrapped stream has
    /// no magic bytes and is never safe to guess.
    pub const fn format(mut self, format: Format) -> Self {
        self.config.format = FormatSelection::Explicit(format);
        self
    }

    /// Detects gzip or zlib framing from an exact two-byte prefix.
    ///
    /// Raw DEFLATE is never auto-detected. An unrecognized prefix produces
    /// [`DecodeError::UnrecognizedFormat`].
    pub const fn auto_detect_format(mut self) -> Self {
        self.config.format = FormatSelection::Auto;
        self
    }

    /// Attaches every decode created by this reusable configuration to a
    /// process-wide execution-slot pool.
    ///
    /// The pool is opt-in. Without it, every decoder retains the existing
    /// private elastic-worker behavior. With it, `decoder_threads` remains the
    /// per-decoder maximum while the pool enforces an additional aggregate
    /// limit across every attached operation. Clone the same pool into every
    /// builder whose work should participate in that budget; cloning the pool
    /// does not allocate threads.
    pub fn decoder_pool(mut self, pool: DecoderPool) -> Self {
        self.config.decoder_pool = Some(pool);
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

    /// Decodes the selected container into `output` and performs all available
    /// integrity checks.
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
        let runtime = RuntimeState::new(
            self.config.decoder_threads,
            self.config.decoder_pool.as_ref(),
        );
        decode_source(source, &self.config, &cancelled, &mut sink, &runtime)
    }

    /// Analyzes every DEFLATE block using bounded default retention limits.
    ///
    /// The walk is sequential because each block depends on its predecessor
    /// history. It validates the same container headers, checksums, sizes, and
    /// trailing-data rules as decoding while retaining only one 32 KiB output
    /// window. Use [`Self::analyze_with_options`] to change result limits or
    /// retain individual predecessor-window references.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] for input, framing, DEFLATE, integrity, output
    /// expectation, or typed analysis-budget failures.
    pub fn analyze<R>(&self, source: &R) -> Result<Analysis, DecodeError>
    where
        R: ReadAt + ?Sized,
    {
        self.analyze_with_options(source, AnalyzeOptions::default())
    }

    /// Analyzes every DEFLATE block with explicit retention limits.
    ///
    /// Detailed back-reference retention is input-wide. Exact summaries remain
    /// available when that budget is exhausted, and each block records its
    /// omitted detail count.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] for input, framing, DEFLATE, integrity, output
    /// expectation, or typed analysis-budget failures.
    pub fn analyze_with_options<R>(
        &self,
        source: &R,
        options: AnalyzeOptions,
    ) -> Result<Analysis, DecodeError>
    where
        R: ReadAt + ?Sized,
    {
        crate::analyze::analyze_source(source, &self.config, options)
    }

    /// Analyzes a non-seekable compressed stream with default limits.
    ///
    /// Input and decompressed history remain bounded; unlike positional
    /// analysis, a stream cannot be re-read if the caller later requests more
    /// retained detail.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] for input, framing, DEFLATE, integrity, output
    /// expectation, or typed analysis-budget failures.
    pub fn analyze_stream<R>(&self, source: R) -> Result<Analysis, DecodeError>
    where
        R: Read,
    {
        self.analyze_stream_with_options(source, AnalyzeOptions::default())
    }

    /// Analyzes a non-seekable compressed stream with explicit limits.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] for input, framing, DEFLATE, integrity, output
    /// expectation, or typed analysis-budget failures.
    pub fn analyze_stream_with_options<R>(
        &self,
        source: R,
        options: AnalyzeOptions,
    ) -> Result<Analysis, DecodeError>
    where
        R: Read,
    {
        crate::analyze::analyze_stream(source, &self.config, options)
    }

    /// Decodes the selected container while collecting a random-access index.
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
        let runtime = RuntimeState::new(
            self.config.decoder_threads,
            self.config.decoder_pool.as_ref(),
        );
        decode_source_with_index(
            source,
            &self.config,
            &cancelled,
            &mut sink,
            &runtime,
            options,
        )
    }

    /// Decodes through a caller-supplied random-access index.
    ///
    /// Every indexed span runs plain zlib-rs inflation from an authoritative
    /// checkpoint. The index is validated against the selected format and
    /// source before workers start; each worker must then reach the next
    /// checkpoint's exact compressed-bit and decompressed-byte offsets.
    /// Invalid or mismatched indexes are errors and never silently select an
    /// unindexed fallback.
    ///
    /// Worker output is handed off in bounded chunks, so a sparse index does
    /// not cause an entire decompressed span to be allocated. Empty gzip
    /// members remain explicit spans and are fully verified.
    ///
    /// When [`DecoderBuilder::count_lines`] is enabled, imported per-checkpoint
    /// and total line counters are recomputed from final ordered output and a
    /// mismatch is rejected. Without line counting, line metadata remains
    /// caller-supplied navigation data and is not authenticated.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rapidgzip_core::{Decoder, DeflateIndex};
    /// use std::fs::File;
    /// use std::io;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut serialized = File::open("reads.fastq.gz.rgzidx")?;
    /// let index = DeflateIndex::read_native(&mut serialized)?;
    /// let source = File::open("reads.fastq.gz")?;
    /// let report = Decoder::default().decode_from_index(
    ///     &source,
    ///     &mut io::sink(),
    ///     &index,
    /// )?;
    /// assert!(report.member_count >= 1);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IndexDecodeError::Index`] for invalid or source-mismatched
    /// metadata, [`IndexDecodeError::FormatMismatch`] when the builder and
    /// index select different containers, or [`IndexDecodeError::Decode`] for
    /// input, DEFLATE, verification, output, limit, or worker failures.
    pub fn decode_from_index<R, W>(
        &self,
        source: &R,
        output: &mut W,
        index: &DeflateIndex,
    ) -> Result<DecodeReport, IndexDecodeError>
    where
        R: ReadAt + ?Sized,
        W: Write,
    {
        let plan = crate::indexed_parallel::IndexedPlan::build(source, &self.config, index)?;
        let cancelled = AtomicBool::new(false);
        let mut sink = DirectOutput::new(output);
        let runtime = RuntimeState::new(
            self.config.decoder_threads,
            self.config.decoder_pool.as_ref(),
        );
        crate::indexed_parallel::decode(
            source,
            &self.config,
            &cancelled,
            &mut sink,
            index,
            &plan,
            &runtime,
        )
        .map_err(IndexDecodeError::from)
    }

    /// Starts decoding an owned positional source and returns `Read + Send`
    /// decompressed output.
    ///
    /// Initial selected framing is validated before the background coordinator
    /// is spawned. Later decoding failures are returned as [`std::io::Error`]
    /// values by [`std::io::Read`], or as [`DecodeError`] by
    /// [`DecoderReader::finish`].
    pub fn reader<R>(&self, source: R) -> Result<DecoderReader, DecodeError>
    where
        R: ReadAt + 'static,
    {
        crate::backend::validate_initial_source(&source, &self.config)?;
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
    /// Returns an initial source or framing failure. Later decode and
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
        crate::backend::validate_initial_source(&source, &self.config)?;
        reader::spawn_indexed(source, self.config.clone(), options)
    }

    /// Starts full-stream decoding through an existing index and returns
    /// owned `Read + Send` output.
    ///
    /// The [`Arc`] permits a large index and its stored windows to be shared
    /// with the background coordinator without cloning them. Validation is
    /// completed before any thread is spawned. Later failures are returned by
    /// [`Read::read`] and preserved by [`DecoderReader::finish`]. The reader's
    /// [`crate::DecoderHandle`] exposes the same telemetry and dynamic worker
    /// ceiling as every other positional parallel path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rapidgzip_core::{Decoder, DeflateIndex};
    /// use std::fs::File;
    /// use std::io;
    /// use std::sync::Arc;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut serialized = File::open("reads.fastq.gz.rgzidx")?;
    /// let index = Arc::new(DeflateIndex::read_native(&mut serialized)?);
    /// let mut reader = Decoder::default().reader_from_index(
    ///     File::open("reads.fastq.gz")?,
    ///     index,
    /// )?;
    /// io::copy(&mut reader, &mut io::sink())?;
    /// reader.finish()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a strict index, format, or initial source validation error, or
    /// a coordinator-thread creation failure.
    pub fn reader_from_index<R>(
        &self,
        source: R,
        index: Arc<DeflateIndex>,
    ) -> Result<DecoderReader, IndexDecodeError>
    where
        R: ReadAt + 'static,
    {
        let plan = crate::indexed_parallel::IndexedPlan::build(&source, &self.config, &index)?;
        reader::spawn_from_index(source, self.config.clone(), index, plan)
            .map_err(IndexDecodeError::from)
    }

    /// Decodes the selected format from a non-seekable source.
    ///
    /// This is the push interface for input that cannot be read positionally,
    /// such as standard input, a FIFO, a process substitution, or a socket. It
    /// mirrors [`Decoder::decode`], including the writer being used only by the
    /// calling thread.
    ///
    /// Validation is identical to [`Decoder::decode`], including gzip CRC32 and
    /// ISIZE, zlib Adler-32, raw-DEFLATE structural completion, trailing-data
    /// rejection, and configured output bounds. The source is read once in
    /// order, so decoding uses one calling thread regardless of
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
    /// println!("completed {} framing units", report.member_count);
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
        let runtime = RuntimeState::new(
            self.config.decoder_threads,
            self.config.decoder_pool.as_ref(),
        );
        let mut cursor = StreamCursor::new(source, self.config.input_page_size);
        decode_stream(&mut cursor, &self.config, &cancelled, &mut sink, &runtime)
    }

    /// Decodes non-seekable input while collecting a coarse but valid index.
    ///
    /// A forward-only source does not expose independently discoverable
    /// interior block boundaries, so the resulting index records gzip member
    /// starts or the single zlib/raw stream start. It can later seek a stable
    /// positional copy of the same compressed bytes.
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
        let runtime = RuntimeState::new(
            self.config.decoder_threads,
            self.config.decoder_pool.as_ref(),
        );
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
    /// a framing-start index.
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

    /// Opens and decodes the selected format from a filesystem path.
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
