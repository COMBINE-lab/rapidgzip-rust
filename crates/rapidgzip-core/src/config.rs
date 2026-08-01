use crate::analyze::{self, ArchiveAnalysis};
use crate::backend::{DirectOutput, decode_source};
use crate::gzip::validate_initial_header;
use crate::indexed_decode;
use crate::reader;
use crate::seek::IndexedReader;
use crate::stream_decode;
use crate::zlib::{looks_like_zlib, validate_initial_zlib_header};
use crate::{DecodeError, DecodeReport, DecoderReader, GzipIndex, ReadAt};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::AtomicBool;

const MIB: usize = 1024 * 1024;

/// Compressed container format selection.
///
/// [`Format::Auto`] (default) inspects the first bytes: gzip magic `1f 8b` vs a
/// valid zlib CMF/FLG header (CM=8, FCHECK). Auto-detection never selects raw
/// DEFLATE (RFC 1951 has no magic bytes). Use [`Format::RawDeflate`] explicitly
/// for unwrapped DEFLATE.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum Format {
    /// Detect gzip vs zlib from the stream prefix.
    #[default]
    Auto,
    /// Require gzip framing (`1f 8b` … CRC32/ISIZE).
    Gzip,
    /// Require zlib framing (CMF/FLG … Adler-32).
    Zlib,
    /// Require raw DEFLATE (RFC 1951) with no gzip/zlib wrapper.
    ///
    /// Sequential zlib-rs raw inflate only (`windowBits = -15`). Single stream
    /// from offset 0 through end-of-stream; leftover compressed bytes after
    /// `Z_STREAM_END` are an error. No on-stream integrity trailer (no
    /// CRC/Adler); optional whole-stream verification is available via
    /// [`DecoderBuilder::raw_crc32_list`] (at most one CRC value). Not selected
    /// by [`Format::Auto`]. Random-access index collection (`keep_index`) is
    /// not supported.
    RawDeflate,
}

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
    /// Container format: auto-detect (default), gzip, zlib, or raw DEFLATE.
    pub(crate) format: Format,
    /// When true (default), integrity trailers are verified.
    ///
    /// For gzip this is the member payload CRC32 footer (ISIZE is always
    /// checked). For zlib this is the Adler-32 trailer. Disabling may improve
    /// throughput by a few percent on large streams.
    pub(crate) crc32_enabled: bool,
    /// When true, collect a [`crate::GzipIndex`] during decode.
    pub(crate) keep_index: bool,
    /// When true (default), keep_index may store predecessor windows
    /// zlib-compressed in memory when the compressed form is smaller.
    pub(crate) compress_index_windows: bool,
    /// When true, count Unix newlines (`\n`) during decode and, together with
    /// [`Self::keep_index`], stamp checkpoint line offsets.
    pub(crate) gather_line_offsets: bool,
    /// Soft target for uncompressed bytes between intermediate checkpoints.
    ///
    /// Actual spacing may be larger (for example only at member boundaries when
    /// intermediate block-accurate positions are unavailable).
    pub(crate) checkpoint_spacing: usize,
    /// Maximum number of decoded windows retained by [`IndexedReader`]'s LRU.
    ///
    /// Zero disables the cache.
    pub(crate) seek_cache_max_chunks: usize,
    /// Maximum total bytes retained by [`IndexedReader`]'s LRU.
    ///
    /// Zero disables the cache. A single chunk larger than this limit is still
    /// admitted after evicting other entries.
    pub(crate) seek_cache_max_bytes: usize,
    /// When true, [`IndexedReader`] decodes the next window into the cache after
    /// serving a miss or cache hit (sequential read-ahead).
    pub(crate) seek_readahead: bool,
    /// Number of uncompressed windows ahead of the active buffer to decode in
    /// background threads after a fill (independent inflate from the nearest
    /// checkpoint). Zero disables background prefetch; sequential read-ahead
    /// is controlled separately by [`Self::seek_readahead`].
    pub(crate) seek_prefetch_windows: usize,
    /// Optional external CRC32 values for raw DEFLATE integrity (gzip-style
    /// IEEE CRC32 of uncompressed output).
    ///
    /// Empty means no external check. At most one value is supported: for
    /// [`Format::RawDeflate`], a single-element list verifies the whole-stream
    /// CRC after a successful inflate. Lists with two or more elements are
    /// rejected by [`DecoderBuilder::build`]. Ignored for gzip and zlib (those
    /// formats use their own trailers).
    pub(crate) raw_crc32_list: Vec<u32>,
}

/// Builder for an immutable, reusable [`Decoder`].
///
/// Defaults use [`std::thread::available_parallelism`] as the maximum decoder
/// budget, 4 MiB decoded chunks, 1 MiB positional input pages and compressed
/// grid spacing, `decoder_threads + 2` in-flight chunks, and no output limit.
/// [`IndexedReader`] defaults to a 16-chunk / 64 MiB decoded-window LRU with
/// sequential read-ahead and two-window background prefetch enabled. The
/// defaults favor throughput; applications with tight memory budgets can reduce
/// the worker budget, decoded chunk size, in-flight count, or seek-cache
/// capacity.
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
                format: Format::Auto,
                crc32_enabled: true,
                keep_index: false,
                compress_index_windows: true,
                gather_line_offsets: false,
                checkpoint_spacing: 4 * MIB,
                seek_cache_max_chunks: 16,
                seek_cache_max_bytes: 64 * MIB,
                seek_readahead: true,
                seek_prefetch_windows: 2,
                raw_crc32_list: Vec::new(),
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

    /// Selects the compressed container format.
    ///
    /// Defaults to [`Format::Auto`], which accepts gzip (`1f 8b`) or zlib
    /// (CMF/FLG with CM=8 and valid FCHECK). Use [`Format::Gzip`],
    /// [`Format::Zlib`], or [`Format::RawDeflate`] to require a specific
    /// container (or raw DEFLATE). Auto-detection never selects raw DEFLATE.
    pub const fn format(mut self, format: Format) -> Self {
        self.config.format = format;
        self
    }

    /// Enables or disables payload integrity-trailer verification.
    ///
    /// Enabled by default. When disabled:
    /// - **gzip**: member body CRC32 footers are not checked; ISIZE is still
    ///   verified. Header FHCRC (if present) is unaffected.
    /// - **zlib**: Adler-32 trailers are not checked.
    /// - **raw DEFLATE**: no trailer exists; this flag has no effect (use
    ///   [`Self::raw_crc32_list`] for optional external whole-stream CRC32).
    ///
    /// Disabling may improve throughput by a few percent.
    pub const fn crc32_enabled(mut self, enabled: bool) -> Self {
        self.config.crc32_enabled = enabled;
        self
    }

    /// Sets an optional external CRC32 list for raw DEFLATE integrity checks.
    ///
    /// Raw DEFLATE (RFC 1951) has no on-stream trailer. Whole-stream external
    /// CRC supports **at most one** value:
    /// - empty list: no external check (default);
    /// - one element: after a successful decode with [`Format::RawDeflate`],
    ///   verifies that the gzip-style IEEE CRC32 of the full uncompressed
    ///   output matches that value;
    /// - two or more elements: rejected by [`Self::build`] with
    ///   [`ConfigError`] (fail-closed; multi-segment CRCs are not supported
    ///   without length boundaries).
    ///
    /// Ignored for gzip and zlib (those formats verify their own trailers when
    /// [`Self::crc32_enabled`] is true). Independent of `crc32_enabled`.
    ///
    /// # Errors
    ///
    /// [`Self::build`] returns [`ConfigError`] when `list.len() > 1`. Decode
    /// mismatch returns [`crate::DecodeError::ChecksumMismatch`] with
    /// `member: 0`.
    pub fn raw_crc32_list(mut self, list: Vec<u32>) -> Self {
        self.config.raw_crc32_list = list;
        self
    }

    /// Enables or disables random-access index collection during decode.
    ///
    /// When enabled, a successful [`crate::DecodeReport`] includes a
    /// [`crate::GzipIndex`] that can be exported as indexed_gzip (`GZIDX`).
    /// Disabled by default so the hot path allocates no checkpoint or window
    /// state.
    ///
    /// Combine with [`Self::gather_line_offsets`] to stamp
    /// [`crate::Checkpoint::line_offset`] on each checkpoint. With the default
    /// [`Self::compress_index_windows`], non-empty predecessor windows may be
    /// held zlib-compressed in memory to reduce RSS for large indexes.
    ///
    /// Incompatible with [`Format::RawDeflate`] (rejected by [`Self::build`]).
    pub const fn keep_index(mut self, enabled: bool) -> Self {
        self.config.keep_index = enabled;
        self
    }

    /// Enables or disables in-memory zlib compression of keep_index windows.
    ///
    /// When enabled (default), checkpoints recorded with
    /// [`Self::keep_index`] store predecessor history as
    /// [`crate::WindowCompression::Zlib`] when the zlib-wrapped payload is
    /// strictly smaller than the raw 32 KiB (or shorter) window. Seek and
    /// export paths decompress on demand via
    /// [`crate::StoredWindow::decompressed`].
    ///
    /// Disable to keep raw windows (faster seek restart at higher RSS). Has
    /// no effect when `keep_index` is false. Import paths always load windows
    /// uncompressed; only the keep_index build path applies this setting.
    ///
    /// # Memory
    ///
    /// Compression is best-effort: empty windows stay empty, and incompressible
    /// history stays raw. [`crate::IndexedReader`] keeps a small LRU of recently
    /// expanded zlib windows (capacity tied to [`Self::seek_cache_chunks`]) so
    /// repeated seeks into the same checkpoints avoid re-inflate of history.
    pub const fn compress_index_windows(mut self, enabled: bool) -> Self {
        self.config.compress_index_windows = enabled;
        self
    }

    /// Enables or disables Unix newline (`\n`) counting during decode.
    ///
    /// When enabled:
    /// - [`crate::DecodeReport::line_count`] is `Some(total_newlines)` (0 for
    ///   empty output).
    /// - With [`Self::keep_index`], each checkpoint's
    ///   [`crate::Checkpoint::line_offset`] is the number of `\n` bytes in the
    ///   uncompressed prefix `[0, uncompressed_offset)`, and
    ///   [`crate::GzipIndex::has_line_offsets`] is set.
    ///
    /// Disabled by default. Line counting does not require `keep_index`; the
    /// total alone is enough for `--count-lines`. Indexed_gzip export does not
    /// persist line offsets.
    pub const fn gather_line_offsets(mut self, enabled: bool) -> Self {
        self.config.gather_line_offsets = enabled;
        self
    }

    /// Sets the soft target spacing between intermediate checkpoints.
    ///
    /// Spacing is measured in **uncompressed** bytes. It is a soft target:
    /// actual gaps may be larger when the decoder can only record safe points
    /// (for example member boundaries with empty windows, or DEFLATE block
    /// ends). Must be non-zero when [`Self::keep_index`] is enabled. Defaults
    /// to 4 MiB, matching the default decoded chunk size.
    pub const fn checkpoint_spacing(mut self, bytes: usize) -> Self {
        self.config.checkpoint_spacing = bytes;
        self
    }

    /// Sets the maximum number of decoded windows kept by [`IndexedReader`].
    ///
    /// Zero disables the seek cache and the expanded zlib index-window cache.
    /// Defaults to 16. The decoded-window cache also respects
    /// [`Self::seek_cache_bytes`]; eviction triggers when either limit is
    /// exceeded. The expanded-window cache uses the same entry count (each
    /// entry is at most 32 KiB of predecessor history).
    pub const fn seek_cache_chunks(mut self, count: usize) -> Self {
        self.config.seek_cache_max_chunks = count;
        self
    }

    /// Sets the maximum total decoded bytes kept by [`IndexedReader`].
    ///
    /// Zero disables the seek cache. Defaults to 64 MiB. A single window larger
    /// than this limit is still cached after other entries are evicted.
    pub const fn seek_cache_bytes(mut self, bytes: usize) -> Self {
        self.config.seek_cache_max_bytes = bytes;
        self
    }

    /// Enables or disables sequential read-ahead into the [`IndexedReader`]
    /// decoded-window cache.
    ///
    /// When enabled (default), after filling a window the reader decodes the
    /// next window into the cache when the active inflate session can continue
    /// without a re-seek. Background parallel prefetch (see
    /// [`Self::seek_prefetch_windows`]) also requires read-ahead to be enabled.
    pub const fn seek_readahead(mut self, enabled: bool) -> Self {
        self.config.seek_readahead = enabled;
        self
    }

    /// Sets how many decoded windows ahead of the active buffer
    /// [`IndexedReader`] should warm via background threads.
    ///
    /// After a buffer fill (and optional sequential read-ahead of the immediate
    /// next window), up to `count` further windows are inflated independently
    /// from the nearest checkpoint and inserted into the LRU. Workers never
    /// share the consumer's live inflate session. Zero disables background
    /// prefetch (sequential read-ahead still applies when enabled). Defaults
    /// to 2. In-flight worker count is capped by
    /// [`Self::decoder_threads`].
    pub const fn seek_prefetch_windows(mut self, count: usize) -> Self {
        self.config.seek_prefetch_windows = count;
        self
    }

    /// Validates the configuration and creates a reusable decoder.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a size or count violates the constraints
    /// documented on its setter, including a [`Self::raw_crc32_list`] with more
    /// than one element.
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
        if self.config.keep_index && self.config.checkpoint_spacing == 0 {
            return Err(ConfigError(
                "checkpoint_spacing must be non-zero when keep_index is enabled",
            ));
        }
        if self.config.keep_index && matches!(self.config.format, Format::RawDeflate) {
            return Err(ConfigError("index not supported for raw deflate"));
        }
        if self.config.raw_crc32_list.len() > 1 {
            return Err(ConfigError(
                "raw_crc32_list supports at most one whole-stream CRC value",
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

    /// Decodes and verifies all gzip, zlib, or raw DEFLATE members into `output`.
    ///
    /// The writer is used only by the calling thread and need not implement
    /// [`Send`]. With the default [`Format::Auto`], the stream is detected as
    /// gzip or zlib from its header. Concatenated multi-stream zlib uses
    /// stream-granularity parallel zlib-rs when `decoder_threads > 1`. Long
    /// single zlib streams use the same estimated marker path as ordinary gzip
    /// when `decoder_threads >= 4` and the compressed size amortizes the grid;
    /// smaller single streams stay sequential. Raw DEFLATE requires an explicit
    /// [`Format::RawDeflate`]; long streams use the same estimated marker path
    /// when `decoder_threads >= 4` and compressed size amortizes the grid.
    /// Raw has no on-stream integrity trailer unless
    /// [`DecoderBuilder::raw_crc32_list`] is set.
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
        decode_source(source, &self.config, &cancelled, &mut sink)
    }

    /// Parallel (when `decoder_threads > 1`) full-stream decode using a
    /// prebuilt or imported index.
    ///
    /// Emits ordered uncompressed bytes for the complete stream into `output`.
    /// Work is split at consecutive checkpoints: each worker resumes raw
    /// DEFLATE at a known bit offset with the stored predecessor window and
    /// produces exactly the span until the next checkpoint. This skips
    /// marker/window speculation used by the index-free parallel path.
    ///
    /// # CRC / verification
    ///
    /// Member payload CRC32 and ISIZE are **not** verified on this path
    /// (segments may start mid-member). Prefer [`Self::decode`] when full
    /// footer verification is required. Correctness relies on the index
    /// checkpoints matching the archive.
    ///
    /// # Report
    ///
    /// [`DecodeReport::index`] is always `None` so large window maps are not
    /// cloned; the caller's `index` is left unchanged. Sizes are taken from the
    /// index when known.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::InvalidIndex`] when the index fails validation,
    /// has no checkpoints, or its recorded compressed size disagrees with
    /// `source.len()`. Inflate failures yield the usual deflate/gzip errors.
    pub fn decode_with_index<R, W>(
        &self,
        source: &R,
        index: &GzipIndex,
        output: &mut W,
    ) -> Result<DecodeReport, DecodeError>
    where
        R: ReadAt + ?Sized,
        W: Write,
    {
        let cancelled = AtomicBool::new(false);
        let mut sink = DirectOutput::new(output);
        indexed_decode::decode_with_index(source, index, &self.config, &cancelled, &mut sink)
    }

    /// Decodes gzip, zlib, or raw DEFLATE from a streaming [`Read`] (stdin, sockets, pipes).
    ///
    /// When `decoder_threads == 1`, pulls compressed bytes page-at-a-time without
    /// buffering the full archive. When `decoder_threads > 1`, spills the stream
    /// to a private temporary file and runs the positional backend (parallel
    /// gzip / multi-stream or marker zlib / marker raw DEFLATE when each
    /// format’s thread and size gates allow). Prefer [`Decoder::decode`] /
    /// [`Decoder::open`] on files when you already have positional input and
    /// want to avoid the spill.
    ///
    /// # Memory
    ///
    /// - **Single-thread**: peak compressed-side memory is a small input page
    ///   (see [`DecoderBuilder::input_page_size`]) plus inflate working set and
    ///   any index/window state — not the full archive.
    /// - **Multi-thread**: compressed size **on disk** (private tempfile) plus
    ///   the usual decoder working set — not a second full RAM copy.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::Io`] when reading from `reader` or writing the
    /// spill tempfile fails. Framing, truncate, checksum, and deflate errors
    /// match the paths used by [`Decoder::decode`]. With
    /// [`DecoderBuilder::keep_index`] enabled, a successful report may include a
    /// [`crate::GzipIndex`].
    pub fn decode_read<R, W>(&self, reader: R, output: &mut W) -> Result<DecodeReport, DecodeError>
    where
        R: Read,
        W: Write,
    {
        stream_decode::decode_read_stream(reader, &self.config, output)
    }

    /// Starts decoding an owned positional source and returns `Read + Send`
    /// decompressed output.
    ///
    /// Initial container framing (gzip and/or zlib per [`DecoderBuilder::format`])
    /// is validated before the background coordinator is spawned. For
    /// [`Format::RawDeflate`] there is no framing check beyond a non-empty
    /// source. Later decoding failures are returned as [`std::io::Error`]
    /// values by [`std::io::Read`], or as [`DecodeError`] by
    /// [`DecoderReader::finish`].
    pub fn reader<R>(&self, source: R) -> Result<DecoderReader, DecodeError>
    where
        R: ReadAt + 'static,
    {
        validate_source_header(&source, &self.config)?;
        reader::spawn(source, self.config.clone())
    }

    /// Opens a compressed file and returns a `Read + Send` decompressed stream.
    ///
    /// The file is owned by the returned reader and accessed positionally.
    /// Accepts gzip or zlib when using the default auto format; use
    /// [`DecoderBuilder::format`] for raw DEFLATE.
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<DecoderReader, DecodeError> {
        let file = File::open(path).map_err(|error| DecodeError::input_io(0, error))?;
        self.reader(file)
    }

    /// Analyzes gzip / BGZF framing and DEFLATE block structure.
    ///
    /// Walks the archive **sequentially** with raw inflate and `Z_BLOCK`. No
    /// index is required and no decompressed payload is emitted. Respects
    /// [`DecoderBuilder::crc32_enabled`] for footer CRC checks; ISIZE is always
    /// verified. Each BGZF block is reported as one member.
    ///
    /// # Errors
    ///
    /// Returns framing, DEFLATE, checksum, or I/O errors when the archive is
    /// truncated or corrupt. Successful return means every member footer was
    /// checked under the configured verification settings.
    pub fn analyze<R: ReadAt + ?Sized>(&self, source: &R) -> Result<ArchiveAnalysis, DecodeError> {
        analyze::analyze_source_with_format(
            source,
            self.config.input_page_size,
            self.config.crc32_enabled,
            self.config.format,
        )
    }

    /// Opens a seekable reader for `source` using a prebuilt or imported index.
    ///
    /// The index must validate, contain at least one checkpoint, and (when it
    /// records a known compressed size) match `source.len()`. See
    /// [`IndexedReader`] for seek/read semantics and CRC limitations.
    pub fn reader_with_index<R: ReadAt + 'static>(
        &self,
        source: R,
        index: GzipIndex,
    ) -> Result<IndexedReader<R>, DecodeError> {
        IndexedReader::new(source, index, self.config.clone())
    }

    /// Opens a compressed file with a prebuilt or imported index for seeking.
    ///
    /// The file is owned by the returned reader and accessed positionally.
    pub fn open_with_index<P: AsRef<Path>>(
        &self,
        path: P,
        index: GzipIndex,
    ) -> Result<IndexedReader<File>, DecodeError> {
        let file = File::open(path).map_err(|error| DecodeError::input_io(0, error))?;
        self.reader_with_index(file, index)
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

fn validate_source_header<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
) -> Result<(), DecodeError> {
    match config.format {
        Format::Gzip => validate_initial_header(source, config.input_page_size),
        Format::Zlib => validate_initial_zlib_header(source, config.input_page_size),
        Format::RawDeflate => {
            // Raw DEFLATE has no magic; only reject empty sources early so the
            // reader path fails before spawning workers.
            let length = source
                .len()
                .map_err(|error| DecodeError::input_io(0, error))?;
            if length == 0 {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: 0,
                    reason: crate::DeflateErrorKind::Truncated,
                });
            }
            Ok(())
        }
        Format::Auto => {
            if looks_like_zlib(source, config.input_page_size)? {
                validate_initial_zlib_header(source, config.input_page_size)
            } else {
                validate_initial_header(source, config.input_page_size)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Decoder, Format, MIB};

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
    fn crc32_enabled_defaults_to_true_and_can_be_disabled() {
        let enabled = Decoder::builder().build().unwrap();
        assert!(enabled.config.crc32_enabled);
        let disabled = Decoder::builder().crc32_enabled(false).build().unwrap();
        assert!(!disabled.config.crc32_enabled);
    }

    #[test]
    fn raw_crc32_list_defaults_empty_and_can_be_set() {
        let default = Decoder::builder().build().unwrap();
        assert!(default.config.raw_crc32_list.is_empty());
        let with_list = Decoder::builder()
            .raw_crc32_list(vec![0x1234_5678])
            .build()
            .unwrap();
        assert_eq!(with_list.config.raw_crc32_list, vec![0x1234_5678]);
    }

    #[test]
    fn raw_crc32_list_rejects_multi_element_at_build() {
        let multi = Decoder::builder().raw_crc32_list(vec![0x1234_5678, 0x9abc_def0]);
        // Setter accepts the list; validation is deferred to build.
        assert_eq!(multi.config.raw_crc32_list, vec![0x1234_5678, 0x9abc_def0]);
        let error = multi.build().unwrap_err();
        assert_eq!(
            error.to_string(),
            "raw_crc32_list supports at most one whole-stream CRC value"
        );
    }

    #[test]
    fn format_defaults_to_auto_and_can_be_forced() {
        let auto = Decoder::builder().build().unwrap();
        assert_eq!(auto.config.format, Format::Auto);
        let gzip = Decoder::builder().format(Format::Gzip).build().unwrap();
        assert_eq!(gzip.config.format, Format::Gzip);
        let zlib = Decoder::builder().format(Format::Zlib).build().unwrap();
        assert_eq!(zlib.config.format, Format::Zlib);
        let raw = Decoder::builder()
            .format(Format::RawDeflate)
            .build()
            .unwrap();
        assert_eq!(raw.config.format, Format::RawDeflate);
    }

    #[test]
    fn keep_index_rejected_for_raw_deflate() {
        let error = Decoder::builder()
            .format(Format::RawDeflate)
            .keep_index(true)
            .build()
            .unwrap_err();
        assert_eq!(error.to_string(), "index not supported for raw deflate");
    }

    #[test]
    fn keep_index_defaults_to_false_and_rejects_zero_spacing() {
        let decoder = Decoder::builder().build().unwrap();
        assert!(!decoder.config.keep_index);
        assert!(decoder.config.compress_index_windows);
        assert!(!decoder.config.gather_line_offsets);
        assert_eq!(decoder.config.checkpoint_spacing, 4 * MIB);

        let enabled = Decoder::builder()
            .keep_index(true)
            .checkpoint_spacing(64 * 1024)
            .build()
            .unwrap();
        assert!(enabled.config.keep_index);
        assert!(enabled.config.compress_index_windows);
        assert_eq!(enabled.config.checkpoint_spacing, 64 * 1024);

        let no_compress = Decoder::builder()
            .keep_index(true)
            .compress_index_windows(false)
            .build()
            .unwrap();
        assert!(!no_compress.config.compress_index_windows);

        let with_lines = Decoder::builder()
            .gather_line_offsets(true)
            .build()
            .unwrap();
        assert!(with_lines.config.gather_line_offsets);

        let error = Decoder::builder()
            .keep_index(true)
            .checkpoint_spacing(0)
            .build()
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "checkpoint_spacing must be non-zero when keep_index is enabled"
        );
    }

    #[test]
    fn seek_cache_defaults_and_setters() {
        let decoder = Decoder::builder().build().unwrap();
        assert_eq!(decoder.config.seek_cache_max_chunks, 16);
        assert_eq!(decoder.config.seek_cache_max_bytes, 64 * MIB);
        assert!(decoder.config.seek_readahead);
        assert_eq!(decoder.config.seek_prefetch_windows, 2);

        let tuned = Decoder::builder()
            .seek_cache_chunks(4)
            .seek_cache_bytes(MIB)
            .seek_readahead(false)
            .seek_prefetch_windows(0)
            .build()
            .unwrap();
        assert_eq!(tuned.config.seek_cache_max_chunks, 4);
        assert_eq!(tuned.config.seek_cache_max_bytes, MIB);
        assert!(!tuned.config.seek_readahead);
        assert_eq!(tuned.config.seek_prefetch_windows, 0);
    }
}
