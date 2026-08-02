//! Random-access indexes for gzip, zlib, and raw-DEFLATE sources.
//!
//! This module is independent of the decoder: it defines the index data model,
//! validates it, and reads and writes the supported on-disk formats. Use an
//! index for random access with [`crate::IndexedReader`].

mod build;
mod gzi;
mod gzidx;
mod gztool;
mod native;
mod window_codec;

pub(crate) use build::IndexCollector;
pub use gzidx::{decode_bit_offset, encode_bit_offset};
pub use gztool::WithLines;
pub(crate) use window_codec::{zlib_compress_window, zlib_decompress_window};

use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read, Write};
use std::num::NonZeroU64;
use std::sync::Arc;

/// DEFLATE history size, in bytes, required at a resume point.
pub const WINDOW_SIZE: usize = 32768;

/// Provenance of the compressed source described by an index.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum IndexKind {
    /// Ordinary gzip, including concatenated gzip members.
    #[default]
    Gzip,
    /// A stream proven to consist entirely of BGZF blocks.
    Bgzf,
    /// One RFC 1950 zlib stream.
    Zlib,
    /// One unwrapped RFC 1951 DEFLATE stream.
    RawDeflate,
}

/// How inflation resumes at a checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CheckpointKind {
    /// The offset points at the gzip magic bytes of a complete member.
    GzipMemberHeader,
    /// The offset points at raw DEFLATE immediately after a known member
    /// header. Keeping the header offset permits full verification while
    /// retaining compatibility with raw-DEFLATE index formats.
    GzipMemberDeflate {
        /// Absolute byte offset of the gzip member header.
        header_offset_in_bytes: u64,
    },
    /// The offset points at the two-byte header of the zlib stream.
    ZlibHeader,
    /// The offset points at the first bit of an unwrapped DEFLATE stream.
    RawDeflateStart,
    /// The offset points at the first bit of a raw DEFLATE block.
    DeflateBlock,
}

/// How predecessor windows are retained while an index is built.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum WindowStorage {
    /// Retain each 32 KiB window verbatim.
    Raw,
    /// Retain the zlib-compressed form when it is smaller.
    #[default]
    Zlib,
}

/// Options for collecting a random-access index during a decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexOptions {
    /// Target distance between retained interior checkpoints.
    pub checkpoint_spacing: NonZeroU64,
    /// In-memory representation of retained predecessor windows.
    pub window_storage: WindowStorage,
}

impl Default for IndexOptions {
    fn default() -> Self {
        Self {
            checkpoint_spacing: NonZeroU64::new(4 * 1024 * 1024).expect("four MiB is non-zero"),
            window_storage: WindowStorage::Zlib,
        }
    }
}

/// Allocation limits applied while parsing an untrusted index file.
///
/// The defaults permit more than four million checkpoints while preventing a
/// small hostile header from requesting an effectively unbounded allocation.
/// Applications opening exceptionally large trusted indexes can raise these
/// limits explicitly with the `read_*_with_options` methods.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexReadOptions {
    /// Maximum number of checkpoint records.
    pub max_checkpoints: usize,
    /// Maximum sum of stored predecessor-window payload bytes.
    pub max_window_bytes: u64,
    /// Maximum bytes in one stored predecessor-window payload.
    pub max_window_payload_bytes: usize,
}

impl Default for IndexReadOptions {
    fn default() -> Self {
        Self {
            max_checkpoints: 4 * 1024 * 1024,
            max_window_bytes: 512 * 1024 * 1024,
            max_window_payload_bytes: 64 * 1024,
        }
    }
}

/// A random-access point into a DEFLATE-based stream.
///
/// The compressed position is a bit offset because a DEFLATE block boundary is
/// not generally byte aligned. [`CheckpointKind`] says whether the offset is a
/// gzip member header, a known member's raw payload, a zlib header, a raw
/// stream start, or an interior DEFLATE block, so a reader never has to infer
/// framing from compressed bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Checkpoint {
    /// Absolute compressed bit offset from the start of the source.
    pub compressed_offset_in_bits: u64,
    /// Absolute decompressed byte offset across all members.
    pub uncompressed_offset_in_bytes: u64,
    /// How a decoder resumes at this compressed position.
    pub kind: CheckpointKind,
    /// Number of lines preceding this point, when supplied by the index.
    pub line_offset: Option<u64>,
}

/// Predecessor history for a checkpoint.
///
/// An empty window means no history is required: the start of the source, a
/// member boundary, or an independent BGZF block. A non-empty window is
/// exactly [`WINDOW_SIZE`] bytes of decompressed history, held either raw or
/// zlib-compressed to bound resident memory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredWindow {
    payload: Vec<u8>,
    compressed: bool,
}

impl StoredWindow {
    /// Returns a window carrying no history.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            payload: Vec::new(),
            compressed: false,
        }
    }

    /// Stores exactly one raw DEFLATE history window.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::InvalidWindowSize`] unless `bytes` contains
    /// exactly [`WINDOW_SIZE`] bytes.
    pub fn from_raw(bytes: impl Into<Vec<u8>>) -> Result<Self, IndexError> {
        let payload = bytes.into();
        validate_expanded_window(&payload)?;
        Ok(Self {
            payload,
            compressed: false,
        })
    }

    /// Returns whether this window carries no history.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    /// Returns the number of bytes currently held, compressed or not.
    #[must_use]
    pub const fn stored_len(&self) -> usize {
        self.payload.len()
    }

    /// Returns whether the payload is held zlib-compressed.
    #[must_use]
    pub const fn is_compressed(&self) -> bool {
        self.compressed
    }

    /// Stores `bytes`, optionally zlib-compressed to reduce resident memory.
    ///
    /// History that does not shrink is stored raw. The input must contain
    /// exactly [`WINDOW_SIZE`] bytes.
    pub fn from_raw_maybe_compress(
        bytes: impl Into<Vec<u8>>,
        compress: bool,
    ) -> Result<Self, IndexError> {
        let bytes = bytes.into();
        validate_expanded_window(&bytes)?;
        if !compress {
            return Self::from_raw(bytes);
        }
        let payload = zlib_compress_window(&bytes)?;
        if payload.len() >= bytes.len() {
            return Self::from_raw(bytes);
        }
        // This payload was produced from the exact-size `bytes` validated
        // above, so re-inflating it here would duplicate codec work at every
        // collected checkpoint. Imported payloads still go through the strict
        // `from_compressed` validation path.
        Ok(Self {
            payload,
            compressed: true,
        })
    }

    /// Returns the window history, expanding it when it is held compressed.
    pub fn decompressed(&self) -> Result<Cow<'_, [u8]>, IndexError> {
        if self.compressed {
            Ok(Cow::Owned(zlib_decompress_window(&self.payload)?))
        } else {
            Ok(Cow::Borrowed(&self.payload))
        }
    }

    pub(crate) fn from_compressed(payload: Vec<u8>) -> Result<Self, IndexError> {
        let expanded = zlib_decompress_window(&payload)?;
        validate_expanded_window(&expanded)?;
        Ok(Self {
            payload,
            compressed: true,
        })
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }
}

fn validate_expanded_window(bytes: &[u8]) -> Result<(), IndexError> {
    if bytes.len() != WINDOW_SIZE {
        return Err(IndexError::InvalidWindowSize(
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        ));
    }
    Ok(())
}

/// Predecessor windows keyed by compressed bit offset.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WindowMap {
    windows: HashMap<u64, StoredWindow>,
}

impl WindowMap {
    /// Returns an empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Associates `window` with `compressed_offset_in_bits`.
    pub fn insert(&mut self, compressed_offset_in_bits: u64, window: StoredWindow) {
        self.windows.insert(compressed_offset_in_bits, window);
    }

    /// Returns the window stored at `compressed_offset_in_bits`, if any.
    #[must_use]
    pub fn get(&self, compressed_offset_in_bits: u64) -> Option<&StoredWindow> {
        self.windows.get(&compressed_offset_in_bits)
    }

    /// Returns the number of stored windows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.windows.len()
    }

    /// Returns whether no windows are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.windows.is_empty()
    }
}

/// An in-memory random-access index for a DEFLATE-based stream.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeflateIndex {
    pub(crate) checkpoints: Vec<Checkpoint>,
    pub(crate) windows: WindowMap,
    pub(crate) kind: IndexKind,
    pub(crate) compressed_size_in_bytes: Option<u64>,
    pub(crate) uncompressed_size_in_bytes: Option<u64>,
    pub(crate) checkpoint_spacing_in_bytes: Option<u64>,
    pub(crate) total_line_count: Option<u64>,
}

impl DeflateIndex {
    /// Returns an empty index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the source/container provenance recorded by this index.
    #[must_use]
    pub const fn kind(&self) -> IndexKind {
        self.kind
    }

    /// Records source/container provenance.
    pub const fn set_kind(&mut self, kind: IndexKind) {
        self.kind = kind;
    }

    /// Returns the known compressed source size in bytes.
    #[must_use]
    pub const fn compressed_size(&self) -> Option<u64> {
        self.compressed_size_in_bytes
    }

    /// Records the compressed source size, or clears it when unknown.
    pub const fn set_compressed_size(&mut self, size: Option<u64>) {
        self.compressed_size_in_bytes = size;
    }

    /// Returns the known total decompressed size in bytes.
    #[must_use]
    pub const fn uncompressed_size(&self) -> Option<u64> {
        self.uncompressed_size_in_bytes
    }

    /// Records the total decompressed size, or clears it when unknown.
    pub const fn set_uncompressed_size(&mut self, size: Option<u64>) {
        self.uncompressed_size_in_bytes = size;
    }

    /// Returns the target decompressed checkpoint spacing, when recorded.
    #[must_use]
    pub const fn checkpoint_spacing(&self) -> Option<u64> {
        self.checkpoint_spacing_in_bytes
    }

    /// Records the target decompressed checkpoint spacing.
    pub const fn set_checkpoint_spacing(&mut self, spacing: Option<u64>) {
        self.checkpoint_spacing_in_bytes = spacing;
    }

    /// Returns the total line count carried by the source index.
    #[must_use]
    pub const fn total_line_count(&self) -> Option<u64> {
        self.total_line_count
    }

    /// Records the total line count, or clears it when unknown.
    pub const fn set_total_line_count(&mut self, count: Option<u64>) {
        self.total_line_count = count;
    }

    /// Appends a checkpoint and its predecessor window.
    ///
    /// Ordering is not checked here; call [`Self::validate`] once the index is
    /// complete.
    pub fn push(&mut self, checkpoint: Checkpoint, window: StoredWindow) -> Result<(), IndexError> {
        if matches!(
            checkpoint.kind,
            CheckpointKind::GzipMemberHeader
                | CheckpointKind::ZlibHeader
                | CheckpointKind::RawDeflateStart
        ) {
            if !checkpoint.compressed_offset_in_bits.is_multiple_of(8) {
                return Err(IndexError::InvalidCheckpoint(
                    "stream-start checkpoint is not byte aligned",
                ));
            }
            if !window.is_empty() {
                return Err(IndexError::InvalidCheckpoint(
                    "stream-start checkpoint carries a predecessor window",
                ));
            }
        }
        if let CheckpointKind::GzipMemberDeflate {
            header_offset_in_bytes,
        } = checkpoint.kind
        {
            if !checkpoint.compressed_offset_in_bits.is_multiple_of(8)
                || header_offset_in_bytes.saturating_mul(8) >= checkpoint.compressed_offset_in_bits
            {
                return Err(IndexError::InvalidCheckpoint(
                    "member-DEFLATE checkpoint has inconsistent header and payload offsets",
                ));
            }
            if !window.is_empty() {
                return Err(IndexError::InvalidCheckpoint(
                    "member-DEFLATE checkpoint carries a predecessor window",
                ));
            }
        }
        if !window.is_empty() {
            validate_expanded_window(&window.decompressed()?)?;
        }
        if !window.is_empty() {
            self.windows
                .insert(checkpoint.compressed_offset_in_bits, window);
        }
        self.checkpoints.push(checkpoint);
        Ok(())
    }

    /// Returns the number of checkpoints.
    #[must_use]
    pub fn checkpoint_count(&self) -> usize {
        self.checkpoints.len()
    }

    /// Returns whether the index holds no checkpoints.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.checkpoints.is_empty()
    }

    /// Returns the checkpoints in order.
    #[must_use]
    pub fn checkpoints(&self) -> &[Checkpoint] {
        &self.checkpoints
    }

    /// Returns the stored predecessor windows.
    #[must_use]
    pub const fn windows(&self) -> &WindowMap {
        &self.windows
    }

    /// Returns the last checkpoint at or before `uncompressed_offset`.
    #[must_use]
    pub fn checkpoint_at_or_before(&self, uncompressed_offset: u64) -> Option<&Checkpoint> {
        let position = self
            .checkpoints
            .partition_point(|point| point.uncompressed_offset_in_bytes <= uncompressed_offset);
        position
            .checked_sub(1)
            .map(|index| &self.checkpoints[index])
    }

    /// Writes this index in the crate's native versioned format.
    ///
    /// The native format is the only one that round-trips every field,
    /// including line offsets and compressed window payloads.
    pub fn write_native(&self, writer: &mut impl Write) -> Result<(), IndexError> {
        native::write_native(self, writer)
    }

    /// Reads an index written by [`Self::write_native`].
    pub fn read_native(reader: &mut impl Read) -> Result<Self, IndexError> {
        Self::read_native_with_options(reader, IndexReadOptions::default())
    }

    /// Reads a native index using explicit untrusted-input limits.
    pub fn read_native_with_options(
        reader: &mut impl Read,
        options: IndexReadOptions,
    ) -> Result<Self, IndexError> {
        native::read_native(reader, options)
    }

    /// Writes this index in indexed_gzip `GZIDX` version 1 format.
    ///
    /// Every non-empty window is written as exactly [`WINDOW_SIZE`] bytes.
    pub fn write_gzidx(&self, writer: &mut impl Write) -> Result<(), IndexError> {
        gzidx::write_gzidx(self, writer)
    }

    /// Reads an indexed_gzip `GZIDX` index, accepting versions 0 and 1.
    ///
    /// When `archive_size` is `Some`, it must equal the compressed size stored
    /// in the index header.
    pub fn read_gzidx(
        reader: &mut impl Read,
        archive_size: Option<u64>,
    ) -> Result<Self, IndexError> {
        Self::read_gzidx_with_options(reader, archive_size, IndexReadOptions::default())
    }

    /// Reads a GZIDX index using explicit untrusted-input limits.
    pub fn read_gzidx_with_options(
        reader: &mut impl Read,
        archive_size: Option<u64>,
        options: IndexReadOptions,
    ) -> Result<Self, IndexError> {
        gzidx::read_gzidx(reader, archive_size, options)
    }

    /// Writes this index in htslib BGZF `.gzi` format.
    ///
    /// Only indexes whose checkpoints all sit on independent member or block
    /// boundaries can be represented; a checkpoint carrying a predecessor
    /// window or a non-byte-aligned offset is refused, because reimporting it
    /// would install an empty window and seek to the wrong place.
    pub fn write_gzi(&self, writer: &mut impl Write) -> Result<(), IndexError> {
        gzi::write_gzi(self, writer)
    }

    /// Reads an htslib BGZF `.gzi` index.
    ///
    /// The format does not record the uncompressed size, so the result leaves
    /// it unknown. `archive_size`, when supplied, is recorded as the compressed
    /// size.
    pub fn read_gzi(reader: &mut impl Read, archive_size: Option<u64>) -> Result<Self, IndexError> {
        Self::read_gzi_with_options(reader, archive_size, IndexReadOptions::default())
    }

    /// Reads a `.gzi` index using explicit untrusted-input limits.
    pub fn read_gzi_with_options(
        reader: &mut impl Read,
        archive_size: Option<u64>,
        options: IndexReadOptions,
    ) -> Result<Self, IndexError> {
        gzi::read_gzi(reader, archive_size, options)
    }

    /// Writes this index in gztool format.
    ///
    /// [`WithLines::Yes`] writes version 1 with per-point line counters;
    /// [`WithLines::No`] writes version 0 and omits them. Windows are stored
    /// zlib-compressed, as gztool does.
    pub fn write_gztool(
        &self,
        writer: &mut impl Write,
        lines: WithLines,
    ) -> Result<(), IndexError> {
        gztool::write_gztool(self, writer, lines)
    }

    /// Reads a complete gztool index of either version.
    ///
    /// gztool does not record the compressed archive size, so `archive_size`,
    /// when supplied, is recorded as the compressed size.
    pub fn read_gztool(
        reader: &mut impl Read,
        archive_size: Option<u64>,
    ) -> Result<Self, IndexError> {
        Self::read_gztool_with_options(reader, archive_size, IndexReadOptions::default())
    }

    /// Reads a gztool index using explicit untrusted-input limits.
    pub fn read_gztool_with_options(
        reader: &mut impl Read,
        archive_size: Option<u64>,
        options: IndexReadOptions,
    ) -> Result<Self, IndexError> {
        gztool::read_gztool(reader, archive_size, options)
    }

    /// Checks the index invariants.
    ///
    /// Compressed offsets must increase strictly and decompressed offsets must
    /// not decrease. Every non-empty window must
    /// be exactly [`WINDOW_SIZE`] bytes, and offsets must fall inside the
    /// recorded sizes when those are known.
    pub fn validate(&self) -> Result<(), IndexError> {
        let mut previous: Option<&Checkpoint> = None;
        for checkpoint in &self.checkpoints {
            if !checkpoint_kind_matches_index(self.kind, checkpoint.kind) {
                return Err(IndexError::InvalidCheckpoint(
                    "checkpoint framing is incompatible with index provenance",
                ));
            }
            if let Some(previous) = previous {
                if checkpoint.compressed_offset_in_bits <= previous.compressed_offset_in_bits {
                    return Err(IndexError::InvalidCheckpoint(
                        "compressed offsets are not strictly increasing",
                    ));
                }
                if checkpoint.uncompressed_offset_in_bytes < previous.uncompressed_offset_in_bytes {
                    return Err(IndexError::InvalidCheckpoint(
                        "uncompressed offsets are decreasing",
                    ));
                }
            }

            if self
                .compressed_size_in_bytes
                .is_some_and(|size| checkpoint.compressed_offset_in_bits > size.saturating_mul(8))
            {
                return Err(IndexError::InvalidCheckpoint(
                    "checkpoint compressed offset is after the source end",
                ));
            }
            if self
                .uncompressed_size_in_bytes
                .is_some_and(|size| checkpoint.uncompressed_offset_in_bytes > size)
            {
                return Err(IndexError::InvalidCheckpoint(
                    "checkpoint uncompressed offset is after the source end",
                ));
            }

            if let Some(window) = self.windows.get(checkpoint.compressed_offset_in_bits) {
                validate_expanded_window(&window.decompressed()?)?;
            }
            if matches!(
                checkpoint.kind,
                CheckpointKind::GzipMemberHeader
                    | CheckpointKind::ZlibHeader
                    | CheckpointKind::RawDeflateStart
            ) {
                if !checkpoint.compressed_offset_in_bits.is_multiple_of(8) {
                    return Err(IndexError::InvalidCheckpoint(
                        "stream-start checkpoint is not byte aligned",
                    ));
                }
                if self
                    .windows
                    .get(checkpoint.compressed_offset_in_bits)
                    .is_some()
                {
                    return Err(IndexError::InvalidCheckpoint(
                        "stream-start checkpoint carries a predecessor window",
                    ));
                }
            }
            if let CheckpointKind::GzipMemberDeflate {
                header_offset_in_bytes,
            } = checkpoint.kind
            {
                if !checkpoint.compressed_offset_in_bits.is_multiple_of(8)
                    || header_offset_in_bytes.saturating_mul(8)
                        >= checkpoint.compressed_offset_in_bits
                {
                    return Err(IndexError::InvalidCheckpoint(
                        "member-DEFLATE checkpoint has inconsistent header and payload offsets",
                    ));
                }
                if self
                    .windows
                    .get(checkpoint.compressed_offset_in_bits)
                    .is_some()
                {
                    return Err(IndexError::InvalidCheckpoint(
                        "member-DEFLATE checkpoint carries a predecessor window",
                    ));
                }
            }
            if matches!(
                checkpoint.kind,
                CheckpointKind::ZlibHeader | CheckpointKind::RawDeflateStart
            ) && (checkpoint.compressed_offset_in_bits != 0
                || checkpoint.uncompressed_offset_in_bytes != 0)
            {
                return Err(IndexError::InvalidCheckpoint(
                    "single-stream start checkpoint is not at the source origin",
                ));
            }
            if matches!(self.kind, IndexKind::Zlib | IndexKind::RawDeflate)
                && matches!(checkpoint.kind, CheckpointKind::DeflateBlock)
                && self
                    .windows
                    .get(checkpoint.compressed_offset_in_bits)
                    .is_none()
            {
                return Err(IndexError::InvalidCheckpoint(
                    "single-stream interior checkpoint has no predecessor window",
                ));
            }

            previous = Some(checkpoint);
        }
        Ok(())
    }
}

const fn checkpoint_kind_matches_index(kind: IndexKind, checkpoint: CheckpointKind) -> bool {
    match kind {
        IndexKind::Gzip | IndexKind::Bgzf => matches!(
            checkpoint,
            CheckpointKind::GzipMemberHeader
                | CheckpointKind::GzipMemberDeflate { .. }
                | CheckpointKind::DeflateBlock
        ),
        IndexKind::Zlib => matches!(
            checkpoint,
            CheckpointKind::ZlibHeader | CheckpointKind::DeflateBlock
        ),
        IndexKind::RawDeflate => matches!(
            checkpoint,
            CheckpointKind::RawDeflateStart | CheckpointKind::DeflateBlock
        ),
    }
}

/// Errors produced while parsing, validating, or writing an index.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum IndexError {
    /// The file did not begin with the expected magic bytes.
    BadMagic {
        /// The bytes actually observed.
        found: Vec<u8>,
    },
    /// The format version is newer than this crate supports.
    UnsupportedVersion(u64),
    /// The index declared a window size other than [`WINDOW_SIZE`].
    InvalidWindowSize(u64),
    /// A declared count or length exceeded the accepted maximum.
    ExcessiveLength {
        /// What the value described.
        what: &'static str,
        /// The value read from the file.
        value: u64,
    },
    /// A checkpoint field was denormal or inconsistent.
    InvalidCheckpoint(&'static str),
    /// A caller-supplied archive size disagreed with the index.
    ArchiveSizeMismatch {
        /// Size recorded in the index.
        index_size: u64,
        /// Size supplied by the caller.
        archive_size: u64,
    },
    /// The index ended before a complete value could be read.
    Truncated,
    /// A window payload could not be compressed or decompressed.
    WindowCodec(&'static str),
    /// The process could not reserve memory within the configured limit.
    AllocationFailed {
        /// What allocation was attempted.
        what: &'static str,
    },
    /// A format contained flags this crate does not understand.
    UnsupportedFlags {
        /// Flags observed in the input.
        flags: u64,
    },
    /// An operation requires metadata not present in this index.
    MissingMetadata(&'static str),
    /// The selected on-disk representation cannot encode this source format.
    IncompatibleFormat {
        /// Operation or on-disk representation that was requested.
        operation: &'static str,
        /// Provenance recorded by the index.
        kind: IndexKind,
    },
    /// An I/O failure occurred.
    Io {
        /// The original error.
        source: Arc<io::Error>,
    },
}

impl IndexError {
    pub(crate) fn io(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            Self::Truncated
        } else {
            Self::Io {
                source: Arc::new(error),
            }
        }
    }
}

impl Display for IndexError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic { found } => write!(formatter, "invalid index magic bytes: {found:?}"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported index format version {version}")
            }
            Self::InvalidWindowSize(size) => write!(
                formatter,
                "invalid index window size {size}, expected {WINDOW_SIZE}"
            ),
            Self::ExcessiveLength { what, value } => {
                write!(formatter, "index declares an excessive {what}: {value}")
            }
            Self::InvalidCheckpoint(reason) => {
                write!(formatter, "invalid index checkpoint: {reason}")
            }
            Self::ArchiveSizeMismatch {
                index_size,
                archive_size,
            } => write!(
                formatter,
                "archive size {archive_size} does not match index size {index_size}"
            ),
            Self::Truncated => formatter.write_str("truncated index"),
            Self::WindowCodec(reason) => write!(formatter, "index window codec failure: {reason}"),
            Self::AllocationFailed { what } => {
                write!(formatter, "could not allocate memory for index {what}")
            }
            Self::UnsupportedFlags { flags } => {
                write!(formatter, "unsupported index flags {flags:#x}")
            }
            Self::MissingMetadata(what) => write!(formatter, "index is missing {what}"),
            Self::IncompatibleFormat { operation, kind } => {
                write!(formatter, "{operation} cannot represent a {kind:?} index")
            }
            Self::Io { source } => write!(formatter, "index I/O error: {source}"),
        }
    }
}

impl Error for IndexError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl PartialEq for IndexError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::BadMagic { found: left }, Self::BadMagic { found: right }) => left == right,
            (Self::UnsupportedVersion(left), Self::UnsupportedVersion(right)) => left == right,
            (Self::InvalidWindowSize(left), Self::InvalidWindowSize(right)) => left == right,
            (
                Self::ExcessiveLength {
                    what: left_what,
                    value: left_value,
                },
                Self::ExcessiveLength {
                    what: right_what,
                    value: right_value,
                },
            ) => left_what == right_what && left_value == right_value,
            (Self::InvalidCheckpoint(left), Self::InvalidCheckpoint(right)) => left == right,
            (
                Self::ArchiveSizeMismatch {
                    index_size: left_index,
                    archive_size: left_archive,
                },
                Self::ArchiveSizeMismatch {
                    index_size: right_index,
                    archive_size: right_archive,
                },
            ) => left_index == right_index && left_archive == right_archive,
            (Self::Truncated, Self::Truncated) => true,
            (Self::WindowCodec(left), Self::WindowCodec(right)) => left == right,
            (Self::AllocationFailed { what: left }, Self::AllocationFailed { what: right }) => {
                left == right
            }
            (Self::UnsupportedFlags { flags: left }, Self::UnsupportedFlags { flags: right }) => {
                left == right
            }
            (Self::MissingMetadata(left), Self::MissingMetadata(right)) => left == right,
            (
                Self::IncompatibleFormat {
                    operation: left_operation,
                    kind: left_kind,
                },
                Self::IncompatibleFormat {
                    operation: right_operation,
                    kind: right_kind,
                },
            ) => left_operation == right_operation && left_kind == right_kind,
            (Self::Io { source: left }, Self::Io { source: right }) => {
                left.kind() == right.kind() && left.to_string() == right.to_string()
            }
            _ => false,
        }
    }
}

impl Eq for IndexError {}

pub(crate) fn read_exact_bytes(
    reader: &mut impl Read,
    buffer: &mut [u8],
) -> Result<(), IndexError> {
    reader.read_exact(buffer).map_err(IndexError::io)
}

pub(crate) fn read_u8(reader: &mut impl Read) -> Result<u8, IndexError> {
    let mut byte = [0u8; 1];
    read_exact_bytes(reader, &mut byte)?;
    Ok(byte[0])
}

macro_rules! integer_io {
    ($read:ident, $write:ident, $type:ty, $from:ident, $to:ident) => {
        #[allow(dead_code)]
        pub(crate) fn $read(reader: &mut impl Read) -> Result<$type, IndexError> {
            let mut bytes = [0u8; size_of::<$type>()];
            read_exact_bytes(reader, &mut bytes)?;
            Ok(<$type>::$from(bytes))
        }

        #[allow(dead_code)]
        pub(crate) fn $write(writer: &mut impl Write, value: $type) -> Result<(), IndexError> {
            writer.write_all(&value.$to()).map_err(IndexError::io)
        }
    };
}

integer_io!(read_u32_le, write_u32_le, u32, from_le_bytes, to_le_bytes);
integer_io!(read_u64_le, write_u64_le, u64, from_le_bytes, to_le_bytes);
integer_io!(read_u32_be, write_u32_be, u32, from_be_bytes, to_be_bytes);
integer_io!(read_u64_be, write_u64_be, u64, from_be_bytes, to_be_bytes);

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint(compressed_bits: u64, uncompressed: u64) -> Checkpoint {
        Checkpoint {
            compressed_offset_in_bits: compressed_bits,
            uncompressed_offset_in_bytes: uncompressed,
            kind: CheckpointKind::DeflateBlock,
            line_offset: None,
        }
    }

    #[test]
    fn validate_accepts_ordered_checkpoints_with_windows() {
        let mut index = DeflateIndex::new();
        index.set_compressed_size(Some(4096));
        index.set_uncompressed_size(Some(1 << 20));
        index
            .push(checkpoint(0, 0), StoredWindow::empty())
            .expect("origin");
        index
            .push(
                checkpoint(8 * 1000, 65536),
                StoredWindow::from_raw(vec![7u8; WINDOW_SIZE]).expect("window"),
            )
            .expect("checkpoint");
        assert_eq!(index.validate(), Ok(()));
        assert_eq!(index.checkpoint_count(), 2);
        assert_eq!(index.windows().len(), 1);
    }

    #[test]
    fn validate_allows_equal_but_rejects_decreasing_uncompressed_offsets() {
        let mut index = DeflateIndex::new();
        index.set_compressed_size(Some(4096));
        index.set_uncompressed_size(Some(1 << 20));
        index
            .push(checkpoint(0, 100), StoredWindow::empty())
            .expect("first");
        index
            .push(checkpoint(8, 100), StoredWindow::empty())
            .expect("equal");
        assert_eq!(index.validate(), Ok(()));
        index
            .push(
                checkpoint(8, 100),
                StoredWindow::from_raw(vec![0u8; WINDOW_SIZE]).expect("window"),
            )
            .expect("duplicate accepted until validate");
        assert!(matches!(
            index.validate(),
            Err(IndexError::InvalidCheckpoint(_))
        ));

        let mut decreasing = DeflateIndex::new();
        decreasing
            .push(checkpoint(0, 100), StoredWindow::empty())
            .expect("first");
        decreasing
            .push(checkpoint(8, 99), StoredWindow::empty())
            .expect("decreasing accepted until validate");
        assert!(matches!(
            decreasing.validate(),
            Err(IndexError::InvalidCheckpoint(_))
        ));
    }

    #[test]
    fn validate_rejects_wrong_window_length() {
        assert_eq!(
            StoredWindow::from_raw(vec![1u8; 10]),
            Err(IndexError::InvalidWindowSize(10))
        );
    }

    #[test]
    fn checkpoint_at_or_before_picks_the_last_not_after_target() {
        let mut index = DeflateIndex::new();
        index.set_compressed_size(Some(4096));
        index.set_uncompressed_size(Some(1 << 20));
        index
            .push(checkpoint(0, 0), StoredWindow::empty())
            .expect("origin");
        index
            .push(
                checkpoint(80, 1000),
                StoredWindow::from_raw(vec![1u8; WINDOW_SIZE]).expect("window"),
            )
            .expect("checkpoint");
        index
            .push(
                checkpoint(160, 2000),
                StoredWindow::from_raw(vec![2u8; WINDOW_SIZE]).expect("window"),
            )
            .expect("checkpoint");

        assert_eq!(
            index
                .checkpoint_at_or_before(1500)
                .map(|point| point.uncompressed_offset_in_bytes),
            Some(1000)
        );
        assert_eq!(
            index
                .checkpoint_at_or_before(2000)
                .map(|point| point.uncompressed_offset_in_bytes),
            Some(2000)
        );
        assert_eq!(
            index
                .checkpoint_at_or_before(0)
                .map(|point| point.uncompressed_offset_in_bytes),
            Some(0)
        );
    }

    #[test]
    fn checkpoint_at_or_before_returns_nothing_for_an_empty_index() {
        assert!(DeflateIndex::new().checkpoint_at_or_before(0).is_none());
    }
}
