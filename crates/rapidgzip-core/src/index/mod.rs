//! Random-access gzip index types and on-disk formats.
//!
//! This module is independent of the decoder: it defines the index data model,
//! validates it, and reads and writes the supported on-disk formats. Using an
//! index for random access lives in [`crate::indexed`].

mod gzi;
mod gzidx;
mod gztool;
mod native;
mod window_codec;

pub use gzidx::{decode_bit_offset, encode_bit_offset};
pub use gztool::WithLines;
pub(crate) use window_codec::{zlib_compress_window, zlib_decompress_window};

use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read, Write};
use std::sync::Arc;

/// DEFLATE history size, in bytes, required at a resume point.
pub const WINDOW_SIZE: usize = 32768;

/// A random-access point into a gzip stream.
///
/// The compressed position is a bit offset because a DEFLATE block boundary is
/// not generally byte aligned. Resuming there requires the predecessor window
/// stored alongside the checkpoint, unless that window is empty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Checkpoint {
    /// Absolute compressed bit offset from the start of the source.
    pub compressed_offset_in_bits: u64,
    /// Absolute decompressed byte offset across all members.
    pub uncompressed_offset_in_bytes: u64,
    /// Number of lines preceding this point, zero when unknown.
    pub line_offset: u64,
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

    /// Stores `bytes` as raw, uncompressed history.
    #[must_use]
    pub fn from_raw(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            payload: bytes.into(),
            compressed: false,
        }
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
    /// An empty input, or history that does not shrink, is stored raw.
    pub fn from_raw_maybe_compress(
        bytes: impl Into<Vec<u8>>,
        compress: bool,
    ) -> Result<Self, IndexError> {
        let bytes = bytes.into();
        if !compress || bytes.is_empty() {
            return Ok(Self::from_raw(bytes));
        }
        let payload = zlib_compress_window(&bytes)?;
        if payload.len() >= bytes.len() {
            return Ok(Self::from_raw(bytes));
        }
        Ok(Self::from_compressed(payload))
    }

    /// Returns the window history, expanding it when it is held compressed.
    pub fn decompressed(&self) -> Result<Cow<'_, [u8]>, IndexError> {
        if self.compressed {
            Ok(Cow::Owned(zlib_decompress_window(&self.payload)?))
        } else {
            Ok(Cow::Borrowed(&self.payload))
        }
    }

    pub(crate) const fn from_compressed(payload: Vec<u8>) -> Self {
        Self {
            payload,
            compressed: true,
        }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Predecessor windows keyed by compressed bit offset.
#[derive(Clone, Debug, Default)]
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

/// An in-memory gzip random-access index.
#[derive(Clone, Debug, Default)]
pub struct GzipIndex {
    pub(crate) checkpoints: Vec<Checkpoint>,
    pub(crate) windows: WindowMap,
    /// Compressed source size in bytes, zero when unknown.
    pub compressed_size_in_bytes: u64,
    /// Total decompressed size in bytes, [`u64::MAX`] when unknown.
    pub uncompressed_size_in_bytes: u64,
    /// Target spacing between checkpoints in decompressed bytes, zero when unknown.
    pub checkpoint_spacing_in_bytes: u64,
    /// Total line count when the source index carried one.
    pub total_line_count: Option<u64>,
}

impl GzipIndex {
    /// Returns an empty index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a checkpoint and its predecessor window.
    ///
    /// Ordering is not checked here; call [`Self::validate`] once the index is
    /// complete.
    pub fn push(&mut self, checkpoint: Checkpoint, window: StoredWindow) {
        if !window.is_empty() {
            self.windows
                .insert(checkpoint.compressed_offset_in_bits, window);
        }
        self.checkpoints.push(checkpoint);
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
        native::read_native(reader)
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
        gzidx::read_gzidx(reader, archive_size)
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
    /// The format does not record the uncompressed size, so the result reports
    /// [`u64::MAX`] for it. `archive_size`, when supplied, is recorded as the
    /// compressed size.
    pub fn read_gzi(reader: &mut impl Read, archive_size: Option<u64>) -> Result<Self, IndexError> {
        gzi::read_gzi(reader, archive_size)
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
        gztool::read_gztool(reader, archive_size)
    }

    /// Checks the index invariants.
    ///
    /// Offsets must increase strictly on both axes, a raw non-empty window must
    /// be exactly [`WINDOW_SIZE`] bytes, and offsets must fall inside the
    /// recorded sizes when those are known.
    pub fn validate(&self) -> Result<(), IndexError> {
        let mut previous: Option<&Checkpoint> = None;
        for checkpoint in &self.checkpoints {
            if let Some(previous) = previous {
                if checkpoint.compressed_offset_in_bits <= previous.compressed_offset_in_bits {
                    return Err(IndexError::InvalidCheckpoint(
                        "compressed offsets are not strictly increasing",
                    ));
                }
                if checkpoint.uncompressed_offset_in_bytes <= previous.uncompressed_offset_in_bytes
                {
                    return Err(IndexError::InvalidCheckpoint(
                        "uncompressed offsets are not strictly increasing",
                    ));
                }
            }

            if self.compressed_size_in_bytes != 0
                && checkpoint.compressed_offset_in_bits
                    > self.compressed_size_in_bytes.saturating_mul(8)
            {
                return Err(IndexError::InvalidCheckpoint(
                    "checkpoint compressed offset is after the source end",
                ));
            }
            if self.uncompressed_size_in_bytes != u64::MAX
                && self.uncompressed_size_in_bytes != 0
                && checkpoint.uncompressed_offset_in_bytes > self.uncompressed_size_in_bytes
            {
                return Err(IndexError::InvalidCheckpoint(
                    "checkpoint uncompressed offset is after the source end",
                ));
            }

            if let Some(window) = self.windows.get(checkpoint.compressed_offset_in_bits) {
                if !window.is_compressed()
                    && !window.is_empty()
                    && window.stored_len() != WINDOW_SIZE
                {
                    return Err(IndexError::InvalidCheckpoint(
                        "non-empty predecessor window is not 32768 bytes",
                    ));
                }
            }

            previous = Some(checkpoint);
        }
        Ok(())
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
    UnsupportedVersion(u8),
    /// The index declared a window size other than [`WINDOW_SIZE`].
    InvalidWindowSize(u32),
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
            line_offset: 0,
        }
    }

    #[test]
    fn validate_accepts_ordered_checkpoints_with_windows() {
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 4096;
        index.uncompressed_size_in_bytes = 1 << 20;
        index.push(checkpoint(0, 0), StoredWindow::empty());
        index.push(
            checkpoint(8 * 1000, 65536),
            StoredWindow::from_raw(vec![7u8; WINDOW_SIZE]),
        );
        assert_eq!(index.validate(), Ok(()));
        assert_eq!(index.checkpoint_count(), 2);
        assert_eq!(index.windows().len(), 1);
    }

    #[test]
    fn validate_rejects_unordered_uncompressed_offsets() {
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 4096;
        index.uncompressed_size_in_bytes = 1 << 20;
        index.push(checkpoint(0, 100), StoredWindow::empty());
        index.push(
            checkpoint(8, 100),
            StoredWindow::from_raw(vec![0u8; WINDOW_SIZE]),
        );
        assert!(matches!(
            index.validate(),
            Err(IndexError::InvalidCheckpoint(_))
        ));
    }

    #[test]
    fn validate_rejects_wrong_window_length() {
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 4096;
        index.uncompressed_size_in_bytes = 1 << 20;
        index.push(checkpoint(0, 0), StoredWindow::empty());
        index.push(checkpoint(64, 10), StoredWindow::from_raw(vec![1u8; 10]));
        assert_eq!(
            index.validate(),
            Err(IndexError::InvalidCheckpoint(
                "non-empty predecessor window is not 32768 bytes"
            ))
        );
    }

    #[test]
    fn checkpoint_at_or_before_picks_the_last_not_after_target() {
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 4096;
        index.uncompressed_size_in_bytes = 1 << 20;
        index.push(checkpoint(0, 0), StoredWindow::empty());
        index.push(
            checkpoint(80, 1000),
            StoredWindow::from_raw(vec![1u8; WINDOW_SIZE]),
        );
        index.push(
            checkpoint(160, 2000),
            StoredWindow::from_raw(vec![2u8; WINDOW_SIZE]),
        );

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
        assert!(GzipIndex::new().checkpoint_at_or_before(0).is_none());
    }
}
