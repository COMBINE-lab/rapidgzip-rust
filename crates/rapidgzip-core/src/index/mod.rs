//! In-memory gzip random-access index and indexed_gzip (GZIDX) persistence.
//!
//! This module provides the data model used by rapidgzip-style seeking:
//! checkpoints into the compressed bit stream, optional 32 KiB predecessor
//! windows, and import/export of the [indexed_gzip](https://github.com/pauldmccarthy/indexed_gzip)
//! `GZIDX` file format. Decode paths optionally collect an index via
//! [`IndexBuilder`]; seeking into decoded output is provided by
//! [`crate::IndexedReader`].
//!
//! Also supports the [gztool](https://github.com/circulosmeos/gztool) index
//! format (`gzipindx` / `gzipindX` with line counters) and the htslib BGZF
//! `.gzi` (BGZI) block index. Use [`read_gzip_index`] for auto-detect import.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read, Write};

use libz_rs_sys as z;

mod bgzi;
mod gztool;
pub use bgzi::{read_bgzi_index, write_bgzi_index};
pub use gztool::{GZTOOL_MAGIC_V0, GZTOOL_MAGIC_V1, read_gztool_index, write_gztool_index};

/// DEFLATE history size required by the indexed_gzip format and by gzip itself.
pub const INDEXED_GZIP_WINDOW_SIZE: u32 = 32 * 1024;

/// Magic bytes identifying an indexed_gzip (`GZIDX`) index file.
pub const INDEXED_GZIP_MAGIC: &[u8; 5] = b"GZIDX";

/// Highest indexed_gzip format version this crate can read.
const INDEXED_GZIP_MAX_VERSION: u8 = 1;

/// A random-access seek point into a gzip stream.
///
/// Offsets pair a compressed bit position with the corresponding uncompressed
/// byte position that can be reached once the associated predecessor window
/// (if any) has been installed as the inflate dictionary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Checkpoint {
    /// Absolute compressed bit offset of this seek point.
    pub compressed_offset_in_bits: u64,
    /// Absolute uncompressed byte offset reached at this seek point.
    pub uncompressed_offset_in_bytes: u64,
    /// Number of Unix newline bytes (`\n`) in the uncompressed prefix
    /// `[0, uncompressed_offset_in_bytes)`.
    ///
    /// Meaningful only when [`GzipIndex::has_line_offsets`] is `true`. Matches
    /// gztool/rapidgzip line-index semantics (count of `\n` only; not CRLF
    /// pairs).
    pub line_offset: u64,
}

/// How a stored predecessor window is held in memory.
///
/// When [`DecoderBuilder::compress_index_windows`](crate::DecoderBuilder::compress_index_windows)
/// is enabled (the default), keep_index may store full windows as
/// [`WindowCompression::Zlib`] to reduce RSS. Import paths and
/// [`StoredWindow::from_raw`] always use [`WindowCompression::None`].
/// Export (GZIDX raw windows, gztool on-disk zlib) always decompresses first
/// via [`StoredWindow::decompressed`], then applies the format-specific encoding.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum WindowCompression {
    /// Window bytes are stored uncompressed.
    #[default]
    None,
    /// Window bytes are stored zlib-compressed (RFC 1950 wrapper).
    Zlib,
}

/// Predecessor history for a checkpoint (at most 32 KiB of uncompressed data).
///
/// An empty window means no history is required (for example at a stream or
/// member boundary, or for independent BGZF blocks).
///
/// # Memory
///
/// With in-memory zlib compression enabled, non-empty windows often shrink
/// well below 32 KiB when the history is compressible. [`Self::decompressed`]
/// still allocates on each call; [`crate::IndexedReader`] caches expanded
/// zlib windows in a small LRU (keyed by compressed bit offset) so repeated
/// seeks avoid re-inflate. Empty windows and incompressible payloads stay raw.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoredWindow {
    /// Raw uncompressed bytes, or a zlib-compressed payload when
    /// [`Self::compression`] is [`WindowCompression::Zlib`].
    data: Vec<u8>,
    /// In-memory encoding of [`Self::data`].
    compression: WindowCompression,
    /// Uncompressed history length in bytes.
    ///
    /// Equal to `data.len()` when compression is [`WindowCompression::None`].
    /// For zlib payloads this is the pre-compress size (used by [`Self::len`]
    /// without decompressing).
    uncompressed_len: u32,
}

impl StoredWindow {
    /// Creates a window from raw uncompressed history bytes.
    ///
    /// The bytes are stored as given (no compression). Indexed_gzip export
    /// truncates to the last 32 KiB when longer and pads short windows with
    /// leading zeros to exactly 32 KiB.
    #[must_use]
    pub fn from_raw(bytes: impl Into<Vec<u8>>) -> Self {
        let data = bytes.into();
        let uncompressed_len = u32::try_from(data.len()).unwrap_or(u32::MAX);
        Self {
            data,
            compression: WindowCompression::None,
            uncompressed_len,
        }
    }

    /// Creates a window, optionally zlib-compressing when that shrinks storage.
    ///
    /// When `compress` is true and the zlib-wrapped payload is strictly smaller
    /// than the raw bytes, the window is stored as [`WindowCompression::Zlib`].
    /// Empty windows and compression failures keep [`WindowCompression::None`].
    #[must_use]
    pub fn from_raw_maybe_compress(bytes: impl Into<Vec<u8>>, compress: bool) -> Self {
        let data = bytes.into();
        // Only compress windows that fit the DEFLATE history size so on-demand
        // inflate stays within the fixed 32 KiB output bound.
        if !compress
            || data.is_empty()
            || data.len() > INDEXED_GZIP_WINDOW_SIZE as usize
        {
            return Self::from_raw(data);
        }
        match zlib_compress_window(&data) {
            Ok(compressed) if compressed.len() < data.len() => {
                let uncompressed_len = data.len() as u32;
                Self {
                    data: compressed,
                    compression: WindowCompression::Zlib,
                    uncompressed_len,
                }
            }
            _ => Self::from_raw(data),
        }
    }

    /// Creates an empty window (no predecessor history required).
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            data: Vec::new(),
            compression: WindowCompression::None,
            uncompressed_len: 0,
        }
    }

    /// Returns `true` when no predecessor history is stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.uncompressed_len == 0
    }

    /// Returns the uncompressed window bytes.
    ///
    /// For [`WindowCompression::None`] this borrows the stored payload. For
    /// [`WindowCompression::Zlib`] the payload is inflated on each call.
    /// Callers that repeatedly expand the same windows (notably
    /// [`crate::IndexedReader`]) should cache the result themselves.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::InvalidCheckpoint`] when a zlib-stored payload
    /// fails to decompress (corrupt in-memory index).
    pub fn decompressed(&self) -> Result<Cow<'_, [u8]>, IndexError> {
        match self.compression {
            WindowCompression::None => Ok(Cow::Borrowed(self.data.as_slice())),
            WindowCompression::Zlib => {
                let raw = zlib_decompress_window(&self.data)?;
                if raw.len() as u32 != self.uncompressed_len {
                    // Length mismatch is a hard invariant break for our compressor.
                    return Err(IndexError::InvalidCheckpoint(
                        "zlib window uncompressed length mismatch",
                    ));
                }
                Ok(Cow::Owned(raw))
            }
        }
    }

    /// Returns how the window is stored in memory.
    #[must_use]
    pub const fn compression(&self) -> WindowCompression {
        self.compression
    }

    /// Number of uncompressed window bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.uncompressed_len as usize
    }

    /// Produces the exactly-`window_size` bytes written by indexed_gzip.
    ///
    /// Empty windows yield `Ok(None)` (no payload). Non-empty windows are
    /// padded with leading zeros or truncated to the trailing `window_size`
    /// bytes. Always uses decompressed history so zlib-stored windows export
    /// correctly.
    fn payload_for_export(&self, window_size: usize) -> Result<Option<Vec<u8>>, IndexError> {
        let raw = self.decompressed()?;
        if raw.is_empty() {
            return Ok(None);
        }
        if raw.len() == window_size {
            return Ok(Some(raw.into_owned()));
        }
        if raw.len() > window_size {
            let start = raw.len() - window_size;
            return Ok(Some(raw[start..].to_vec()));
        }
        let mut padded = vec![0u8; window_size];
        let start = window_size - raw.len();
        padded[start..].copy_from_slice(&raw);
        Ok(Some(padded))
    }
}

/// zlib-wrapper compress (RFC 1950), matching gztool `compress_chunk`.
///
/// Used for in-memory keep_index windows and gztool on-disk window payloads.
pub(crate) fn zlib_compress_window(data: &[u8]) -> Result<Vec<u8>, IndexError> {
    let bound = z::compressBound_z(data.len());
    let mut dest = vec![0u8; bound];
    let mut dest_len = dest.len();
    // SAFETY:
    // - `dest` is a live exclusive allocation of length `dest_len`.
    // - `data` is a live immutable slice for the call duration.
    // - `compress_z` only writes up to `*dest_len` and reports the used length.
    let status = unsafe {
        z::compress_z(
            dest.as_mut_ptr(),
            &mut dest_len,
            data.as_ptr(),
            data.len(),
        )
    };
    if status != z::Z_OK {
        return Err(IndexError::InvalidCheckpoint(
            "failed to zlib-compress index window",
        ));
    }
    dest.truncate(dest_len);
    Ok(dest)
}

/// zlib-wrapper decompress of a window payload into at most 32 KiB.
///
/// Empty payloads yield an empty buffer. Non-empty payloads must inflate under
/// the zlib wrapper.
pub(crate) fn zlib_decompress_window(compressed: &[u8]) -> Result<Vec<u8>, IndexError> {
    if compressed.is_empty() {
        return Ok(Vec::new());
    }
    let max_out = INDEXED_GZIP_WINDOW_SIZE as usize;
    let mut dest = vec![0u8; max_out];
    let mut dest_len = dest.len();
    // SAFETY:
    // - `dest` is a live exclusive allocation of length `dest_len` (32 KiB).
    // - `compressed` is a live immutable slice for the call duration.
    // - `uncompress_z` only writes up to `*dest_len` and reports the used length.
    let status = unsafe {
        z::uncompress_z(
            dest.as_mut_ptr(),
            &mut dest_len,
            compressed.as_ptr(),
            compressed.len(),
        )
    };
    if status != z::Z_OK {
        return Err(IndexError::InvalidCheckpoint(
            "failed to zlib-decompress index window",
        ));
    }
    dest.truncate(dest_len);
    Ok(dest)
}

/// Map from compressed bit offset to predecessor window.
///
/// Keys are absolute compressed bit offsets matching
/// [`Checkpoint::compressed_offset_in_bits`]. A simple owned map is sufficient
/// until the parallel decoder owns a shared index.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WindowMap {
    windows: BTreeMap<u64, StoredWindow>,
}

impl WindowMap {
    /// Creates an empty map.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            windows: BTreeMap::new(),
        }
    }

    /// Inserts or replaces the window for `compressed_offset_in_bits`.
    pub fn insert(&mut self, compressed_offset_in_bits: u64, window: StoredWindow) {
        self.windows.insert(compressed_offset_in_bits, window);
    }

    /// Returns the window stored for `compressed_offset_in_bits`, if any.
    #[must_use]
    pub fn get(&self, compressed_offset_in_bits: u64) -> Option<&StoredWindow> {
        self.windows.get(&compressed_offset_in_bits)
    }

    /// Number of stored windows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.windows.len()
    }

    /// Returns `true` when no windows are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.windows.is_empty()
    }

    /// Iterates over `(compressed_offset_in_bits, window)` in offset order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &StoredWindow)> {
        self.windows.iter().map(|(&offset, window)| (offset, window))
    }
}

/// In-memory gzip random-access index.
///
/// Checkpoints must be sorted by both compressed and uncompressed offsets.
/// Windows are keyed by [`Checkpoint::compressed_offset_in_bits`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GzipIndex {
    /// Total compressed size of the archive in bytes.
    pub compressed_size_in_bytes: u64,
    /// Total uncompressed size of the archive in bytes.
    pub uncompressed_size_in_bytes: u64,
    /// Suggested spacing between checkpoints in uncompressed bytes.
    ///
    /// This is guidance only and may be `0`.
    pub checkpoint_spacing: u32,
    /// Window size in bytes. Must be [`INDEXED_GZIP_WINDOW_SIZE`] (32768) for
    /// indexed_gzip import/export.
    pub window_size_in_bytes: u32,
    /// Seek points, sorted by compressed and uncompressed offsets.
    pub checkpoints: Vec<Checkpoint>,
    /// Predecessor windows keyed by compressed bit offset.
    pub windows: WindowMap,
    /// Whether checkpoints carry meaningful [`Checkpoint::line_offset`] values.
    ///
    /// Indexed_gzip (`GZIDX`) import always sets this to `false` because the
    /// on-disk format does not store line offsets. Values are populated only
    /// when decoding with [`crate::DecoderBuilder::gather_line_offsets`].
    pub has_line_offsets: bool,
}

impl GzipIndex {
    /// Creates an empty index with the default 32 KiB window size.
    #[must_use]
    pub fn new() -> Self {
        Self {
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            ..Self::default()
        }
    }

    /// Number of checkpoints.
    #[must_use]
    pub fn checkpoint_count(&self) -> usize {
        self.checkpoints.len()
    }

    /// Returns `true` when there are no checkpoints.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.checkpoints.is_empty()
    }

    /// Total newline count for the archive when line offsets were gathered.
    ///
    /// Returns the [`Checkpoint::line_offset`] of the last checkpoint (newlines
    /// in `[0, uncompressed_size)`), or `None` when [`Self::has_line_offsets`]
    /// is false or the index is empty.
    #[must_use]
    pub fn total_line_count(&self) -> Option<u64> {
        if !self.has_line_offsets {
            return None;
        }
        self.checkpoints.last().map(|checkpoint| checkpoint.line_offset)
    }

    /// Largest checkpoint with `uncompressed_offset_in_bytes <= target`.
    ///
    /// Returns `None` when the index is empty or every checkpoint lies after
    /// `uncompressed_offset`.
    #[must_use]
    pub fn checkpoint_at_or_before(&self, uncompressed_offset: u64) -> Option<&Checkpoint> {
        let index = self
            .checkpoints
            .partition_point(|checkpoint| checkpoint.uncompressed_offset_in_bytes <= uncompressed_offset);
        index.checked_sub(1).map(|i| &self.checkpoints[i])
    }

    /// Largest checkpoint with `line_offset <= line_offset_target`.
    ///
    /// `line_offset_target` is a cumulative newline count (newlines strictly
    /// before some uncompressed offset), not a 1-based line number. Requires
    /// [`Self::has_line_offsets`]. Returns `None` when line offsets are absent
    /// or the index is empty.
    #[must_use]
    pub fn checkpoint_at_or_before_line(&self, line_offset_target: u64) -> Option<&Checkpoint> {
        if !self.has_line_offsets || self.checkpoints.is_empty() {
            return None;
        }
        let index = self
            .checkpoints
            .partition_point(|checkpoint| checkpoint.line_offset <= line_offset_target);
        index.checked_sub(1).map(|i| &self.checkpoints[i])
    }

    /// Window stored for a checkpoint's compressed bit offset.
    ///
    /// Missing entries should be treated as empty windows (no predecessor
    /// history).
    #[must_use]
    pub fn window_for(&self, compressed_offset_in_bits: u64) -> Option<&StoredWindow> {
        self.windows.get(compressed_offset_in_bits)
    }

    /// Checks basic internal consistency of the index.
    ///
    /// Verifies that checkpoints are non-decreasing in both offset domains and
    /// that offsets do not exceed the recorded archive sizes when those sizes
    /// are finite and known (not `u64::MAX`).
    pub fn validate(&self) -> Result<(), IndexError> {
        let mut prev_compressed = 0u64;
        let mut prev_uncompressed = 0u64;
        let mut first = true;
        for checkpoint in &self.checkpoints {
            if !first {
                if checkpoint.compressed_offset_in_bits < prev_compressed {
                    return Err(IndexError::InvalidCheckpoint(
                        "checkpoints are not sorted by compressed offset",
                    ));
                }
                if checkpoint.uncompressed_offset_in_bytes < prev_uncompressed {
                    return Err(IndexError::InvalidCheckpoint(
                        "checkpoints are not sorted by uncompressed offset",
                    ));
                }
            }
            first = false;
            prev_compressed = checkpoint.compressed_offset_in_bits;
            prev_uncompressed = checkpoint.uncompressed_offset_in_bytes;

            if self.compressed_size_in_bytes != u64::MAX {
                let max_bits = self
                    .compressed_size_in_bytes
                    .checked_mul(8)
                    .ok_or(IndexError::InvalidCheckpoint(
                        "compressed size overflows bit count",
                    ))?;
                if checkpoint.compressed_offset_in_bits > max_bits {
                    return Err(IndexError::InvalidCheckpoint(
                        "checkpoint compressed offset exceeds archive size",
                    ));
                }
            }
            if self.uncompressed_size_in_bytes != u64::MAX
                && checkpoint.uncompressed_offset_in_bytes > self.uncompressed_size_in_bytes
            {
                return Err(IndexError::InvalidCheckpoint(
                    "checkpoint uncompressed offset exceeds archive size",
                ));
            }
        }
        Ok(())
    }

    /// Serializes this index in indexed_gzip (`GZIDX`) format.
    pub fn export_indexed_gzip(&self, writer: &mut impl Write) -> Result<(), IndexError> {
        write_indexed_gzip_index(self, writer)
    }

    /// Deserializes an indexed_gzip (`GZIDX`) index.
    ///
    /// When `archive_size` is `Some`, it must equal the compressed size stored
    /// in the index header.
    pub fn import_indexed_gzip(
        reader: &mut impl Read,
        archive_size: Option<u64>,
    ) -> Result<Self, IndexError> {
        read_indexed_gzip_index(reader, archive_size)
    }

    /// Serializes this index in gztool format.
    ///
    /// When `with_lines` is true, writes version 1 (`gzipindX`) with per-point
    /// line counters from [`Checkpoint::line_offset`]. When false, writes
    /// version 0 (`gzipindx`) and omits line fields.
    ///
    /// Windows are zlib-compressed on disk (empty windows use size 0).
    pub fn export_gztool(
        &self,
        writer: &mut impl Write,
        with_lines: bool,
    ) -> Result<(), IndexError> {
        write_gztool_index(self, writer, with_lines)
    }

    /// Deserializes a gztool index (`gzipindx` / `gzipindX`).
    ///
    /// When `archive_size` is `Some`, it is recorded as
    /// [`Self::compressed_size_in_bytes`] (gztool indexes do not store the
    /// compressed archive size).
    pub fn import_gztool(
        reader: &mut impl Read,
        archive_size: Option<u64>,
    ) -> Result<Self, IndexError> {
        read_gztool_index(reader, archive_size)
    }

    /// Serializes this index in htslib BGZF `.gzi` (BGZI) format.
    ///
    /// Writes pairs for every checkpoint after the first (the first block is
    /// implicit at offset 0). Compressed offsets must be byte-aligned; windows
    /// are not stored. Prefer this format for independent BGZF members.
    pub fn export_bgzi(&self, writer: &mut impl Write) -> Result<(), IndexError> {
        write_bgzi_index(self, writer)
    }

    /// Deserializes an htslib BGZF `.gzi` (BGZI) index.
    ///
    /// When `archive_size` is `Some`, it is recorded as
    /// [`Self::compressed_size_in_bytes`]. Checkpoints use empty predecessor
    /// windows (BGZF block boundaries).
    pub fn import_bgzi(
        reader: &mut impl Read,
        archive_size: Option<u64>,
    ) -> Result<Self, IndexError> {
        read_bgzi_index(reader, archive_size)
    }
}

/// Errors produced while parsing, validating, or writing a gzip index.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum IndexError {
    /// The file does not begin with the expected magic bytes.
    BadMagic {
        /// Bytes actually observed (up to the magic length).
        found: Vec<u8>,
    },
    /// The format version is newer than this crate supports.
    UnsupportedVersion(u8),
    /// The index declares a window size other than 32768.
    InvalidWindowSize(u32),
    /// Optional archive size does not match the size stored in the index.
    ArchiveSizeMismatch {
        /// Size from the index header.
        index_size: u64,
        /// Size supplied by the caller.
        archive_size: u64,
    },
    /// A checkpoint field is denormal or inconsistent.
    InvalidCheckpoint(&'static str),
    /// The index stream ended before a complete value could be read.
    Truncated,
    /// An I/O failure occurred while reading or writing the index.
    Io {
        /// Shared original I/O error.
        source: std::sync::Arc<io::Error>,
    },
}

impl IndexError {
    fn io(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            Self::Truncated
        } else {
            Self::Io {
                source: std::sync::Arc::new(error),
            }
        }
    }
}

impl Display for IndexError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic { found } => {
                write!(formatter, "invalid index magic bytes: {found:?}")
            }
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported indexed_gzip format version {version}"
                )
            }
            Self::InvalidWindowSize(size) => {
                write!(
                    formatter,
                    "invalid index window size {size}; expected {INDEXED_GZIP_WINDOW_SIZE}"
                )
            }
            Self::ArchiveSizeMismatch {
                index_size,
                archive_size,
            } => write!(
                formatter,
                "archive size {archive_size} does not match index compressed size {index_size}"
            ),
            Self::InvalidCheckpoint(reason) => {
                write!(formatter, "invalid index checkpoint: {reason}")
            }
            Self::Truncated => formatter.write_str("truncated gzip index"),
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
            (Self::BadMagic { found: a }, Self::BadMagic { found: b }) => a == b,
            (Self::UnsupportedVersion(a), Self::UnsupportedVersion(b)) => a == b,
            (Self::InvalidWindowSize(a), Self::InvalidWindowSize(b)) => a == b,
            (
                Self::ArchiveSizeMismatch {
                    index_size: ia,
                    archive_size: aa,
                },
                Self::ArchiveSizeMismatch {
                    index_size: ib,
                    archive_size: ab,
                },
            ) => ia == ib && aa == ab,
            (Self::InvalidCheckpoint(a), Self::InvalidCheckpoint(b)) => a == b,
            (Self::Truncated, Self::Truncated) => true,
            (Self::Io { source: a }, Self::Io { source: b }) => {
                a.kind() == b.kind() && a.to_string() == b.to_string()
            }
            _ => false,
        }
    }
}

impl Eq for IndexError {}

/// Encodes a compressed bit offset into the indexed_gzip byte/bits pair.
///
/// Matches zran / indexed_gzip:
/// - `bits_field = compressed_offset_in_bits % 8`
/// - if `bits_field == 0`: store `byte_offset = bits / 8`, field `0`
/// - else: store `byte_offset = bits / 8 + 1`, field `8 - bits_field`
#[must_use]
pub fn encode_bit_offset(compressed_offset_in_bits: u64) -> (u64, u8) {
    let bits = (compressed_offset_in_bits % 8) as u8;
    if bits == 0 {
        (compressed_offset_in_bits / 8, 0)
    } else {
        (compressed_offset_in_bits / 8 + 1, 8 - bits)
    }
}

/// Decodes an indexed_gzip byte/bits pair back to a compressed bit offset.
///
/// When `bits_field > 0`, the bit offset is `byte_offset * 8 - bits_field`.
pub fn decode_bit_offset(byte_offset: u64, bits_field: u8) -> Result<u64, IndexError> {
    if bits_field >= 8 {
        return Err(IndexError::InvalidCheckpoint(
            "denormal compressed offset: bit field >= 8",
        ));
    }
    let bit_offset = byte_offset.checked_mul(8).ok_or(IndexError::InvalidCheckpoint(
        "compressed byte offset overflows bit count",
    ))?;
    if bits_field == 0 {
        return Ok(bit_offset);
    }
    if bit_offset == 0 {
        return Err(IndexError::InvalidCheckpoint(
            "denormal bits for checkpoint: effectively negative offset",
        ));
    }
    Ok(bit_offset - u64::from(bits_field))
}

/// Writes `index` in indexed_gzip (`GZIDX`) version-1 format.
///
/// Window size is always written as [`INDEXED_GZIP_WINDOW_SIZE`]. Non-empty
/// windows are exported as exactly that many bytes (leading-zero padded or
/// trailing-truncated as needed). Empty or missing windows set `data_flag = 0`
/// and emit no payload.
pub fn write_indexed_gzip_index(index: &GzipIndex, writer: &mut impl Write) -> Result<(), IndexError> {
    let window_size = INDEXED_GZIP_WINDOW_SIZE as usize;

    writer
        .write_all(INDEXED_GZIP_MAGIC)
        .map_err(IndexError::io)?;
    writer.write_all(&[0x01, 0x00]).map_err(IndexError::io)?;

    let checkpoint_spacing = effective_checkpoint_spacing(index, INDEXED_GZIP_WINDOW_SIZE);

    write_u64_le(writer, index.compressed_size_in_bytes)?;
    write_u64_le(writer, index.uncompressed_size_in_bytes)?;
    write_u32_le(writer, checkpoint_spacing)?;
    write_u32_le(writer, INDEXED_GZIP_WINDOW_SIZE)?;
    write_u32_le(
        writer,
        u32::try_from(index.checkpoints.len()).map_err(|_| {
            IndexError::InvalidCheckpoint("checkpoint count does not fit in u32")
        })?,
    )?;

    for checkpoint in &index.checkpoints {
        let (byte_offset, bits_field) = encode_bit_offset(checkpoint.compressed_offset_in_bits);
        write_u64_le(writer, byte_offset)?;
        write_u64_le(writer, checkpoint.uncompressed_offset_in_bytes)?;
        writer.write_all(&[bits_field]).map_err(IndexError::io)?;

        let data_flag = match index.windows.get(checkpoint.compressed_offset_in_bits) {
            Some(window) if !window.is_empty() => 1u8,
            _ => 0u8,
        };
        writer.write_all(&[data_flag]).map_err(IndexError::io)?;
    }

    for checkpoint in &index.checkpoints {
        let Some(window) = index.windows.get(checkpoint.compressed_offset_in_bits) else {
            continue;
        };
        let Some(payload) = window.payload_for_export(window_size)? else {
            continue;
        };
        writer.write_all(&payload).map_err(IndexError::io)?;
    }

    Ok(())
}

/// Reads an indexed_gzip (`GZIDX`) index from `reader`.
///
/// Supports format versions `0` and `1`. Version `0` has no per-checkpoint
/// data flag: every checkpoint after the first carries a window. Version `1`
/// is the required modern format.
///
/// When `archive_size` is `Some`, it must equal the compressed size stored in
/// the header.
pub fn read_indexed_gzip_index(
    reader: &mut impl Read,
    archive_size: Option<u64>,
) -> Result<GzipIndex, IndexError> {
    let mut magic = [0u8; 5];
    read_exact(reader, &mut magic)?;
    if &magic != INDEXED_GZIP_MAGIC {
        return Err(IndexError::BadMagic {
            found: magic.to_vec(),
        });
    }

    let version = read_u8(reader)?;
    if version > INDEXED_GZIP_MAX_VERSION {
        return Err(IndexError::UnsupportedVersion(version));
    }
    let _flags = read_u8(reader)?;

    let compressed_size_in_bytes = read_u64_le(reader)?;
    let uncompressed_size_in_bytes = read_u64_le(reader)?;
    let checkpoint_spacing = read_u32_le(reader)?;
    let window_size_in_bytes = read_u32_le(reader)?;

    if window_size_in_bytes != INDEXED_GZIP_WINDOW_SIZE {
        return Err(IndexError::InvalidWindowSize(window_size_in_bytes));
    }

    if let Some(archive_size) = archive_size
        && archive_size != compressed_size_in_bytes
    {
        return Err(IndexError::ArchiveSizeMismatch {
            index_size: compressed_size_in_bytes,
            archive_size,
        });
    }

    let checkpoint_count = read_u32_le(reader)? as usize;
    let mut checkpoints = Vec::with_capacity(checkpoint_count);
    let mut window_sizes = Vec::with_capacity(checkpoint_count);

    for i in 0..checkpoint_count {
        let byte_offset = read_u64_le(reader)?;
        let uncompressed_offset_in_bytes = read_u64_le(reader)?;
        let bits_field = read_u8(reader)?;
        let compressed_offset_in_bits = decode_bit_offset(byte_offset, bits_field)?;

        // Original indexed_gzip compares the stored byte offset (before bit
        // adjustment) against the compressed size in bytes.
        if byte_offset > compressed_size_in_bytes {
            return Err(IndexError::InvalidCheckpoint(
                "checkpoint compressed offset is after the file end",
            ));
        }
        if uncompressed_offset_in_bytes > uncompressed_size_in_bytes {
            return Err(IndexError::InvalidCheckpoint(
                "checkpoint uncompressed offset is after the file end",
            ));
        }

        let window_size = if version == 0 {
            if i != 0 {
                window_size_in_bytes as usize
            } else {
                0
            }
        } else {
            let data_flag = read_u8(reader)?;
            if data_flag != 0 {
                window_size_in_bytes as usize
            } else {
                0
            }
        };

        checkpoints.push(Checkpoint {
            compressed_offset_in_bits,
            uncompressed_offset_in_bytes,
            line_offset: 0,
        });
        window_sizes.push(window_size);
    }

    let mut windows = WindowMap::new();
    for (checkpoint, window_size) in checkpoints.iter().zip(window_sizes.iter().copied()) {
        if window_size == 0 {
            // Record an empty window so callers can distinguish "no history
            // needed" from "window not present", matching rapidgzip.
            windows.insert(checkpoint.compressed_offset_in_bits, StoredWindow::empty());
            continue;
        }
        let mut data = vec![0u8; window_size];
        read_exact(reader, &mut data)?;
        windows.insert(
            checkpoint.compressed_offset_in_bits,
            StoredWindow::from_raw(data),
        );
    }

    let index = GzipIndex {
        compressed_size_in_bytes,
        uncompressed_size_in_bytes,
        checkpoint_spacing,
        window_size_in_bytes,
        checkpoints,
        windows,
        has_line_offsets: false,
    };
    index.validate()?;
    Ok(index)
}

/// Counts Unix newline bytes (`\n`) in `bytes`.
#[inline]
pub(crate) fn count_newlines(bytes: &[u8]) -> u64 {
    bytes.iter().filter(|&&b| b == b'\n').count() as u64
}

/// Reads a gzip index, auto-detecting indexed_gzip (`GZIDX`), gztool, or BGZI.
///
/// Detection order:
/// 1. `GZIDX` magic → indexed_gzip
/// 2. big-endian zero `u64` plus `gzipindx` / `gzipindX` → gztool
/// 3. otherwise buffer the remainder and try htslib `.gzi` (BGZI) when the
///    total length is exactly `8 + 16*n` with monotonic offsets
///
/// Any other content yields [`IndexError::BadMagic`].
pub fn read_gzip_index(
    reader: &mut impl Read,
    archive_size: Option<u64>,
) -> Result<GzipIndex, IndexError> {
    let mut prefix = [0u8; 16];
    let mut filled = 0usize;
    while filled < prefix.len() {
        match reader.read(&mut prefix[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(IndexError::io(error)),
        }
    }
    if filled < 5 {
        return Err(IndexError::Truncated);
    }

    if filled >= 5 && &prefix[..5] == INDEXED_GZIP_MAGIC.as_slice() {
        let mut chained = io::Cursor::new(&prefix[..filled]).chain(reader);
        return read_indexed_gzip_index(&mut chained, archive_size);
    }

    if gztool::is_gztool_prefix(&prefix[..filled]) {
        let mut chained = io::Cursor::new(&prefix[..filled]).chain(reader);
        return read_gztool_index(&mut chained, archive_size);
    }

    // BGZI has no magic: require an exact-length buffer (8 + 16*n) and a
    // successful structural parse so random files are unlikely to match.
    let mut buf = prefix[..filled].to_vec();
    reader.read_to_end(&mut buf).map_err(IndexError::io)?;
    match bgzi::try_read_bgzi_buffer(&buf, archive_size) {
        Ok(index) => Ok(index),
        Err(IndexError::BadMagic { .. }) | Err(IndexError::InvalidCheckpoint(_)) => {
            Err(IndexError::BadMagic {
                found: buf[..buf.len().min(16)].to_vec(),
            })
        }
        Err(other) => Err(other),
    }
}

/// Accumulates a [`GzipIndex`] and/or line counts while a decode path emits
/// verified output.
///
/// When constructed with `enabled == false` and `gather_lines == false`, all
/// methods are no-ops and [`Self::finish`] returns `None`, so the hot path pays
/// no window or checkpoint allocation cost.
///
/// Line offsets on checkpoints are assigned in [`Self::finish`] from samples
/// recorded at each [`Self::push_output`] boundary so parallel paths that call
/// [`Self::checkpoint_at`] before emitting matching output still get correct
/// values once all output has been pushed.
pub(crate) struct IndexBuilder {
    enabled: bool,
    gather_lines: bool,
    /// When true, non-empty windows may be stored zlib-compressed if smaller.
    compress_windows: bool,
    /// Soft target in uncompressed bytes between intermediate checkpoints.
    spacing: u64,
    index: GzipIndex,
    /// Last up to [`INDEXED_GZIP_WINDOW_SIZE`] decoded bytes (oldest → newest).
    rolling: Vec<u8>,
    /// Uncompressed bytes observed via [`Self::push_output`].
    uncompressed_cursor: u64,
    /// Uncompressed offset of the most recently inserted checkpoint.
    last_checkpoint_uncompressed: u64,
    /// Newlines in `[0, uncompressed_cursor)` after the last `push_output`.
    line_cursor: u64,
    /// Samples of `(uncompressed_offset, newlines in [0, offset))` at push
    /// boundaries, used to stamp checkpoint line offsets on finish.
    line_samples: BTreeMap<u64, u64>,
}

impl IndexBuilder {
    /// Creates a builder.
    ///
    /// - `enabled`: collect checkpoints and windows ([`crate::DecoderBuilder::keep_index`]).
    /// - `gather_lines`: count `\n` bytes and, when `enabled`, stamp
    ///   [`Checkpoint::line_offset`] on finish.
    /// - `compress_windows`: zlib-compress stored windows when smaller
    ///   ([`crate::DecoderBuilder::compress_index_windows`]).
    pub(crate) fn new(
        enabled: bool,
        gather_lines: bool,
        spacing: usize,
        compress_windows: bool,
    ) -> Self {
        let spacing = spacing.max(1) as u64;
        let mut index = GzipIndex::new();
        index.checkpoint_spacing = u32::try_from(spacing).unwrap_or(u32::MAX);
        let mut line_samples = BTreeMap::new();
        if gather_lines {
            line_samples.insert(0, 0);
        }
        Self {
            enabled,
            gather_lines,
            compress_windows: enabled && compress_windows,
            spacing,
            index,
            rolling: if enabled {
                Vec::with_capacity(INDEXED_GZIP_WINDOW_SIZE as usize)
            } else {
                Vec::new()
            },
            uncompressed_cursor: 0,
            last_checkpoint_uncompressed: 0,
            line_cursor: 0,
            line_samples,
        }
    }

    /// Returns whether checkpoint/window collection is active.
    #[inline]
    pub(crate) const fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns whether either index collection or line counting is active.
    #[inline]
    pub(crate) const fn tracks_output(&self) -> bool {
        self.enabled || self.gather_lines
    }

    /// Line count for [`crate::DecodeReport`] when gathering was enabled.
    #[inline]
    pub(crate) fn report_line_count(&self) -> Option<u64> {
        if self.gather_lines {
            Some(self.line_cursor)
        } else {
            None
        }
    }

    /// Updates the rolling 32 KiB window, uncompressed cursor, and line count.
    pub(crate) fn push_output(&mut self, bytes: &[u8]) {
        if !self.tracks_output() || bytes.is_empty() {
            return;
        }
        if self.gather_lines {
            self.line_cursor = self.line_cursor.saturating_add(count_newlines(bytes));
        }
        self.uncompressed_cursor = self
            .uncompressed_cursor
            .saturating_add(bytes.len() as u64);
        if self.gather_lines {
            self.line_samples
                .insert(self.uncompressed_cursor, self.line_cursor);
        }
        if !self.enabled {
            return;
        }
        let window_size = INDEXED_GZIP_WINDOW_SIZE as usize;
        if bytes.len() >= window_size {
            self.rolling.clear();
            self.rolling
                .extend_from_slice(&bytes[bytes.len() - window_size..]);
        } else {
            let keep = window_size.saturating_sub(bytes.len());
            if self.rolling.len() > keep {
                self.rolling.drain(..self.rolling.len() - keep);
            }
            self.rolling.extend_from_slice(bytes);
        }
    }

    /// Records a checkpoint at the current uncompressed cursor when enough
    /// uncompressed bytes have been seen since the last checkpoint.
    ///
    /// Uses a copy of the rolling predecessor window (may be shorter than
    /// 32 KiB near the start of a member).
    pub(crate) fn maybe_checkpoint(&mut self, compressed_offset_in_bits: u64) {
        if !self.enabled {
            return;
        }
        if !self.index.checkpoints.is_empty()
            && self
                .uncompressed_cursor
                .saturating_sub(self.last_checkpoint_uncompressed)
                < self.spacing
        {
            return;
        }
        self.insert(
            compressed_offset_in_bits,
            self.uncompressed_cursor,
            WindowSource::Rolling,
        );
    }

    /// Always records a checkpoint at the current uncompressed cursor.
    ///
    /// Used for member boundaries and other forced points. When
    /// `empty_window` is true the stored window is empty (history resets);
    /// otherwise the rolling window is copied.
    pub(crate) fn force_checkpoint(
        &mut self,
        compressed_offset_in_bits: u64,
        empty_window: bool,
    ) {
        if !self.enabled {
            return;
        }
        let window = if empty_window {
            WindowSource::Empty
        } else {
            WindowSource::Rolling
        };
        self.insert(
            compressed_offset_in_bits,
            self.uncompressed_cursor,
            window,
        );
    }

    /// Records a checkpoint at explicit offsets, subject to spacing unless
    /// `force` is set.
    ///
    /// Used by parallel paths that know both the compressed bit position and
    /// the uncompressed offset independently of sequential `push_output`
    /// ordering (for example estimated-grid chunk starts). Line offsets for
    /// these points are filled in on [`Self::finish`] from push samples.
    pub(crate) fn checkpoint_at(
        &mut self,
        compressed_offset_in_bits: u64,
        uncompressed_offset_in_bytes: u64,
        window_bytes: Option<&[u8]>,
        force: bool,
    ) {
        if !self.enabled {
            return;
        }
        if !force
            && !self.index.checkpoints.is_empty()
            && uncompressed_offset_in_bytes
                .saturating_sub(self.last_checkpoint_uncompressed)
                < self.spacing
        {
            return;
        }
        let window = match window_bytes {
            None | Some([]) => WindowSource::Empty,
            Some(bytes) => WindowSource::Bytes(bytes),
        };
        self.insert(
            compressed_offset_in_bits,
            uncompressed_offset_in_bytes,
            window,
        );
    }

    fn line_offset_for(&self, uncompressed_offset_in_bytes: u64) -> u64 {
        if !self.gather_lines {
            return 0;
        }
        if let Some(&lines) = self.line_samples.get(&uncompressed_offset_in_bytes) {
            return lines;
        }
        // Exact sample missing (for example checkpoint recorded ahead of
        // emit): use the greatest sample at or before this offset.
        let index = self
            .line_samples
            .range(..=uncompressed_offset_in_bytes)
            .next_back();
        match index {
            Some((&sample_offset, &lines)) if sample_offset == uncompressed_offset_in_bytes => {
                lines
            }
            Some((&sample_offset, &lines)) => {
                // Between samples the true count is unknown without the
                // bytes; prefer the lower sample so seek starts early enough.
                debug_assert!(sample_offset < uncompressed_offset_in_bytes);
                lines
            }
            None => 0,
        }
    }

    fn insert(
        &mut self,
        compressed_offset_in_bits: u64,
        uncompressed_offset_in_bytes: u64,
        window: WindowSource<'_>,
    ) {
        if let Some(last) = self.index.checkpoints.last() {
            // Skip exact duplicates.
            if last.compressed_offset_in_bits == compressed_offset_in_bits
                && last.uncompressed_offset_in_bytes == uncompressed_offset_in_bytes
            {
                return;
            }
            // Keep both offset domains sorted; drop out-of-order points.
            if compressed_offset_in_bits < last.compressed_offset_in_bits
                || uncompressed_offset_in_bytes < last.uncompressed_offset_in_bytes
            {
                return;
            }
        }

        let compress = self.compress_windows;
        let stored = match window {
            WindowSource::Empty => StoredWindow::empty(),
            WindowSource::Rolling => {
                StoredWindow::from_raw_maybe_compress(self.rolling.clone(), compress)
            }
            WindowSource::Bytes(bytes) => {
                let window_size = INDEXED_GZIP_WINDOW_SIZE as usize;
                let truncated = if bytes.len() > window_size {
                    bytes[bytes.len() - window_size..].to_vec()
                } else {
                    bytes.to_vec()
                };
                StoredWindow::from_raw_maybe_compress(truncated, compress)
            }
        };

        // Provisional line_offset; finish re-stamps from samples when gathering.
        let line_offset = self.line_offset_for(uncompressed_offset_in_bytes);
        self.index.checkpoints.push(Checkpoint {
            compressed_offset_in_bits,
            uncompressed_offset_in_bytes,
            line_offset,
        });
        self.index
            .windows
            .insert(compressed_offset_in_bits, stored);
        self.last_checkpoint_uncompressed = uncompressed_offset_in_bytes;
    }

    /// Finalizes archive sizes and appends an EOF checkpoint when needed.
    ///
    /// Returns `None` when checkpoint collection was disabled. When line
    /// gathering was enabled, every checkpoint receives a line offset from
    /// push samples and [`GzipIndex::has_line_offsets`] is set.
    pub(crate) fn finish(
        mut self,
        compressed_size: u64,
        uncompressed_size: u64,
    ) -> Option<GzipIndex> {
        if !self.enabled {
            return None;
        }
        // Ensure a sample exists at the final uncompressed size (empty trailing
        // segment or paths that never pushed the last zero-length span).
        if self.gather_lines {
            self.line_samples
                .entry(uncompressed_size)
                .or_insert(self.line_cursor);
        }
        // Final seek point at EOF: empty window is acceptable.
        let eof_bits = compressed_size.saturating_mul(8);
        self.insert(eof_bits, uncompressed_size, WindowSource::Empty);
        if self.gather_lines {
            let line_offsets: Vec<u64> = self
                .index
                .checkpoints
                .iter()
                .map(|checkpoint| self.line_offset_for(checkpoint.uncompressed_offset_in_bytes))
                .collect();
            for (checkpoint, line_offset) in self
                .index
                .checkpoints
                .iter_mut()
                .zip(line_offsets)
            {
                checkpoint.line_offset = line_offset;
            }
            self.index.has_line_offsets = true;
        }
        self.index.compressed_size_in_bytes = compressed_size;
        self.index.uncompressed_size_in_bytes = uncompressed_size;
        Some(self.index)
    }
}

enum WindowSource<'a> {
    Empty,
    Rolling,
    Bytes(&'a [u8]),
}

/// Chooses a checkpoint spacing value for export, matching rapidgzip's heuristic
/// when the stored guidance is below the window size.
fn effective_checkpoint_spacing(index: &GzipIndex, window_size: u32) -> u32 {
    let spacing = index.checkpoint_spacing;
    if index.checkpoints.is_empty() || spacing >= window_size {
        return spacing;
    }
    // With fewer than two checkpoints there is no adjacent delta; rapidgzip's
    // accumulate over an empty adjacent-difference range yields 0, so the
    // result collapses to the window size.
    let mut min_spacing = u64::MAX;
    for pair in index.checkpoints.windows(2) {
        let delta = pair[1]
            .uncompressed_offset_in_bytes
            .saturating_sub(pair[0].uncompressed_offset_in_bytes);
        min_spacing = min_spacing.min(delta);
    }
    let min_spacing = if min_spacing == u64::MAX {
        0
    } else {
        u32::try_from(min_spacing).unwrap_or(u32::MAX)
    };
    window_size.max(min_spacing)
}

fn write_u32_le(writer: &mut impl Write, value: u32) -> Result<(), IndexError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(IndexError::io)
}

pub(super) fn write_u64_le(writer: &mut impl Write, value: u64) -> Result<(), IndexError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(IndexError::io)
}

pub(super) fn read_exact(reader: &mut impl Read, buf: &mut [u8]) -> Result<(), IndexError> {
    reader.read_exact(buf).map_err(IndexError::io)
}

fn read_u8(reader: &mut impl Read) -> Result<u8, IndexError> {
    let mut buf = [0u8; 1];
    read_exact(reader, &mut buf)?;
    Ok(buf[0])
}

fn read_u32_le(reader: &mut impl Read) -> Result<u32, IndexError> {
    let mut buf = [0u8; 4];
    read_exact(reader, &mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub(super) fn read_u64_le(reader: &mut impl Read) -> Result<u64, IndexError> {
    let mut buf = [0u8; 8];
    read_exact(reader, &mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn round_trip(index: &GzipIndex) -> GzipIndex {
        let mut bytes = Vec::new();
        index.export_indexed_gzip(&mut bytes).expect("export");
        GzipIndex::import_indexed_gzip(&mut Cursor::new(&bytes), None).expect("import")
    }

    #[test]
    fn encode_decode_bit_offsets() {
        for bits in [0u64, 1, 3, 7, 8, 9, 15, 16, 100, 1_000_003] {
            let (byte_offset, bits_field) = encode_bit_offset(bits);
            assert!(bits_field < 8);
            let decoded = decode_bit_offset(byte_offset, bits_field).expect("decode");
            assert_eq!(decoded, bits, "round-trip bit offset {bits}");
        }

        // Documented packing for non-aligned offsets.
        assert_eq!(encode_bit_offset(0), (0, 0));
        assert_eq!(encode_bit_offset(8), (1, 0));
        // bit offset 3 → store byte 1, bits field 5 (8-3)
        assert_eq!(encode_bit_offset(3), (1, 5));
        // bit offset 7 → store byte 1, bits field 1
        assert_eq!(encode_bit_offset(7), (1, 1));
        // bit offset 11 = 8+3 → store byte 2, bits field 5
        assert_eq!(encode_bit_offset(11), (2, 5));
    }

    #[test]
    fn round_trip_empty_index() {
        let index = GzipIndex {
            compressed_size_in_bytes: 0,
            uncompressed_size_in_bytes: 0,
            checkpoint_spacing: 0,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints: Vec::new(),
            windows: WindowMap::new(),
            has_line_offsets: false,
        };
        let restored = round_trip(&index);
        assert_eq!(restored.compressed_size_in_bytes, 0);
        assert_eq!(restored.uncompressed_size_in_bytes, 0);
        assert!(restored.checkpoints.is_empty());
        assert!(restored.windows.is_empty());
        assert_eq!(restored.window_size_in_bytes, INDEXED_GZIP_WINDOW_SIZE);
    }

    #[test]
    fn round_trip_single_checkpoint_empty_window() {
        let mut windows = WindowMap::new();
        windows.insert(0, StoredWindow::empty());
        let index = GzipIndex {
            compressed_size_in_bytes: 100,
            uncompressed_size_in_bytes: 1000,
            checkpoint_spacing: 64 * 1024,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints: vec![Checkpoint {
                compressed_offset_in_bits: 0,
                uncompressed_offset_in_bytes: 0,
                line_offset: 0,
            }],
            windows,
            has_line_offsets: false,
        };
        let restored = round_trip(&index);
        assert_eq!(restored.checkpoints.len(), 1);
        assert_eq!(restored.checkpoints[0].compressed_offset_in_bits, 0);
        assert_eq!(restored.checkpoints[0].uncompressed_offset_in_bytes, 0);
        let window = restored.windows.get(0).expect("empty window recorded");
        assert!(window.is_empty());
    }

    #[test]
    fn round_trip_multiple_bit_offsets() {
        // bits fields 0, 3, and 7 in the packing sense:
        // offset 16 → bits 0; offset 3 → non-zero packing; offset 7 → packing with field 1
        // Use compressed bit offsets whose % 8 are 0, 3, and 7.
        let offsets = [0u64, 8 * 10 + 3, 8 * 20 + 7]; // %8 = 0, 3, 7
        let mut windows = WindowMap::new();
        let mut checkpoints = Vec::new();
        for (i, &bits) in offsets.iter().enumerate() {
            checkpoints.push(Checkpoint {
                compressed_offset_in_bits: bits,
                uncompressed_offset_in_bytes: (i as u64) * 50_000,
                line_offset: 0,
            });
            // First has empty window; later ones non-empty short windows.
            if i == 0 {
                windows.insert(bits, StoredWindow::empty());
            } else {
                windows.insert(bits, StoredWindow::from_raw(vec![0xAB; 16]));
            }
        }
        let index = GzipIndex {
            compressed_size_in_bytes: 1024,
            uncompressed_size_in_bytes: 200_000,
            checkpoint_spacing: 50_000,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints,
            windows,
            has_line_offsets: false,
        };

        // Packing invariants before export.
        assert_eq!(encode_bit_offset(offsets[0]).1, 0);
        assert_eq!(offsets[1] % 8, 3);
        assert_eq!(offsets[2] % 8, 7);

        let mut bytes = Vec::new();
        write_indexed_gzip_index(&index, &mut bytes).expect("export");
        let restored = read_indexed_gzip_index(&mut Cursor::new(&bytes), Some(1024)).expect("import");

        assert_eq!(restored.checkpoints.len(), 3);
        for (i, &bits) in offsets.iter().enumerate() {
            assert_eq!(
                restored.checkpoints[i].compressed_offset_in_bits, bits,
                "checkpoint {i} bit offset"
            );
            assert_eq!(
                restored.checkpoints[i].uncompressed_offset_in_bytes,
                (i as u64) * 50_000
            );
        }
        assert!(restored.windows.get(offsets[0]).unwrap().is_empty());
        // Short windows come back padded to 32 KiB with leading zeros.
        for &bits in &offsets[1..] {
            let window = restored.windows.get(bits).expect("window");
            assert_eq!(window.len(), INDEXED_GZIP_WINDOW_SIZE as usize);
            let raw = window.decompressed().unwrap();
            assert!(raw[..INDEXED_GZIP_WINDOW_SIZE as usize - 16]
                .iter()
                .all(|&b| b == 0));
            assert_eq!(
                &raw[INDEXED_GZIP_WINDOW_SIZE as usize - 16..],
                &[0xAB; 16]
            );
        }
    }

    #[test]
    fn round_trip_full_window_payload() {
        let mut payload = vec![0u8; INDEXED_GZIP_WINDOW_SIZE as usize];
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }
        let mut windows = WindowMap::new();
        windows.insert(0, StoredWindow::empty());
        windows.insert(1000, StoredWindow::from_raw(payload.clone()));
        let index = GzipIndex {
            compressed_size_in_bytes: 5000,
            uncompressed_size_in_bytes: 100_000,
            checkpoint_spacing: 64 * 1024,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints: vec![
                Checkpoint {
                    compressed_offset_in_bits: 0,
                    uncompressed_offset_in_bytes: 0,
                    line_offset: 0,
                },
                Checkpoint {
                    compressed_offset_in_bits: 1000,
                    uncompressed_offset_in_bytes: 50_000,
                    line_offset: 0,
                },
            ],
            windows,
            has_line_offsets: false,
        };
        let restored = round_trip(&index);
        assert_eq!(
            restored
                .windows
                .get(1000)
                .unwrap()
                .decompressed()
                .unwrap()
                .as_ref(),
            payload.as_slice()
        );
    }

    #[test]
    fn round_trip_truncated_long_window_to_last_32kib() {
        let mut long = vec![0x11u8; 40 * 1024];
        for (i, byte) in long.iter_mut().enumerate() {
            *byte = (i % 200) as u8;
        }
        let expected = long[long.len() - INDEXED_GZIP_WINDOW_SIZE as usize..].to_vec();
        let mut windows = WindowMap::new();
        windows.insert(8, StoredWindow::from_raw(long));
        let index = GzipIndex {
            compressed_size_in_bytes: 4096,
            uncompressed_size_in_bytes: 80_000,
            checkpoint_spacing: 0,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints: vec![Checkpoint {
                compressed_offset_in_bits: 8,
                uncompressed_offset_in_bytes: 10_000,
                line_offset: 0,
            }],
            windows,
            has_line_offsets: false,
        };
        let restored = round_trip(&index);
        assert_eq!(
            restored
                .windows
                .get(8)
                .unwrap()
                .decompressed()
                .unwrap()
                .as_ref(),
            expected.as_slice()
        );
    }

    #[test]
    fn reject_bad_magic() {
        let err = read_indexed_gzip_index(&mut Cursor::new(b"XXXXX"), None).unwrap_err();
        assert!(matches!(err, IndexError::BadMagic { .. }));
    }

    #[test]
    fn reject_wrong_window_size() {
        // Craft a minimal header with window size 16384.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(INDEXED_GZIP_MAGIC);
        bytes.push(0x01); // version
        bytes.push(0x00); // flags
        bytes.extend_from_slice(&100u64.to_le_bytes()); // compressed
        bytes.extend_from_slice(&1000u64.to_le_bytes()); // uncompressed
        bytes.extend_from_slice(&0u32.to_le_bytes()); // spacing
        bytes.extend_from_slice(&16384u32.to_le_bytes()); // wrong window
        bytes.extend_from_slice(&0u32.to_le_bytes()); // checkpoint count
        let err = read_indexed_gzip_index(&mut Cursor::new(&bytes), None).unwrap_err();
        assert_eq!(err, IndexError::InvalidWindowSize(16384));
    }

    #[test]
    fn reject_unsupported_version() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(INDEXED_GZIP_MAGIC);
        bytes.push(0x02);
        bytes.push(0x00);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&INDEXED_GZIP_WINDOW_SIZE.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        let err = read_indexed_gzip_index(&mut Cursor::new(&bytes), None).unwrap_err();
        assert_eq!(err, IndexError::UnsupportedVersion(2));
    }

    #[test]
    fn archive_size_mismatch() {
        let index = GzipIndex {
            compressed_size_in_bytes: 42,
            uncompressed_size_in_bytes: 0,
            checkpoint_spacing: 0,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints: Vec::new(),
            windows: WindowMap::new(),
            has_line_offsets: false,
        };
        let mut bytes = Vec::new();
        write_indexed_gzip_index(&index, &mut bytes).unwrap();
        let err =
            read_indexed_gzip_index(&mut Cursor::new(&bytes), Some(99)).unwrap_err();
        assert_eq!(
            err,
            IndexError::ArchiveSizeMismatch {
                index_size: 42,
                archive_size: 99,
            }
        );
    }

    #[test]
    fn bit_packing_written_bytes_match_known_layout() {
        // Single checkpoint at bit offset 11 (byte 1 with remainder 3).
        // Encoded as byte_offset=2, bits_field=5.
        let mut windows = WindowMap::new();
        windows.insert(11, StoredWindow::empty());
        let index = GzipIndex {
            compressed_size_in_bytes: 64,
            uncompressed_size_in_bytes: 128,
            checkpoint_spacing: 0,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints: vec![Checkpoint {
                compressed_offset_in_bits: 11,
                uncompressed_offset_in_bytes: 5,
                line_offset: 0,
            }],
            windows,
            has_line_offsets: false,
        };
        let mut bytes = Vec::new();
        write_indexed_gzip_index(&index, &mut bytes).unwrap();

        // Header is 31 bytes + 4 checkpoint count = 35, then checkpoint:
        // u64 byte_offset, u64 uncompressed, u8 bits, u8 data_flag
        let header_len = 5 + 1 + 1 + 8 + 8 + 4 + 4 + 4;
        assert_eq!(&bytes[0..5], INDEXED_GZIP_MAGIC);
        assert_eq!(bytes[5], 0x01);
        assert_eq!(bytes[6], 0x00);

        let byte_offset = u64::from_le_bytes(bytes[header_len..header_len + 8].try_into().unwrap());
        let uncompressed =
            u64::from_le_bytes(bytes[header_len + 8..header_len + 16].try_into().unwrap());
        let bits_field = bytes[header_len + 16];
        let data_flag = bytes[header_len + 17];
        assert_eq!(byte_offset, 2);
        assert_eq!(uncompressed, 5);
        assert_eq!(bits_field, 5);
        assert_eq!(data_flag, 0);

        let restored = read_indexed_gzip_index(&mut Cursor::new(&bytes), None).unwrap();
        assert_eq!(restored.checkpoints[0].compressed_offset_in_bits, 11);
        assert_eq!(restored.checkpoints[0].uncompressed_offset_in_bytes, 5);
    }

    #[test]
    fn stored_window_helpers() {
        assert!(StoredWindow::empty().is_empty());
        let w = StoredWindow::from_raw(vec![1, 2, 3]);
        assert_eq!(w.decompressed().unwrap().as_ref(), &[1, 2, 3]);
        assert_eq!(w.compression(), WindowCompression::None);
        assert_eq!(w.len(), 3);

        // Compressible full window should store as zlib when requested.
        let full = vec![0xABu8; INDEXED_GZIP_WINDOW_SIZE as usize];
        let zlib = StoredWindow::from_raw_maybe_compress(full.clone(), true);
        assert_eq!(zlib.compression(), WindowCompression::Zlib);
        assert_eq!(zlib.len(), full.len());
        assert_eq!(zlib.decompressed().unwrap().as_ref(), full.as_slice());
        // Compress off keeps None.
        let raw = StoredWindow::from_raw_maybe_compress(full.clone(), false);
        assert_eq!(raw.compression(), WindowCompression::None);
        assert_eq!(raw.decompressed().unwrap().as_ref(), full.as_slice());
        // Empty stays empty / None.
        let empty = StoredWindow::from_raw_maybe_compress(Vec::new(), true);
        assert!(empty.is_empty());
        assert_eq!(empty.compression(), WindowCompression::None);
    }

    #[test]
    fn validate_detects_unsorted_checkpoints() {
        let index = GzipIndex {
            compressed_size_in_bytes: 100,
            uncompressed_size_in_bytes: 1000,
            checkpoint_spacing: 0,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints: vec![
                Checkpoint {
                    compressed_offset_in_bits: 80,
                    uncompressed_offset_in_bytes: 10,
                    line_offset: 0,
                },
                Checkpoint {
                    compressed_offset_in_bits: 40,
                    uncompressed_offset_in_bytes: 20,
                    line_offset: 0,
                },
            ],
            windows: WindowMap::new(),
            has_line_offsets: false,
        };
        assert!(matches!(
            index.validate(),
            Err(IndexError::InvalidCheckpoint(_))
        ));
    }

    #[test]
    fn version0_all_but_first_have_windows() {
        // Manually craft a version-0 index: no data_flag bytes; first checkpoint
        // has no window, subsequent ones do.
        let window_size = INDEXED_GZIP_WINDOW_SIZE as usize;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(INDEXED_GZIP_MAGIC);
        bytes.push(0x00); // version 0
        bytes.push(0x00);
        bytes.extend_from_slice(&100u64.to_le_bytes());
        bytes.extend_from_slice(&10_000u64.to_le_bytes());
        bytes.extend_from_slice(&1024u32.to_le_bytes());
        bytes.extend_from_slice(&INDEXED_GZIP_WINDOW_SIZE.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        // checkpoint 0 at bit 0
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.push(0); // bits
        // checkpoint 1 at bit 16
        bytes.extend_from_slice(&2u64.to_le_bytes()); // byte offset 2
        bytes.extend_from_slice(&5000u64.to_le_bytes());
        bytes.push(0); // bits
        // one window payload for checkpoint 1
        bytes.extend(std::iter::repeat_n(0x5A, window_size));

        let index = read_indexed_gzip_index(&mut Cursor::new(&bytes), None).expect("v0 import");
        assert_eq!(index.checkpoints.len(), 2);
        assert!(index.windows.get(0).unwrap().is_empty());
        assert_eq!(
            index.windows.get(16).unwrap().decompressed().unwrap().as_ref(),
            &vec![0x5A; window_size][..]
        );
    }

    fn sample_index_with_windows() -> GzipIndex {
        let offsets = [0u64, 8 * 10 + 3, 8 * 20 + 7]; // %8 = 0, 3, 7
        let mut windows = WindowMap::new();
        let mut checkpoints = Vec::new();
        for (i, &bits) in offsets.iter().enumerate() {
            checkpoints.push(Checkpoint {
                compressed_offset_in_bits: bits,
                uncompressed_offset_in_bytes: (i as u64) * 50_000,
                line_offset: 0,
            });
            if i == 0 {
                windows.insert(bits, StoredWindow::empty());
            } else {
                windows.insert(bits, StoredWindow::from_raw(vec![0xAB; 16]));
            }
        }
        GzipIndex {
            compressed_size_in_bytes: 1024,
            uncompressed_size_in_bytes: 200_000,
            checkpoint_spacing: 50_000,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints,
            windows,
            has_line_offsets: false,
        }
    }

    #[test]
    fn read_gzip_index_auto_detects_gzidx_and_gztool() {
        let index = sample_index_with_windows();

        let mut gzidx = Vec::new();
        index.export_indexed_gzip(&mut gzidx).unwrap();
        let via_auto = read_gzip_index(&mut Cursor::new(&gzidx), Some(1024)).expect("gzidx auto");
        assert_eq!(via_auto.checkpoints.len(), index.checkpoints.len());
        assert_eq!(via_auto.compressed_size_in_bytes, 1024);

        let mut tool = Vec::new();
        index.export_gztool(&mut tool, false).unwrap();
        let via_auto = read_gzip_index(&mut Cursor::new(&tool), Some(1024)).expect("gztool auto");
        assert_eq!(via_auto.checkpoints.len(), index.checkpoints.len());
        assert!(!via_auto.has_line_offsets);

        let mut tool_lines = Vec::new();
        index.export_gztool(&mut tool_lines, true).unwrap();
        let via_auto =
            read_gzip_index(&mut Cursor::new(&tool_lines), Some(1024)).expect("gztool v1 auto");
        assert!(via_auto.has_line_offsets);
    }

    #[test]
    fn read_gzip_index_auto_detects_bgzi() {
        // Byte-aligned empty-window checkpoints only (BGZI-compatible).
        // Include a synthetic EOF point; export must strip it.
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 500;
        index.uncompressed_size_in_bytes = 90_000;
        for (c_bytes, u) in [(0u64, 0u64), (120u64, 30_000u64), (500u64, 90_000u64)] {
            let bits = c_bytes * 8;
            index.checkpoints.push(Checkpoint {
                compressed_offset_in_bits: bits,
                uncompressed_offset_in_bytes: u,
                line_offset: 0,
            });
            index.windows.insert(bits, StoredWindow::empty());
        }

        let mut bgzi = Vec::new();
        index.export_bgzi(&mut bgzi).unwrap();
        let via_auto = read_gzip_index(&mut Cursor::new(&bgzi), Some(500)).expect("bgzi auto");
        // Origin + one real block start (EOF stripped).
        assert_eq!(via_auto.checkpoints.len(), 2);
        assert_eq!(via_auto.compressed_size_in_bytes, 500);
        // htslib does not store total uncompressed size.
        assert_eq!(via_auto.uncompressed_size_in_bytes, u64::MAX);
        assert_eq!(via_auto.checkpoints[1].uncompressed_offset_in_bytes, 30_000);
        assert!(via_auto.windows.get(0).unwrap().is_empty());
    }

    #[test]
    fn read_gzip_index_prefers_gzidx_and_gztool_over_bgzi_shape() {
        // A real GZIDX must not be misread as BGZI even if length is 8+16*n.
        let index = sample_index_with_windows();
        let mut gzidx = Vec::new();
        index.export_indexed_gzip(&mut gzidx).unwrap();
        let via = read_gzip_index(&mut Cursor::new(&gzidx), Some(1024)).unwrap();
        assert_eq!(via.checkpoints.len(), index.checkpoints.len());
        // Windows survived (BGZI would have emptied them).
        let mid = index.checkpoints[1].compressed_offset_in_bits;
        assert!(!via.windows.get(mid).unwrap().is_empty());
    }

    #[test]
    fn read_gzip_index_rejects_random_bytes() {
        let err = read_gzip_index(&mut Cursor::new(b"not-an-index!!!!"), None).unwrap_err();
        assert!(matches!(err, IndexError::BadMagic { .. }));

        // Leading zeros but wrong magic / wrong BGZI length.
        let mut bad = vec![0u8; 8];
        bad.extend_from_slice(b"notmagic");
        let err = read_gzip_index(&mut Cursor::new(&bad), None).unwrap_err();
        assert!(matches!(err, IndexError::BadMagic { .. }));
    }
}
