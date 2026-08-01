# Random-Access Index and Seeking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a random-access index as a by-product of decoding, persist it in four on-disk formats, and expose `Read + Seek` random access over compressed input.

**Architecture:** A self-contained `index/` module holds pure index types and format codecs with no knowledge of the decoder. Decode paths feed an `IndexSink` at boundaries they already compute. A separate `indexed/` module implements single-threaded random access on top of a raw-inflate wrapper extracted from `backend.rs`.

**Tech Stack:** Rust 2024, `libz-rs-sys` (already a dependency, used for both inflate and the deflate side needed by zlib-wrapped window payloads), `proptest` (dev-dependency, already present).

Spec: `docs/superpowers/specs/2026-08-01-index-seek-design.md`

## Global Constraints

- No new runtime dependencies. Window compression uses `libz-rs-sys` (`z` alias), which the crate already links.
- The crate has `#![deny(missing_docs)]` and `#![deny(unsafe_op_in_unsafe_fn)]`. Every public item needs a doc comment; every `unsafe` block needs a `// SAFETY:` comment in the style already used in `backend.rs`.
- New `unsafe` is allowed only in `crates/rapidgzip-core/src/inflate.rs`. `index/` and `indexed/` contain none.
- No file created by this plan exceeds 600 lines. Split reader and writer into sibling files if a format module approaches that.
- No em dashes in code comments, docs, or commit messages.
- Every count or length read from an index file is bounds-checked against an explicit maximum before it is used to allocate.
- Verification for every task: `cargo test -p rapidgzip-core`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`.
- Work happens on branch `index-and-seek`, based on the current `streaming-input` commits.

## File Structure

| Path | Responsibility |
| --- | --- |
| `src/index/mod.rs` | `Checkpoint`, `StoredWindow`, `WindowMap`, `GzipIndex`, `IndexError`, `validate`, shared little/big-endian read/write helpers |
| `src/index/window_codec.rs` | zlib-wrapper compress and decompress of window payloads |
| `src/index/native.rs` | native versioned format |
| `src/index/gzidx.rs` | indexed_gzip GZIDX format, zran bit packing |
| `src/index/gzi.rs` | htslib BGZF `.gzi` format |
| `src/index/gztool.rs` | gztool format |
| `src/index/build.rs` | `IndexSink` trait, `IndexBuilder` |
| `src/inflate.rs` | raw-inflate wrapper moved out of `backend.rs`, plus `prime` and `set_dictionary` |
| `src/indexed/mod.rs` | `IndexedReader<R: ReadAt>` implementing `Read + Seek` |
| `src/indexed/window.rs` | bounded LRU of expanded windows |
| `tests/index_formats.rs` | format round-trips, rejection tests, golden files |
| `tests/indexed_seek.rs` | seek equals full decode, across corpora |
| `tests/index_interop.rs` | `#[ignore]` interop against `bgzip`, `indexed_gzip`, `gztool` |

---

### Task 1: Index core types

**Files:**
- Create: `crates/rapidgzip-core/src/index/mod.rs`
- Modify: `crates/rapidgzip-core/src/lib.rs`
- Test: inline `#[cfg(test)]` module in `src/index/mod.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `Checkpoint { compressed_offset_in_bits: u64, uncompressed_offset_in_bytes: u64, line_offset: u64 }`, `StoredWindow`, `WindowMap`, `GzipIndex`, `IndexError`, `GzipIndex::validate(&self) -> Result<(), IndexError>`, `GzipIndex::checkpoint_at_or_before(&self, uncompressed_offset: u64) -> Option<&Checkpoint>`, and crate-internal helpers `read_u64_le`, `write_u64_le`, `read_u32_le`, `write_u32_le`, `read_u64_be`, `write_u64_be`, `read_u32_be`, `write_u32_be`, `read_u8`, `read_exact_bytes`.

- [ ] **Step 1: Write the failing test**

Add to the bottom of the new `src/index/mod.rs`:

```rust
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
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p rapidgzip-core index::`
Expected: compilation failure, `GzipIndex` and friends are undefined.

- [ ] **Step 3: Write the types**

In `src/index/mod.rs`, above the test module:

```rust
//! Random-access gzip index types and on-disk formats.
//!
//! This module is independent of the decoder: it defines the index data model,
//! validates it, and reads and writes the supported on-disk formats. Building
//! an index during a decode lives in [`build`]; using one for random access
//! lives in [`crate::indexed`].

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
    /// Checkpoints ordered by both compressed and uncompressed offset.
    pub(crate) checkpoints: Vec<Checkpoint>,
    /// Predecessor windows keyed by compressed bit offset.
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

    /// Returns the stored windows.
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
        position.checked_sub(1).map(|index| &self.checkpoints[index])
    }

    /// Checks the index invariants.
    ///
    /// Offsets must increase strictly on both axes, a non-empty window must be
    /// exactly [`WINDOW_SIZE`] bytes when held raw, and offsets must fall
    /// inside the recorded sizes when those are known.
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

            if let Some(window) = self.windows.get(checkpoint.compressed_offset_in_bits)
                && !window.is_compressed()
                && !window.is_empty()
                && window.stored_len() != WINDOW_SIZE
            {
                return Err(IndexError::InvalidCheckpoint(
                    "non-empty predecessor window is not 32768 bytes",
                ));
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
            Self::InvalidWindowSize(size) => {
                write!(
                    formatter,
                    "invalid index window size {size}, expected {WINDOW_SIZE}"
                )
            }
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

pub(crate) fn read_exact_bytes(reader: &mut impl Read, buffer: &mut [u8]) -> Result<(), IndexError> {
    reader.read_exact(buffer).map_err(IndexError::io)
}

pub(crate) fn read_u8(reader: &mut impl Read) -> Result<u8, IndexError> {
    let mut byte = [0u8; 1];
    read_exact_bytes(reader, &mut byte)?;
    Ok(byte[0])
}

macro_rules! integer_io {
    ($read:ident, $write:ident, $type:ty, $from:ident, $to:ident) => {
        pub(crate) fn $read(reader: &mut impl Read) -> Result<$type, IndexError> {
            let mut bytes = [0u8; size_of::<$type>()];
            read_exact_bytes(reader, &mut bytes)?;
            Ok(<$type>::$from(bytes))
        }

        pub(crate) fn $write(writer: &mut impl Write, value: $type) -> Result<(), IndexError> {
            writer.write_all(&value.$to()).map_err(IndexError::io)
        }
    };
}

integer_io!(read_u32_le, write_u32_le, u32, from_le_bytes, to_le_bytes);
integer_io!(read_u64_le, write_u64_le, u64, from_le_bytes, to_le_bytes);
integer_io!(read_u32_be, write_u32_be, u32, from_be_bytes, to_be_bytes);
integer_io!(read_u64_be, write_u64_be, u64, from_be_bytes, to_be_bytes);
```

- [ ] **Step 4: Register the module**

In `src/lib.rs`, add `pub mod index;` next to `pub mod parallel;`, and re-export the main types:

```rust
pub use index::{Checkpoint, GzipIndex, IndexError, StoredWindow, WindowMap};
```

- [ ] **Step 5: Run the tests and lints**

Run: `cargo test -p rapidgzip-core index::` then `cargo clippy --all-targets -- -D warnings`
Expected: PASS, no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/rapidgzip-core/src/index/mod.rs crates/rapidgzip-core/src/lib.rs
git commit -m "Add gzip index types and validation"
```

---

### Task 2: Window payload codec

**Files:**
- Create: `crates/rapidgzip-core/src/index/window_codec.rs`
- Modify: `crates/rapidgzip-core/src/index/mod.rs`
- Test: inline `#[cfg(test)]` module in `src/index/window_codec.rs`

**Interfaces:**
- Consumes: `IndexError`, `WINDOW_SIZE`, `StoredWindow` from Task 1.
- Produces: `pub(crate) fn zlib_compress_window(bytes: &[u8]) -> Result<Vec<u8>, IndexError>`, `pub(crate) fn zlib_decompress_window(payload: &[u8]) -> Result<Vec<u8>, IndexError>`, `StoredWindow::from_raw_maybe_compress(bytes: impl Into<Vec<u8>>, compress: bool) -> Result<StoredWindow, IndexError>`, `StoredWindow::decompressed(&self) -> Result<Cow<'_, [u8]>, IndexError>`.

- [ ] **Step 1: Write the failing test**

At the bottom of the new `src/index/window_codec.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::StoredWindow;

    #[test]
    fn round_trips_a_compressible_window() {
        let window = vec![0x5au8; WINDOW_SIZE];
        let compressed = zlib_compress_window(&window).expect("compress");
        assert!(compressed.len() < window.len());
        assert_eq!(zlib_decompress_window(&compressed).expect("decompress"), window);
    }

    #[test]
    fn round_trips_an_incompressible_window() {
        let mut window = vec![0u8; WINDOW_SIZE];
        let mut state = 0x12345678u32;
        for byte in &mut window {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            *byte = (state >> 24) as u8;
        }
        let compressed = zlib_compress_window(&window).expect("compress");
        assert_eq!(zlib_decompress_window(&compressed).expect("decompress"), window);
    }

    #[test]
    fn round_trips_an_empty_payload() {
        assert!(zlib_decompress_window(&[]).expect("decompress").is_empty());
    }

    #[test]
    fn rejects_a_payload_that_expands_beyond_the_window() {
        let oversized = vec![0u8; WINDOW_SIZE + 1];
        let compressed = zlib_compress_window(&oversized).expect("compress");
        assert_eq!(
            zlib_decompress_window(&compressed),
            Err(IndexError::WindowCodec(
                "window payload expands beyond 32768 bytes"
            ))
        );
    }

    #[test]
    fn rejects_corrupt_payloads() {
        assert!(matches!(
            zlib_decompress_window(&[0xff, 0xff, 0xff, 0xff]),
            Err(IndexError::WindowCodec(_))
        ));
    }

    #[test]
    fn stored_window_hides_whether_it_is_compressed() {
        let window = vec![0x11u8; WINDOW_SIZE];
        let stored = StoredWindow::from_raw_maybe_compress(window.clone(), true).expect("store");
        assert!(stored.is_compressed());
        assert_eq!(stored.decompressed().expect("expand").as_ref(), &window[..]);

        let raw = StoredWindow::from_raw_maybe_compress(window.clone(), false).expect("store");
        assert!(!raw.is_compressed());
        assert_eq!(raw.decompressed().expect("expand").as_ref(), &window[..]);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p rapidgzip-core window_codec`
Expected: compilation failure, the module does not exist.

- [ ] **Step 3: Implement the codec**

`src/index/window_codec.rs`:

```rust
//! zlib-wrapper codec for stored predecessor windows.
//!
//! The gztool on-disk format stores windows zlib-compressed, and the native
//! format and in-memory index reuse the same encoding to bound resident
//! memory. Payloads never exceed [`WINDOW_SIZE`] once expanded.

use super::{IndexError, WINDOW_SIZE};
use libz_rs_sys as z;
use std::ffi::c_int;

/// Compression level used for stored windows, matching gztool.
const LEVEL: c_int = 9;

/// Compresses `bytes` under a zlib wrapper.
pub(crate) fn zlib_compress_window(bytes: &[u8]) -> Result<Vec<u8>, IndexError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let mut stream = z::z_stream::default();
    // SAFETY: `stream` is a live, uniquely borrowed `z_stream`, `zlibVersion`
    // returns a static NUL-terminated string, and the reported structure size
    // matches the exact Rust ABI type passed.
    let status = unsafe {
        z::deflateInit_(
            &mut stream,
            LEVEL,
            z::zlibVersion(),
            size_of::<z::z_stream>() as i32,
        )
    };
    if status != z::Z_OK {
        return Err(IndexError::WindowCodec("deflate initialization failed"));
    }
    let guard = DeflateGuard(&mut stream);

    // `deflateBound` is an upper bound for the whole input in one pass.
    // SAFETY: the stream is initialized and uniquely borrowed by `guard`.
    let bound = unsafe { z::deflateBound(guard.0, bytes.len() as u64) } as usize;
    let mut output = vec![0u8; bound.max(64)];

    guard.0.next_in = bytes.as_ptr();
    guard.0.avail_in = bytes.len() as u32;
    guard.0.next_out = output.as_mut_ptr();
    guard.0.avail_out = output.len() as u32;

    // SAFETY: input and output pointers refer to the live slices above and the
    // available counts match their lengths.
    let status = unsafe { z::deflate(guard.0, z::Z_FINISH) };
    if status != z::Z_STREAM_END {
        return Err(IndexError::WindowCodec("deflate did not finish in one pass"));
    }
    let produced = output.len() - guard.0.avail_out as usize;
    output.truncate(produced);
    Ok(output)
}

/// Expands a zlib-wrapped window payload, refusing anything over
/// [`WINDOW_SIZE`] bytes.
pub(crate) fn zlib_decompress_window(payload: &[u8]) -> Result<Vec<u8>, IndexError> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }

    let mut stream = z::z_stream::default();
    // SAFETY: as in `zlib_compress_window`, with `15` selecting the zlib
    // wrapper and a 32 KiB window.
    let status = unsafe {
        z::inflateInit2_(
            &mut stream,
            15,
            z::zlibVersion(),
            size_of::<z::z_stream>() as i32,
        )
    };
    if status != z::Z_OK {
        return Err(IndexError::WindowCodec("inflate initialization failed"));
    }
    let guard = InflateGuard(&mut stream);

    // One extra byte detects payloads that would expand past a full window.
    let mut output = vec![0u8; WINDOW_SIZE + 1];
    guard.0.next_in = payload.as_ptr();
    guard.0.avail_in = payload.len() as u32;
    guard.0.next_out = output.as_mut_ptr();
    guard.0.avail_out = output.len() as u32;

    // SAFETY: input and output pointers refer to the live slices above.
    let status = unsafe { z::inflate(guard.0, z::Z_FINISH) };
    let produced = output.len() - guard.0.avail_out as usize;
    if produced > WINDOW_SIZE {
        return Err(IndexError::WindowCodec(
            "window payload expands beyond 32768 bytes",
        ));
    }
    if status != z::Z_STREAM_END {
        return Err(IndexError::WindowCodec("invalid window payload"));
    }
    output.truncate(produced);
    Ok(output)
}

struct DeflateGuard<'a>(&'a mut z::z_stream);

impl Drop for DeflateGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: the guard owns exactly one successfully initialized stream
        // and ends it exactly once.
        let _ = unsafe { z::deflateEnd(self.0) };
    }
}

struct InflateGuard<'a>(&'a mut z::z_stream);

impl Drop for InflateGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: the guard owns exactly one successfully initialized stream
        // and ends it exactly once.
        let _ = unsafe { z::inflateEnd(self.0) };
    }
}
```

- [ ] **Step 4: Extend `StoredWindow`**

In `src/index/mod.rs`, add `mod window_codec;` plus `pub(crate) use window_codec::{zlib_compress_window, zlib_decompress_window};`, `use std::borrow::Cow;`, and these methods on `StoredWindow`:

```rust
    /// Stores `bytes`, optionally zlib-compressed to reduce resident memory.
    ///
    /// An empty input is always stored raw.
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

    /// Returns the window history, expanding it if it is held compressed.
    pub fn decompressed(&self) -> Result<Cow<'_, [u8]>, IndexError> {
        if self.compressed {
            Ok(Cow::Owned(zlib_decompress_window(&self.payload)?))
        } else {
            Ok(Cow::Borrowed(&self.payload))
        }
    }
```

- [ ] **Step 5: Run the tests and lints**

Run: `cargo test -p rapidgzip-core index` then `cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/rapidgzip-core/src/index/
git commit -m "Add zlib codec for stored index windows"
```

---

### Task 3: Native index format

**Files:**
- Create: `crates/rapidgzip-core/src/index/native.rs`
- Modify: `crates/rapidgzip-core/src/index/mod.rs`
- Test: `crates/rapidgzip-core/tests/index_formats.rs`

**Interfaces:**
- Consumes: Task 1 types and helpers, Task 2 codec.
- Produces: `GzipIndex::write_native(&self, writer: &mut impl Write) -> Result<(), IndexError>` and `GzipIndex::read_native(reader: &mut impl Read) -> Result<GzipIndex, IndexError>`.

On-disk layout, all little-endian: magic `RGZIDX01` (8 bytes), `u16` version (1), `u16` flags (0), `u64` compressed size, `u64` uncompressed size, `u64` checkpoint spacing, `u64` total line count (`u64::MAX` when absent), `u64` checkpoint count. Then per checkpoint: `u64` compressed bit offset, `u64` uncompressed offset, `u64` line offset, `u8` window kind (0 absent, 1 raw, 2 zlib), `u32` payload length, payload bytes.

- [ ] **Step 1: Write the failing test**

Create `crates/rapidgzip-core/tests/index_formats.rs`:

```rust
use rapidgzip_core::index::WINDOW_SIZE;
use rapidgzip_core::{Checkpoint, GzipIndex, IndexError, StoredWindow};

fn sample_index() -> GzipIndex {
    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = 1_000_000;
    index.uncompressed_size_in_bytes = 8_000_000;
    index.checkpoint_spacing_in_bytes = 4 * 1024 * 1024;
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 0,
            uncompressed_offset_in_bytes: 0,
            line_offset: 0,
        },
        StoredWindow::empty(),
    );
    index.push(
        Checkpoint {
            // Deliberately not byte aligned.
            compressed_offset_in_bits: 8 * 4096 + 3,
            uncompressed_offset_in_bytes: 4 * 1024 * 1024,
            line_offset: 1234,
        },
        StoredWindow::from_raw(vec![0xa5u8; WINDOW_SIZE]),
    );
    index
}

fn assert_same_index(left: &GzipIndex, right: &GzipIndex) {
    assert_eq!(left.checkpoints(), right.checkpoints());
    assert_eq!(
        left.compressed_size_in_bytes,
        right.compressed_size_in_bytes
    );
    assert_eq!(
        left.uncompressed_size_in_bytes,
        right.uncompressed_size_in_bytes
    );
    for checkpoint in left.checkpoints() {
        let key = checkpoint.compressed_offset_in_bits;
        let expected = left.windows().get(key).map(|window| {
            window
                .decompressed()
                .expect("expand expected window")
                .into_owned()
        });
        let actual = right.windows().get(key).map(|window| {
            window
                .decompressed()
                .expect("expand actual window")
                .into_owned()
        });
        assert_eq!(expected, actual, "window mismatch at bit offset {key}");
    }
}

#[test]
fn native_round_trips() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    let restored = GzipIndex::read_native(&mut bytes.as_slice()).expect("read");
    assert_same_index(&index, &restored);
}

#[test]
fn native_round_trips_an_empty_index() {
    let index = GzipIndex::new();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    let restored = GzipIndex::read_native(&mut bytes.as_slice()).expect("read");
    assert_eq!(restored.checkpoint_count(), 0);
}

#[test]
fn native_rejects_bad_magic() {
    let bytes = vec![0u8; 64];
    assert!(matches!(
        GzipIndex::read_native(&mut bytes.as_slice()),
        Err(IndexError::BadMagic { .. })
    ));
}

#[test]
fn native_rejects_truncation_at_every_prefix() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    for length in 1..bytes.len() {
        let result = GzipIndex::read_native(&mut &bytes[..length]);
        assert!(
            result.is_err(),
            "prefix of {length} bytes was accepted as a complete index"
        );
    }
}

#[test]
fn native_rejects_a_hostile_checkpoint_count() {
    let index = GzipIndex::new();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    // The checkpoint count is the last u64 of the header.
    let count_at = bytes.len() - 8;
    bytes[count_at..].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(matches!(
        GzipIndex::read_native(&mut bytes.as_slice()),
        Err(IndexError::ExcessiveLength { .. })
    ));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p rapidgzip-core --test index_formats`
Expected: compilation failure, `write_native` is undefined.

- [ ] **Step 3: Implement the format**

`src/index/native.rs`:

```rust
//! Native versioned index format.
//!
//! This is the only format under this crate's control, so it stores everything
//! the in-memory index holds and round-trips exactly, including line offsets
//! and compressed window payloads.

use super::{
    Checkpoint, GzipIndex, IndexError, StoredWindow, WINDOW_SIZE, read_exact_bytes, read_u32_le,
    read_u64_le, read_u8, write_u32_le, write_u64_le,
};
use std::io::{Read, Write};

/// Magic bytes identifying a native index.
pub(crate) const MAGIC: &[u8; 8] = b"RGZIDX01";

/// Highest native version this crate reads.
const MAX_VERSION: u16 = 1;

/// Upper bound on the checkpoint count, guarding against hostile headers.
const MAX_CHECKPOINTS: u64 = 1 << 28;

/// Upper bound on a stored window payload.
const MAX_PAYLOAD: u32 = (WINDOW_SIZE as u32) + 1024;

const WINDOW_ABSENT: u8 = 0;
const WINDOW_RAW: u8 = 1;
const WINDOW_ZLIB: u8 = 2;

pub(crate) fn write_native(index: &GzipIndex, writer: &mut impl Write) -> Result<(), IndexError> {
    writer.write_all(MAGIC).map_err(IndexError::io)?;
    writer
        .write_all(&MAX_VERSION.to_le_bytes())
        .map_err(IndexError::io)?;
    writer.write_all(&0u16.to_le_bytes()).map_err(IndexError::io)?;
    write_u64_le(writer, index.compressed_size_in_bytes)?;
    write_u64_le(writer, index.uncompressed_size_in_bytes)?;
    write_u64_le(writer, index.checkpoint_spacing_in_bytes)?;
    write_u64_le(writer, index.total_line_count.unwrap_or(u64::MAX))?;
    write_u64_le(writer, index.checkpoints.len() as u64)?;

    for checkpoint in &index.checkpoints {
        write_u64_le(writer, checkpoint.compressed_offset_in_bits)?;
        write_u64_le(writer, checkpoint.uncompressed_offset_in_bytes)?;
        write_u64_le(writer, checkpoint.line_offset)?;

        match index.windows.get(checkpoint.compressed_offset_in_bits) {
            None => {
                writer.write_all(&[WINDOW_ABSENT]).map_err(IndexError::io)?;
                write_u32_le(writer, 0)?;
            }
            Some(window) if window.is_empty() => {
                writer.write_all(&[WINDOW_ABSENT]).map_err(IndexError::io)?;
                write_u32_le(writer, 0)?;
            }
            Some(window) => {
                let kind = if window.is_compressed() {
                    WINDOW_ZLIB
                } else {
                    WINDOW_RAW
                };
                let payload = window.payload();
                let length = u32::try_from(payload.len()).map_err(|_| IndexError::ExcessiveLength {
                    what: "window payload length",
                    value: payload.len() as u64,
                })?;
                writer.write_all(&[kind]).map_err(IndexError::io)?;
                write_u32_le(writer, length)?;
                writer.write_all(payload).map_err(IndexError::io)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn read_native(reader: &mut impl Read) -> Result<GzipIndex, IndexError> {
    let mut magic = [0u8; 8];
    read_exact_bytes(reader, &mut magic)?;
    if &magic != MAGIC {
        return Err(IndexError::BadMagic {
            found: magic.to_vec(),
        });
    }

    let mut version = [0u8; 2];
    read_exact_bytes(reader, &mut version)?;
    let version = u16::from_le_bytes(version);
    if version == 0 || version > MAX_VERSION {
        return Err(IndexError::UnsupportedVersion(version.min(255) as u8));
    }
    let mut flags = [0u8; 2];
    read_exact_bytes(reader, &mut flags)?;

    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = read_u64_le(reader)?;
    index.uncompressed_size_in_bytes = read_u64_le(reader)?;
    index.checkpoint_spacing_in_bytes = read_u64_le(reader)?;
    let total_lines = read_u64_le(reader)?;
    index.total_line_count = (total_lines != u64::MAX).then_some(total_lines);

    let count = read_u64_le(reader)?;
    if count > MAX_CHECKPOINTS {
        return Err(IndexError::ExcessiveLength {
            what: "checkpoint count",
            value: count,
        });
    }
    index.checkpoints.reserve(count as usize);

    for _ in 0..count {
        let checkpoint = Checkpoint {
            compressed_offset_in_bits: read_u64_le(reader)?,
            uncompressed_offset_in_bytes: read_u64_le(reader)?,
            line_offset: read_u64_le(reader)?,
        };
        let kind = read_u8(reader)?;
        let length = read_u32_le(reader)?;
        if length > MAX_PAYLOAD {
            return Err(IndexError::ExcessiveLength {
                what: "window payload length",
                value: u64::from(length),
            });
        }
        let window = match kind {
            WINDOW_ABSENT => {
                if length != 0 {
                    return Err(IndexError::InvalidCheckpoint(
                        "absent window declares a payload",
                    ));
                }
                StoredWindow::empty()
            }
            WINDOW_RAW => {
                let mut payload = vec![0u8; length as usize];
                read_exact_bytes(reader, &mut payload)?;
                StoredWindow::from_raw(payload)
            }
            WINDOW_ZLIB => {
                let mut payload = vec![0u8; length as usize];
                read_exact_bytes(reader, &mut payload)?;
                StoredWindow::from_compressed(payload)
            }
            _ => return Err(IndexError::InvalidCheckpoint("unknown window kind")),
        };
        index.push(checkpoint, window);
    }

    index.validate()?;
    Ok(index)
}
```

- [ ] **Step 4: Expose the methods**

In `src/index/mod.rs`, add `mod native;` and:

```rust
impl GzipIndex {
    /// Writes this index in the crate's native versioned format.
    pub fn write_native(&self, writer: &mut impl Write) -> Result<(), IndexError> {
        native::write_native(self, writer)
    }

    /// Reads an index written by [`Self::write_native`].
    pub fn read_native(reader: &mut impl Read) -> Result<Self, IndexError> {
        native::read_native(reader)
    }
}
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p rapidgzip-core --test index_formats`
Expected: PASS, including the truncation sweep.

- [ ] **Step 6: Commit**

```bash
git add crates/rapidgzip-core/src/index/ crates/rapidgzip-core/tests/index_formats.rs
git commit -m "Add native index serialization"
```

---

### Task 4: indexed_gzip GZIDX format

**Files:**
- Create: `crates/rapidgzip-core/src/index/gzidx.rs`
- Modify: `crates/rapidgzip-core/src/index/mod.rs`
- Test: `crates/rapidgzip-core/tests/index_formats.rs`

**Interfaces:**
- Consumes: Task 1 and Task 2.
- Produces: `pub fn encode_bit_offset(compressed_offset_in_bits: u64) -> (u64, u8)`, `pub fn decode_bit_offset(byte_offset: u64, bits_field: u8) -> Result<u64, IndexError>`, `GzipIndex::write_gzidx(&self, writer: &mut impl Write) -> Result<(), IndexError>`, `GzipIndex::read_gzidx(reader: &mut impl Read, archive_size: Option<u64>) -> Result<GzipIndex, IndexError>`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/index_formats.rs`:

```rust
#[test]
fn zran_bit_packing_round_trips() {
    use rapidgzip_core::index::{decode_bit_offset, encode_bit_offset};

    for offset in [0u64, 1, 7, 8, 9, 4095, 4096, 8 * 4096 + 3] {
        let (byte_offset, bits) = encode_bit_offset(offset);
        assert_eq!(decode_bit_offset(byte_offset, bits), Ok(offset));
    }

    // Byte-aligned offsets store a zero bits field.
    assert_eq!(encode_bit_offset(64), (8, 0));
    // Three bits into byte 4096 stores the next byte with five remaining bits.
    assert_eq!(encode_bit_offset(8 * 4096 + 3), (4097, 5));
}

#[test]
fn zran_bit_packing_rejects_denormal_values() {
    use rapidgzip_core::index::decode_bit_offset;

    assert!(decode_bit_offset(0, 3).is_err());
    assert!(decode_bit_offset(10, 8).is_err());
}

#[test]
fn gzidx_round_trips() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");
    let restored = GzipIndex::read_gzidx(&mut bytes.as_slice(), Some(1_000_000)).expect("read");
    assert_eq!(restored.checkpoints().len(), index.checkpoints().len());
    for (expected, actual) in index.checkpoints().iter().zip(restored.checkpoints()) {
        assert_eq!(
            expected.compressed_offset_in_bits,
            actual.compressed_offset_in_bits
        );
        assert_eq!(
            expected.uncompressed_offset_in_bytes,
            actual.uncompressed_offset_in_bytes
        );
    }
    assert_same_index(&index, &restored);
}

#[test]
fn gzidx_rejects_a_mismatched_archive_size() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");
    assert_eq!(
        GzipIndex::read_gzidx(&mut bytes.as_slice(), Some(42)),
        Err(IndexError::ArchiveSizeMismatch {
            index_size: 1_000_000,
            archive_size: 42,
        })
    );
}

#[test]
fn gzidx_rejects_a_foreign_window_size() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");
    // Header layout: magic(5) version(1) flags(1) csize(8) usize(8) spacing(4)
    // then the window size at offset 27.
    bytes[27..31].copy_from_slice(&4096u32.to_le_bytes());
    assert_eq!(
        GzipIndex::read_gzidx(&mut bytes.as_slice(), None),
        Err(IndexError::InvalidWindowSize(4096))
    );
}

#[test]
fn gzidx_rejects_truncation_at_every_prefix() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");
    for length in 1..bytes.len() {
        assert!(
            GzipIndex::read_gzidx(&mut &bytes[..length], None).is_err(),
            "prefix of {length} bytes was accepted"
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rapidgzip-core --test index_formats gzidx`
Expected: compilation failure.

- [ ] **Step 3: Implement the format**

`src/index/gzidx.rs`:

```rust
//! [indexed_gzip](https://github.com/pauldmccarthy/indexed_gzip) `GZIDX`
//! import and export.
//!
//! Layout, little-endian: magic `GZIDX`, `u8` version, `u8` flags, `u64`
//! compressed size, `u64` uncompressed size, `u32` checkpoint spacing, `u32`
//! window size (always 32768), `u32` checkpoint count. Then one record per
//! checkpoint holding `u64` compressed byte offset, `u64` uncompressed offset,
//! `u8` bits field and, from version 1, a `u8` data flag. Window payloads
//! follow the record table, one fixed-size payload per set data flag.

use super::{
    Checkpoint, GzipIndex, IndexError, StoredWindow, WINDOW_SIZE, read_exact_bytes, read_u32_le,
    read_u64_le, read_u8, write_u32_le, write_u64_le,
};
use std::io::{Read, Write};

/// Magic bytes identifying an indexed_gzip index.
pub(crate) const MAGIC: &[u8; 5] = b"GZIDX";

/// Highest indexed_gzip version this crate reads.
const MAX_VERSION: u8 = 1;

/// Upper bound on the checkpoint count.
const MAX_CHECKPOINTS: u32 = 1 << 28;

/// Encodes a compressed bit offset into the zran byte and bits pair.
///
/// With `remainder = offset % 8`, a zero remainder stores `offset / 8` and a
/// bits field of zero. Otherwise the stored byte offset is `offset / 8 + 1`
/// and the bits field is `8 - remainder`, matching zran and indexed_gzip.
#[must_use]
pub fn encode_bit_offset(compressed_offset_in_bits: u64) -> (u64, u8) {
    let remainder = (compressed_offset_in_bits % 8) as u8;
    if remainder == 0 {
        (compressed_offset_in_bits / 8, 0)
    } else {
        (compressed_offset_in_bits / 8 + 1, 8 - remainder)
    }
}

/// Decodes a zran byte and bits pair back into a compressed bit offset.
pub fn decode_bit_offset(byte_offset: u64, bits_field: u8) -> Result<u64, IndexError> {
    if bits_field >= 8 {
        return Err(IndexError::InvalidCheckpoint(
            "denormal compressed offset: bits field is 8 or more",
        ));
    }
    let bit_offset = byte_offset
        .checked_mul(8)
        .ok_or(IndexError::InvalidCheckpoint(
            "compressed byte offset overflows a bit count",
        ))?;
    if bits_field == 0 {
        return Ok(bit_offset);
    }
    if bit_offset == 0 {
        return Err(IndexError::InvalidCheckpoint(
            "denormal compressed offset: bits field before the source start",
        ));
    }
    Ok(bit_offset - u64::from(bits_field))
}

pub(crate) fn write_gzidx(index: &GzipIndex, writer: &mut impl Write) -> Result<(), IndexError> {
    writer.write_all(MAGIC).map_err(IndexError::io)?;
    writer
        .write_all(&[MAX_VERSION, 0])
        .map_err(IndexError::io)?;
    write_u64_le(writer, index.compressed_size_in_bytes)?;
    write_u64_le(writer, index.uncompressed_size_in_bytes)?;
    write_u32_le(
        writer,
        u32::try_from(index.checkpoint_spacing_in_bytes).unwrap_or(u32::MAX),
    )?;
    write_u32_le(writer, WINDOW_SIZE as u32)?;
    write_u32_le(
        writer,
        u32::try_from(index.checkpoints.len()).map_err(|_| IndexError::ExcessiveLength {
            what: "checkpoint count",
            value: index.checkpoints.len() as u64,
        })?,
    )?;

    for checkpoint in &index.checkpoints {
        let (byte_offset, bits_field) = encode_bit_offset(checkpoint.compressed_offset_in_bits);
        write_u64_le(writer, byte_offset)?;
        write_u64_le(writer, checkpoint.uncompressed_offset_in_bytes)?;
        let has_window = index
            .windows
            .get(checkpoint.compressed_offset_in_bits)
            .is_some_and(|window| !window.is_empty());
        writer
            .write_all(&[bits_field, u8::from(has_window)])
            .map_err(IndexError::io)?;
    }

    for checkpoint in &index.checkpoints {
        let Some(window) = index.windows.get(checkpoint.compressed_offset_in_bits) else {
            continue;
        };
        if window.is_empty() {
            continue;
        }
        let expanded = window.decompressed()?;
        if expanded.len() != WINDOW_SIZE {
            return Err(IndexError::InvalidCheckpoint(
                "non-empty predecessor window is not 32768 bytes",
            ));
        }
        writer.write_all(&expanded).map_err(IndexError::io)?;
    }
    Ok(())
}

pub(crate) fn read_gzidx(
    reader: &mut impl Read,
    archive_size: Option<u64>,
) -> Result<GzipIndex, IndexError> {
    let mut magic = [0u8; 5];
    read_exact_bytes(reader, &mut magic)?;
    if &magic != MAGIC {
        return Err(IndexError::BadMagic {
            found: magic.to_vec(),
        });
    }

    let version = read_u8(reader)?;
    if version > MAX_VERSION {
        return Err(IndexError::UnsupportedVersion(version));
    }
    let _flags = read_u8(reader)?;

    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = read_u64_le(reader)?;
    index.uncompressed_size_in_bytes = read_u64_le(reader)?;
    index.checkpoint_spacing_in_bytes = u64::from(read_u32_le(reader)?);

    let window_size = read_u32_le(reader)?;
    if window_size != WINDOW_SIZE as u32 {
        return Err(IndexError::InvalidWindowSize(window_size));
    }

    if let Some(archive_size) = archive_size
        && archive_size != index.compressed_size_in_bytes
    {
        return Err(IndexError::ArchiveSizeMismatch {
            index_size: index.compressed_size_in_bytes,
            archive_size,
        });
    }

    let count = read_u32_le(reader)?;
    if count > MAX_CHECKPOINTS {
        return Err(IndexError::ExcessiveLength {
            what: "checkpoint count",
            value: u64::from(count),
        });
    }

    let mut records = Vec::with_capacity(count as usize);
    for position in 0..count {
        let byte_offset = read_u64_le(reader)?;
        let uncompressed_offset_in_bytes = read_u64_le(reader)?;
        let bits_field = read_u8(reader)?;
        let has_window = if version == 0 {
            position != 0
        } else {
            read_u8(reader)? != 0
        };

        if index.compressed_size_in_bytes != 0 && byte_offset > index.compressed_size_in_bytes {
            return Err(IndexError::InvalidCheckpoint(
                "checkpoint compressed offset is after the source end",
            ));
        }
        records.push((
            Checkpoint {
                compressed_offset_in_bits: decode_bit_offset(byte_offset, bits_field)?,
                uncompressed_offset_in_bytes,
                line_offset: 0,
            },
            has_window,
        ));
    }

    for (checkpoint, has_window) in records {
        let window = if has_window {
            let mut payload = vec![0u8; WINDOW_SIZE];
            read_exact_bytes(reader, &mut payload)?;
            StoredWindow::from_raw(payload)
        } else {
            StoredWindow::empty()
        };
        index.push(checkpoint, window);
    }

    index.validate()?;
    Ok(index)
}
```

- [ ] **Step 4: Expose the methods**

In `src/index/mod.rs`, add `mod gzidx;`, `pub use gzidx::{decode_bit_offset, encode_bit_offset};`, and:

```rust
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
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p rapidgzip-core --test index_formats`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/rapidgzip-core/src/index/ crates/rapidgzip-core/tests/index_formats.rs
git commit -m "Add indexed_gzip GZIDX import and export"
```

---

### Task 5: htslib BGZF `.gzi` format

**Files:**
- Create: `crates/rapidgzip-core/src/index/gzi.rs`
- Modify: `crates/rapidgzip-core/src/index/mod.rs`
- Test: `crates/rapidgzip-core/tests/index_formats.rs`

**Interfaces:**
- Consumes: Task 1.
- Produces: `GzipIndex::write_gzi(&self, writer: &mut impl Write) -> Result<(), IndexError>`, `GzipIndex::read_gzi(reader: &mut impl Read, archive_size: Option<u64>) -> Result<GzipIndex, IndexError>`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/index_formats.rs`:

```rust
fn bgzf_style_index() -> GzipIndex {
    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = 300;
    index.uncompressed_size_in_bytes = 3000;
    for block in 0..3u64 {
        index.push(
            Checkpoint {
                compressed_offset_in_bits: block * 8 * 100,
                uncompressed_offset_in_bytes: block * 1000,
                line_offset: 0,
            },
            StoredWindow::empty(),
        );
    }
    index
}

#[test]
fn gzi_round_trips_block_starts_and_skips_the_origin() {
    let index = bgzf_style_index();
    let mut bytes = Vec::new();
    index.write_gzi(&mut bytes).expect("write");

    // u64 pair count plus two pairs of two u64 values.
    assert_eq!(bytes.len(), 8 + 2 * 16);
    assert_eq!(u64::from_le_bytes(bytes[..8].try_into().unwrap()), 2);

    let restored = GzipIndex::read_gzi(&mut bytes.as_slice(), Some(300)).expect("read");
    assert_eq!(restored.checkpoints(), index.checkpoints());
    assert_eq!(restored.uncompressed_size_in_bytes, u64::MAX);
}

#[test]
fn gzi_refuses_checkpoints_that_need_a_window() {
    let mut index = bgzf_style_index();
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 8 * 250,
            uncompressed_offset_in_bytes: 4000,
            line_offset: 0,
        },
        StoredWindow::from_raw(vec![3u8; WINDOW_SIZE]),
    );
    let mut bytes = Vec::new();
    assert_eq!(
        index.write_gzi(&mut bytes),
        Err(IndexError::InvalidCheckpoint(
            "BGZF index cannot store a predecessor window"
        ))
    );
}

#[test]
fn gzi_refuses_unaligned_offsets() {
    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = 300;
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 0,
            uncompressed_offset_in_bytes: 0,
            line_offset: 0,
        },
        StoredWindow::empty(),
    );
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 8 * 100 + 1,
            uncompressed_offset_in_bytes: 1000,
            line_offset: 0,
        },
        StoredWindow::empty(),
    );
    let mut bytes = Vec::new();
    assert_eq!(
        index.write_gzi(&mut bytes),
        Err(IndexError::InvalidCheckpoint(
            "BGZF index requires byte-aligned compressed offsets"
        ))
    );
}

#[test]
fn gzi_rejects_a_hostile_pair_count() {
    let mut bytes = u64::MAX.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[0u8; 16]);
    assert!(matches!(
        GzipIndex::read_gzi(&mut bytes.as_slice(), None),
        Err(IndexError::ExcessiveLength { .. })
    ));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rapidgzip-core --test index_formats gzi_`
Expected: compilation failure.

- [ ] **Step 3: Implement the format**

`src/index/gzi.rs`:

```rust
//! [htslib](https://github.com/samtools/htslib) BGZF block index (`.gzi`).
//!
//! Layout, little-endian, matching `bgzf_index_dump_hfile`: a `u64` pair count
//! followed by that many `(compressed_offset, uncompressed_offset)` pairs
//! describing block starts after the first. The origin pair is implicit.
//!
//! The format stores neither predecessor windows nor a total uncompressed
//! size, so import records the uncompressed size as unknown ([`u64::MAX`]) and
//! export refuses any checkpoint that needs a window.

use super::{
    Checkpoint, GzipIndex, IndexError, StoredWindow, read_u64_le, write_u64_le,
};
use std::io::{Read, Write};

/// Upper bound on the pair count, guarding against hostile headers.
const MAX_PAIRS: u64 = 1 << 28;

pub(crate) fn write_gzi(index: &GzipIndex, writer: &mut impl Write) -> Result<(), IndexError> {
    let mut pairs = Vec::new();
    for checkpoint in &index.checkpoints {
        if index
            .windows
            .get(checkpoint.compressed_offset_in_bits)
            .is_some_and(|window| !window.is_empty())
        {
            return Err(IndexError::InvalidCheckpoint(
                "BGZF index cannot store a predecessor window",
            ));
        }
        if !checkpoint.compressed_offset_in_bits.is_multiple_of(8) {
            return Err(IndexError::InvalidCheckpoint(
                "BGZF index requires byte-aligned compressed offsets",
            ));
        }
        if checkpoint.compressed_offset_in_bits == 0 && checkpoint.uncompressed_offset_in_bytes == 0
        {
            continue;
        }
        pairs.push((
            checkpoint.compressed_offset_in_bits / 8,
            checkpoint.uncompressed_offset_in_bytes,
        ));
    }

    write_u64_le(writer, pairs.len() as u64)?;
    for (compressed, uncompressed) in pairs {
        write_u64_le(writer, compressed)?;
        write_u64_le(writer, uncompressed)?;
    }
    Ok(())
}

pub(crate) fn read_gzi(
    reader: &mut impl Read,
    archive_size: Option<u64>,
) -> Result<GzipIndex, IndexError> {
    let pair_count = read_u64_le(reader)?;
    if pair_count > MAX_PAIRS {
        return Err(IndexError::ExcessiveLength {
            what: "BGZF index pair count",
            value: pair_count,
        });
    }

    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = archive_size.unwrap_or(0);
    index.uncompressed_size_in_bytes = u64::MAX;
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 0,
            uncompressed_offset_in_bytes: 0,
            line_offset: 0,
        },
        StoredWindow::empty(),
    );

    for _ in 0..pair_count {
        let compressed = read_u64_le(reader)?;
        let uncompressed = read_u64_le(reader)?;
        let compressed_offset_in_bits =
            compressed
                .checked_mul(8)
                .ok_or(IndexError::InvalidCheckpoint(
                    "compressed byte offset overflows a bit count",
                ))?;
        index.push(
            Checkpoint {
                compressed_offset_in_bits,
                uncompressed_offset_in_bytes: uncompressed,
                line_offset: 0,
            },
            StoredWindow::empty(),
        );
    }

    index.validate()?;
    Ok(index)
}
```

- [ ] **Step 4: Expose the methods**

In `src/index/mod.rs`, add `mod gzi;` and:

```rust
    /// Writes this index in htslib BGZF `.gzi` format.
    ///
    /// Only indexes whose checkpoints all sit on independent member or block
    /// boundaries can be represented; a checkpoint carrying a predecessor
    /// window or a non-byte-aligned offset is refused.
    pub fn write_gzi(&self, writer: &mut impl Write) -> Result<(), IndexError> {
        gzi::write_gzi(self, writer)
    }

    /// Reads an htslib BGZF `.gzi` index.
    ///
    /// The format does not record the uncompressed size, so the result
    /// reports [`u64::MAX`] for it. `archive_size`, when supplied, is recorded
    /// as the compressed size.
    pub fn read_gzi(
        reader: &mut impl Read,
        archive_size: Option<u64>,
    ) -> Result<Self, IndexError> {
        gzi::read_gzi(reader, archive_size)
    }
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p rapidgzip-core --test index_formats`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/rapidgzip-core/src/index/ crates/rapidgzip-core/tests/index_formats.rs
git commit -m "Add htslib BGZF gzi import and export"
```

---

### Task 6: gztool format

**Files:**
- Create: `crates/rapidgzip-core/src/index/gztool.rs`
- Modify: `crates/rapidgzip-core/src/index/mod.rs`
- Test: `crates/rapidgzip-core/tests/index_formats.rs`

**Interfaces:**
- Consumes: Tasks 1, 2, and `encode_bit_offset` / `decode_bit_offset` from Task 4.
- Produces: `pub enum WithLines { No, Yes }`, `GzipIndex::write_gztool(&self, writer: &mut impl Write, lines: WithLines) -> Result<(), IndexError>`, `GzipIndex::read_gztool(reader: &mut impl Read, archive_size: Option<u64>) -> Result<GzipIndex, IndexError>`.

Layout, big-endian: eight zero bytes, then magic `gzipindx` (version 0) or `gzipindX` (version 1), and for version 1 a `u32` line-number format. Then `u64` `have` and `u64` `size`, which must be equal. Then per point: `u64` uncompressed offset, `u64` compressed byte offset, `u32` bits field, `u32` compressed window length, the zlib payload, and for version 1 a `u64` line counter. The file ends with the `u64` uncompressed size and, for version 1, the total line count.

- [ ] **Step 1: Write the failing tests**

Append to `tests/index_formats.rs`:

```rust
#[test]
fn gztool_round_trips_without_lines() {
    use rapidgzip_core::index::WithLines;

    let index = sample_index();
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::No)
        .expect("write");
    assert_eq!(&bytes[..8], &[0u8; 8]);
    assert_eq!(&bytes[8..16], b"gzipindx");

    let restored = GzipIndex::read_gztool(&mut bytes.as_slice(), Some(1_000_000)).expect("read");
    assert_same_index(&index, &restored);
    assert_eq!(restored.total_line_count, None);
}

#[test]
fn gztool_round_trips_with_lines() {
    use rapidgzip_core::index::WithLines;

    let index = sample_index();
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::Yes)
        .expect("write");
    assert_eq!(&bytes[8..16], b"gzipindX");

    let restored = GzipIndex::read_gztool(&mut bytes.as_slice(), Some(1_000_000)).expect("read");
    assert_eq!(
        restored
            .checkpoints()
            .iter()
            .map(|point| point.line_offset)
            .collect::<Vec<_>>(),
        vec![0, 1234]
    );
    assert_eq!(restored.total_line_count, Some(1234));
}

#[test]
fn gztool_rejects_an_incomplete_index() {
    use rapidgzip_core::index::WithLines;

    let index = sample_index();
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::No)
        .expect("write");
    // `size` follows `have` at offset 16; make them disagree.
    bytes[24..32].copy_from_slice(&99u64.to_be_bytes());
    assert_eq!(
        GzipIndex::read_gztool(&mut bytes.as_slice(), None),
        Err(IndexError::InvalidCheckpoint(
            "gztool index is incomplete"
        ))
    );
}

#[test]
fn gztool_rejects_an_excessive_window_length() {
    use rapidgzip_core::index::WithLines;

    let index = sample_index();
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::No)
        .expect("write");
    // First point: 8 uncompressed + 8 compressed + 4 bits, then the length,
    // starting after the 16-byte header and two u64 counters.
    let length_at = 16 + 16 + 8 + 8 + 4;
    bytes[length_at..length_at + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        GzipIndex::read_gztool(&mut bytes.as_slice(), None),
        Err(IndexError::ExcessiveLength { .. })
    ));
}

#[test]
fn gztool_rejects_truncation_at_every_prefix() {
    use rapidgzip_core::index::WithLines;

    let index = sample_index();
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::Yes)
        .expect("write");
    for length in 1..bytes.len() {
        assert!(
            GzipIndex::read_gztool(&mut &bytes[..length], None).is_err(),
            "prefix of {length} bytes was accepted"
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rapidgzip-core --test index_formats gztool`
Expected: compilation failure.

- [ ] **Step 3: Implement the format**

`src/index/gztool.rs`:

```rust
//! [gztool](https://github.com/circulosmeos/gztool) index import and export.
//!
//! All integers are big-endian. Windows are stored zlib-compressed. Only
//! complete indexes (`have == size`) are accepted; gztool's growing-index
//! placeholders are refused rather than silently truncated.

use super::{
    Checkpoint, GzipIndex, IndexError, StoredWindow, WINDOW_SIZE, decode_bit_offset,
    encode_bit_offset, read_exact_bytes, read_u32_be, read_u64_be, write_u32_be, write_u64_be,
    zlib_compress_window,
};
use std::io::{Read, Write};

/// Magic for indexes without line counters.
pub(crate) const MAGIC_V0: &[u8; 8] = b"gzipindx";

/// Magic for indexes with line counters.
pub(crate) const MAGIC_V1: &[u8; 8] = b"gzipindX";

/// Upper bound on the point count.
const MAX_POINTS: u64 = 1 << 28;

/// Upper bound on a stored compressed window payload.
const MAX_COMPRESSED_WINDOW: u32 = 40 * 1024;

/// Whether a gztool index carries per-point line counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WithLines {
    /// Version 0, no line counters.
    No,
    /// Version 1, one line counter per point.
    Yes,
}

pub(crate) fn write_gztool(
    index: &GzipIndex,
    writer: &mut impl Write,
    lines: WithLines,
) -> Result<(), IndexError> {
    let with_lines = lines == WithLines::Yes;
    write_u64_be(writer, 0)?;
    if with_lines {
        writer.write_all(MAGIC_V1).map_err(IndexError::io)?;
        // Line-number format 0: LF, which also covers CRLF.
        write_u32_be(writer, 0)?;
    } else {
        writer.write_all(MAGIC_V0).map_err(IndexError::io)?;
    }

    let have = index.checkpoints.len() as u64;
    write_u64_be(writer, have)?;
    write_u64_be(writer, have)?;

    let mut maximum_line = 0u64;
    for checkpoint in &index.checkpoints {
        let (byte_offset, bits_field) = encode_bit_offset(checkpoint.compressed_offset_in_bits);
        write_u64_be(writer, checkpoint.uncompressed_offset_in_bytes)?;
        write_u64_be(writer, byte_offset)?;
        write_u32_be(writer, u32::from(bits_field))?;

        match index.windows.get(checkpoint.compressed_offset_in_bits) {
            Some(window) if !window.is_empty() => {
                let expanded = window.decompressed()?;
                if expanded.len() != WINDOW_SIZE {
                    return Err(IndexError::InvalidCheckpoint(
                        "non-empty predecessor window is not 32768 bytes",
                    ));
                }
                let payload = zlib_compress_window(&expanded)?;
                let length =
                    u32::try_from(payload.len()).map_err(|_| IndexError::ExcessiveLength {
                        what: "compressed window length",
                        value: payload.len() as u64,
                    })?;
                if length > MAX_COMPRESSED_WINDOW {
                    return Err(IndexError::ExcessiveLength {
                        what: "compressed window length",
                        value: u64::from(length),
                    });
                }
                write_u32_be(writer, length)?;
                writer.write_all(&payload).map_err(IndexError::io)?;
            }
            _ => write_u32_be(writer, 0)?,
        }

        if with_lines {
            write_u64_be(writer, checkpoint.line_offset)?;
            maximum_line = maximum_line.max(checkpoint.line_offset);
        }
    }

    write_u64_be(writer, index.uncompressed_size_in_bytes)?;
    if with_lines {
        write_u64_be(writer, index.total_line_count.unwrap_or(maximum_line))?;
    }
    Ok(())
}

pub(crate) fn read_gztool(
    reader: &mut impl Read,
    archive_size: Option<u64>,
) -> Result<GzipIndex, IndexError> {
    let mut header = [0u8; 16];
    read_exact_bytes(reader, &mut header)?;
    if header[..8] != [0u8; 8] {
        return Err(IndexError::BadMagic {
            found: header.to_vec(),
        });
    }
    let with_lines = if &header[8..16] == MAGIC_V0 {
        false
    } else if &header[8..16] == MAGIC_V1 {
        true
    } else {
        return Err(IndexError::BadMagic {
            found: header.to_vec(),
        });
    };
    if with_lines {
        let _line_number_format = read_u32_be(reader)?;
    }

    let have = read_u64_be(reader)?;
    let size = read_u64_be(reader)?;
    if have != size {
        return Err(IndexError::InvalidCheckpoint("gztool index is incomplete"));
    }
    if have > MAX_POINTS {
        return Err(IndexError::ExcessiveLength {
            what: "gztool point count",
            value: have,
        });
    }

    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = archive_size.unwrap_or(0);
    index.checkpoints.reserve(have as usize);

    for _ in 0..have {
        let uncompressed_offset_in_bytes = read_u64_be(reader)?;
        let byte_offset = read_u64_be(reader)?;
        let bits_field = u8::try_from(read_u32_be(reader)?)
            .map_err(|_| IndexError::InvalidCheckpoint("bits field does not fit in a byte"))?;
        let payload_length = read_u32_be(reader)?;
        if payload_length > MAX_COMPRESSED_WINDOW {
            return Err(IndexError::ExcessiveLength {
                what: "compressed window length",
                value: u64::from(payload_length),
            });
        }
        let window = if payload_length == 0 {
            StoredWindow::empty()
        } else {
            let mut payload = vec![0u8; payload_length as usize];
            read_exact_bytes(reader, &mut payload)?;
            let expanded = super::zlib_decompress_window(&payload)?;
            StoredWindow::from_raw(expanded)
        };
        let line_offset = if with_lines { read_u64_be(reader)? } else { 0 };

        index.push(
            Checkpoint {
                compressed_offset_in_bits: decode_bit_offset(byte_offset, bits_field)?,
                uncompressed_offset_in_bytes,
                line_offset,
            },
            window,
        );
    }

    index.uncompressed_size_in_bytes = read_u64_be(reader)?;
    index.total_line_count = if with_lines {
        Some(read_u64_be(reader)?)
    } else {
        None
    };

    index.validate()?;
    Ok(index)
}
```

- [ ] **Step 4: Expose the methods**

In `src/index/mod.rs`, add `mod gztool;`, `pub use gztool::WithLines;`, and:

```rust
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
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p rapidgzip-core --test index_formats`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/rapidgzip-core/src/index/ crates/rapidgzip-core/tests/index_formats.rs
git commit -m "Add gztool index import and export"
```

---

### Task 7: Extract the raw-inflate wrapper

**Files:**
- Create: `crates/rapidgzip-core/src/inflate.rs`
- Modify: `crates/rapidgzip-core/src/backend.rs:60-131`, `crates/rapidgzip-core/src/lib.rs`
- Test: inline `#[cfg(test)]` module in `src/inflate.rs`

This is a refactor plus two new capabilities. Behavior of existing paths must not change; the existing test suite is the regression net.

**Interfaces:**
- Consumes: `DecodeError`, `DeflateErrorKind`.
- Produces: `pub(crate) struct RawInflater` with `new()`, `reset(bit_offset: u64)`, `message()`, plus new `prime(&mut self, bits: u8, value: u32) -> Result<(), DecodeError>` and `set_dictionary(&mut self, window: &[u8]) -> Result<(), DecodeError>`, and public accessor `stream(&mut self) -> &mut z::z_stream`.

- [ ] **Step 1: Move the type**

Cut `RawInflater`, its `impl` blocks at `backend.rs:60-131`, and the `impl RawInflater` block at `backend.rs:1940` into a new `src/inflate.rs` with a module doc comment. Add `mod inflate;` to `src/lib.rs` and `use crate::inflate::RawInflater;` to `backend.rs`. Make the struct and its methods `pub(crate)`.

- [ ] **Step 2: Run the existing suite to prove nothing changed**

Run: `cargo test -p rapidgzip-core`
Expected: PASS, same set of tests as before the move.

- [ ] **Step 3: Commit the pure move**

```bash
git add crates/rapidgzip-core/src/inflate.rs crates/rapidgzip-core/src/backend.rs crates/rapidgzip-core/src/lib.rs
git commit -m "Move the raw inflate wrapper into its own module"
```

- [ ] **Step 4: Write the failing test for the new capabilities**

At the bottom of `src/inflate.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Inflates `compressed` starting at `bit_offset`, seeded with `window`.
    fn inflate_from(
        compressed: &[u8],
        bit_offset: u64,
        window: &[u8],
        expected_len: usize,
    ) -> Vec<u8> {
        let mut inflater = RawInflater::new().expect("initialize");
        let byte_offset = (bit_offset / 8) as usize;
        let remainder = (bit_offset % 8) as u8;
        let mut input_start = byte_offset;
        if remainder != 0 {
            inflater
                .prime(8 - remainder, u32::from(compressed[byte_offset] >> remainder))
                .expect("prime");
            input_start += 1;
        }
        if !window.is_empty() {
            inflater.set_dictionary(window).expect("set dictionary");
        }

        let mut output = vec![0u8; expected_len];
        let produced = inflater
            .inflate_into(&compressed[input_start..], &mut output)
            .expect("inflate");
        output.truncate(produced);
        output
    }

    #[test]
    fn priming_and_dictionary_reproduce_a_mid_stream_resume() {
        // A short raw DEFLATE stream is built by the crate's own test helpers
        // in `tests/indexed_seek.rs`; here we only assert the wrapper accepts
        // both calls and returns bytes for a byte-aligned start with no
        // dictionary.
        let compressed = super::tests_support::deflate_raw(b"hello hello hello hello");
        let output = inflate_from(&compressed, 0, &[], 64);
        assert_eq!(output, b"hello hello hello hello");
    }

    #[test]
    fn set_dictionary_rejects_an_oversized_window() {
        let mut inflater = RawInflater::new().expect("initialize");
        assert!(inflater.set_dictionary(&vec![0u8; 32769]).is_err());
    }
}
```

- [ ] **Step 5: Run it to verify it fails**

Run: `cargo test -p rapidgzip-core inflate::`
Expected: FAIL, `prime`, `set_dictionary`, `inflate_into` and `tests_support` are undefined.

- [ ] **Step 6: Implement the new methods**

Add to `src/inflate.rs`:

```rust
impl RawInflater {
    /// Injects `bits` leading bits of `value` before the next input byte.
    ///
    /// This is how a resume point that is not byte aligned starts: the caller
    /// primes the remaining bits of the straddled byte and then feeds input
    /// from the following byte.
    pub(crate) fn prime(&mut self, bits: u8, value: u32) -> Result<(), DecodeError> {
        if bits > 32 {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: 0,
                reason: DeflateErrorKind::InvalidData,
            });
        }
        // SAFETY: this wrapper owns a successfully initialized stream and holds
        // its unique mutable borrow. `bits` is within the range zlib accepts.
        let status = unsafe { z::inflatePrime(&mut self.stream, i32::from(bits), value as i32) };
        if status == z::Z_OK {
            Ok(())
        } else {
            Err(DecodeError::InvalidDeflate {
                bit_offset: 0,
                reason: DeflateErrorKind::BackendStatus(status),
            })
        }
    }

    /// Installs `window` as the raw-inflate history.
    ///
    /// The window must be at most 32768 bytes, the DEFLATE history size.
    pub(crate) fn set_dictionary(&mut self, window: &[u8]) -> Result<(), DecodeError> {
        if window.len() > 32768 {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: 0,
                reason: DeflateErrorKind::InvalidData,
            });
        }
        // SAFETY: this wrapper owns a successfully initialized stream and holds
        // its unique mutable borrow. The pointer and length describe the live
        // `window` slice for the duration of the call.
        let status = unsafe {
            z::inflateSetDictionary(&mut self.stream, window.as_ptr(), window.len() as u32)
        };
        if status == z::Z_OK {
            Ok(())
        } else {
            Err(DecodeError::InvalidDeflate {
                bit_offset: 0,
                reason: DeflateErrorKind::BackendStatus(status),
            })
        }
    }

    /// Inflates from `input` into `output`, returning the bytes produced.
    ///
    /// Stops at the end of the DEFLATE stream or when `output` is full,
    /// whichever comes first.
    pub(crate) fn inflate_into(
        &mut self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<usize, DecodeError> {
        self.stream.next_in = input.as_ptr();
        self.stream.avail_in = input.len() as u32;
        self.stream.next_out = output.as_mut_ptr();
        self.stream.avail_out = output.len() as u32;

        // SAFETY: the pointers and counts above describe the live `input` and
        // `output` slices, and the stream is initialized and uniquely borrowed.
        let status = unsafe { z::inflate(&mut self.stream, z::Z_NO_FLUSH) };
        match status {
            z::Z_OK | z::Z_STREAM_END | z::Z_BUF_ERROR => {
                Ok(output.len() - self.stream.avail_out as usize)
            }
            other => Err(DecodeError::InvalidDeflate {
                bit_offset: 0,
                reason: DeflateErrorKind::BackendStatus(other),
            }),
        }
    }

    /// Returns whether the current stream reached its final block.
    pub(crate) fn finished(&self) -> bool {
        self.stream.avail_in == 0 && self.stream.avail_out != 0
    }

    /// Returns the number of input bytes not yet consumed.
    pub(crate) fn unconsumed_input(&self) -> usize {
        self.stream.avail_in as usize
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use libz_rs_sys as z;

    /// Compresses `bytes` as a raw DEFLATE stream for tests.
    pub(crate) fn deflate_raw(bytes: &[u8]) -> Vec<u8> {
        let mut stream = z::z_stream::default();
        // SAFETY: `stream` is live and uniquely borrowed; `-15` selects raw
        // DEFLATE; the reported size matches the ABI type passed.
        let status = unsafe {
            z::deflateInit2_(
                &mut stream,
                6,
                z::Z_DEFLATED,
                -15,
                8,
                z::Z_DEFAULT_STRATEGY,
                z::zlibVersion(),
                size_of::<z::z_stream>() as i32,
            )
        };
        assert_eq!(status, z::Z_OK);

        let mut output = vec![0u8; bytes.len() + 128];
        stream.next_in = bytes.as_ptr();
        stream.avail_in = bytes.len() as u32;
        stream.next_out = output.as_mut_ptr();
        stream.avail_out = output.len() as u32;
        // SAFETY: the pointers above describe the live slices.
        let status = unsafe { z::deflate(&mut stream, z::Z_FINISH) };
        assert_eq!(status, z::Z_STREAM_END);
        let produced = output.len() - stream.avail_out as usize;
        // SAFETY: the stream was initialized above and is ended exactly once.
        unsafe { z::deflateEnd(&mut stream) };
        output.truncate(produced);
        output
    }
}
```

- [ ] **Step 7: Run the tests**

Run: `cargo test -p rapidgzip-core inflate::` then `cargo test -p rapidgzip-core`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/rapidgzip-core/src/inflate.rs
git commit -m "Add priming and dictionary support to the inflate wrapper"
```

---

### Task 8: IndexedReader

**Files:**
- Create: `crates/rapidgzip-core/src/indexed/mod.rs`, `crates/rapidgzip-core/src/indexed/window.rs`
- Modify: `crates/rapidgzip-core/src/lib.rs`
- Test: `crates/rapidgzip-core/tests/indexed_seek.rs`

**Interfaces:**
- Consumes: `GzipIndex`, `Checkpoint`, `StoredWindow` (Tasks 1 to 6), `RawInflater` (Task 7), `ReadAt`, and `parse_member_header` plus `SourceCursor` from `src/gzip.rs`.
- Produces: `pub struct IndexedReader<R: ReadAt>` with `new(source: R, index: GzipIndex) -> Result<Self, DecodeError>`, `index(&self) -> &GzipIndex`, `into_inner(self) -> (R, GzipIndex)`, `with_window_cache_bytes(self, bytes: usize) -> Self`, and `impl Read`, `impl Seek`.

Seek semantics: `SeekFrom::Start(n)` positions at decompressed byte `n`; `SeekFrom::Current` is relative to the current decompressed position; `SeekFrom::End` requires a known uncompressed size and otherwise returns an `io::Error` of kind `Unsupported`. Seeking past the end is allowed and reads return zero bytes, matching `std::fs::File`.

Resume procedure at a checkpoint:
1. Take the checkpoint's window, expanding it through the cache.
2. Reset the inflater, then, when the bit offset is not byte aligned, prime `8 - (offset % 8)` bits from `source[offset / 8] >> (offset % 8)` and start feeding input at `offset / 8 + 1`; otherwise start at `offset / 8`.
3. When the window is empty and the bytes at the byte offset are a gzip member header, parse and skip that header first, then resume raw inflate after it.
4. When the window is non-empty, call `set_dictionary` before the first `inflate_into`.
5. Discard output until the target offset is reached.

- [ ] **Step 1: Write the failing test**

Create `crates/rapidgzip-core/tests/indexed_seek.rs`:

```rust
use rapidgzip_core::{Decoder, GzipIndex, IndexedReader};
use std::io::{Cursor, Read, Seek, SeekFrom};

/// Builds a deterministic, moderately compressible corpus.
fn corpus(size: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(size);
    let mut state = 0x243f6a88u32;
    while bytes.len() < size {
        state = state.wrapping_mul(1664525).wrapping_add(1013904223);
        let line = format!("record {} value {}\n", bytes.len(), state >> 16);
        bytes.extend_from_slice(line.as_bytes());
    }
    bytes.truncate(size);
    bytes
}

/// Compresses `bytes` as a single gzip member using the system gzip-compatible
/// encoder available to tests.
fn gzip_single_member(bytes: &[u8]) -> Vec<u8> {
    // Implemented in tests/common/mod.rs, shared with the existing suite.
    common::gzip(bytes, 1)
}

mod common;

fn index_for(compressed: &[u8]) -> GzipIndex {
    let decoder = Decoder::builder()
        .build_index(true)
        .decoded_chunk_size(64 * 1024)
        .build()
        .expect("builder");
    let mut reader = decoder.reader(compressed.to_vec()).expect("reader");
    std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
    reader
        .finish()
        .expect("report")
        .index
        .expect("index requested")
}

#[test]
fn seek_matches_full_decode_on_a_single_member() {
    let plain = corpus(4 * 1024 * 1024);
    let compressed = gzip_single_member(&plain);
    let index = index_for(&compressed);
    assert!(index.checkpoint_count() > 1, "corpus produced no checkpoints");

    let mut reader = IndexedReader::new(compressed.clone(), index).expect("indexed reader");
    for target in [0usize, 1, 1000, 1 << 20, plain.len() - 4096] {
        reader
            .seek(SeekFrom::Start(target as u64))
            .expect("seek");
        let mut buffer = vec![0u8; 4096.min(plain.len() - target)];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(
            buffer,
            &plain[target..target + buffer.len()],
            "mismatch after seeking to {target}"
        );
    }
}

#[test]
fn seek_matches_full_decode_across_members() {
    let first = corpus(1 << 20);
    let second = corpus(3 << 20);
    let mut plain = first.clone();
    plain.extend_from_slice(&second);

    let mut compressed = gzip_single_member(&first);
    compressed.extend_from_slice(&gzip_single_member(&second));

    let index = index_for(&compressed);
    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");

    // Just before, exactly at, and just after the member boundary.
    for target in [first.len() - 10, first.len(), first.len() + 10] {
        reader.seek(SeekFrom::Start(target as u64)).expect("seek");
        let mut buffer = vec![0u8; 1024];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(buffer, &plain[target..target + 1024]);
    }
}

#[test]
fn reading_from_the_start_reproduces_the_whole_stream() {
    let plain = corpus(512 * 1024);
    let compressed = gzip_single_member(&plain);
    let index = index_for(&compressed);

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    let mut output = Vec::new();
    reader.read_to_end(&mut output).expect("read to end");
    assert_eq!(output, plain);
}

#[test]
fn seeking_past_the_end_reads_nothing() {
    let plain = corpus(64 * 1024);
    let compressed = gzip_single_member(&plain);
    let index = index_for(&compressed);

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    reader
        .seek(SeekFrom::Start(plain.len() as u64 + 1000))
        .expect("seek");
    let mut buffer = [0u8; 16];
    assert_eq!(reader.read(&mut buffer).expect("read"), 0);
}

#[test]
fn seek_from_end_without_a_known_size_is_unsupported() {
    let plain = corpus(64 * 1024);
    let compressed = gzip_single_member(&plain);
    let mut index = index_for(&compressed);
    index.uncompressed_size_in_bytes = u64::MAX;

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    let error = reader.seek(SeekFrom::End(-10)).expect_err("unsupported");
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
}

#[test]
fn a_reused_reader_serves_repeated_backward_seeks() {
    let plain = corpus(2 * 1024 * 1024);
    let compressed = gzip_single_member(&plain);
    let index = index_for(&compressed);

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    for _ in 0..3 {
        for target in [1_500_000usize, 10, 900_000, 10] {
            reader.seek(SeekFrom::Start(target as u64)).expect("seek");
            let mut buffer = vec![0u8; 512];
            reader.read_exact(&mut buffer).expect("read");
            assert_eq!(buffer, &plain[target..target + 512]);
        }
    }
    let _ = Cursor::new(Vec::<u8>::new());
}
```

If `crates/rapidgzip-core/tests/common/mod.rs` does not exist, create it by moving the gzip-producing helper already used by `tests/decode.rs` into it and re-exporting it from both test binaries.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p rapidgzip-core --test indexed_seek`
Expected: compilation failure, `IndexedReader` and `build_index` are undefined. Task 9 supplies `build_index`; until then this test does not compile, which is expected. Implement the reader first and re-run after Task 9.

- [ ] **Step 3: Implement the window cache**

`src/indexed/window.rs`:

```rust
//! Bounded cache of expanded predecessor windows.
//!
//! Repeated seeks in the same region reuse the same checkpoint window. The
//! cache holds expanded copies under a byte budget and evicts least recently
//! used entries first.

use std::collections::HashMap;

/// Default cache budget: eight full windows.
pub(crate) const DEFAULT_BUDGET: usize = 8 * 32768;

pub(crate) struct WindowCache {
    entries: HashMap<u64, (Vec<u8>, u64)>,
    budget: usize,
    used: usize,
    clock: u64,
}

impl WindowCache {
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            entries: HashMap::new(),
            budget,
            used: 0,
            clock: 0,
        }
    }

    pub(crate) fn get(&mut self, key: u64) -> Option<&[u8]> {
        self.clock += 1;
        let clock = self.clock;
        let entry = self.entries.get_mut(&key)?;
        entry.1 = clock;
        Some(&entry.0)
    }

    pub(crate) fn insert(&mut self, key: u64, window: Vec<u8>) {
        if window.len() > self.budget {
            return;
        }
        self.clock += 1;
        if let Some((previous, _)) = self.entries.remove(&key) {
            self.used -= previous.len();
        }
        while self.used + window.len() > self.budget {
            let Some((&oldest, _)) = self
                .entries
                .iter()
                .min_by_key(|(_, (_, last_used))| *last_used)
            else {
                break;
            };
            if let Some((evicted, _)) = self.entries.remove(&oldest) {
                self.used -= evicted.len();
            }
        }
        self.used += window.len();
        let clock = self.clock;
        self.entries.insert(key, (window, clock));
    }
}
```

- [ ] **Step 4: Implement the reader**

`src/indexed/mod.rs`, using `RawInflater::prime`, `set_dictionary`, `inflate_into`, the `WindowCache`, and `crate::gzip::{SourceCursor, parse_member_header}` for the member-header skip. Keep the file under 400 lines by delegating window expansion to `window.rs` and the resume procedure to a private `fn resume_at(&mut self, checkpoint: Checkpoint) -> io::Result<()>`.

The read loop:

```rust
impl<R: ReadAt> Read for IndexedReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if let Some(pending) = self.pending_seek.take() {
            self.resume_for(pending)?;
        }
        loop {
            if !self.decoded.is_empty() {
                let count = self.decoded.len().min(output.len());
                output[..count].copy_from_slice(&self.decoded[..count]);
                self.decoded.drain(..count);
                self.position += count as u64;
                return Ok(count);
            }
            if !self.fill()? {
                return Ok(0);
            }
        }
    }
}
```

`fill` reads the next input page through `ReadAt`, runs `inflate_into` into a reusable buffer, appends to `self.decoded`, and returns `false` at the end of the last member. When an inflate call reports the end of a stream and input remains, it parses the next member header and continues, which is how reads cross member boundaries.

- [ ] **Step 5: Register the module**

In `src/lib.rs`, add `mod indexed;` and `pub use indexed::IndexedReader;`.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p rapidgzip-core --test indexed_seek`
Expected: still failing on `build_index` only. Proceed to Task 9 and re-run there.

- [ ] **Step 7: Commit**

```bash
git add crates/rapidgzip-core/src/indexed/ crates/rapidgzip-core/src/lib.rs crates/rapidgzip-core/tests/indexed_seek.rs
git commit -m "Add IndexedReader for random access through an index"
```

---

### Task 9: Index construction during decode

**Files:**
- Create: `crates/rapidgzip-core/src/index/build.rs`
- Modify: `crates/rapidgzip-core/src/config.rs`, `crates/rapidgzip-core/src/error.rs:241-250`, `crates/rapidgzip-core/src/backend.rs` (the four emit sites near lines 512, 1830, 2017, 3650), `crates/rapidgzip-core/src/reader.rs`
- Test: `crates/rapidgzip-core/tests/indexed_seek.rs`, plus a new test in `tests/decode.rs`

**Interfaces:**
- Consumes: Tasks 1 to 8.
- Produces: `pub(crate) trait IndexSink { fn checkpoint(&mut self, checkpoint: Checkpoint, window: StoredWindow); fn finish(&mut self, compressed_bytes: u64, uncompressed_bytes: u64); }`, `pub(crate) struct IndexBuilder`, `DecoderBuilder::build_index(self, enabled: bool) -> Self`, `DecodeReport::index: Option<GzipIndex>`.

- [ ] **Step 1: Write the failing test**

Append to `tests/indexed_seek.rs`:

```rust
#[test]
fn an_index_is_absent_unless_requested() {
    let plain = corpus(256 * 1024);
    let compressed = gzip_single_member(&plain);
    let decoder = Decoder::builder().build().expect("builder");
    let mut reader = decoder.reader(compressed).expect("reader");
    std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
    assert!(reader.finish().expect("report").index.is_none());
}

#[test]
fn a_built_index_validates_and_covers_the_stream() {
    let plain = corpus(4 * 1024 * 1024);
    let compressed = gzip_single_member(&plain);
    let index = index_for(&compressed);

    index.validate().expect("index invariants hold");
    assert_eq!(index.uncompressed_size_in_bytes, plain.len() as u64);
    assert_eq!(index.compressed_size_in_bytes, compressed.len() as u64);
    assert_eq!(
        index.checkpoints().first().map(|point| point.uncompressed_offset_in_bytes),
        Some(0)
    );
}

#[test]
fn an_index_survives_a_native_round_trip_and_still_seeks() {
    let plain = corpus(2 * 1024 * 1024);
    let compressed = gzip_single_member(&plain);
    let index = index_for(&compressed);

    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    let restored = GzipIndex::read_native(&mut bytes.as_slice()).expect("read");

    let mut reader = IndexedReader::new(compressed, restored).expect("indexed reader");
    reader.seek(SeekFrom::Start(1_000_000)).expect("seek");
    let mut buffer = vec![0u8; 4096];
    reader.read_exact(&mut buffer).expect("read");
    assert_eq!(buffer, &plain[1_000_000..1_004_096]);
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p rapidgzip-core --test indexed_seek`
Expected: FAIL, `build_index` is undefined.

- [ ] **Step 3: Implement the sink**

`src/index/build.rs`:

```rust
//! Index construction during a decode.
//!
//! Decode paths call [`IndexSink::checkpoint`] at boundaries they already
//! compute, once the associated window is resolved. [`IndexBuilder`] collects
//! those points, enforces spacing, and produces a validated [`GzipIndex`].

use super::{Checkpoint, GzipIndex, IndexError, StoredWindow};

/// Receives checkpoints discovered during a decode.
pub(crate) trait IndexSink: Send {
    /// Offers a checkpoint and its resolved predecessor window.
    fn checkpoint(&mut self, checkpoint: Checkpoint, window: StoredWindow);

    /// Records the final sizes once the decode is verified.
    fn finish(&mut self, compressed_bytes: u64, uncompressed_bytes: u64);
}

/// Collects checkpoints into a [`GzipIndex`].
pub(crate) struct IndexBuilder {
    index: GzipIndex,
    spacing: u64,
    last_uncompressed: Option<u64>,
    compress_windows: bool,
    error: Option<IndexError>,
}

impl IndexBuilder {
    /// Creates a builder targeting `spacing` decompressed bytes between
    /// checkpoints.
    pub(crate) fn new(spacing: u64, compress_windows: bool) -> Self {
        let mut index = GzipIndex::new();
        index.checkpoint_spacing_in_bytes = spacing;
        index.uncompressed_size_in_bytes = u64::MAX;
        Self {
            index,
            spacing,
            last_uncompressed: None,
            compress_windows,
            error: None,
        }
    }

    /// Returns the collected index, or the first error encountered.
    pub(crate) fn finish_index(mut self) -> Result<GzipIndex, IndexError> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        self.index.validate()?;
        Ok(self.index)
    }
}

impl IndexSink for IndexBuilder {
    fn checkpoint(&mut self, checkpoint: Checkpoint, window: StoredWindow) {
        if self.error.is_some() {
            return;
        }
        if let Some(last) = self.last_uncompressed {
            if checkpoint.uncompressed_offset_in_bytes <= last {
                return;
            }
            let is_member_boundary = window.is_empty();
            if !is_member_boundary
                && checkpoint.uncompressed_offset_in_bytes - last < self.spacing
            {
                return;
            }
        }

        let window = if self.compress_windows && !window.is_empty() {
            match window.decompressed() {
                Ok(expanded) => {
                    match StoredWindow::from_raw_maybe_compress(expanded.into_owned(), true) {
                        Ok(window) => window,
                        Err(error) => {
                            self.error = Some(error);
                            return;
                        }
                    }
                }
                Err(error) => {
                    self.error = Some(error);
                    return;
                }
            }
        } else {
            window
        };

        self.last_uncompressed = Some(checkpoint.uncompressed_offset_in_bytes);
        self.index.push(checkpoint, window);
    }

    fn finish(&mut self, compressed_bytes: u64, uncompressed_bytes: u64) {
        self.index.compressed_size_in_bytes = compressed_bytes;
        self.index.uncompressed_size_in_bytes = uncompressed_bytes;
    }
}
```

- [ ] **Step 4: Thread the option through configuration**

In `src/config.rs`, add `build_index: bool` to `Config` (default `false`) and `compress_index_windows: bool` (default `true`), plus builder methods:

```rust
    /// Enables collecting a random-access index during decoding.
    ///
    /// The index appears in [`DecodeReport::index`] once the decode completes.
    /// It costs one predecessor window per checkpoint in memory.
    pub const fn build_index(mut self, enabled: bool) -> Self {
        self.config.build_index = enabled;
        self
    }

    /// Sets whether index windows are held zlib-compressed in memory.
    ///
    /// Compression trades a small amount of time per checkpoint for a large
    /// reduction in resident memory on compressible data. Ignored when
    /// [`Self::build_index`] is disabled.
    pub const fn compress_index_windows(mut self, enabled: bool) -> Self {
        self.config.compress_index_windows = enabled;
        self
    }
```

In `src/error.rs`, add to `DecodeReport`:

```rust
    /// Random-access index, present when index building was requested.
    pub index: Option<GzipIndex>,
```

Update every `DecodeReport` construction site to set `index: None`, then set it from the builder in the paths wired below.

- [ ] **Step 5: Wire the sequential and independent-member paths**

In `backend.rs`, at the emit site near line 2017 (independent members) and the stored-block path near line 512, call `sink.checkpoint(...)` with the member start bit offset, the running decompressed offset, and `StoredWindow::empty()` for member starts. Pass `Option<&mut dyn IndexSink>` down through `decode_source` rather than storing it globally.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p rapidgzip-core --test indexed_seek`
Expected: PASS for `an_index_is_absent_unless_requested`, `a_built_index_validates_and_covers_the_stream`, and the multi-member seek test. The single-member seek test may still report only one checkpoint until Task 10.

- [ ] **Step 7: Commit**

```bash
git add crates/rapidgzip-core/src/index/build.rs crates/rapidgzip-core/src/config.rs crates/rapidgzip-core/src/error.rs crates/rapidgzip-core/src/backend.rs crates/rapidgzip-core/src/reader.rs crates/rapidgzip-core/tests/indexed_seek.rs
git commit -m "Collect a random-access index during sequential decoding"
```

---

### Task 10: Index from the parallel and BGZF paths

**Files:**
- Modify: `crates/rapidgzip-core/src/backend.rs` (ordered-output sites near lines 1793-1848 and 3629-3650)
- Test: `crates/rapidgzip-core/tests/indexed_seek.rs`

**Interfaces:**
- Consumes: `IndexSink` from Task 9, `Chunk::start_bit`, `Chunk::backend_continuation`, `ChunkOutput::window_after`.
- Produces: no new API; the existing tests gain checkpoints on parallel decodes.

- [ ] **Step 1: Write the failing test**

Append to `tests/indexed_seek.rs`:

```rust
#[test]
fn the_parallel_path_produces_interior_checkpoints() {
    let plain = corpus(16 * 1024 * 1024);
    let compressed = gzip_single_member(&plain);

    let decoder = Decoder::builder()
        .decoder_threads(4)
        .build_index(true)
        .build()
        .expect("builder");
    let mut reader = decoder.reader(compressed.clone()).expect("reader");
    std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
    let index = reader.finish().expect("report").index.expect("index");

    assert!(
        index.checkpoint_count() >= 3,
        "single member produced only {} checkpoints",
        index.checkpoint_count()
    );
    index.validate().expect("invariants");

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    for target in [8_000_000usize, 2_000_000, 15_000_000] {
        reader.seek(SeekFrom::Start(target as u64)).expect("seek");
        let mut buffer = vec![0u8; 2048];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(buffer, &plain[target..target + 2048]);
    }
}

#[test]
fn a_bgzf_index_exports_as_gzi() {
    let plain = corpus(4 * 1024 * 1024);
    let compressed = common::bgzip(&plain);

    let decoder = Decoder::builder().build_index(true).build().expect("builder");
    let mut reader = decoder.reader(compressed.clone()).expect("reader");
    std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
    let index = reader.finish().expect("report").index.expect("index");

    let mut bytes = Vec::new();
    index.write_gzi(&mut bytes).expect("gzi export");
    assert!(bytes.len() > 8, "no BGZF block starts were recorded");

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    reader.seek(SeekFrom::Start(3_000_000)).expect("seek");
    let mut buffer = vec![0u8; 1024];
    reader.read_exact(&mut buffer).expect("read");
    assert_eq!(buffer, &plain[3_000_000..3_001_024]);
}
```

Add a `bgzip` helper to `tests/common/mod.rs` that produces BGZF by invoking the same construction the existing BGZF tests in `tests/decode.rs` already use.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p rapidgzip-core --test indexed_seek parallel`
Expected: FAIL with too few checkpoints.

- [ ] **Step 3: Emit checkpoints from the ordered-output loop**

At the native estimated-grid ordered-output site, immediately before `output.emit(decoded.bytes)`, the chunk's start bit offset and the running decompressed offset are both known, and the predecessor window is the window used to resolve that chunk's markers. Emit the checkpoint there, before the bytes leave the loop, so a checkpoint is only recorded for a chunk whose window is resolved.

For BGZF, emit at each block start with an empty window, since BGZF blocks are independent.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rapidgzip-core`
Expected: PASS, including the whole existing suite.

- [ ] **Step 5: Commit**

```bash
git add crates/rapidgzip-core/src/backend.rs crates/rapidgzip-core/tests/
git commit -m "Collect index checkpoints from the parallel and BGZF paths"
```

---

### Task 11: Index from the streaming path

**Files:**
- Modify: `crates/rapidgzip-core/src/backend.rs` (`decode_stream`, near line 609)
- Test: `crates/rapidgzip-core/tests/indexed_seek.rs`

**Interfaces:**
- Consumes: Task 9.
- Produces: no new API.

- [ ] **Step 1: Write the failing test**

Append to `tests/indexed_seek.rs`:

```rust
#[test]
fn a_streaming_decode_builds_the_same_index_as_a_positional_one() {
    let plain = corpus(2 * 1024 * 1024);
    let compressed = gzip_single_member(&plain);

    let positional = index_for(&compressed);

    let decoder = Decoder::builder()
        .build_index(true)
        .decoded_chunk_size(64 * 1024)
        .build()
        .expect("builder");
    let mut reader = decoder
        .stream_reader(Cursor::new(compressed.clone()))
        .expect("stream reader");
    std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
    let streamed = reader.finish().expect("report").index.expect("index");

    assert_eq!(streamed.checkpoints(), positional.checkpoints());

    let mut reader = IndexedReader::new(compressed, streamed).expect("indexed reader");
    reader.seek(SeekFrom::Start(1_500_000)).expect("seek");
    let mut buffer = vec![0u8; 1024];
    reader.read_exact(&mut buffer).expect("read");
    assert_eq!(buffer, &plain[1_500_000..1_501_024]);
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p rapidgzip-core --test indexed_seek streaming`
Expected: FAIL, the streamed index is empty.

- [ ] **Step 3: Wire the sink into `decode_stream`**

The streaming path runs the sequential zlib inflate over a sliding input page. It knows the absolute compressed byte offset of each page and the running decompressed offset, so it emits a checkpoint at each member start with an empty window and, at the configured spacing, a checkpoint carrying the last 32 KiB of decompressed output as its window.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rapidgzip-core`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/rapidgzip-core/src/backend.rs crates/rapidgzip-core/tests/indexed_seek.rs
git commit -m "Collect an index while decoding non-seekable input"
```

---

### Task 12: Interop tests and CI

**Files:**
- Create: `crates/rapidgzip-core/tests/index_interop.rs`
- Modify: `.github/workflows/ci.yml`
- Test: the new file itself

**Interfaces:**
- Consumes: every format from Tasks 3 to 6.
- Produces: no new API.

- [ ] **Step 1: Write the interop tests**

`crates/rapidgzip-core/tests/index_interop.rs`, every test marked `#[ignore]` with a comment saying which tool it needs:

```rust
//! Interop tests against the reference tools.
//!
//! These are `#[ignore]` by default because they need `bgzip`,
//! `indexed_gzip`, and `gztool` on the path. The `interop` CI job installs
//! them and runs `cargo test -p rapidgzip-core --test index_interop -- --ignored`.

use rapidgzip_core::{Decoder, GzipIndex};
use std::process::Command;

mod common;

#[test]
#[ignore = "requires bgzip"]
fn bgzip_reads_our_gzi() {
    // Write a BGZF file plus our .gzi next to it, then have `bgzip -b` seek
    // with it and compare the bytes it prints against the plain corpus.
}

#[test]
#[ignore = "requires bgzip"]
fn we_read_the_gzi_written_by_bgzip() {
    // Run `bgzip -i`, load the produced .gzi, and check that seeking through
    // IndexedReader matches the plain corpus.
}

#[test]
#[ignore = "requires python3 with indexed_gzip"]
fn indexed_gzip_reads_our_gzidx() {
    // Write our GZIDX, then run a short python3 snippet that opens the file
    // with indexed_gzip, imports the index, seeks, and prints a checksum.
}

#[test]
#[ignore = "requires python3 with indexed_gzip"]
fn we_read_the_gzidx_written_by_indexed_gzip() {
    // Export an index from indexed_gzip, import it, and compare seeks.
}

#[test]
#[ignore = "requires gztool"]
fn gztool_reads_our_index() {
    // Write our gztool index, then run `gztool -b <offset>` and compare.
}

#[test]
#[ignore = "requires gztool"]
fn we_read_the_index_written_by_gztool() {
    // Run `gztool -i`, import the result, and compare seeks.
}
```

Each body follows the same shape: build the corpus, write the file into a temporary directory, invoke the tool through `Command`, and compare its output against the expected slice. Skip the test with an explanatory message when the tool is absent rather than failing.

- [ ] **Step 2: Run them locally where the tool exists**

Run: `cargo test -p rapidgzip-core --test index_interop -- --ignored`
Expected: the `bgzip` tests pass locally; the others report a missing tool.

- [ ] **Step 3: Add the CI job**

In `.github/workflows/ci.yml`, add a job that installs `bgzip` (via `apt-get install -y tabix`), `indexed_gzip` (via `pip install indexed_gzip`), and `gztool` (build from source at a pinned tag), then runs the ignored tests.

- [ ] **Step 4: Commit**

```bash
git add crates/rapidgzip-core/tests/index_interop.rs .github/workflows/ci.yml
git commit -m "Test index interoperability against the reference tools"
```

---

### Task 13: Documentation

**Files:**
- Modify: `crates/rapidgzip-core/src/lib.rs` (crate docs), `ARCHITECTURE.md`, `README.md`, `CHANGELOG.md`
- Test: `cargo test -p rapidgzip-core --doc`

**Interfaces:**
- Consumes: the whole feature.
- Produces: no new API.

- [ ] **Step 1: Update the crate docs**

The crate-level docs currently say "Encoding, index persistence, and decoded-output seeking are outside this crate's current scope." Replace that with a short section describing index building, the four formats, and `IndexedReader`, including a compiling doctest:

```rust
//! # Random access
//!
//! ```no_run
//! use rapidgzip_core::{Decoder, GzipIndex, IndexedReader};
//! use std::fs::File;
//! use std::io::{self, Read, Seek, SeekFrom};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let decoder = Decoder::builder().build_index(true).build()?;
//! let mut reader = decoder.open("reads.fastq.gz")?;
//! io::copy(&mut reader, &mut io::sink())?;
//! let index = reader.finish()?.index.expect("index requested");
//!
//! index.write_native(&mut File::create("reads.fastq.gz.idx")?)?;
//!
//! let mut random = IndexedReader::new(File::open("reads.fastq.gz")?, index)?;
//! random.seek(SeekFrom::Start(4_000_000_000))?;
//! let mut buffer = [0u8; 4096];
//! random.read_exact(&mut buffer)?;
//! # Ok(())
//! # }
//! ```
```

- [ ] **Step 2: Update `ARCHITECTURE.md`**

Add a section describing `index/` and `indexed/`, the checkpoint invariants, and where the decode paths call the sink.

- [ ] **Step 3: Update `README.md` and `CHANGELOG.md`**

README gains a random-access example and a note on format interoperability. CHANGELOG gains an entry under an unreleased heading.

- [ ] **Step 4: Run the full suite**

Run: `cargo test -p rapidgzip-core` and `cargo test --doc -p rapidgzip-core` and `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/rapidgzip-core/src/lib.rs ARCHITECTURE.md README.md CHANGELOG.md
git commit -m "Document random-access indexing and seeking"
```

---

## Self-Review Notes

- Spec coverage: index construction (Tasks 9 to 11), four formats (Tasks 3 to 6), `IndexedReader` (Task 8), error model (Task 1), untrusted-input bounds (every format task), testing strategy (Tasks 3 to 12), documentation (Task 13). The spec's LRU window cache is Task 8 step 3.
- Type consistency: `StoredWindow::from_raw`, `from_raw_maybe_compress`, `decompressed`, `is_compressed`, `stored_len`, and `payload` are defined in Tasks 1 and 2 and used unchanged afterward. `encode_bit_offset` and `decode_bit_offset` are defined in Task 4 and reused in Task 6.
- Known ordering wrinkle: Task 8's test file needs `build_index` from Task 9 to compile. That is called out in Task 8 step 2 and resolved in Task 9 step 6. Implementers working strictly task-by-task should expect a red test file between those two tasks.
