//! [htslib](https://github.com/samtools/htslib) BGZF block index (`.gzi` / BGZI)
//! import/export.
//!
//! Little-endian on-disk layout matches `bgzf_index_dump_hfile` /
//! `bgzf_index_load_hfile` in htslib `bgzf.c`:
//!
//! - `u64` number of pairs `n`
//! - then `n` pairs of `(u64 compressed_offset, u64 uncompressed_offset)` —
//!   offsets of BGZF block starts **after** the first block (the first block
//!   is always implicit at compressed/uncompressed 0).
//!
//! Files are commonly named `file.gz.gzi`. BGZF members are independent, so
//! imported checkpoints use empty predecessor windows (member boundaries).
//! Seek / `decode_with_index` skip a gzip header when an empty-window
//! checkpoint lands on member magic (htslib stores block header starts).
//!
//! # Uncompressed size
//!
//! htslib indexes list block *starts* only; they do **not** store the total
//! uncompressed size. Import therefore sets
//! [`GzipIndex::uncompressed_size_in_bytes`] to `u64::MAX` (unknown). Full
//! decode uses an open-ended final segment; `--count` without a known size
//! falls back to a real decode.

use super::{
    Checkpoint, GzipIndex, INDEXED_GZIP_WINDOW_SIZE, IndexError, StoredWindow, WindowMap,
    read_u64_le, write_u64_le,
};
use std::io::{Read, Write};

/// Maximum accepted pair count (guards against hostile `n` values).
const BGZI_MAX_PAIRS: u64 = 1 << 28;

/// Returns true when `checkpoint` looks like a synthetic EOF point rather than
/// a real BGZF block header start (and must not be written to `.gzi`).
///
/// Requires a known finite archive size: default/`0` sizes are not treated as
/// bounds (that would mark every checkpoint as EOF).
fn is_synthetic_eof(index: &GzipIndex, checkpoint: &Checkpoint) -> bool {
    let known_u = index.uncompressed_size_in_bytes;
    if known_u != 0 && known_u != u64::MAX && checkpoint.uncompressed_offset_in_bytes >= known_u {
        return true;
    }
    let known_c = index.compressed_size_in_bytes;
    if known_c != 0 && known_c != u64::MAX {
        let max_bits = known_c.saturating_mul(8);
        if checkpoint.compressed_offset_in_bits >= max_bits {
            return true;
        }
    }
    false
}

/// Writes `index` in htslib BGZF `.gzi` (BGZI) format.
///
/// Emits one pair for every **empty-window**, non-EOF checkpoint after the
/// first (htslib skips the implicit `(0, 0)` entry). Mid-stream checkpoints
/// that require a predecessor window are refused — BGZI cannot represent
/// them and re-import would install empty windows (wrong seeks).
///
/// When the index is empty, writes `n = 0`.
pub fn write_bgzi_index(index: &GzipIndex, writer: &mut impl Write) -> Result<(), IndexError> {
    // Validate and collect exportable pairs (after the implicit origin).
    let mut pairs: Vec<(u64, u64)> = Vec::new();
    let mut prev_c = 0u64;
    let mut prev_u = 0u64;

    for (i, checkpoint) in index.checkpoints.iter().enumerate() {
        if is_synthetic_eof(index, checkpoint) {
            continue;
        }
        if !checkpoint.compressed_offset_in_bits.is_multiple_of(8) {
            return Err(IndexError::InvalidCheckpoint(
                "BGZI requires byte-aligned compressed offsets",
            ));
        }
        let caddr = checkpoint.compressed_offset_in_bits / 8;
        let uaddr = checkpoint.uncompressed_offset_in_bytes;

        // Non-empty predecessor windows cannot be stored in BGZI.
        let window = index.window_for(checkpoint.compressed_offset_in_bits);
        let empty = window.map(|w| w.is_empty()).unwrap_or(true);
        if !empty {
            return Err(IndexError::InvalidCheckpoint(
                "BGZI export requires empty windows at every checkpoint \
                 (BGZF member boundaries only; not ordinary gzip mid-stream points)",
            ));
        }

        if i == 0 {
            // Implicit origin; do not emit a pair, but seed monotonicity.
            prev_c = caddr;
            prev_u = uaddr;
            continue;
        }

        if caddr < prev_c {
            return Err(IndexError::InvalidCheckpoint(
                "BGZI compressed offsets are not monotonic",
            ));
        }
        if uaddr < prev_u {
            return Err(IndexError::InvalidCheckpoint(
                "BGZI uncompressed offsets are not monotonic",
            ));
        }
        pairs.push((caddr, uaddr));
        prev_c = caddr;
        prev_u = uaddr;
    }

    let n = u64::try_from(pairs.len())
        .map_err(|_| IndexError::InvalidCheckpoint("BGZI pair count does not fit in u64"))?;
    if n > BGZI_MAX_PAIRS {
        return Err(IndexError::InvalidCheckpoint(
            "BGZI pair count exceeds maximum",
        ));
    }
    write_u64_le(writer, n)?;
    for (caddr, uaddr) in pairs {
        write_u64_le(writer, caddr)?;
        write_u64_le(writer, uaddr)?;
    }
    Ok(())
}

/// Reads an htslib BGZF `.gzi` (BGZI) index.
///
/// Reconstructs checkpoints as: always `(compressed=0, uncompressed=0)`, then
/// one checkpoint per pair. Every window is empty. [`GzipIndex::has_line_offsets`]
/// is `false`.
///
/// When `archive_size` is `Some`, it becomes
/// [`GzipIndex::compressed_size_in_bytes`]. Uncompressed size is always
/// `u64::MAX` (unknown): htslib pairs are block starts, not an EOF bound.
///
/// Validates that offsets are non-decreasing and that compressed byte offsets
/// do not exceed `archive_size` when known.
pub fn read_bgzi_index(
    reader: &mut impl Read,
    archive_size: Option<u64>,
) -> Result<GzipIndex, IndexError> {
    let n = read_u64_le(reader)?;
    if n > BGZI_MAX_PAIRS {
        return Err(IndexError::InvalidCheckpoint(
            "BGZI pair count exceeds maximum",
        ));
    }
    if n > usize::MAX as u64 {
        return Err(IndexError::InvalidCheckpoint(
            "BGZI pair count does not fit in usize",
        ));
    }
    let pair_count = n as usize;

    let compressed_size_in_bytes = archive_size.unwrap_or(u64::MAX);

    let mut checkpoints = Vec::with_capacity(pair_count + 1);
    let mut windows = WindowMap::new();

    // Implicit first block.
    checkpoints.push(Checkpoint {
        compressed_offset_in_bits: 0,
        uncompressed_offset_in_bytes: 0,
        line_offset: 0,
    });
    windows.insert(0, StoredWindow::empty());

    let mut prev_c = 0u64;
    let mut prev_u = 0u64;

    for _ in 0..pair_count {
        let caddr = read_u64_le(reader)?;
        let uaddr = read_u64_le(reader)?;

        if caddr < prev_c {
            return Err(IndexError::InvalidCheckpoint(
                "BGZI compressed offsets are not monotonic",
            ));
        }
        if uaddr < prev_u {
            return Err(IndexError::InvalidCheckpoint(
                "BGZI uncompressed offsets are not monotonic",
            ));
        }
        if compressed_size_in_bytes != u64::MAX && caddr > compressed_size_in_bytes {
            return Err(IndexError::InvalidCheckpoint(
                "checkpoint compressed offset is after the file end",
            ));
        }

        let compressed_offset_in_bits = caddr.checked_mul(8).ok_or(
            IndexError::InvalidCheckpoint("compressed byte offset overflows bit count"),
        )?;

        // Skip exact duplicates of the implicit origin (defensive).
        if compressed_offset_in_bits == 0 && uaddr == 0 {
            prev_c = caddr;
            prev_u = uaddr;
            continue;
        }

        checkpoints.push(Checkpoint {
            compressed_offset_in_bits,
            uncompressed_offset_in_bytes: uaddr,
            line_offset: 0,
        });
        windows.insert(compressed_offset_in_bits, StoredWindow::empty());
        prev_c = caddr;
        prev_u = uaddr;
    }

    let index = GzipIndex {
        compressed_size_in_bytes,
        // htslib does not store total uncompressed size.
        uncompressed_size_in_bytes: u64::MAX,
        checkpoint_spacing: 0,
        window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
        checkpoints,
        windows,
        has_line_offsets: false,
    };
    index.validate()?;
    Ok(index)
}

/// Attempts to parse a BGZI index from a fully buffered byte slice.
///
/// Used by [`super::read_gzip_index`] auto-detect: accepts only when the buffer
/// length is exactly `8 + 16*n` and the structure validates.
pub(super) fn try_read_bgzi_buffer(
    bytes: &[u8],
    archive_size: Option<u64>,
) -> Result<GzipIndex, IndexError> {
    if bytes.len() < 8 {
        return Err(IndexError::Truncated);
    }
    let n = u64::from_le_bytes(bytes[0..8].try_into().expect("8 bytes"));
    if n > BGZI_MAX_PAIRS {
        return Err(IndexError::BadMagic {
            found: bytes[..bytes.len().min(16)].to_vec(),
        });
    }
    let expected = 8usize
        .checked_add(
            (n as usize)
                .checked_mul(16)
                .ok_or(IndexError::InvalidCheckpoint(
                    "BGZI size calculation overflow",
                ))?,
        )
        .ok_or(IndexError::InvalidCheckpoint(
            "BGZI size calculation overflow",
        ))?;
    if bytes.len() != expected {
        return Err(IndexError::BadMagic {
            found: bytes[..bytes.len().min(16)].to_vec(),
        });
    }
    read_bgzi_index(&mut std::io::Cursor::new(bytes), archive_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn sample_bgzf_style_index() -> GzipIndex {
        // Three real block starts + synthetic EOF (must be stripped on export).
        let points = [
            (0u64, 0u64),
            (100u64, 50_000u64),
            (200u64, 100_000u64),
            (280u64, 150_000u64), // EOF: compressed end, full uncompressed
        ];
        let mut windows = WindowMap::new();
        let mut checkpoints = Vec::new();
        for &(c_bytes, u) in &points {
            let bits = c_bytes * 8;
            checkpoints.push(Checkpoint {
                compressed_offset_in_bits: bits,
                uncompressed_offset_in_bytes: u,
                line_offset: 0,
            });
            windows.insert(bits, StoredWindow::empty());
        }
        GzipIndex {
            compressed_size_in_bytes: 280,
            uncompressed_size_in_bytes: 150_000,
            checkpoint_spacing: 0,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints,
            windows,
            has_line_offsets: false,
        }
    }

    #[test]
    fn bgzi_round_trip_strips_eof_and_leaves_size_unknown() {
        let index = sample_bgzf_style_index();
        let mut bytes = Vec::new();
        write_bgzi_index(&index, &mut bytes).expect("export");
        // n = 2 pairs (skip origin + strip EOF).
        assert_eq!(u64::from_le_bytes(bytes[0..8].try_into().unwrap()), 2);
        assert_eq!(bytes.len(), 8 + 2 * 16);

        let restored = read_bgzi_index(&mut Cursor::new(&bytes), Some(280)).expect("import");
        // Origin + two block starts (no EOF).
        assert_eq!(restored.checkpoints.len(), 3);
        assert!(!restored.has_line_offsets);
        assert_eq!(restored.compressed_size_in_bytes, 280);
        assert_eq!(restored.uncompressed_size_in_bytes, u64::MAX);
        assert_eq!(restored.checkpoints[1].uncompressed_offset_in_bytes, 50_000);
        assert_eq!(
            restored.checkpoints[2].uncompressed_offset_in_bytes,
            100_000
        );
        for cp in &restored.checkpoints {
            assert!(
                restored
                    .windows
                    .get(cp.compressed_offset_in_bits)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn bgzi_empty_index() {
        let index = GzipIndex::new();
        let mut bytes = Vec::new();
        write_bgzi_index(&index, &mut bytes).unwrap();
        assert_eq!(bytes, 0u64.to_le_bytes());
        let restored = read_bgzi_index(&mut Cursor::new(&bytes), None).unwrap();
        // Implicit origin only.
        assert_eq!(restored.checkpoints.len(), 1);
        assert_eq!(restored.checkpoints[0].compressed_offset_in_bits, 0);
        assert_eq!(restored.uncompressed_size_in_bytes, u64::MAX);
    }

    #[test]
    fn bgzi_rejects_non_empty_window_export() {
        let mut index = sample_bgzf_style_index();
        // Mid-stream history at second checkpoint.
        index
            .windows
            .insert(100 * 8, StoredWindow::from_raw(vec![1u8; 32]));
        let err = write_bgzi_index(&index, &mut Vec::new()).unwrap_err();
        assert!(matches!(err, IndexError::InvalidCheckpoint(_)));
    }

    #[test]
    fn bgzi_rejects_non_monotonic() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u64.to_le_bytes());
        bytes.extend_from_slice(&100u64.to_le_bytes());
        bytes.extend_from_slice(&1000u64.to_le_bytes());
        // Second pair goes backwards in compressed domain.
        bytes.extend_from_slice(&50u64.to_le_bytes());
        bytes.extend_from_slice(&2000u64.to_le_bytes());
        let err = read_bgzi_index(&mut Cursor::new(&bytes), None).unwrap_err();
        assert!(matches!(err, IndexError::InvalidCheckpoint(_)));
    }

    #[test]
    fn bgzi_rejects_truncated() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u64.to_le_bytes());
        bytes.extend_from_slice(&100u64.to_le_bytes());
        // Missing rest of pairs.
        let err = read_bgzi_index(&mut Cursor::new(&bytes), None).unwrap_err();
        assert!(matches!(err, IndexError::Truncated));
    }

    #[test]
    fn bgzi_rejects_non_byte_aligned_export() {
        let mut index = GzipIndex::new();
        index.checkpoints = vec![
            Checkpoint {
                compressed_offset_in_bits: 0,
                uncompressed_offset_in_bytes: 0,
                line_offset: 0,
            },
            Checkpoint {
                compressed_offset_in_bits: 3, // not byte-aligned
                uncompressed_offset_in_bytes: 100,
                line_offset: 0,
            },
        ];
        index.windows.insert(0, StoredWindow::empty());
        index.windows.insert(3, StoredWindow::empty());
        let err = write_bgzi_index(&index, &mut Vec::new()).unwrap_err();
        assert!(matches!(err, IndexError::InvalidCheckpoint(_)));
    }

    #[test]
    fn try_read_bgzi_buffer_rejects_wrong_length() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&10u64.to_le_bytes());
        bytes.extend_from_slice(&20u64.to_le_bytes());
        bytes.push(0xFF); // trailing
        let err = try_read_bgzi_buffer(&bytes, None).unwrap_err();
        assert!(matches!(err, IndexError::BadMagic { .. }));
    }

    #[test]
    fn htslib_layout_pair_order_is_caddr_then_uaddr() {
        let index = sample_bgzf_style_index();
        let mut bytes = Vec::new();
        write_bgzi_index(&index, &mut bytes).unwrap();
        // First pair: caddr=100, uaddr=50_000 (EOF stripped).
        assert_eq!(u64::from_le_bytes(bytes[8..16].try_into().unwrap()), 100);
        assert_eq!(
            u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            50_000
        );
    }

    #[test]
    fn htslib_shaped_import_has_unknown_size() {
        // Pure block starts: no EOF pair (as `bgzip -i` produces).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u64.to_le_bytes());
        bytes.extend_from_slice(&100u64.to_le_bytes());
        bytes.extend_from_slice(&50_000u64.to_le_bytes());
        bytes.extend_from_slice(&200u64.to_le_bytes());
        bytes.extend_from_slice(&100_000u64.to_le_bytes());
        let restored = read_bgzi_index(&mut Cursor::new(&bytes), Some(280)).unwrap();
        assert_eq!(restored.checkpoints.len(), 3);
        assert_eq!(restored.uncompressed_size_in_bytes, u64::MAX);
        assert_eq!(
            restored.checkpoints[2].uncompressed_offset_in_bytes,
            100_000
        );
    }
}
