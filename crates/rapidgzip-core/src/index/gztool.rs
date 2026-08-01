//! [gztool](https://github.com/circulosmeos/gztool) index file import/export.
//!
//! On-disk layout matches `serialize_index_to_file` / `deserialize_index_from_file`
//! for **complete** indexes (`have == size`). Growing/incomplete indexes are
//! rejected. All multi-byte integers are big-endian. Predecessor windows are
//! stored zlib-compressed (default zlib wrapper), matching gztool's
//! `compress_chunk` / `decompress_chunk`.

use super::{
    Checkpoint, GzipIndex, INDEXED_GZIP_WINDOW_SIZE, IndexError, StoredWindow, WindowMap,
    decode_bit_offset, encode_bit_offset, read_exact, zlib_compress_window, zlib_decompress_window,
};
use std::io::{Read, Write};

/// gztool magic for indexes without per-point line counters (version 0).
pub const GZTOOL_MAGIC_V0: &[u8; 8] = b"gzipindx";

/// gztool magic for indexes with per-point line counters (version 1).
pub const GZTOOL_MAGIC_V1: &[u8; 8] = b"gzipindX";

/// Maximum accepted on-disk compressed window payload (zlib wrapper over ≤32 KiB).
const GZTOOL_MAX_COMPRESSED_WINDOW: u32 = 40 * 1024;

/// Writes `index` in [gztool](https://github.com/circulosmeos/gztool) format.
///
/// Layout matches `serialize_index_to_file` for a **complete** index:
/// 16-byte header (`0u64` + magic), optional v1 line-format field, point
/// count twice (`have` / `size`), each point (`out`, `in`, `bits`,
/// zlib-compressed window with length prefix), then uncompressed size and
/// (v1 only) total line count. All multi-byte integers are big-endian.
///
/// Bit offsets use the same zran packing as [`encode_bit_offset`].
pub fn write_gztool_index(
    index: &GzipIndex,
    writer: &mut impl Write,
    with_lines: bool,
) -> Result<(), IndexError> {
    let window_size = INDEXED_GZIP_WINDOW_SIZE as usize;
    let have = index.checkpoints.len() as u64;

    write_u64_be(writer, 0)?;
    if with_lines {
        writer.write_all(GZTOOL_MAGIC_V1).map_err(IndexError::io)?;
        // line_number_format: 0 = Unix LF / Windows CRLF (gztool default).
        write_u32_be(writer, 0)?;
    } else {
        writer.write_all(GZTOOL_MAGIC_V0).map_err(IndexError::io)?;
    }

    // Complete index: have == size.
    write_u64_be(writer, have)?;
    write_u64_be(writer, have)?;

    let mut max_line = 0u64;
    for checkpoint in &index.checkpoints {
        let (byte_offset, bits_field) = encode_bit_offset(checkpoint.compressed_offset_in_bits);
        write_u64_be(writer, checkpoint.uncompressed_offset_in_bytes)?;
        write_u64_be(writer, byte_offset)?;
        write_u32_be(writer, u32::from(bits_field))?;

        let compressed_window = match index.windows.get(checkpoint.compressed_offset_in_bits) {
            Some(window) if !window.is_empty() => {
                let payload = window.payload_for_export(window_size)?.ok_or(
                    IndexError::InvalidCheckpoint("non-empty window produced no export payload"),
                )?;
                Some(zlib_compress_window(&payload)?)
            }
            _ => None,
        };

        match compressed_window {
            Some(payload) => {
                let len = u32::try_from(payload.len()).map_err(|_| {
                    IndexError::InvalidCheckpoint("compressed window does not fit in u32")
                })?;
                if len > GZTOOL_MAX_COMPRESSED_WINDOW {
                    return Err(IndexError::InvalidCheckpoint(
                        "compressed window exceeds maximum size",
                    ));
                }
                write_u32_be(writer, len)?;
                writer.write_all(&payload).map_err(IndexError::io)?;
            }
            None => {
                write_u32_be(writer, 0)?;
            }
        }

        if with_lines {
            write_u64_be(writer, checkpoint.line_offset)?;
            max_line = max_line.max(checkpoint.line_offset);
        }
    }

    write_u64_be(writer, index.uncompressed_size_in_bytes)?;
    if with_lines {
        write_u64_be(writer, max_line)?;
    }
    Ok(())
}

/// Reads a complete [gztool](https://github.com/circulosmeos/gztool) index.
///
/// Accepts version 0 (`gzipindx`) and version 1 (`gzipindX`). Incomplete
/// indexes (`have != size`, including growing-index placeholders) are
/// rejected. Windows are zlib-decompressed into [`StoredWindow::from_raw`];
/// if zlib inflate fails and the on-disk payload is ≤ 32 KiB, the payload is
/// treated as an uncompressed window (rare uncompressed path).
///
/// When `archive_size` is `Some`, it becomes
/// [`GzipIndex::compressed_size_in_bytes`].
pub fn read_gztool_index(
    reader: &mut impl Read,
    archive_size: Option<u64>,
) -> Result<GzipIndex, IndexError> {
    let mut header = [0u8; 16];
    read_exact(reader, &mut header)?;
    if header[..8] != [0u8; 8] {
        return Err(IndexError::BadMagic {
            found: header.to_vec(),
        });
    }
    let with_lines = if &header[8..16] == GZTOOL_MAGIC_V0 {
        false
    } else if &header[8..16] == GZTOOL_MAGIC_V1 {
        true
    } else {
        return Err(IndexError::BadMagic {
            found: header.to_vec(),
        });
    };

    if with_lines {
        // line_number_format — accepted but not interpreted.
        let _line_number_format = read_u32_be(reader)?;
    }

    let have = read_u64_be(reader)?;
    let size = read_u64_be(reader)?;
    if have != size {
        return Err(IndexError::InvalidCheckpoint(
            "incomplete gztool index (have != size)",
        ));
    }
    if have > usize::MAX as u64 {
        return Err(IndexError::InvalidCheckpoint(
            "gztool checkpoint count does not fit in usize",
        ));
    }
    let checkpoint_count = have as usize;

    let compressed_size_in_bytes = archive_size.unwrap_or(u64::MAX);
    let mut checkpoints = Vec::with_capacity(checkpoint_count);
    let mut windows = WindowMap::new();

    for _ in 0..checkpoint_count {
        let uncompressed_offset_in_bytes = read_u64_be(reader)?;
        let byte_offset = read_u64_be(reader)?;
        let bits = read_u32_be(reader)?;
        if bits > 7 {
            return Err(IndexError::InvalidCheckpoint(
                "denormal compressed offset: bit field >= 8",
            ));
        }
        let compressed_offset_in_bits = decode_bit_offset(byte_offset, bits as u8)?;

        if compressed_size_in_bytes != u64::MAX && byte_offset > compressed_size_in_bytes {
            return Err(IndexError::InvalidCheckpoint(
                "checkpoint compressed offset is after the file end",
            ));
        }

        let window_size = read_u32_be(reader)?;
        if window_size > GZTOOL_MAX_COMPRESSED_WINDOW {
            return Err(IndexError::InvalidCheckpoint(
                "compressed window exceeds maximum size",
            ));
        }
        let stored = if window_size == 0 {
            StoredWindow::empty()
        } else {
            let mut compressed = vec![0u8; window_size as usize];
            read_exact(reader, &mut compressed)?;
            decompress_gztool_window(&compressed)?
        };

        let line_offset = if with_lines { read_u64_be(reader)? } else { 0 };

        checkpoints.push(Checkpoint {
            compressed_offset_in_bits,
            uncompressed_offset_in_bytes,
            line_offset,
        });
        windows.insert(compressed_offset_in_bits, stored);
    }

    // Tail: uncompressed size; v1 also stores total line count.
    let uncompressed_size_in_bytes = read_u64_be(reader)?;
    if with_lines {
        let _number_of_lines = read_u64_be(reader)?;
    }

    if uncompressed_size_in_bytes != u64::MAX {
        for checkpoint in &checkpoints {
            if checkpoint.uncompressed_offset_in_bytes > uncompressed_size_in_bytes {
                return Err(IndexError::InvalidCheckpoint(
                    "checkpoint uncompressed offset is after the file end",
                ));
            }
        }
    }

    let index = GzipIndex {
        compressed_size_in_bytes,
        uncompressed_size_in_bytes,
        checkpoint_spacing: 0,
        window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
        checkpoints,
        windows,
        has_line_offsets: with_lines,
    };
    index.validate()?;
    Ok(index)
}

/// Returns true when `prefix` is a full 16-byte gztool header magic.
pub(super) fn is_gztool_prefix(prefix: &[u8]) -> bool {
    prefix.len() >= 16
        && prefix[..8] == [0u8; 8]
        && (&prefix[8..16] == GZTOOL_MAGIC_V0.as_slice()
            || &prefix[8..16] == GZTOOL_MAGIC_V1.as_slice())
}

fn write_u32_be(writer: &mut impl Write, value: u32) -> Result<(), IndexError> {
    writer
        .write_all(&value.to_be_bytes())
        .map_err(IndexError::io)
}

fn write_u64_be(writer: &mut impl Write, value: u64) -> Result<(), IndexError> {
    writer
        .write_all(&value.to_be_bytes())
        .map_err(IndexError::io)
}

fn read_u32_be(reader: &mut impl Read) -> Result<u32, IndexError> {
    let mut buf = [0u8; 4];
    read_exact(reader, &mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_u64_be(reader: &mut impl Read) -> Result<u64, IndexError> {
    let mut buf = [0u8; 8];
    read_exact(reader, &mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

/// zlib-wrapper decompress of a gztool window payload into at most 32 KiB.
///
/// Empty payloads are empty windows. Non-empty payloads must inflate under the
/// zlib wrapper; corrupt short blobs are not accepted as raw windows (that
/// would install garbage dictionaries and produce wrong seeks without error).
fn decompress_gztool_window(compressed: &[u8]) -> Result<StoredWindow, IndexError> {
    if compressed.is_empty() {
        return Ok(StoredWindow::empty());
    }
    let dest = zlib_decompress_window(compressed)?;
    Ok(StoredWindow::from_raw(dest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::WindowCompression;
    use std::io::Cursor;

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

    fn round_trip_gztool(index: &GzipIndex, with_lines: bool) -> GzipIndex {
        let mut bytes = Vec::new();
        write_gztool_index(index, &mut bytes, with_lines).expect("gztool export");
        read_gztool_index(
            &mut Cursor::new(&bytes),
            Some(index.compressed_size_in_bytes),
        )
        .expect("gztool import")
    }

    #[test]
    fn gztool_round_trip_empty_index() {
        let index = GzipIndex {
            compressed_size_in_bytes: 0,
            uncompressed_size_in_bytes: 0,
            checkpoint_spacing: 0,
            window_size_in_bytes: INDEXED_GZIP_WINDOW_SIZE,
            checkpoints: Vec::new(),
            windows: WindowMap::new(),
            has_line_offsets: false,
        };
        let restored = round_trip_gztool(&index, false);
        assert!(restored.checkpoints.is_empty());
        assert_eq!(restored.uncompressed_size_in_bytes, 0);
        assert!(!restored.has_line_offsets);
        assert_eq!(restored.compressed_size_in_bytes, 0);
    }

    #[test]
    fn gztool_round_trip_checkpoints_and_windows() {
        let index = sample_index_with_windows();
        let restored = round_trip_gztool(&index, false);

        assert_eq!(restored.checkpoints.len(), index.checkpoints.len());
        assert!(!restored.has_line_offsets);
        for (orig, got) in index.checkpoints.iter().zip(restored.checkpoints.iter()) {
            assert_eq!(
                got.compressed_offset_in_bits,
                orig.compressed_offset_in_bits
            );
            assert_eq!(
                got.uncompressed_offset_in_bytes,
                orig.uncompressed_offset_in_bytes
            );
            assert_eq!(got.line_offset, 0);
        }

        let empty_bits = index.checkpoints[0].compressed_offset_in_bits;
        assert!(restored.windows.get(empty_bits).unwrap().is_empty());

        for cp in &index.checkpoints[1..] {
            let window = restored
                .windows
                .get(cp.compressed_offset_in_bits)
                .expect("window");
            assert_eq!(window.len(), INDEXED_GZIP_WINDOW_SIZE as usize);
            let raw = window.decompressed().unwrap();
            assert!(
                raw[..INDEXED_GZIP_WINDOW_SIZE as usize - 16]
                    .iter()
                    .all(|&b| b == 0)
            );
            assert_eq!(&raw[INDEXED_GZIP_WINDOW_SIZE as usize - 16..], &[0xAB; 16]);
        }
    }

    #[test]
    fn gztool_round_trip_with_line_offsets() {
        let mut index = sample_index_with_windows();
        index.has_line_offsets = true;
        for (i, cp) in index.checkpoints.iter_mut().enumerate() {
            cp.line_offset = (i as u64 + 1) * 10;
        }
        let restored = round_trip_gztool(&index, true);
        assert!(restored.has_line_offsets);
        for (i, cp) in restored.checkpoints.iter().enumerate() {
            assert_eq!(cp.line_offset, (i as u64 + 1) * 10);
        }
        assert_eq!(
            restored.checkpoints[0].compressed_offset_in_bits,
            index.checkpoints[0].compressed_offset_in_bits
        );
    }

    #[test]
    fn gztool_export_magic_bytes() {
        let index = GzipIndex::new();
        let mut v0 = Vec::new();
        write_gztool_index(&index, &mut v0, false).unwrap();
        assert_eq!(&v0[0..8], &[0u8; 8]);
        assert_eq!(&v0[8..16], GZTOOL_MAGIC_V0);

        let mut v1 = Vec::new();
        write_gztool_index(&index, &mut v1, true).unwrap();
        assert_eq!(&v1[0..8], &[0u8; 8]);
        assert_eq!(&v1[8..16], GZTOOL_MAGIC_V1);
    }

    #[test]
    fn gztool_rejects_incomplete_index() {
        // Complete empty header shape but have != size.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0u64.to_be_bytes());
        bytes.extend_from_slice(GZTOOL_MAGIC_V0);
        bytes.extend_from_slice(&0u64.to_be_bytes()); // have
        bytes.extend_from_slice(&u64::MAX.to_be_bytes()); // size (growing)
        let err = read_gztool_index(&mut Cursor::new(&bytes), None).unwrap_err();
        assert!(matches!(err, IndexError::InvalidCheckpoint(_)));
    }

    #[test]
    fn zlib_round_trip_window_payload() {
        let payload = vec![0xCDu8; INDEXED_GZIP_WINDOW_SIZE as usize];
        let compressed = zlib_compress_window(&payload).unwrap();
        assert!(compressed.len() < payload.len());
        let restored = decompress_gztool_window(&compressed).unwrap();
        assert_eq!(
            restored.decompressed().unwrap().as_ref(),
            payload.as_slice()
        );
        assert_eq!(restored.compression(), WindowCompression::None);
    }
}
