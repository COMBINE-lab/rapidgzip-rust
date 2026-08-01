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
    Checkpoint, GzipIndex, IndexError, StoredWindow, WINDOW_SIZE, read_exact_bytes, read_u8,
    read_u32_le, read_u64_le, write_u32_le, write_u64_le,
};
use std::io::{Read, Write};

/// Magic bytes identifying an indexed_gzip index.
const MAGIC: &[u8; 5] = b"GZIDX";

/// Highest indexed_gzip version this crate reads.
const MAX_VERSION: u8 = 1;

/// Upper bound on the checkpoint count, guarding against hostile headers.
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
///
/// A bits field of 8 or more, or a non-zero bits field at byte offset zero,
/// describes a position before the start of the source and is refused.
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

    if let Some(archive_size) = archive_size {
        if archive_size != index.compressed_size_in_bytes {
            return Err(IndexError::ArchiveSizeMismatch {
                index_size: index.compressed_size_in_bytes,
                archive_size,
            });
        }
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
        // Version 0 has no per-record data flag: every point after the first
        // carries a window.
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
