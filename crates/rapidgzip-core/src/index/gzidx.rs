//! indexed_gzip `GZIDX` import and export.

use super::{
    Checkpoint, CheckpointKind, GzipIndex, IndexError, IndexReadOptions, StoredWindow, WINDOW_SIZE,
    read_exact_bytes, read_u8, read_u32_le, read_u64_le, write_u32_le, write_u64_le,
};
use std::io::{Read, Write};

const MAGIC: &[u8; 5] = b"GZIDX";
const MAX_VERSION: u8 = 1;

/// Encodes an absolute raw-DEFLATE bit offset as zran's byte/bits pair.
#[must_use]
pub fn encode_bit_offset(compressed_offset_in_bits: u64) -> (u64, u8) {
    let remainder = (compressed_offset_in_bits % 8) as u8;
    if remainder == 0 {
        (compressed_offset_in_bits / 8, 0)
    } else {
        (compressed_offset_in_bits / 8 + 1, 8 - remainder)
    }
}

/// Decodes zran's byte/bits pair into an absolute raw-DEFLATE bit offset.
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
    index.validate()?;
    let compressed_size = index
        .compressed_size_in_bytes
        .ok_or(IndexError::MissingMetadata(
            "compressed size for GZIDX export",
        ))?;
    let uncompressed_size = index
        .uncompressed_size_in_bytes
        .ok_or(IndexError::MissingMetadata(
            "uncompressed size for GZIDX export",
        ))?;
    let spacing = index
        .checkpoint_spacing_in_bytes
        .ok_or(IndexError::MissingMetadata(
            "checkpoint spacing for GZIDX export",
        ))?;
    let spacing = u32::try_from(spacing).map_err(|_| IndexError::ExcessiveLength {
        what: "GZIDX checkpoint spacing",
        value: spacing,
    })?;
    let count =
        u32::try_from(index.checkpoints.len()).map_err(|_| IndexError::ExcessiveLength {
            what: "checkpoint count",
            value: u64::try_from(index.checkpoints.len()).unwrap_or(u64::MAX),
        })?;
    if index
        .checkpoints
        .iter()
        .any(|point| matches!(point.kind, CheckpointKind::MemberHeader))
    {
        return Err(IndexError::InvalidCheckpoint(
            "GZIDX export requires raw-DEFLATE resume offsets",
        ));
    }

    writer.write_all(MAGIC).map_err(IndexError::io)?;
    writer
        .write_all(&[MAX_VERSION, 0])
        .map_err(IndexError::io)?;
    write_u64_le(writer, compressed_size)?;
    write_u64_le(writer, uncompressed_size)?;
    write_u32_le(writer, spacing)?;
    write_u32_le(writer, WINDOW_SIZE as u32)?;
    write_u32_le(writer, count)?;

    for checkpoint in &index.checkpoints {
        let (byte_offset, bits_field) = encode_bit_offset(checkpoint.compressed_offset_in_bits);
        write_u64_le(writer, byte_offset)?;
        write_u64_le(writer, checkpoint.uncompressed_offset_in_bytes)?;
        let has_window = index
            .windows
            .get(checkpoint.compressed_offset_in_bits)
            .is_some();
        writer
            .write_all(&[bits_field, u8::from(has_window)])
            .map_err(IndexError::io)?;
    }
    for checkpoint in &index.checkpoints {
        if let Some(window) = index.windows.get(checkpoint.compressed_offset_in_bits) {
            writer
                .write_all(&window.decompressed()?)
                .map_err(IndexError::io)?;
        }
    }
    Ok(())
}

pub(crate) fn read_gzidx(
    reader: &mut impl Read,
    archive_size: Option<u64>,
    options: IndexReadOptions,
) -> Result<GzipIndex, IndexError> {
    let mut magic = [0_u8; 5];
    read_exact_bytes(reader, &mut magic)?;
    if &magic != MAGIC {
        return Err(IndexError::BadMagic {
            found: magic.to_vec(),
        });
    }
    let version = read_u8(reader)?;
    if version > MAX_VERSION {
        return Err(IndexError::UnsupportedVersion(u64::from(version)));
    }
    let flags = read_u8(reader)?;
    if flags != 0 {
        return Err(IndexError::UnsupportedFlags {
            flags: u64::from(flags),
        });
    }

    let compressed_size = read_u64_le(reader)?;
    let uncompressed_size = read_u64_le(reader)?;
    let spacing = read_u32_le(reader)?;
    let window_size = read_u32_le(reader)?;
    if window_size as usize != WINDOW_SIZE {
        return Err(IndexError::InvalidWindowSize(u64::from(window_size)));
    }
    if archive_size.is_some_and(|size| size != compressed_size) {
        return Err(IndexError::ArchiveSizeMismatch {
            index_size: compressed_size,
            archive_size: archive_size.expect("checked as some"),
        });
    }

    let count = read_u32_le(reader)? as usize;
    if count > options.max_checkpoints {
        return Err(IndexError::ExcessiveLength {
            what: "checkpoint count",
            value: count as u64,
        });
    }
    let mut records = Vec::new();
    records
        .try_reserve(count)
        .map_err(|_| IndexError::AllocationFailed {
            what: "GZIDX checkpoint records",
        })?;
    let mut window_count = 0_u64;
    for position in 0..count {
        let byte_offset = read_u64_le(reader)?;
        let uncompressed_offset_in_bytes = read_u64_le(reader)?;
        let bits_field = read_u8(reader)?;
        let has_window = if version == 0 {
            position != 0
        } else {
            match read_u8(reader)? {
                0 => false,
                1 => true,
                _ => {
                    return Err(IndexError::InvalidCheckpoint(
                        "GZIDX window flag is not zero or one",
                    ));
                }
            }
        };
        if byte_offset > compressed_size {
            return Err(IndexError::InvalidCheckpoint(
                "checkpoint compressed offset is after the source end",
            ));
        }
        if has_window {
            window_count = window_count
                .checked_add(1)
                .ok_or(IndexError::ExcessiveLength {
                    what: "aggregate window bytes",
                    value: u64::MAX,
                })?;
        }
        records.push((
            Checkpoint {
                compressed_offset_in_bits: decode_bit_offset(byte_offset, bits_field)?,
                uncompressed_offset_in_bytes,
                kind: CheckpointKind::DeflateBlock,
                line_offset: None,
            },
            has_window,
        ));
    }
    let window_bytes =
        window_count
            .checked_mul(WINDOW_SIZE as u64)
            .ok_or(IndexError::ExcessiveLength {
                what: "aggregate window bytes",
                value: u64::MAX,
            })?;
    if WINDOW_SIZE > options.max_window_payload_bytes || window_bytes > options.max_window_bytes {
        return Err(IndexError::ExcessiveLength {
            what: "aggregate window bytes",
            value: window_bytes,
        });
    }

    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = Some(compressed_size);
    index.uncompressed_size_in_bytes = Some(uncompressed_size);
    index.checkpoint_spacing_in_bytes = (spacing != 0).then_some(u64::from(spacing));
    for (checkpoint, has_window) in records {
        let window = if has_window {
            let mut payload = Vec::new();
            payload
                .try_reserve_exact(WINDOW_SIZE)
                .map_err(|_| IndexError::AllocationFailed {
                    what: "GZIDX window",
                })?;
            payload.resize(WINDOW_SIZE, 0);
            read_exact_bytes(reader, &mut payload)?;
            StoredWindow::from_raw(payload)?
        } else {
            StoredWindow::empty()
        };
        index.push(checkpoint, window)?;
    }
    index.validate()?;
    Ok(index)
}
