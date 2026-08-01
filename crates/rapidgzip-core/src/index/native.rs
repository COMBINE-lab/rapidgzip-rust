//! Native versioned index format.
//!
//! This is the only format under this crate's control, so it stores everything
//! the in-memory index holds and round-trips exactly, including line offsets
//! and compressed window payloads.
//!
//! Layout, little-endian: magic `RGZIDX01`, `u16` version, `u16` flags, `u64`
//! compressed size, `u64` uncompressed size, `u64` checkpoint spacing, `u64`
//! total line count ([`u64::MAX`] when absent), `u64` checkpoint count. Then
//! per checkpoint: `u64` compressed bit offset, `u64` uncompressed offset,
//! `u64` line offset, `u8` window kind, `u32` payload length, payload bytes.

use super::{
    Checkpoint, GzipIndex, IndexError, StoredWindow, WINDOW_SIZE, read_exact_bytes, read_u8,
    read_u32_le, read_u64_le, write_u32_le, write_u64_le,
};
use std::io::{Read, Write};

/// Magic bytes identifying a native index.
const MAGIC: &[u8; 8] = b"RGZIDX01";

/// Highest native version this crate reads and writes.
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
    writer
        .write_all(&0u16.to_le_bytes())
        .map_err(IndexError::io)?;
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
            Some(window) if !window.is_empty() => {
                let kind = if window.is_compressed() {
                    WINDOW_ZLIB
                } else {
                    WINDOW_RAW
                };
                let payload = window.payload();
                let length =
                    u32::try_from(payload.len()).map_err(|_| IndexError::ExcessiveLength {
                        what: "window payload length",
                        value: payload.len() as u64,
                    })?;
                writer.write_all(&[kind]).map_err(IndexError::io)?;
                write_u32_le(writer, length)?;
                writer.write_all(payload).map_err(IndexError::io)?;
            }
            _ => {
                writer.write_all(&[WINDOW_ABSENT]).map_err(IndexError::io)?;
                write_u32_le(writer, 0)?;
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

    let mut version_bytes = [0u8; 2];
    read_exact_bytes(reader, &mut version_bytes)?;
    let version = u16::from_le_bytes(version_bytes);
    if version == 0 || version > MAX_VERSION {
        return Err(IndexError::UnsupportedVersion(
            u8::try_from(version).unwrap_or(u8::MAX),
        ));
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
