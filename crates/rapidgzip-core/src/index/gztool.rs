//! gztool index import and export.

use super::{
    Checkpoint, CheckpointKind, DeflateIndex, IndexError, IndexKind, IndexReadOptions,
    StoredWindow, decode_bit_offset, encode_bit_offset, read_exact_bytes, read_u32_be, read_u64_be,
    write_u32_be, write_u64_be, zlib_compress_window,
};
use std::io::{Read, Write};

const MAGIC_V0: &[u8; 8] = b"gzipindx";
const MAGIC_V1: &[u8; 8] = b"gzipindX";
const MAX_FORMAT_PAYLOAD: usize = 40 * 1024;

/// Whether a gztool index carries complete per-point line counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WithLines {
    /// Version 0 without line counters.
    No,
    /// Version 1. Every line counter must already be present.
    Yes,
}

pub(crate) fn write_gztool(
    index: &DeflateIndex,
    writer: &mut impl Write,
    lines: WithLines,
) -> Result<(), IndexError> {
    index.validate()?;
    if !matches!(index.kind, IndexKind::Gzip | IndexKind::Bgzf) {
        return Err(IndexError::IncompatibleFormat {
            operation: "gztool export",
            kind: index.kind,
        });
    }
    if index
        .checkpoints
        .iter()
        .any(|point| matches!(point.kind, CheckpointKind::GzipMemberHeader))
    {
        return Err(IndexError::InvalidCheckpoint(
            "gztool export requires raw-DEFLATE resume offsets",
        ));
    }
    let uncompressed_size = index
        .uncompressed_size_in_bytes
        .ok_or(IndexError::MissingMetadata(
            "uncompressed size for gztool export",
        ))?;
    if lines == WithLines::Yes
        && (index.total_line_count.is_none()
            || index
                .checkpoints
                .iter()
                .any(|point| point.line_offset.is_none()))
    {
        return Err(IndexError::MissingMetadata(
            "complete line counters for gztool version 1 export",
        ));
    }
    let count =
        u64::try_from(index.checkpoints.len()).map_err(|_| IndexError::ExcessiveLength {
            what: "gztool point count",
            value: u64::MAX,
        })?;

    write_u64_be(writer, 0)?;
    if lines == WithLines::Yes {
        writer.write_all(MAGIC_V1).map_err(IndexError::io)?;
        write_u32_be(writer, 0)?; // LF/CRLF line counting.
    } else {
        writer.write_all(MAGIC_V0).map_err(IndexError::io)?;
    }
    write_u64_be(writer, count)?;
    write_u64_be(writer, count)?;

    for checkpoint in &index.checkpoints {
        let (byte_offset, bits_field) = encode_bit_offset(checkpoint.compressed_offset_in_bits);
        write_u64_be(writer, checkpoint.uncompressed_offset_in_bytes)?;
        write_u64_be(writer, byte_offset)?;
        write_u32_be(writer, u32::from(bits_field))?;
        match index.windows.get(checkpoint.compressed_offset_in_bits) {
            Some(window) => {
                let payload = zlib_compress_window(&window.decompressed()?)?;
                if payload.len() > MAX_FORMAT_PAYLOAD {
                    return Err(IndexError::ExcessiveLength {
                        what: "compressed window length",
                        value: payload.len() as u64,
                    });
                }
                write_u32_be(writer, payload.len() as u32)?;
                writer.write_all(&payload).map_err(IndexError::io)?;
            }
            None => write_u32_be(writer, 0)?,
        }
        if lines == WithLines::Yes {
            write_u64_be(
                writer,
                checkpoint
                    .line_offset
                    .ok_or(IndexError::MissingMetadata("checkpoint line counter"))?,
            )?;
        }
    }
    write_u64_be(writer, uncompressed_size)?;
    if lines == WithLines::Yes {
        write_u64_be(
            writer,
            index
                .total_line_count
                .ok_or(IndexError::MissingMetadata("total line count"))?,
        )?;
    }
    Ok(())
}

pub(crate) fn read_gztool(
    reader: &mut impl Read,
    archive_size: Option<u64>,
    options: IndexReadOptions,
) -> Result<DeflateIndex, IndexError> {
    let mut header = [0_u8; 16];
    read_exact_bytes(reader, &mut header)?;
    if header[..8] != [0_u8; 8] {
        return Err(IndexError::BadMagic {
            found: header.to_vec(),
        });
    }
    let with_lines = if &header[8..] == MAGIC_V0 {
        false
    } else if &header[8..] == MAGIC_V1 {
        true
    } else {
        return Err(IndexError::BadMagic {
            found: header.to_vec(),
        });
    };
    if with_lines {
        let line_number_format = read_u32_be(reader)?;
        if line_number_format != 0 {
            return Err(IndexError::UnsupportedFlags {
                flags: u64::from(line_number_format),
            });
        }
    }

    let have = read_u64_be(reader)?;
    let size = read_u64_be(reader)?;
    if have != size {
        return Err(IndexError::InvalidCheckpoint("gztool index is incomplete"));
    }
    let count = usize::try_from(have).map_err(|_| IndexError::ExcessiveLength {
        what: "gztool point count",
        value: have,
    })?;
    if count > options.max_checkpoints {
        return Err(IndexError::ExcessiveLength {
            what: "gztool point count",
            value: have,
        });
    }

    let mut index = DeflateIndex::new();
    index.compressed_size_in_bytes = archive_size;
    index
        .checkpoints
        .try_reserve(count)
        .map_err(|_| IndexError::AllocationFailed {
            what: "gztool checkpoint records",
        })?;
    let payload_limit = options.max_window_payload_bytes.min(MAX_FORMAT_PAYLOAD);
    let mut aggregate_payload = 0_u64;
    for _ in 0..count {
        let uncompressed_offset_in_bytes = read_u64_be(reader)?;
        let byte_offset = read_u64_be(reader)?;
        let bits_field = u8::try_from(read_u32_be(reader)?)
            .map_err(|_| IndexError::InvalidCheckpoint("bits field does not fit in a byte"))?;
        let payload_length = read_u32_be(reader)? as usize;
        if payload_length > payload_limit {
            return Err(IndexError::ExcessiveLength {
                what: "compressed window length",
                value: payload_length as u64,
            });
        }
        aggregate_payload = aggregate_payload.checked_add(payload_length as u64).ok_or(
            IndexError::ExcessiveLength {
                what: "aggregate window bytes",
                value: u64::MAX,
            },
        )?;
        if aggregate_payload > options.max_window_bytes {
            return Err(IndexError::ExcessiveLength {
                what: "aggregate window bytes",
                value: aggregate_payload,
            });
        }
        let window = if payload_length == 0 {
            StoredWindow::empty()
        } else {
            let mut payload = Vec::new();
            payload.try_reserve_exact(payload_length).map_err(|_| {
                IndexError::AllocationFailed {
                    what: "gztool window payload",
                }
            })?;
            payload.resize(payload_length, 0);
            read_exact_bytes(reader, &mut payload)?;
            StoredWindow::from_compressed(payload)?
        };
        let line_offset = if with_lines {
            Some(read_u64_be(reader)?)
        } else {
            None
        };
        index.push(
            Checkpoint {
                compressed_offset_in_bits: decode_bit_offset(byte_offset, bits_field)?,
                uncompressed_offset_in_bytes,
                kind: CheckpointKind::DeflateBlock,
                line_offset,
            },
            window,
        )?;
    }
    index.uncompressed_size_in_bytes = Some(read_u64_be(reader)?);
    index.total_line_count = if with_lines {
        Some(read_u64_be(reader)?)
    } else {
        None
    };
    index.validate()?;
    Ok(index)
}
