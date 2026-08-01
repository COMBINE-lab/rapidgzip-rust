//! [gztool](https://github.com/circulosmeos/gztool) index import and export.
//!
//! All integers are big-endian. Windows are stored zlib-compressed. Only
//! complete indexes (`have == size`) are accepted; gztool's growing-index
//! placeholders are refused rather than silently truncated.
//!
//! Layout: eight zero bytes, then magic `gzipindx` (version 0) or `gzipindX`
//! (version 1, with line counters), and for version 1 a `u32` line-number
//! format. Then `u64` `have` and `u64` `size`. Then per point: `u64`
//! uncompressed offset, `u64` compressed byte offset, `u32` bits field, `u32`
//! compressed window length, the zlib payload, and for version 1 a `u64` line
//! counter. The file ends with the `u64` uncompressed size and, for version 1,
//! the total line count.

use super::{
    Checkpoint, GzipIndex, IndexError, StoredWindow, WINDOW_SIZE, decode_bit_offset,
    encode_bit_offset, read_exact_bytes, read_u32_be, read_u64_be, write_u32_be, write_u64_be,
    zlib_compress_window, zlib_decompress_window,
};
use std::io::{Read, Write};

/// Magic for indexes without line counters.
const MAGIC_V0: &[u8; 8] = b"gzipindx";

/// Magic for indexes with line counters.
const MAGIC_V1: &[u8; 8] = b"gzipindX";

/// Upper bound on the point count, guarding against hostile headers.
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
            StoredWindow::from_raw(zlib_decompress_window(&payload)?)
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
