//! Native rapidgzip-rust index format.
//!
//! Version 1 is finalized before the first public index-format release. It
//! records optional aggregate metadata, source provenance, explicit resume
//! kinds, optional line offsets, and raw or zlib-compressed windows.

use super::{
    Checkpoint, CheckpointKind, DeflateIndex, IndexError, IndexKind, IndexReadOptions,
    StoredWindow, read_exact_bytes, read_u8, read_u32_le, read_u64_le, write_u32_le, write_u64_le,
};
use std::io::{Read, Write};

const MAGIC: &[u8; 8] = b"RGZIDX01";
const VERSION: u16 = 1;

const HAS_COMPRESSED_SIZE: u16 = 1 << 0;
const HAS_UNCOMPRESSED_SIZE: u16 = 1 << 1;
const HAS_SPACING: u16 = 1 << 2;
const HAS_TOTAL_LINES: u16 = 1 << 3;
const SOURCE_IS_BGZF: u16 = 1 << 4;
const SOURCE_IS_ZLIB: u16 = 1 << 5;
const SOURCE_IS_RAW_DEFLATE: u16 = 1 << 6;
const SOURCE_KIND_FLAGS: u16 = SOURCE_IS_BGZF | SOURCE_IS_ZLIB | SOURCE_IS_RAW_DEFLATE;
const KNOWN_HEADER_FLAGS: u16 =
    HAS_COMPRESSED_SIZE | HAS_UNCOMPRESSED_SIZE | HAS_SPACING | HAS_TOTAL_LINES | SOURCE_KIND_FLAGS;

const HAS_LINE_OFFSET: u8 = 1 << 0;
const IS_DEFLATE_BLOCK: u8 = 1 << 1;
const IS_MEMBER_DEFLATE: u8 = 1 << 2;
const IS_ZLIB_HEADER: u8 = 1 << 3;
const IS_RAW_DEFLATE_START: u8 = 1 << 4;
const RESUME_KIND_FLAGS: u8 =
    IS_DEFLATE_BLOCK | IS_MEMBER_DEFLATE | IS_ZLIB_HEADER | IS_RAW_DEFLATE_START;
const KNOWN_CHECKPOINT_FLAGS: u8 = HAS_LINE_OFFSET | RESUME_KIND_FLAGS;

const WINDOW_ABSENT: u8 = 0;
const WINDOW_RAW: u8 = 1;
const WINDOW_ZLIB: u8 = 2;

pub(crate) fn write_native(
    index: &DeflateIndex,
    writer: &mut impl Write,
) -> Result<(), IndexError> {
    index.validate()?;

    let mut flags = 0_u16;
    if index.compressed_size_in_bytes.is_some() {
        flags |= HAS_COMPRESSED_SIZE;
    }
    if index.uncompressed_size_in_bytes.is_some() {
        flags |= HAS_UNCOMPRESSED_SIZE;
    }
    if index.checkpoint_spacing_in_bytes.is_some() {
        flags |= HAS_SPACING;
    }
    if index.total_line_count.is_some() {
        flags |= HAS_TOTAL_LINES;
    }
    flags |= match index.kind {
        IndexKind::Gzip => 0,
        IndexKind::Bgzf => SOURCE_IS_BGZF,
        IndexKind::Zlib => SOURCE_IS_ZLIB,
        IndexKind::RawDeflate => SOURCE_IS_RAW_DEFLATE,
    };

    writer.write_all(MAGIC).map_err(IndexError::io)?;
    writer
        .write_all(&VERSION.to_le_bytes())
        .map_err(IndexError::io)?;
    writer
        .write_all(&flags.to_le_bytes())
        .map_err(IndexError::io)?;
    write_u64_le(writer, index.compressed_size_in_bytes.unwrap_or(0))?;
    write_u64_le(writer, index.uncompressed_size_in_bytes.unwrap_or(0))?;
    write_u64_le(writer, index.checkpoint_spacing_in_bytes.unwrap_or(0))?;
    write_u64_le(writer, index.total_line_count.unwrap_or(0))?;
    write_u64_le(
        writer,
        u64::try_from(index.checkpoints.len()).map_err(|_| IndexError::ExcessiveLength {
            what: "checkpoint count",
            value: u64::MAX,
        })?,
    )?;

    for checkpoint in &index.checkpoints {
        write_u64_le(writer, checkpoint.compressed_offset_in_bits)?;
        write_u64_le(writer, checkpoint.uncompressed_offset_in_bytes)?;
        write_u64_le(writer, checkpoint.line_offset.unwrap_or(0))?;
        write_u64_le(
            writer,
            match checkpoint.kind {
                CheckpointKind::GzipMemberDeflate {
                    header_offset_in_bytes,
                } => header_offset_in_bytes,
                _ => 0,
            },
        )?;
        let mut checkpoint_flags = 0_u8;
        if checkpoint.line_offset.is_some() {
            checkpoint_flags |= HAS_LINE_OFFSET;
        }
        match checkpoint.kind {
            CheckpointKind::GzipMemberHeader => {}
            CheckpointKind::GzipMemberDeflate { .. } => checkpoint_flags |= IS_MEMBER_DEFLATE,
            CheckpointKind::ZlibHeader => checkpoint_flags |= IS_ZLIB_HEADER,
            CheckpointKind::RawDeflateStart => checkpoint_flags |= IS_RAW_DEFLATE_START,
            CheckpointKind::DeflateBlock => checkpoint_flags |= IS_DEFLATE_BLOCK,
        }

        let (window_kind, payload) = match index.windows.get(checkpoint.compressed_offset_in_bits) {
            Some(window) if !window.is_empty() => (
                if window.is_compressed() {
                    WINDOW_ZLIB
                } else {
                    WINDOW_RAW
                },
                window.payload(),
            ),
            _ => (WINDOW_ABSENT, &[][..]),
        };
        let payload_length =
            u32::try_from(payload.len()).map_err(|_| IndexError::ExcessiveLength {
                what: "window payload length",
                value: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            })?;
        writer
            .write_all(&[checkpoint_flags, window_kind])
            .map_err(IndexError::io)?;
        write_u32_le(writer, payload_length)?;
        writer.write_all(payload).map_err(IndexError::io)?;
    }
    Ok(())
}

pub(crate) fn read_native(
    reader: &mut impl Read,
    options: IndexReadOptions,
) -> Result<DeflateIndex, IndexError> {
    let mut magic = [0_u8; 8];
    read_exact_bytes(reader, &mut magic)?;
    if &magic != MAGIC {
        return Err(IndexError::BadMagic {
            found: magic.to_vec(),
        });
    }

    let mut version_bytes = [0_u8; 2];
    read_exact_bytes(reader, &mut version_bytes)?;
    let version = u16::from_le_bytes(version_bytes);
    if version != VERSION {
        return Err(IndexError::UnsupportedVersion(u64::from(version)));
    }
    let mut flags_bytes = [0_u8; 2];
    read_exact_bytes(reader, &mut flags_bytes)?;
    let flags = u16::from_le_bytes(flags_bytes);
    if flags & !KNOWN_HEADER_FLAGS != 0 {
        return Err(IndexError::UnsupportedFlags {
            flags: u64::from(flags),
        });
    }
    if (flags & SOURCE_KIND_FLAGS).count_ones() > 1 {
        return Err(IndexError::UnsupportedFlags {
            flags: u64::from(flags),
        });
    }

    let compressed_size = read_u64_le(reader)?;
    let uncompressed_size = read_u64_le(reader)?;
    let spacing = read_u64_le(reader)?;
    let total_lines = read_u64_le(reader)?;
    let count = read_u64_le(reader)?;
    let count = usize::try_from(count).map_err(|_| IndexError::ExcessiveLength {
        what: "checkpoint count",
        value: count,
    })?;
    if count > options.max_checkpoints {
        return Err(IndexError::ExcessiveLength {
            what: "checkpoint count",
            value: count as u64,
        });
    }

    let mut index = DeflateIndex::new();
    index.kind = match flags & SOURCE_KIND_FLAGS {
        SOURCE_IS_BGZF => IndexKind::Bgzf,
        SOURCE_IS_ZLIB => IndexKind::Zlib,
        SOURCE_IS_RAW_DEFLATE => IndexKind::RawDeflate,
        _ => IndexKind::Gzip,
    };
    index.compressed_size_in_bytes = (flags & HAS_COMPRESSED_SIZE != 0).then_some(compressed_size);
    index.uncompressed_size_in_bytes =
        (flags & HAS_UNCOMPRESSED_SIZE != 0).then_some(uncompressed_size);
    index.checkpoint_spacing_in_bytes = (flags & HAS_SPACING != 0).then_some(spacing);
    index.total_line_count = (flags & HAS_TOTAL_LINES != 0).then_some(total_lines);
    index
        .checkpoints
        .try_reserve(count)
        .map_err(|_| IndexError::AllocationFailed {
            what: "checkpoint records",
        })?;

    let mut window_bytes = 0_u64;
    for _ in 0..count {
        let compressed_offset_in_bits = read_u64_le(reader)?;
        let uncompressed_offset_in_bytes = read_u64_le(reader)?;
        let line_value = read_u64_le(reader)?;
        let member_header_value = read_u64_le(reader)?;
        let checkpoint_flags = read_u8(reader)?;
        if checkpoint_flags & !KNOWN_CHECKPOINT_FLAGS != 0 {
            return Err(IndexError::UnsupportedFlags {
                flags: u64::from(checkpoint_flags),
            });
        }
        if (checkpoint_flags & RESUME_KIND_FLAGS).count_ones() > 1 {
            return Err(IndexError::InvalidCheckpoint(
                "checkpoint declares multiple resume kinds",
            ));
        }
        let window_kind = read_u8(reader)?;
        let payload_length = read_u32_le(reader)? as usize;
        if payload_length > options.max_window_payload_bytes {
            return Err(IndexError::ExcessiveLength {
                what: "window payload length",
                value: payload_length as u64,
            });
        }
        window_bytes =
            window_bytes
                .checked_add(payload_length as u64)
                .ok_or(IndexError::ExcessiveLength {
                    what: "aggregate window bytes",
                    value: u64::MAX,
                })?;
        if window_bytes > options.max_window_bytes {
            return Err(IndexError::ExcessiveLength {
                what: "aggregate window bytes",
                value: window_bytes,
            });
        }
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(payload_length)
            .map_err(|_| IndexError::AllocationFailed {
                what: "window payload",
            })?;
        payload.resize(payload_length, 0);
        read_exact_bytes(reader, &mut payload)?;

        let window = match window_kind {
            WINDOW_ABSENT if payload.is_empty() => StoredWindow::empty(),
            WINDOW_ABSENT => {
                return Err(IndexError::InvalidCheckpoint(
                    "absent window declares a payload",
                ));
            }
            WINDOW_RAW => StoredWindow::from_raw(payload)?,
            WINDOW_ZLIB => StoredWindow::from_compressed(payload)?,
            _ => return Err(IndexError::InvalidCheckpoint("unknown window kind")),
        };
        index.push(
            Checkpoint {
                compressed_offset_in_bits,
                uncompressed_offset_in_bytes,
                kind: if checkpoint_flags & IS_DEFLATE_BLOCK != 0 {
                    CheckpointKind::DeflateBlock
                } else if checkpoint_flags & IS_MEMBER_DEFLATE != 0 {
                    CheckpointKind::GzipMemberDeflate {
                        header_offset_in_bytes: member_header_value,
                    }
                } else if checkpoint_flags & IS_ZLIB_HEADER != 0 {
                    CheckpointKind::ZlibHeader
                } else if checkpoint_flags & IS_RAW_DEFLATE_START != 0 {
                    CheckpointKind::RawDeflateStart
                } else {
                    CheckpointKind::GzipMemberHeader
                },
                line_offset: (checkpoint_flags & HAS_LINE_OFFSET != 0).then_some(line_value),
            },
            window,
        )?;
    }

    index.validate()?;
    Ok(index)
}
