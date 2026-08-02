//! htslib BGZF block index (`.gzi`).

use super::{
    Checkpoint, CheckpointKind, DeflateIndex, IndexError, IndexKind, IndexReadOptions,
    StoredWindow, read_u64_le, write_u64_le,
};
use std::io::{Read, Write};

pub(crate) fn write_gzi(index: &DeflateIndex, writer: &mut impl Write) -> Result<(), IndexError> {
    index.validate()?;
    if index.kind != IndexKind::Bgzf {
        return Err(IndexError::IncompatibleFormat {
            operation: ".gzi export",
            kind: index.kind,
        });
    }

    let mut pair_count = 0_u64;
    for checkpoint in &index.checkpoints {
        let header_offset = member_header_offset(checkpoint)?;
        if index
            .windows
            .get(checkpoint.compressed_offset_in_bits)
            .is_some()
        {
            return Err(IndexError::InvalidCheckpoint(
                "BGZF index cannot store a predecessor window",
            ));
        }
        if header_offset == 0 && checkpoint.uncompressed_offset_in_bytes == 0 {
            continue;
        }
        pair_count = pair_count
            .checked_add(1)
            .ok_or(IndexError::ExcessiveLength {
                what: "BGZF index pair count",
                value: u64::MAX,
            })?;
    }

    write_u64_le(writer, pair_count)?;
    for checkpoint in &index.checkpoints {
        let header_offset = member_header_offset(checkpoint)?;
        if header_offset == 0 && checkpoint.uncompressed_offset_in_bytes == 0 {
            continue;
        }
        write_u64_le(writer, header_offset)?;
        write_u64_le(writer, checkpoint.uncompressed_offset_in_bytes)?;
    }
    Ok(())
}

fn member_header_offset(checkpoint: &Checkpoint) -> Result<u64, IndexError> {
    match checkpoint.kind {
        CheckpointKind::GzipMemberHeader => Ok(checkpoint.compressed_offset_in_bits / 8),
        CheckpointKind::GzipMemberDeflate {
            header_offset_in_bytes,
        } => Ok(header_offset_in_bytes),
        CheckpointKind::DeflateBlock
        | CheckpointKind::ZlibHeader
        | CheckpointKind::RawDeflateStart => Err(IndexError::InvalidCheckpoint(
            "BGZF index requires member-boundary checkpoints",
        )),
    }
}

pub(crate) fn read_gzi(
    reader: &mut impl Read,
    archive_size: Option<u64>,
    options: IndexReadOptions,
) -> Result<DeflateIndex, IndexError> {
    let pair_count = read_u64_le(reader)?;
    let pair_count = usize::try_from(pair_count).map_err(|_| IndexError::ExcessiveLength {
        what: "BGZF index pair count",
        value: pair_count,
    })?;
    if pair_count >= options.max_checkpoints {
        return Err(IndexError::ExcessiveLength {
            what: "BGZF index pair count",
            value: pair_count as u64,
        });
    }

    let mut index = DeflateIndex::new();
    index.kind = IndexKind::Bgzf;
    index.compressed_size_in_bytes = archive_size;
    index
        .checkpoints
        .try_reserve(pair_count.saturating_add(1))
        .map_err(|_| IndexError::AllocationFailed {
            what: "BGZF checkpoint records",
        })?;
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 0,
            uncompressed_offset_in_bytes: 0,
            kind: CheckpointKind::GzipMemberHeader,
            line_offset: None,
        },
        StoredWindow::empty(),
    )?;

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
                kind: CheckpointKind::GzipMemberHeader,
                line_offset: None,
            },
            StoredWindow::empty(),
        )?;
    }

    index.validate()?;
    Ok(index)
}
