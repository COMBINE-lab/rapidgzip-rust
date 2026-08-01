//! [htslib](https://github.com/samtools/htslib) BGZF block index (`.gzi`).
//!
//! Layout, little-endian, matching `bgzf_index_dump_hfile`: a `u64` pair count
//! followed by that many `(compressed_offset, uncompressed_offset)` pairs
//! describing block starts after the first. The origin pair is implicit.
//!
//! The format stores neither predecessor windows nor a total uncompressed
//! size, so import records the uncompressed size as unknown ([`u64::MAX`]) and
//! export refuses any checkpoint that needs a window.

use super::{Checkpoint, GzipIndex, IndexError, StoredWindow, read_u64_le, write_u64_le};
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
