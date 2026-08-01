//! Structural analysis against what the decoder produces.
//!
//! The properties checked here are the ones a wrong walk would violate:
//! block sizes summing to the decoded total, offsets that only increase, and
//! a final block ending every stream.

mod common;

use common::{bgzf, corpus, gzip, raw_deflate, zlib};
use rapidgzip_core::{BlockType, Decoder, Format, StreamFooter, StreamHeader};
use std::sync::Arc;

fn analyze(compressed: Vec<u8>) -> rapidgzip_core::Analysis {
    Decoder::default()
        .analyze(&Arc::<[u8]>::from(compressed))
        .expect("analyze")
}

/// Asserts the invariants every analysis must satisfy.
fn assert_consistent(analysis: &rapidgzip_core::Analysis, decoded_size: u64) {
    assert_eq!(analysis.uncompressed_size_in_bytes, decoded_size);

    let summed: u64 = analysis
        .blocks
        .iter()
        .map(|block| block.uncompressed_size_in_bytes)
        .sum();
    assert_eq!(summed, decoded_size, "block sizes must sum to the total");

    let mut previous_bits = 0_u64;
    let mut previous_bytes = 0_u64;
    for (position, block) in analysis.blocks.iter().enumerate() {
        if position > 0 {
            assert!(
                block.compressed_offset_in_bits > previous_bits,
                "block {position} does not advance in the compressed stream"
            );
            assert!(
                block.uncompressed_offset_in_bytes >= previous_bytes,
                "block {position} goes backwards in the output"
            );
        }
        assert!(
            block.compressed_data_offset_in_bits >= block.compressed_offset_in_bits,
            "block {position} has data before its own header"
        );
        if block.block_type != BlockType::Uncompressed {
            // A stored block copies bytes verbatim, so it emits neither
            // literal symbols nor back-reference copies.
            assert_eq!(
                block.literal_symbols + block.copied_bytes,
                block.uncompressed_size_in_bytes,
                "block {position} output is not literals plus copies"
            );
        }
        previous_bits = block.compressed_offset_in_bits;
        previous_bytes = block.uncompressed_offset_in_bytes;
    }

    let final_blocks = analysis
        .blocks
        .iter()
        .filter(|block| block.is_final)
        .count();
    assert_eq!(
        final_blocks,
        analysis.streams.len(),
        "each stream ends with exactly one final block"
    );
}

#[test]
fn a_single_gzip_member_is_walked() {
    let decoded = corpus(2 * 1024 * 1024);
    let analysis = analyze(gzip(&decoded, 6));

    assert_eq!(analysis.format, Format::Gzip);
    assert_eq!(analysis.streams.len(), 1);
    assert!(analysis.blocks.len() > 1, "a 2 MiB corpus spans blocks");
    assert_consistent(&analysis, decoded.len() as u64);

    assert!(matches!(analysis.streams[0].header, StreamHeader::Gzip(_)));
    assert!(matches!(
        analysis.streams[0].footer,
        StreamFooter::Gzip { .. }
    ));
}

#[test]
fn concatenated_members_are_separate_streams() {
    let first = corpus(200 * 1024);
    let second = corpus(100 * 1024);
    let mut compressed = gzip(&first, 6);
    compressed.extend_from_slice(&gzip(&second, 6));

    let analysis = analyze(compressed);
    assert_eq!(analysis.streams.len(), 2);
    assert_eq!(
        analysis.streams[1].uncompressed_offset_in_bytes,
        first.len() as u64
    );
    assert_consistent(&analysis, (first.len() + second.len()) as u64);
}

#[test]
fn bgzf_blocks_are_walked() {
    let decoded = corpus(512 * 1024);
    let analysis = analyze(bgzf(&decoded, 32 * 1024));
    assert!(analysis.streams.len() > 1, "BGZF is many small members");
    assert_consistent(&analysis, decoded.len() as u64);
}

#[test]
fn stored_blocks_are_recognized() {
    // A stored member is what `common::gzip` produces at level 0.
    let decoded = corpus(256 * 1024);
    let analysis = analyze(gzip(&decoded, 0));
    assert!(
        analysis
            .blocks
            .iter()
            .any(|block| block.block_type == BlockType::Uncompressed),
        "level 0 should store"
    );
    assert_consistent(&analysis, decoded.len() as u64);
}

#[test]
fn zlib_and_raw_deflate_are_walked() {
    let decoded = corpus(256 * 1024);

    let analysis = Decoder::default()
        .analyze(&Arc::<[u8]>::from(zlib(&decoded, 6)))
        .expect("analyze");
    assert_eq!(analysis.format, Format::Zlib);
    assert!(matches!(
        analysis.streams[0].footer,
        StreamFooter::Zlib { .. }
    ));
    assert_consistent(&analysis, decoded.len() as u64);

    let decoder = Decoder::builder()
        .format(Format::RawDeflate)
        .build()
        .expect("decoder");
    let analysis = decoder
        .analyze(&Arc::<[u8]>::from(raw_deflate(&decoded, 6)))
        .expect("analyze");
    assert_eq!(analysis.format, Format::RawDeflate);
    assert_eq!(analysis.streams[0].footer, StreamFooter::None);
    assert_consistent(&analysis, decoded.len() as u64);
}

#[test]
fn dynamic_blocks_report_their_alphabets() {
    let decoded = corpus(1024 * 1024);
    let analysis = analyze(gzip(&decoded, 6));

    let dynamic = analysis
        .blocks
        .iter()
        .find(|block| block.block_type == BlockType::DynamicHuffman)
        .expect("a compressible corpus produces dynamic blocks");

    let precode = dynamic
        .precode
        .as_ref()
        .expect("dynamic blocks declare one");
    assert_eq!(precode.code_lengths.len(), 19);
    assert!(precode.declared_count >= 4 && precode.declared_count <= 19);

    let literal = dynamic.literal.as_ref().expect("declared");
    assert!(literal.code_lengths.len() >= 257 && literal.code_lengths.len() <= 286);
    assert!(literal.used_count() > 0);
    let (minimum, maximum) = literal.length_range().expect("some code is used");
    assert!(minimum <= maximum && maximum <= 15);

    // The per-length counts must account for every declared symbol.
    let counted: usize = literal
        .counts_by_length()
        .iter()
        .map(|&(_, count)| count)
        .sum();
    assert_eq!(counted, literal.code_lengths.len());
}

#[test]
fn back_references_into_the_window_are_measured() {
    let decoded = corpus(4 * 1024 * 1024);
    let analysis = analyze(gzip(&decoded, 6));

    let first = &analysis.blocks[0];
    assert_eq!(
        first.farthest_backreference, 0,
        "the first block has no preceding window to reach into"
    );
    assert_eq!(first.window_backreference_count, 0);

    let later = analysis
        .blocks
        .iter()
        .skip(1)
        .find(|block| block.window_backreference_count > 0)
        .expect("later blocks reference the preceding window");
    assert!(later.farthest_backreference > 0);
    assert!(later.farthest_backreference <= 32768);
    assert!(later.merged_window_backreference_count <= later.window_backreference_count);
    assert_eq!(
        later.backreference_lengths.len() as u64,
        later.window_backreference_count
    );
    if let Some(used) = later.used_window_symbols {
        assert!(used <= 32768);
    }
}

#[test]
fn block_type_counts_cover_every_block() {
    let decoded = corpus(1024 * 1024);
    let analysis = analyze(gzip(&decoded, 6));
    let total: u64 = analysis
        .block_type_counts()
        .iter()
        .map(|&(_, count)| count)
        .sum();
    assert_eq!(total, analysis.blocks.len() as u64);
}

#[test]
fn a_truncated_member_is_rejected() {
    let decoded = corpus(64 * 1024);
    let mut compressed = gzip(&decoded, 6);
    compressed.truncate(compressed.len() / 2);
    assert!(
        Decoder::default()
            .analyze(&Arc::<[u8]>::from(compressed))
            .is_err(),
        "analysis must not accept a truncated stream"
    );
}
