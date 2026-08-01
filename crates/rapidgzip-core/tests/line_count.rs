//! Newline counting across every decode path.
//!
//! The count is produced by the coordinator, on resolved bytes, so the paths
//! that differ most in how they produce those bytes have to agree: the
//! sequential loop, the marker/window grid, independent BGZF blocks, and the
//! forward-only stream cursor.

mod common;

use common::{bgzf, corpus, gzip, raw_deflate, zlib};
use rapidgzip_core::{Decoder, Format};
use std::io;
use std::sync::Arc;

/// Builds text with a known newline count and no trailing newline.
fn lines(count: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for index in 0..count {
        bytes.extend_from_slice(format!("line {index} of the corpus").as_bytes());
        if index + 1 < count {
            bytes.push(b'\n');
        }
    }
    bytes
}

fn decode_counting(compressed: Vec<u8>, threads: usize) -> (Vec<u8>, Option<u64>) {
    let decoder = Decoder::builder()
        .decoder_threads(threads)
        .count_lines(true)
        .build()
        .expect("decoder");
    let mut output = Vec::new();
    let report = decoder
        .decode(&Arc::<[u8]>::from(compressed), &mut output)
        .expect("decode");
    (output, report.line_count)
}

#[test]
fn counting_is_off_by_default() {
    let decoder = Decoder::default();
    let report = decoder
        .decode(&Arc::<[u8]>::from(gzip(b"a\nb\n", 6)), &mut io::sink())
        .expect("decode");
    assert_eq!(report.line_count, None);
}

#[test]
fn sequential_and_parallel_agree_on_the_same_corpus() {
    let decoded = corpus(4 * 1024 * 1024);
    let expected = decoded.iter().filter(|&&byte| byte == b'\n').count() as u64;
    let compressed = gzip(&decoded, 6);

    let (sequential, sequential_lines) = decode_counting(compressed.clone(), 1);
    let (parallel, parallel_lines) = decode_counting(compressed, 8);

    assert_eq!(sequential, decoded);
    assert_eq!(parallel, decoded);
    assert_eq!(sequential_lines, Some(expected));
    assert_eq!(parallel_lines, Some(expected));
}

#[test]
fn bgzf_blocks_agree() {
    let decoded = lines(20_000);
    let expected = 19_999;
    let (output, count) = decode_counting(bgzf(&decoded, 16 * 1024), 4);
    assert_eq!(output, decoded);
    assert_eq!(count, Some(expected));
}

#[test]
fn concatenated_members_count_across_the_boundary() {
    let first = b"one\ntwo\n".to_vec();
    let second = b"three\nfour".to_vec();
    let mut compressed = gzip(&first, 6);
    compressed.extend_from_slice(&gzip(&second, 6));

    let (output, count) = decode_counting(compressed, 1);
    assert_eq!(output, [first, second].concat());
    assert_eq!(count, Some(3));
}

#[test]
fn a_missing_trailing_newline_is_not_counted() {
    let (_, with) = decode_counting(gzip(b"a\nb\n", 6), 1);
    let (_, without) = decode_counting(gzip(b"a\nb", 6), 1);
    assert_eq!(with, Some(2));
    assert_eq!(without, Some(1));
}

#[test]
fn empty_output_counts_zero() {
    let (output, count) = decode_counting(gzip(b"", 6), 1);
    assert!(output.is_empty());
    assert_eq!(count, Some(0));
}

#[test]
fn the_streaming_path_counts_identically() {
    let decoded = lines(5_000);
    let expected = 4_999;
    let decoder = Decoder::builder()
        .count_lines(true)
        .build()
        .expect("decoder");
    let mut output = Vec::new();
    let report = decoder
        .decode_stream(gzip(&decoded, 6).as_slice(), &mut output)
        .expect("decode");
    assert_eq!(output, decoded);
    assert_eq!(report.line_count, Some(expected));
}

#[test]
fn zlib_and_raw_deflate_count_too() {
    let decoded = lines(1_000);
    let expected = 999;

    let (_, zlib_lines) = decode_counting(zlib(&decoded, 6), 1);
    assert_eq!(zlib_lines, Some(expected));

    let decoder = Decoder::builder()
        .decoder_threads(1)
        .count_lines(true)
        .format(Format::RawDeflate)
        .build()
        .expect("decoder");
    let report = decoder
        .decode(
            &Arc::<[u8]>::from(raw_deflate(&decoded, 6)),
            &mut io::sink(),
        )
        .expect("decode");
    assert_eq!(report.line_count, Some(expected));
}

#[test]
fn an_index_built_while_counting_carries_line_offsets() {
    // A grid chunk is at least 1 MiB of compressed input, so the corpus has
    // to be poorly compressible and large enough to span several of them
    // before any interior checkpoint exists at all.
    let decoded = corpus(24 * 1024 * 1024);
    let expected = decoded.iter().filter(|&&byte| byte == b'\n').count() as u64;
    let decoder = Decoder::builder()
        .decoder_threads(4)
        .count_lines(true)
        .build_index(true)
        .index_spacing(256 * 1024)
        .build()
        .expect("decoder");
    let report = decoder
        .decode(&Arc::<[u8]>::from(gzip(&decoded, 6)), &mut io::sink())
        .expect("decode");

    assert_eq!(report.line_count, Some(expected));
    let index = report.index.expect("index requested");
    assert_eq!(index.total_line_count, Some(expected));

    let checkpoints = index.checkpoints();
    assert!(checkpoints.len() > 2, "expected interior checkpoints");
    assert_eq!(checkpoints[0].line_offset, 0);
    for pair in checkpoints.windows(2) {
        assert!(
            pair[0].line_offset <= pair[1].line_offset,
            "line offsets must not go backwards: {} then {}",
            pair[0].line_offset,
            pair[1].line_offset
        );
    }

    // Every checkpoint's line offset must equal the newlines actually
    // preceding its decompressed offset, which is the only claim the format
    // makes and the one gztool relies on.
    for checkpoint in checkpoints {
        let offset = checkpoint.uncompressed_offset_in_bytes as usize;
        let actual = decoded[..offset].iter().filter(|&&b| b == b'\n').count() as u64;
        assert_eq!(
            checkpoint.line_offset, actual,
            "wrong line offset at decompressed byte {offset}"
        );
    }
}

#[test]
fn an_index_built_without_counting_claims_no_line_count() {
    let decoded = lines(50_000);
    let decoder = Decoder::builder()
        .decoder_threads(4)
        .build_index(true)
        .build()
        .expect("decoder");
    let report = decoder
        .decode(&Arc::<[u8]>::from(gzip(&decoded, 6)), &mut io::sink())
        .expect("decode");
    let index = report.index.expect("index requested");
    assert_eq!(index.total_line_count, None);
}

#[test]
fn bgzf_index_line_offsets_are_exact() {
    let decoded = lines(30_000);
    let decoder = Decoder::builder()
        .decoder_threads(4)
        .count_lines(true)
        .build_index(true)
        .build()
        .expect("decoder");
    let report = decoder
        .decode(
            &Arc::<[u8]>::from(bgzf(&decoded, 16 * 1024)),
            &mut io::sink(),
        )
        .expect("decode");
    let index = report.index.expect("index requested");
    assert!(index.total_line_count.is_some());
    for checkpoint in index.checkpoints() {
        let offset = checkpoint.uncompressed_offset_in_bytes as usize;
        let actual = decoded[..offset].iter().filter(|&&b| b == b'\n').count() as u64;
        assert_eq!(checkpoint.line_offset, actual);
    }
}
