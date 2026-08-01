//! Decoding driven by an index, which puts every worker on plain zlib.

mod common;

use common::{bgzf, corpus, gzip};
use rapidgzip_core::{Decoder, DecoderPath, GzipIndex};
use std::io;
use std::sync::Arc;

/// Bytes of corpus needed before the grid produces interior checkpoints.
///
/// A grid chunk is at least 1 MiB of compressed input, so a smaller corpus
/// yields an index too sparse to split, and the decode would quietly fall back
/// to the speculative path while the test believed otherwise.
const SPLITTABLE: usize = 24 * 1024 * 1024;

/// Decodes once to collect an index dense enough to split.
fn index_for(compressed: &Arc<[u8]>, spacing: u64) -> GzipIndex {
    let decoder = Decoder::builder()
        .build_index(true)
        .index_spacing(spacing)
        .build()
        .expect("decoder");
    decoder
        .decode(compressed, &mut io::sink())
        .expect("decode")
        .index
        .expect("index requested")
}

fn decode_with(compressed: &Arc<[u8]>, index: Option<GzipIndex>, workers: usize) -> Vec<u8> {
    let decoder = Decoder::builder()
        .decoder_threads(workers)
        .index(index)
        .build()
        .expect("decoder");
    let mut output = Vec::new();
    decoder.decode(compressed, &mut output).expect("decode");
    output
}

#[test]
fn an_indexed_decode_matches_an_unindexed_one() {
    let decoded = corpus(SPLITTABLE);
    let compressed: Arc<[u8]> = gzip(&decoded, 6).into();
    let index = index_for(&compressed, 512 * 1024);
    assert!(
        index.checkpoint_count() >= 3,
        "the index must be splittable"
    );

    for workers in [2, 3, 4, 8] {
        assert_eq!(
            decode_with(&compressed, Some(index.clone()), workers),
            decoded,
            "indexed decode differs at {workers} workers"
        );
    }
}

#[test]
fn concatenated_members_verify_across_span_boundaries() {
    // A member's CRC32 is split across spans here, so it can only pass if the
    // per-span checksums are combined correctly.
    let first = corpus(3 * 1024 * 1024);
    let second = corpus(2 * 1024 * 1024);
    let mut plain = first.clone();
    plain.extend_from_slice(&second);
    let mut bytes = gzip(&first, 6);
    bytes.extend_from_slice(&gzip(&second, 6));
    let compressed: Arc<[u8]> = bytes.into();

    let index = index_for(&compressed, 256 * 1024);
    assert_eq!(decode_with(&compressed, Some(index), 4), plain);
}

#[test]
fn bgzf_decodes_through_its_index() {
    let decoded = corpus(2 * 1024 * 1024);
    let compressed: Arc<[u8]> = bgzf(&decoded, 64 * 1024).into();
    let index = index_for(&compressed, 64 * 1024);
    assert_eq!(decode_with(&compressed, Some(index), 4), decoded);
}

#[test]
fn telemetry_reports_the_indexed_path() {
    let decoded = corpus(SPLITTABLE);
    let compressed: Arc<[u8]> = gzip(&decoded, 6).into();
    let index = index_for(&compressed, 256 * 1024);

    let decoder = Decoder::builder()
        .decoder_threads(4)
        .index(Some(index))
        .build()
        .expect("decoder");
    let mut reader = decoder.reader(Arc::clone(&compressed)).expect("reader");
    let handle = reader.handle();
    io::copy(&mut reader, &mut io::sink()).expect("decode");
    assert_eq!(handle.stats().path, DecoderPath::Indexed);
}

#[test]
fn a_corrupt_member_is_rejected_before_its_bytes_are_emitted() {
    let decoded = corpus(SPLITTABLE);
    let mut bytes = gzip(&decoded, 6);
    let index = index_for(&Arc::<[u8]>::from(bytes.clone()), 256 * 1024);

    // Flip a byte in the footer's CRC32, which no span can detect alone.
    let last = bytes.len() - 8;
    bytes[last] ^= 0xff;
    let compressed: Arc<[u8]> = bytes.into();

    assert!(
        index.checkpoint_count() >= 3,
        "the index must be splittable"
    );
    let decoder = Decoder::builder()
        .decoder_threads(4)
        .index(Some(index))
        .build()
        .expect("decoder");
    let mut output = Vec::new();
    let error = decoder
        .decode(&compressed, &mut output)
        .expect_err("the checksum must fail");
    assert!(
        matches!(error, rapidgzip_core::DecodeError::ChecksumMismatch { .. }),
        "expected a checksum mismatch, got {error}"
    );
}

#[test]
fn an_index_for_another_file_is_refused_rather_than_used() {
    let decoded = corpus(SPLITTABLE);
    let compressed: Arc<[u8]> = gzip(&decoded, 6).into();
    let index = index_for(&compressed, 256 * 1024);

    // A different file of a different size: the recorded compressed size no
    // longer agrees, so the index must be ignored and the decode must still
    // produce the right bytes.
    let other_plain = corpus(3 * 1024 * 1024);
    let other: Arc<[u8]> = gzip(&other_plain, 6).into();
    assert_eq!(decode_with(&other, Some(index), 4), other_plain);
}

#[test]
fn one_worker_ignores_the_index() {
    let decoded = corpus(SPLITTABLE);
    let compressed: Arc<[u8]> = gzip(&decoded, 6).into();
    let index = index_for(&compressed, 256 * 1024);

    let decoder = Decoder::builder()
        .decoder_threads(1)
        .index(Some(index))
        .build()
        .expect("decoder");
    let mut reader = decoder.reader(Arc::clone(&compressed)).expect("reader");
    let handle = reader.handle();
    let mut output = Vec::new();
    io::copy(&mut reader, &mut output).expect("decode");
    assert_eq!(output, decoded);
    assert_eq!(handle.stats().path, DecoderPath::Sequential);
}
