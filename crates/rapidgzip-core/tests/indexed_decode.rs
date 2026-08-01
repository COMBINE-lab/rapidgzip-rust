//! Parallel full-stream decode using an imported [`GzipIndex`].

use rapidgzip_core::{DecodeError, Decoder, GzipIndex};
use std::io::Cursor;

fn crc32(bytes: &[u8]) -> u32 {
    let mut value = u32::MAX;
    for &byte in bytes {
        value ^= u32::from(byte);
        for _ in 0..8 {
            value = (value >> 1) ^ (0xEDB8_8320 & 0_u32.wrapping_sub(value & 1));
        }
    }
    !value
}

fn stored_deflate(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    if bytes.is_empty() {
        encoded.extend_from_slice(&[1, 0, 0, 0xFF, 0xFF]);
        return encoded;
    }
    let chunks = bytes.chunks(u16::MAX as usize);
    let chunk_count = chunks.len();
    for (index, chunk) in chunks.enumerate() {
        encoded.push(u8::from(index + 1 == chunk_count));
        let length = chunk.len() as u16;
        encoded.extend_from_slice(&length.to_le_bytes());
        encoded.extend_from_slice(&(!length).to_le_bytes());
        encoded.extend_from_slice(chunk);
    }
    encoded
}

fn member(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
    encoded.extend_from_slice(&stored_deflate(bytes));
    encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    encoded
}

fn bgzf_member(bytes: &[u8]) -> Vec<u8> {
    let deflate = stored_deflate(bytes);
    let total_size = 18 + deflate.len() + 8;
    assert!(total_size <= u16::MAX as usize + 1);
    let block_size = (total_size - 1) as u16;
    let mut encoded = b"\x1f\x8b\x08\x04\0\0\0\0\x00\xff\x06\x00BC\x02\x00".to_vec();
    encoded.extend_from_slice(&block_size.to_le_bytes());
    encoded.extend_from_slice(&deflate);
    encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    encoded
}

fn bgzf_eof() -> Vec<u8> {
    vec![
        31, 139, 8, 4, 0, 0, 0, 0, 0, 255, 6, 0, 66, 67, 2, 0, 27, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]
}

fn build_index(compressed: &[u8], checkpoint_spacing: usize) -> (Vec<u8>, GzipIndex) {
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .checkpoint_spacing(checkpoint_spacing)
        .decoded_chunk_size(4 * 1024)
        .build()
        .unwrap()
        .decode(compressed, &mut decoded)
        .unwrap();
    let index = report.index.expect("keep_index");
    (decoded, index)
}

fn decode_with_index_bytes(
    compressed: &[u8],
    index: &GzipIndex,
    threads: usize,
) -> (Vec<u8>, rapidgzip_core::DecodeReport) {
    let mut out = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(threads)
        .decoded_chunk_size(4 * 1024)
        .input_page_size(4 * 1024)
        .build()
        .unwrap()
        .decode_with_index(compressed, index, &mut out)
        .unwrap();
    (out, report)
}

#[test]
fn parallel_index_decode_matches_plain_decode() {
    let payload: Vec<u8> = (0..80_000u32).map(|i| (i % 251) as u8).collect();
    let compressed = member(&payload);
    let (expected, index) = build_index(&compressed, 4_096);
    assert_eq!(expected, payload);
    assert!(
        index.checkpoint_count() >= 3,
        "need intermediate checkpoints for parallelism, got {}",
        index.checkpoint_count()
    );

    let (decoded, report) = decode_with_index_bytes(&compressed, &index, 4);
    assert_eq!(decoded, payload);
    assert_eq!(report.decompressed_bytes, payload.len() as u64);
    assert_eq!(report.compressed_bytes, compressed.len() as u64);
    assert!(report.index.is_none(), "must not clone caller's index");
}

#[test]
fn threads_one_and_four_produce_identical_output() {
    let payload: Vec<u8> = (0..60_000u32).map(|i| (i.wrapping_mul(17) % 256) as u8).collect();
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 8_192);

    let (out1, _) = decode_with_index_bytes(&compressed, &index, 1);
    let (out4, _) = decode_with_index_bytes(&compressed, &index, 4);
    assert_eq!(out1, payload);
    assert_eq!(out4, payload);
    assert_eq!(out1, out4);
}

#[test]
fn bgzf_multi_block_parallel_index_decode() {
    let b1: Vec<u8> = (0..3_000u32).map(|i| (i % 200) as u8).collect();
    let b2: Vec<u8> = (0..4_000u32).map(|i| ((i * 3) % 200) as u8).collect();
    let b3: Vec<u8> = (0..2_500u32).map(|i| ((i * 7) % 200) as u8).collect();
    let mut compressed = bgzf_member(&b1);
    compressed.extend(bgzf_member(&b2));
    compressed.extend(bgzf_member(&b3));
    compressed.extend(bgzf_eof());

    let expected: Vec<u8> = [b1.as_slice(), b2.as_slice(), b3.as_slice()].concat();
    let (decoded_ref, index) = build_index(&compressed, 1_024);
    assert_eq!(decoded_ref, expected);

    let (decoded, report) = decode_with_index_bytes(&compressed, &index, 4);
    assert_eq!(decoded, expected);
    assert_eq!(report.decompressed_bytes, expected.len() as u64);
}

#[test]
fn multi_member_index_decode() {
    let m1: Vec<u8> = b"first-member-payload-".repeat(500);
    let m2: Vec<u8> = b"second-".repeat(800);
    let m3: Vec<u8> = b"third-and-final-".repeat(600);
    let mut compressed = member(&m1);
    compressed.extend(member(&m2));
    compressed.extend(member(&m3));

    let expected: Vec<u8> = [m1.as_slice(), m2.as_slice(), m3.as_slice()].concat();
    let (decoded_ref, index) = build_index(&compressed, 2_048);
    assert_eq!(decoded_ref, expected);

    let (decoded, report) = decode_with_index_bytes(&compressed, &index, 4);
    assert_eq!(decoded, expected);
    assert_eq!(report.decompressed_bytes, expected.len() as u64);
    // At least one STREAM_END per member when segments cover full members.
    assert!(report.member_count >= 1);
}

#[test]
fn mismatched_archive_size_errors() {
    let payload = b"size mismatch test payload";
    let compressed = member(payload);
    let (_, mut index) = build_index(&compressed, 1_000);
    index.compressed_size_in_bytes = compressed.len() as u64 + 1;

    let mut out = Vec::new();
    let err = Decoder::builder()
        .decoder_threads(2)
        .build()
        .unwrap()
        .decode_with_index(&compressed, &index, &mut out)
        .unwrap_err();
    assert!(
        matches!(err, DecodeError::InvalidIndex(_)),
        "expected InvalidIndex, got {err:?}"
    );
}

#[test]
fn empty_checkpoints_errors() {
    let payload = b"empty index";
    let compressed = member(payload);
    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = compressed.len() as u64;
    index.uncompressed_size_in_bytes = payload.len() as u64;

    let mut out = Vec::new();
    let err = Decoder::builder()
        .build()
        .unwrap()
        .decode_with_index(&compressed, &index, &mut out)
        .unwrap_err();
    assert!(matches!(err, DecodeError::InvalidIndex(_)));
}

#[test]
fn single_checkpoint_with_known_size() {
    // Build a normal index then strip intermediate checkpoints, leaving start
    // only (no EOF pair). decode_with_index should still produce full output.
    let payload: Vec<u8> = (0..5_000u32).map(|i| (i % 100) as u8).collect();
    let compressed = member(&payload);
    let (_, mut index) = build_index(&compressed, 1_024);
    assert!(index.checkpoint_count() >= 2);
    let first = index.checkpoints[0];
    index.checkpoints = vec![first];
    // Keep only the start window if present.
    let start_bit = first.compressed_offset_in_bits;
    let windows = index.windows.clone();
    index.windows = rapidgzip_core::WindowMap::new();
    if let Some(w) = windows.get(start_bit) {
        index.windows.insert(start_bit, w.clone());
    }

    let (decoded, report) = decode_with_index_bytes(&compressed, &index, 1);
    assert_eq!(decoded, payload);
    assert_eq!(report.decompressed_bytes, payload.len() as u64);
}

#[test]
fn empty_payload_index_decode() {
    let payload = b"";
    let compressed = member(payload);
    let (expected, index) = build_index(&compressed, 1_024);
    assert_eq!(expected, payload);

    let (decoded, report) = decode_with_index_bytes(&compressed, &index, 2);
    assert_eq!(decoded, payload);
    assert_eq!(report.decompressed_bytes, 0);
}

#[test]
fn bgzi_export_import_then_parallel_decode_bgzf() {
    let b1: Vec<u8> = (0..3_000u32).map(|i| (i % 200) as u8).collect();
    let b2: Vec<u8> = (0..4_000u32).map(|i| ((i * 3) % 200) as u8).collect();
    let b3: Vec<u8> = (0..2_500u32).map(|i| ((i * 7) % 200) as u8).collect();
    let mut compressed = bgzf_member(&b1);
    compressed.extend(bgzf_member(&b2));
    compressed.extend(bgzf_member(&b3));
    compressed.extend(bgzf_eof());
    let expected: Vec<u8> = [b1.as_slice(), b2.as_slice(), b3.as_slice()].concat();

    let (decoded_ref, index) = build_index(&compressed, usize::MAX / 4);
    assert_eq!(decoded_ref, expected);

    let mut exported = Vec::new();
    index.export_bgzi(&mut exported).unwrap();
    let restored = GzipIndex::import_bgzi(
        &mut std::io::Cursor::new(&exported),
        Some(compressed.len() as u64),
    )
    .unwrap();

    let (decoded, report) = decode_with_index_bytes(&compressed, &restored, 4);
    assert_eq!(decoded, expected);
    assert_eq!(report.decompressed_bytes, expected.len() as u64);
}

#[test]
fn export_import_round_trip_then_parallel_decode() {
    let payload: Vec<u8> = (0..40_000u32).map(|i| ((i * 13) % 256) as u8).collect();
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 4_096);

    let mut exported = Vec::new();
    index.export_indexed_gzip(&mut exported).unwrap();
    let restored = GzipIndex::import_indexed_gzip(
        &mut Cursor::new(&exported),
        Some(compressed.len() as u64),
    )
    .unwrap();

    let (decoded, _) = decode_with_index_bytes(&compressed, &restored, 4);
    assert_eq!(decoded, payload);
}
