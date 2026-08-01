//! Random-access reads through an index.

mod common;

use common::{corpus, gzip};
use rapidgzip_core::{Checkpoint, Decoder, GzipIndex, IndexedReader, StoredWindow};
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

/// Bytes in a minimal gzip member header, which carries no optional fields.
const GZIP_HEADER_BYTES: u64 = 10;

/// An index holding only the first member boundary, which every reader can
/// build without help from the decoder.
fn origin_index(compressed: &[u8], uncompressed_size: u64) -> GzipIndex {
    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = compressed.len() as u64;
    index.uncompressed_size_in_bytes = uncompressed_size;
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 0,
            uncompressed_offset_in_bytes: 0,
            line_offset: 0,
        },
        StoredWindow::empty(),
    );
    index
}

#[test]
fn reading_from_the_origin_reproduces_the_whole_member() {
    let plain = corpus(512 * 1024);
    let compressed = gzip(&plain, 6);
    let index = origin_index(&compressed, plain.len() as u64);

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    let mut output = Vec::new();
    reader.read_to_end(&mut output).expect("read to end");
    assert_eq!(output, plain);
}

#[test]
fn reading_from_the_origin_crosses_member_boundaries() {
    let first = corpus(200 * 1024);
    let second = corpus(300 * 1024);
    let mut plain = first.clone();
    plain.extend_from_slice(&second);

    let mut compressed = gzip(&first, 6);
    compressed.extend_from_slice(&gzip(&second, 6));
    let index = origin_index(&compressed, plain.len() as u64);

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    let mut output = Vec::new();
    reader.read_to_end(&mut output).expect("read to end");
    assert_eq!(output.len(), plain.len());
    assert_eq!(output, plain);
}

#[test]
fn seeking_forward_from_the_origin_matches_a_full_decode() {
    let plain = corpus(1024 * 1024);
    let compressed = gzip(&plain, 6);
    let index = origin_index(&compressed, plain.len() as u64);

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    for target in [0usize, 1, 1000, 500_000, plain.len() - 4096] {
        reader.seek(SeekFrom::Start(target as u64)).expect("seek");
        let mut buffer = vec![0u8; 4096.min(plain.len() - target)];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(
            buffer,
            &plain[target..target + buffer.len()],
            "mismatch after seeking to {target}"
        );
    }
}

#[test]
fn seeking_to_a_member_boundary_checkpoint_skips_its_header() {
    let first = corpus(100 * 1024);
    let second = corpus(100 * 1024);
    let mut plain = first.clone();
    plain.extend_from_slice(&second);

    let first_compressed = gzip(&first, 6);
    let mut compressed = first_compressed.clone();
    compressed.extend_from_slice(&gzip(&second, 6));

    let mut index = origin_index(&compressed, plain.len() as u64);
    index.push(
        Checkpoint {
            compressed_offset_in_bits: first_compressed.len() as u64 * 8,
            uncompressed_offset_in_bytes: first.len() as u64,
            line_offset: 0,
        },
        StoredWindow::empty(),
    );

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    for target in [first.len(), first.len() + 10, first.len() + 50_000] {
        reader.seek(SeekFrom::Start(target as u64)).expect("seek");
        let mut buffer = vec![0u8; 1024];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(buffer, &plain[target..target + 1024], "target {target}");
    }
}

#[test]
fn repeated_backward_seeks_stay_correct() {
    let plain = corpus(1024 * 1024);
    let compressed = gzip(&plain, 6);
    let index = origin_index(&compressed, plain.len() as u64);

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    for _ in 0..3 {
        for target in [900_000usize, 10, 400_000, 10] {
            reader.seek(SeekFrom::Start(target as u64)).expect("seek");
            let mut buffer = vec![0u8; 512];
            reader.read_exact(&mut buffer).expect("read");
            assert_eq!(buffer, &plain[target..target + 512]);
        }
    }
}

#[test]
fn seeking_past_the_end_reads_nothing() {
    let plain = corpus(64 * 1024);
    let compressed = gzip(&plain, 6);
    let index = origin_index(&compressed, plain.len() as u64);

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    reader
        .seek(SeekFrom::Start(plain.len() as u64 + 1000))
        .expect("seek");
    let mut buffer = [0u8; 16];
    assert_eq!(reader.read(&mut buffer).expect("read"), 0);
}

#[test]
fn seek_from_end_uses_the_recorded_size() {
    let plain = corpus(64 * 1024);
    let compressed = gzip(&plain, 6);
    let index = origin_index(&compressed, plain.len() as u64);

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    assert_eq!(
        reader.seek(SeekFrom::End(-16)).expect("seek"),
        plain.len() as u64 - 16
    );
    let mut buffer = [0u8; 16];
    reader.read_exact(&mut buffer).expect("read");
    assert_eq!(&buffer[..], &plain[plain.len() - 16..]);
}

#[test]
fn seek_from_end_without_a_known_size_is_unsupported() {
    let plain = corpus(64 * 1024);
    let compressed = gzip(&plain, 6);
    let mut index = origin_index(&compressed, plain.len() as u64);
    index.uncompressed_size_in_bytes = u64::MAX;

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    let error = reader.seek(SeekFrom::End(-10)).expect_err("unsupported");
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
}

#[test]
fn seeking_relative_to_the_current_position_works() {
    let plain = corpus(256 * 1024);
    let compressed = gzip(&plain, 6);
    let index = origin_index(&compressed, plain.len() as u64);

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    reader.seek(SeekFrom::Start(1000)).expect("seek");
    let mut buffer = [0u8; 100];
    reader.read_exact(&mut buffer).expect("read");
    assert_eq!(reader.seek(SeekFrom::Current(400)).expect("seek"), 1500);
    reader.read_exact(&mut buffer).expect("read");
    assert_eq!(&buffer[..], &plain[1500..1600]);
    assert_eq!(reader.seek(SeekFrom::Current(-600)).expect("seek"), 1000);
    reader.read_exact(&mut buffer).expect("read");
    assert_eq!(&buffer[..], &plain[1000..1100]);
}

#[test]
fn an_empty_index_reports_a_useful_error() {
    let plain = corpus(1024);
    let compressed = gzip(&plain, 6);
    let mut reader = IndexedReader::new(compressed, GzipIndex::new()).expect("indexed reader");
    let mut buffer = [0u8; 16];
    let error = reader.read(&mut buffer).expect_err("no checkpoint");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

/// Decodes `compressed` fully and returns the index the decoder collected.
fn built_index(compressed: &[u8], threads: usize) -> GzipIndex {
    let decoder = Decoder::builder()
        .decoder_threads(threads)
        .build_index(true)
        .build()
        .expect("builder");
    let mut reader = decoder.reader(compressed.to_vec()).expect("reader");
    std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
    reader
        .finish()
        .expect("report")
        .index
        .expect("index was requested")
}

#[test]
fn an_index_is_absent_unless_requested() {
    let plain = corpus(256 * 1024);
    let compressed = gzip(&plain, 6);
    let decoder = Decoder::builder().build().expect("builder");
    let mut reader = decoder.reader(compressed).expect("reader");
    std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
    assert!(reader.finish().expect("report").index.is_none());
}

#[test]
fn a_built_index_records_member_starts_and_sizes() {
    let first = corpus(300 * 1024);
    let second = corpus(200 * 1024);
    let mut plain = first.clone();
    plain.extend_from_slice(&second);

    let first_compressed = gzip(&first, 6);
    let mut compressed = first_compressed.clone();
    compressed.extend_from_slice(&gzip(&second, 6));

    let index = built_index(&compressed, 1);
    index.validate().expect("invariants hold");
    assert_eq!(index.compressed_size_in_bytes, compressed.len() as u64);
    assert_eq!(index.uncompressed_size_in_bytes, plain.len() as u64);
    assert_eq!(
        index
            .checkpoints()
            .iter()
            .map(|point| (
                point.compressed_offset_in_bits,
                point.uncompressed_offset_in_bytes
            ))
            .collect::<Vec<_>>(),
        // A member checkpoint records where DEFLATE begins, past the ten-byte
        // member header, which is the offset indexed_gzip and gztool resume
        // from.
        vec![
            (GZIP_HEADER_BYTES * 8, 0),
            (
                (first_compressed.len() as u64 + GZIP_HEADER_BYTES) * 8,
                first.len() as u64
            )
        ]
    );
}

#[test]
fn a_built_index_seeks_correctly() {
    let first = corpus(300 * 1024);
    let second = corpus(200 * 1024);
    let mut plain = first.clone();
    plain.extend_from_slice(&second);

    let mut compressed = gzip(&first, 6);
    compressed.extend_from_slice(&gzip(&second, 6));

    let index = built_index(&compressed, 1);
    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    for target in [0usize, first.len() - 5, first.len(), first.len() + 1000] {
        reader.seek(SeekFrom::Start(target as u64)).expect("seek");
        let mut buffer = vec![0u8; 512];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(buffer, &plain[target..target + 512], "target {target}");
    }
}

#[test]
fn a_built_index_survives_a_native_round_trip() {
    let plain = corpus(400 * 1024);
    let compressed = gzip(&plain, 6);
    let index = built_index(&compressed, 1);

    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    let restored = GzipIndex::read_native(&mut bytes.as_slice()).expect("read");
    assert_eq!(restored, index);

    let mut reader = IndexedReader::new(compressed, restored).expect("indexed reader");
    reader.seek(SeekFrom::Start(200_000)).expect("seek");
    let mut buffer = vec![0u8; 1024];
    reader.read_exact(&mut buffer).expect("read");
    assert_eq!(buffer, &plain[200_000..201_024]);
}

#[test]
fn a_streaming_decode_builds_the_same_index() {
    use std::io::Cursor;

    let first = corpus(200 * 1024);
    let second = corpus(150 * 1024);
    let mut compressed = gzip(&first, 6);
    compressed.extend_from_slice(&gzip(&second, 6));

    let positional = built_index(&compressed, 1);

    let decoder = Decoder::builder()
        .build_index(true)
        .build()
        .expect("builder");
    let mut reader = decoder
        .stream_reader(Cursor::new(compressed.clone()))
        .expect("stream reader");
    std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
    let streamed = reader
        .finish()
        .expect("report")
        .index
        .expect("index was requested");

    assert_eq!(streamed, positional);
}

#[test]
fn the_parallel_path_produces_interior_checkpoints() {
    let plain = corpus(24 * 1024 * 1024);
    let compressed = gzip(&plain, 6);

    let index = built_index(&compressed, 4);
    index.validate().expect("invariants hold");
    assert!(
        index.checkpoint_count() >= 3,
        "one member produced only {} checkpoints",
        index.checkpoint_count()
    );
    assert!(
        index
            .checkpoints()
            .iter()
            .skip(1)
            .any(|point| !point.compressed_offset_in_bits.is_multiple_of(8)),
        "no interior checkpoint landed off a byte boundary"
    );

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    for target in [20_000_000usize, 5_000_000, 12_000_000, 1000] {
        reader.seek(SeekFrom::Start(target as u64)).expect("seek");
        let mut buffer = vec![0u8; 2048];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(buffer, &plain[target..target + 2048], "target {target}");
    }
}

#[test]
fn interior_checkpoints_survive_a_gzidx_round_trip() {
    let plain = corpus(24 * 1024 * 1024);
    let compressed = gzip(&plain, 6);
    let index = built_index(&compressed, 4);

    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");
    let restored =
        GzipIndex::read_gzidx(&mut bytes.as_slice(), Some(compressed.len() as u64)).expect("read");
    assert_eq!(restored.checkpoint_count(), index.checkpoint_count());

    let mut reader = IndexedReader::new(compressed, restored).expect("indexed reader");
    reader.seek(SeekFrom::Start(15_000_000)).expect("seek");
    let mut buffer = vec![0u8; 4096];
    reader.read_exact(&mut buffer).expect("read");
    assert_eq!(buffer, &plain[15_000_000..15_004_096]);
}

#[test]
fn a_bgzf_index_records_every_block_start() {
    let plain = corpus(2 * 1024 * 1024);
    let compressed = common::bgzf(&plain, 48 * 1024);
    let expected_blocks = plain.len().div_ceil(48 * 1024);

    let index = built_index(&compressed, 4);
    index.validate().expect("invariants hold");
    assert_eq!(index.checkpoint_count(), expected_blocks);
    assert!(
        index.windows().is_empty(),
        "BGZF blocks need no predecessor window"
    );

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    for target in [0usize, 100_000, 1_500_000, plain.len() - 1024] {
        reader.seek(SeekFrom::Start(target as u64)).expect("seek");
        let mut buffer = vec![0u8; 1024.min(plain.len() - target)];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(buffer, &plain[target..target + buffer.len()], "at {target}");
    }
}

#[test]
fn a_bgzf_index_exports_as_gzi() {
    let plain = corpus(1024 * 1024);
    let compressed = common::bgzf(&plain, 48 * 1024);
    let index = built_index(&compressed, 4);

    let mut bytes = Vec::new();
    index.write_gzi(&mut bytes).expect("gzi export");
    let pairs = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    assert_eq!(pairs as usize, index.checkpoint_count() - 1);

    let restored =
        GzipIndex::read_gzi(&mut bytes.as_slice(), Some(compressed.len() as u64)).expect("read");
    assert_eq!(restored.checkpoints(), index.checkpoints());

    let mut reader = IndexedReader::new(compressed, restored).expect("indexed reader");
    reader.seek(SeekFrom::Start(700_000)).expect("seek");
    let mut buffer = vec![0u8; 1024];
    reader.read_exact(&mut buffer).expect("read");
    assert_eq!(buffer, &plain[700_000..701_024]);
}

/// Builds text whose lines are long enough to span several checkpoints.
fn numbered_lines(count: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for index in 0..count {
        bytes.extend_from_slice(format!("{index:08} {}\n", "payload".repeat(8)).as_bytes());
    }
    bytes
}

fn line_indexed_reader(decoded: &[u8]) -> IndexedReader<Arc<[u8]>> {
    let compressed: Arc<[u8]> = gzip(decoded, 6).into();
    let decoder = Decoder::builder()
        .decoder_threads(4)
        .count_lines(true)
        .build_index(true)
        .index_spacing(64 * 1024)
        .build()
        .expect("decoder");
    let report = decoder
        .decode(&compressed, &mut std::io::sink())
        .expect("decode");
    IndexedReader::new(compressed, report.index.expect("index")).expect("reader")
}

#[test]
fn seeking_to_a_line_lands_on_its_first_byte() {
    let decoded = numbered_lines(20_000);
    let mut reader = line_indexed_reader(&decoded);

    for line in [0_u64, 1, 999, 10_000, 19_999] {
        let offset = reader.seek_to_line(line).expect("seek");
        let expected = decoded
            .iter()
            .enumerate()
            .filter(|&(_, &byte)| byte == b'\n')
            .nth(line.wrapping_sub(1) as usize)
            .map_or(0, |(index, _)| index as u64 + 1);
        assert_eq!(offset, expected, "wrong offset for line {line}");

        let mut buffer = vec![0_u8; 8];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(buffer, format!("{line:08}").as_bytes());
    }
}

#[test]
fn seeking_past_the_last_line_lands_at_the_end() {
    let decoded = numbered_lines(500);
    let mut reader = line_indexed_reader(&decoded);
    let offset = reader.seek_to_line(10_000).expect("seek");
    assert_eq!(offset, decoded.len() as u64);
    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).expect("read");
    assert!(rest.is_empty());
}

#[test]
fn seeking_by_line_needs_an_index_that_records_them() {
    let decoded = numbered_lines(500);
    let compressed: Arc<[u8]> = gzip(&decoded, 6).into();
    let decoder = Decoder::builder()
        .build_index(true)
        .build()
        .expect("decoder");
    let report = decoder
        .decode(&compressed, &mut std::io::sink())
        .expect("decode");
    let mut reader = IndexedReader::new(compressed, report.index.expect("index")).expect("reader");

    let error = reader.seek_to_line(1).expect_err("no line counters");
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
}
