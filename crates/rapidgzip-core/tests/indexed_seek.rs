//! Random-access reads through an index.

mod common;

use common::{corpus, gzip};
use rapidgzip_core::{Checkpoint, GzipIndex, IndexedReader, StoredWindow};
use std::io::{Read, Seek, SeekFrom};

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
