//! Random-access reads through an index.

mod common;

use common::{corpus, gzip};
use rapidgzip_core::{
    Checkpoint, CheckpointKind, Decoder, DeflateIndex, IndexKind, IndexOptions, IndexedReader,
    StoredWindow,
};
use std::io::{Read, Seek, SeekFrom};

/// An index holding only the first member boundary, which every reader can
/// build without help from the decoder.
fn origin_index(compressed: &[u8], uncompressed_size: u64) -> DeflateIndex {
    let mut index = DeflateIndex::new();
    index.set_compressed_size(Some(compressed.len() as u64));
    index.set_uncompressed_size(Some(uncompressed_size));
    index
        .push(
            Checkpoint {
                compressed_offset_in_bits: 0,
                uncompressed_offset_in_bytes: 0,
                kind: CheckpointKind::GzipMemberHeader,
                line_offset: None,
            },
            StoredWindow::empty(),
        )
        .expect("origin checkpoint");
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
    index
        .push(
            Checkpoint {
                compressed_offset_in_bits: first_compressed.len() as u64 * 8,
                uncompressed_offset_in_bytes: first.len() as u64,
                kind: CheckpointKind::GzipMemberHeader,
                line_offset: None,
            },
            StoredWindow::empty(),
        )
        .expect("second member checkpoint");

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
    index.set_uncompressed_size(None);

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
    let mut reader = IndexedReader::new(compressed, DeflateIndex::new()).expect("indexed reader");
    let mut buffer = [0u8; 16];
    let error = reader.read(&mut buffer).expect_err("no checkpoint");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

fn built_index(compressed: &[u8], threads: usize) -> DeflateIndex {
    let decoder = Decoder::builder()
        .decoder_threads(threads)
        .build()
        .expect("builder");
    let mut reader = decoder
        .reader_with_index(compressed.to_vec(), IndexOptions::default())
        .expect("reader");
    std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
    reader.finish().expect("indexed report").index
}

#[test]
fn explicit_indexing_preserves_the_copy_decode_report() {
    fn scalar<T: AsRef<rapidgzip_core::DecodeReport>>(value: &T) -> u64 {
        value.as_ref().decompressed_bytes
    }

    let plain = corpus(256 * 1024);
    let compressed = gzip(&plain, 6);
    let decoder = Decoder::builder()
        .decoder_threads(1)
        .build()
        .expect("builder");
    let mut output = Vec::new();
    let indexed = decoder
        .decode_with_index(&compressed, &mut output, IndexOptions::default())
        .expect("indexed decode");
    let copied = indexed.decode;

    assert_eq!(output, plain);
    assert_eq!(scalar(&indexed), plain.len() as u64);
    assert_eq!(scalar(&copied), plain.len() as u64);
    assert_eq!(indexed.into_parts().0, copied);
}

#[test]
fn sequential_and_streaming_apis_build_the_same_member_index() {
    use std::io::Cursor;

    let first = corpus(200 * 1024);
    let second = corpus(150 * 1024);
    let mut compressed = gzip(&first, 6);
    compressed.extend_from_slice(&gzip(&second, 6));
    let decoder = Decoder::builder()
        .decoder_threads(1)
        .build()
        .expect("builder");

    let mut positional_output = Vec::new();
    let positional = decoder
        .decode_with_index(&compressed, &mut positional_output, IndexOptions::default())
        .expect("positional index");

    let mut streaming_output = Vec::new();
    let streaming = decoder
        .decode_stream_with_index(
            Cursor::new(compressed.clone()),
            &mut streaming_output,
            IndexOptions::default(),
        )
        .expect("stream index");

    let mut pull = decoder
        .stream_reader_with_index(Cursor::new(compressed), IndexOptions::default())
        .expect("stream reader");
    std::io::copy(&mut pull, &mut std::io::sink()).expect("pull decode");
    let pull = pull.finish().expect("pull index");

    assert_eq!(positional_output, [first, second].concat());
    assert_eq!(streaming_output, positional_output);
    assert_eq!(streaming.index, positional.index);
    assert_eq!(pull.index, positional.index);
    assert!(
        positional
            .index
            .checkpoints()
            .iter()
            .all(|point| matches!(point.kind, CheckpointKind::GzipMemberDeflate { .. }))
    );
}

#[test]
fn dense_members_publish_only_authenticated_member_boundaries() {
    let members: Vec<_> = (0..12).map(|_| corpus(96 * 1024)).collect();
    let plain = members.concat();
    let compressed: Vec<_> = members.iter().flat_map(|member| gzip(member, 6)).collect();

    let index = built_index(&compressed, 4);
    index.validate().expect("valid index");
    assert_eq!(index.checkpoint_count(), members.len());
    assert!(index.windows().is_empty());

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    reader.seek(SeekFrom::Start(700_000)).expect("seek");
    let mut output = vec![0; 4096];
    reader.read_exact(&mut output).expect("read");
    assert_eq!(output, &plain[700_000..704_096]);
}

#[test]
fn stored_streams_publish_independent_block_boundaries() {
    let plain = corpus(10 * 1024 * 1024);
    let compressed = gzip(&plain, 0);
    let index = built_index(&compressed, 4);

    assert!(index.checkpoint_count() > 2);
    assert!(
        index
            .checkpoints()
            .iter()
            .skip(1)
            .any(|point| matches!(point.kind, CheckpointKind::DeflateBlock))
    );
    assert!(index.windows().is_empty());

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    reader.seek(SeekFrom::Start(8_500_000)).expect("seek");
    let mut output = vec![0; 2048];
    reader.read_exact(&mut output).expect("read");
    assert_eq!(output, &plain[8_500_000..8_502_048]);
}

#[test]
fn automatic_index_is_seekable_with_or_without_marker_admission() {
    let plain = corpus(24 * 1024 * 1024);
    let compressed = gzip(&plain, 6);
    let index = built_index(&compressed, 4);

    let has_interior_windows = index
        .checkpoints()
        .iter()
        .any(|point| matches!(point.kind, CheckpointKind::DeflateBlock));
    if has_interior_windows {
        assert!(index.checkpoint_count() >= 3);
        assert!(!index.windows().is_empty());
    } else {
        // Automatic path admission is intentionally timing-dependent. A
        // debug build may choose authoritative sequential decoding and its
        // coarser member-boundary index; seeking must remain equivalent.
        assert_eq!(index.checkpoint_count(), 1);
        assert!(index.windows().is_empty());
    }

    let mut reader = IndexedReader::new(compressed.clone(), index.clone()).expect("indexed reader");
    for target in [20_000_000usize, 5_000_000, 12_000_000, 1000] {
        reader.seek(SeekFrom::Start(target as u64)).expect("seek");
        let mut output = vec![0; 2048];
        reader.read_exact(&mut output).expect("read");
        assert_eq!(output, &plain[target..target + 2048], "target {target}");
    }

    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("GZIDX write");
    let restored = DeflateIndex::read_gzidx(&mut bytes.as_slice(), Some(compressed.len() as u64))
        .expect("GZIDX read");
    let mut reader = IndexedReader::new(compressed, restored).expect("restored reader");
    reader.seek(SeekFrom::Start(15_000_000)).expect("seek");
    let mut output = vec![0; 4096];
    reader.read_exact(&mut output).expect("read");
    assert_eq!(output, &plain[15_000_000..15_004_096]);
}

#[test]
fn bgzf_builds_every_nonempty_block_and_exports_gzi() {
    let plain = corpus(2 * 1024 * 1024);
    let compressed = common::bgzf(&plain, 48 * 1024);
    let expected_blocks = plain.len().div_ceil(48 * 1024);
    let index = built_index(&compressed, 4);

    assert_eq!(index.kind(), IndexKind::Bgzf);
    assert_eq!(index.checkpoint_count(), expected_blocks);
    assert!(index.windows().is_empty());

    let mut gzi = Vec::new();
    index.write_gzi(&mut gzi).expect("gzi write");
    assert_eq!(
        u64::from_le_bytes(gzi[..8].try_into().expect("pair count")) as usize,
        expected_blocks - 1
    );
    let restored = DeflateIndex::read_gzi(&mut gzi.as_slice(), Some(compressed.len() as u64))
        .expect("gzi read");
    let mut reader = IndexedReader::new(compressed, restored).expect("indexed reader");
    reader.seek(SeekFrom::Start(1_500_000)).expect("seek");
    let mut output = vec![0; 1024];
    reader.read_exact(&mut output).expect("read");
    assert_eq!(output, &plain[1_500_000..1_501_024]);
}

#[test]
fn member_checkpoints_detect_footer_corruption_after_a_seek() {
    let plain = corpus(512 * 1024);
    let mut compressed = gzip(&plain, 6);
    let index = built_index(&compressed, 1);
    let crc_byte = compressed.len() - 8;
    compressed[crc_byte] ^= 1;

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    reader.seek(SeekFrom::Start(100_000)).expect("seek");
    let error = reader.read_to_end(&mut Vec::new()).expect_err("bad CRC");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn indexed_reader_rejects_a_known_source_size_mismatch_at_open() {
    let plain = corpus(64 * 1024);
    let mut compressed = gzip(&plain, 6);
    let index = built_index(&compressed, 1);
    compressed.push(0);

    let error = IndexedReader::new(compressed, index)
        .err()
        .expect("source-size mismatch");
    assert!(matches!(
        error,
        rapidgzip_core::IndexedReaderError::Index(
            rapidgzip_core::IndexError::ArchiveSizeMismatch { .. }
        )
    ));
}

#[test]
fn indexed_reader_rejects_a_truncated_footer() {
    let plain = corpus(64 * 1024);
    let mut compressed = gzip(&plain, 6);
    let mut index = built_index(&compressed, 1);
    compressed.pop();
    index.set_compressed_size(Some(compressed.len() as u64));

    let mut reader = IndexedReader::new(compressed, index).expect("indexed reader");
    let error = reader
        .read_to_end(&mut Vec::new())
        .expect_err("truncated footer");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn indexed_reader_rejects_isize_mismatch_and_trailing_garbage() {
    let plain = corpus(64 * 1024);
    let compressed = gzip(&plain, 6);
    let index = built_index(&compressed, 1);

    let mut bad_size = compressed.clone();
    let final_byte = bad_size.len() - 1;
    bad_size[final_byte] ^= 1;
    let mut reader = IndexedReader::new(bad_size, index.clone()).expect("indexed reader");
    let error = reader.read_to_end(&mut Vec::new()).expect_err("bad ISIZE");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

    let mut trailing = compressed;
    trailing.extend_from_slice(b"not gzip");
    let mut index = index;
    index.set_compressed_size(None);
    let mut reader = IndexedReader::new(trailing, index).expect("indexed reader");
    let error = reader
        .read_to_end(&mut Vec::new())
        .expect_err("trailing garbage");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}
