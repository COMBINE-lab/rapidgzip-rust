//! Newline counting and line-aware index construction across decode paths.

mod common;

use common::{bgzf, corpus, gzip, raw_deflate, zlib};
use rapidgzip_core::{Decoder, DecoderPath, Format, IndexOptions, IndexedReader};
use std::io::{self, Cursor, Read};
use std::num::NonZeroU64;
use std::sync::Arc;

fn text(line_count: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for line in 0..line_count {
        bytes.extend_from_slice(format!("{line:08} {}", "payload".repeat(8)).as_bytes());
        if line + 1 < line_count {
            bytes.push(b'\n');
        }
    }
    bytes
}

fn newline_count(bytes: &[u8]) -> u64 {
    bytes.iter().filter(|&&byte| byte == b'\n').count() as u64
}

fn counting_decoder(threads: usize) -> Decoder {
    Decoder::builder()
        .decoder_threads(threads)
        .count_lines(true)
        .build()
        .expect("decoder")
}

#[test]
fn counting_is_off_by_default() {
    let report = Decoder::default()
        .decode(&gzip(b"a\nb\n", 6), &mut io::sink())
        .expect("decode");
    assert_eq!(report.line_count, None);
}

#[test]
fn sequential_parallel_bgzf_and_streaming_counts_agree() {
    let decoded = corpus(6 * 1024 * 1024);
    let expected = newline_count(&decoded);
    let compressed = gzip(&decoded, 6);

    for threads in [1, 8] {
        let mut output = Vec::new();
        let report = counting_decoder(threads)
            .decode(&compressed, &mut output)
            .expect("positional decode");
        assert_eq!(output, decoded);
        assert_eq!(report.line_count, Some(expected));
    }

    let mut output = Vec::new();
    let report = counting_decoder(8)
        .decode(&bgzf(&decoded, 16 * 1024), &mut output)
        .expect("BGZF decode");
    assert_eq!(output, decoded);
    assert_eq!(report.line_count, Some(expected));

    let mut output = Vec::new();
    let report = counting_decoder(8)
        .decode_stream(Cursor::new(compressed.clone()), &mut output)
        .expect("streaming push decode");
    assert_eq!(output, decoded);
    assert_eq!(report.line_count, Some(expected));

    let mut reader = counting_decoder(8)
        .stream_reader(Cursor::new(compressed))
        .expect("streaming reader");
    io::copy(&mut reader, &mut io::sink()).expect("read");
    assert_eq!(reader.finish().expect("finish").line_count, Some(expected));
}

#[test]
fn every_pull_reader_surface_reports_the_same_line_count() {
    let decoded = corpus(2 * 1024 * 1024);
    let expected = newline_count(&decoded);
    let compressed: Arc<[u8]> = gzip(&decoded, 6).into();
    let decoder = counting_decoder(4);

    let mut reader = decoder
        .reader(Arc::clone(&compressed))
        .expect("positional reader");
    io::copy(&mut reader, &mut io::sink()).expect("read positional");
    assert_eq!(reader.finish().expect("finish").line_count, Some(expected));

    let mut indexing = decoder
        .reader_with_index(Arc::clone(&compressed), IndexOptions::default())
        .expect("indexing reader");
    io::copy(&mut indexing, &mut io::sink()).expect("read indexing");
    let indexed = indexing.finish().expect("finish indexing");
    assert_eq!(indexed.decode.line_count, Some(expected));
    assert_eq!(indexed.index.total_line_count(), Some(expected));

    let mut from_index = decoder
        .reader_from_index(Arc::clone(&compressed), Arc::new(indexed.index))
        .expect("reader from index");
    io::copy(&mut from_index, &mut io::sink()).expect("read from index");
    assert_eq!(
        from_index.finish().expect("finish from index").line_count,
        Some(expected)
    );

    let mut stream_indexing = decoder
        .stream_reader_with_index(Cursor::new(compressed), IndexOptions::default())
        .expect("stream indexing reader");
    io::copy(&mut stream_indexing, &mut io::sink()).expect("read stream indexing");
    let streamed = stream_indexing.finish().expect("finish stream indexing");
    assert_eq!(streamed.decode.line_count, Some(expected));
    assert_eq!(streamed.index.total_line_count(), Some(expected));
}

#[test]
fn specialized_positional_paths_count_only_final_ordered_output() {
    let stored_plain = corpus(10 * 1024 * 1024);
    let bgzf_plain = corpus(4 * 1024 * 1024);
    let dense_plain = corpus(4 * 1024 * 1024);
    let mut dense = Vec::new();
    for member in dense_plain.chunks(64 * 1024) {
        dense.extend_from_slice(&gzip(member, 6));
    }

    for (compressed, plain, expected_path) in [
        (gzip(&stored_plain, 0), stored_plain, DecoderPath::Stored),
        (bgzf(&bgzf_plain, 16 * 1024), bgzf_plain, DecoderPath::Bgzf),
        (dense, dense_plain, DecoderPath::DenseMembers),
    ] {
        let expected = newline_count(&plain);
        let mut reader = counting_decoder(8)
            .reader(compressed)
            .expect("specialized reader");
        let handle = reader.handle();
        io::copy(&mut reader, &mut io::sink()).expect("read specialized path");
        let report = reader.finish().expect("finish specialized path");
        assert_eq!(report.line_count, Some(expected));
        assert_eq!(handle.stats().path, expected_path);
    }
}

#[test]
fn concatenated_and_empty_members_count_in_output_order() {
    let first = b"one\ntwo\n";
    let second = b"three\nfour";
    let mut compressed = gzip(first, 6);
    compressed.extend_from_slice(&gzip(b"", 6));
    compressed.extend_from_slice(&gzip(second, 6));

    let decoder = Decoder::builder()
        .decoder_threads(8)
        .count_lines(true)
        .build()
        .expect("decoder");
    let indexed = decoder
        .decode_with_index(&compressed, &mut io::sink(), IndexOptions::default())
        .expect("decode and index");
    assert_eq!(indexed.decode.line_count, Some(3));
    assert_eq!(indexed.index.total_line_count(), Some(3));
    for checkpoint in indexed.index.checkpoints() {
        let expected = match checkpoint.uncompressed_offset_in_bytes {
            0 => 0,
            offset if offset == first.len() as u64 => 2,
            offset if offset == (first.len() + second.len()) as u64 => 3,
            offset => panic!("unexpected member checkpoint at {offset}"),
        };
        assert_eq!(checkpoint.line_offset, Some(expected));
    }
}

#[test]
fn missing_trailing_newline_and_empty_output_are_exact() {
    for (decoded, expected) in [(&b"a\nb\n"[..], 2), (&b"a\nb"[..], 1), (&b""[..], 0)] {
        let report = counting_decoder(1)
            .decode(&gzip(decoded, 6), &mut io::sink())
            .expect("decode");
        assert_eq!(report.line_count, Some(expected));
    }
}

#[test]
fn zlib_raw_and_strict_indexed_decode_count_lines() {
    let decoded = text(30_000);
    let expected = newline_count(&decoded);

    for (format, compressed) in [
        (Format::Zlib, zlib(&decoded, 6)),
        (Format::RawDeflate, raw_deflate(&decoded, 6)),
    ] {
        let decoder = Decoder::builder()
            .decoder_threads(4)
            .format(format)
            .count_lines(true)
            .build()
            .expect("decoder");
        let report = decoder
            .decode(&compressed, &mut io::sink())
            .expect("decode");
        assert_eq!(report.line_count, Some(expected));
    }

    let compressed = gzip(&decoded, 6);
    let indexed = Decoder::builder()
        .decoder_threads(1)
        .build()
        .expect("builder")
        .decode_with_index(&compressed, &mut io::sink(), IndexOptions::default())
        .expect("build index");
    let report = counting_decoder(4)
        .decode_from_index(&compressed, &mut io::sink(), &indexed.index)
        .expect("strict indexed decode");
    assert_eq!(report.line_count, Some(expected));
}

#[test]
fn every_retained_checkpoint_receives_the_exact_line_offset() {
    let decoded = corpus(24 * 1024 * 1024);
    let expected = newline_count(&decoded);
    let options = IndexOptions {
        checkpoint_spacing: NonZeroU64::new(256 * 1024).expect("non-zero"),
        ..IndexOptions::default()
    };
    let decoder = Decoder::builder()
        .decoder_threads(8)
        .count_lines(true)
        .build()
        .expect("decoder");
    let indexed = decoder
        .decode_with_index(&gzip(&decoded, 6), &mut io::sink(), options)
        .expect("decode and index");

    assert_eq!(indexed.decode.line_count, Some(expected));
    assert_eq!(indexed.index.total_line_count(), Some(expected));
    assert!(indexed.index.checkpoint_count() > 2);
    for checkpoint in indexed.index.checkpoints() {
        let offset = checkpoint.uncompressed_offset_in_bytes as usize;
        assert_eq!(
            checkpoint.line_offset,
            Some(newline_count(&decoded[..offset])),
            "checkpoint at decoded offset {offset}",
        );
    }
}

#[test]
fn seek_to_line_uses_annotated_checkpoints() {
    let decoded = text(20_000);
    let compressed: Arc<[u8]> = gzip(&decoded, 6).into();
    let options = IndexOptions {
        checkpoint_spacing: NonZeroU64::new(64 * 1024).expect("non-zero"),
        ..IndexOptions::default()
    };
    let indexed = counting_decoder(4)
        .decode_with_index(&compressed, &mut io::sink(), options)
        .expect("index");
    let mut reader = IndexedReader::new(Arc::clone(&compressed), indexed.index).expect("reader");

    for line in [0_u64, 1, 999, 10_000, 19_999] {
        let expected = if line == 0 {
            0
        } else {
            decoded
                .iter()
                .enumerate()
                .filter(|&(_, &byte)| byte == b'\n')
                .nth(line as usize - 1)
                .map(|(offset, _)| offset as u64 + 1)
                .expect("line exists")
        };
        assert_eq!(reader.seek_to_line(line).expect("seek"), expected);
        let mut number = [0_u8; 8];
        reader.read_exact(&mut number).expect("read line number");
        assert_eq!(number, format!("{line:08}").as_bytes());
    }
    assert_eq!(
        reader.seek_to_line(100_000).expect("seek past lines"),
        decoded.len() as u64,
    );
}

#[test]
fn seek_to_line_rejects_an_unannotated_index() {
    let decoded = text(100);
    let compressed = gzip(&decoded, 6);
    let index = Decoder::default()
        .decode_with_index(&compressed, &mut io::sink(), IndexOptions::default())
        .expect("index")
        .index;
    let mut reader = IndexedReader::new(compressed, index).expect("reader");
    let error = reader.seek_to_line(1).expect_err("missing line metadata");
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
}
