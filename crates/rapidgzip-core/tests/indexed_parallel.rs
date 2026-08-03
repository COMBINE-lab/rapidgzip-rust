//! Full-stream decoding driven by an existing index.

mod common;

use common::{bgzf, corpus, gzip, raw_deflate, zlib};
use libz_rs_sys as z;
use rapidgzip_core::index::{WINDOW_SIZE, WithLines};
use rapidgzip_core::{
    Checkpoint, CheckpointKind, Decoder, DecoderPath, DeflateIndex, Format, IndexDecodeError,
    IndexKind, IndexOptions, StoredWindow,
};
use std::io::{self, Read, Write};
use std::mem::size_of;
use std::num::NonZeroU64;
use std::sync::Arc;

fn built_index(compressed: &Arc<[u8]>, workers: usize) -> DeflateIndex {
    built_index_for_format(compressed, workers, Format::Gzip)
}

fn built_index_for_format(compressed: &Arc<[u8]>, workers: usize, format: Format) -> DeflateIndex {
    Decoder::builder()
        .decoder_threads(workers)
        .format(format)
        .build()
        .expect("decoder")
        .decode_with_index(compressed, &mut io::sink(), IndexOptions::default())
        .expect("indexed decode")
        .index
}

fn decode_from_index(
    compressed: &Arc<[u8]>,
    index: &DeflateIndex,
    workers: usize,
    format: Format,
) -> Vec<u8> {
    let decoder = Decoder::builder()
        .decoder_threads(workers)
        .format(format)
        .build()
        .expect("decoder");
    let mut output = Vec::new();
    decoder
        .decode_from_index(compressed, &mut output, index)
        .expect("indexed decode");
    output
}

#[test]
fn multi_member_and_empty_member_spans_are_preserved() {
    let parts = [
        corpus(256 * 1024),
        Vec::new(),
        corpus(384 * 1024),
        corpus(128 * 1024),
        Vec::new(),
        corpus(512 * 1024),
    ];
    let mut plain = Vec::new();
    let mut compressed = Vec::new();
    for part in &parts {
        plain.extend_from_slice(part);
        compressed.extend_from_slice(&gzip(part, 6));
    }
    let compressed: Arc<[u8]> = compressed.into();
    let index = built_index(&compressed, 1);
    assert_eq!(index.checkpoint_count(), parts.len());

    for workers in [1, 2, 4, 8] {
        assert_eq!(
            decode_from_index(&compressed, &index, workers, Format::Gzip),
            plain,
            "indexed output differed at {workers} workers"
        );
    }
}

#[test]
fn imported_gzi_without_a_total_size_decodes_bgzf() {
    let plain = corpus(2 * 1024 * 1024);
    let compressed: Arc<[u8]> = bgzf(&plain, 48 * 1024).into();
    let index = built_index(&compressed, 4);
    assert_eq!(index.kind(), IndexKind::Bgzf);
    let mut encoded = Vec::new();
    index.write_gzi(&mut encoded).expect("write gzi");
    let imported = DeflateIndex::read_gzi(&mut encoded.as_slice(), Some(compressed.len() as u64))
        .expect("read gzi");
    assert_eq!(imported.uncompressed_size(), None);

    assert_eq!(
        decode_from_index(&compressed, &imported, 8, Format::Gzip),
        plain
    );
}

#[test]
fn persisted_gzip_index_formats_drive_complete_parallel_decode() {
    let plain = corpus(8 * 1024 * 1024);
    let compressed: Arc<[u8]> = gzip(&plain, 6).into();
    let options = IndexOptions {
        checkpoint_spacing: NonZeroU64::new(512 * 1024).expect("nonzero"),
        ..IndexOptions::default()
    };
    let index = Decoder::builder()
        .decoder_threads(1)
        .build()
        .expect("decoder")
        .decode_with_index(&compressed, &mut io::sink(), options)
        .expect("index build")
        .index;
    assert!(index.checkpoint_count() > 2);

    let mut native_bytes = Vec::new();
    index.write_native(&mut native_bytes).expect("write native");
    let native = DeflateIndex::read_native(&mut native_bytes.as_slice()).expect("read native");

    let mut gzidx_bytes = Vec::new();
    index.write_gzidx(&mut gzidx_bytes).expect("write gzidx");
    let gzidx =
        DeflateIndex::read_gzidx(&mut gzidx_bytes.as_slice(), Some(compressed.len() as u64))
            .expect("read gzidx");

    let mut gztool_bytes = Vec::new();
    index
        .write_gztool(&mut gztool_bytes, WithLines::No)
        .expect("write gztool");
    let gztool =
        DeflateIndex::read_gztool(&mut gztool_bytes.as_slice(), Some(compressed.len() as u64))
            .expect("read gztool");

    for (name, imported) in [("native", native), ("gzidx", gzidx), ("gztool", gztool)] {
        assert_eq!(
            decode_from_index(&compressed, &imported, 4, Format::Gzip),
            plain,
            "{name} index output differed"
        );
    }
}

#[test]
fn zlib_and_raw_single_span_indexes_are_supported() {
    let plain = corpus(1024 * 1024);
    for (format, compressed) in [
        (Format::Zlib, Arc::<[u8]>::from(zlib(&plain, 6))),
        (
            Format::RawDeflate,
            Arc::<[u8]>::from(raw_deflate(&plain, 6)),
        ),
    ] {
        let index = built_index_for_format(&compressed, 1, format);
        assert_eq!(decode_from_index(&compressed, &index, 4, format), plain);
    }
}

#[test]
fn sequential_index_builds_reusable_interior_checkpoints() {
    let plain = corpus(8 * 1024 * 1024);
    for (format, compressed) in [
        (Format::Gzip, Arc::<[u8]>::from(gzip(&plain, 6))),
        (Format::Zlib, Arc::<[u8]>::from(zlib(&plain, 6))),
        (
            Format::RawDeflate,
            Arc::<[u8]>::from(raw_deflate(&plain, 6)),
        ),
    ] {
        let decoder = Decoder::builder()
            .decoder_threads(1)
            .format(format)
            .build()
            .expect("decoder");
        let options = IndexOptions {
            checkpoint_spacing: NonZeroU64::new(512 * 1024).expect("nonzero"),
            ..IndexOptions::default()
        };
        let index = decoder
            .decode_with_index(&compressed, &mut io::sink(), options)
            .expect("index build")
            .index;
        assert!(
            index.checkpoint_count() > 1,
            "sequential {format:?} index did not retain an interior checkpoint"
        );
        assert_eq!(decode_from_index(&compressed, &index, 4, format), plain);
    }
}

#[test]
fn format_and_source_mismatches_are_strict_errors() {
    let plain = corpus(512 * 1024);
    let compressed: Arc<[u8]> = gzip(&plain, 6).into();
    let index = built_index(&compressed, 1);
    let wrong_format = Decoder::builder()
        .format(Format::Zlib)
        .build()
        .expect("decoder")
        .decode_from_index(&compressed, &mut io::sink(), &index)
        .expect_err("format mismatch");
    assert!(matches!(
        wrong_format,
        IndexDecodeError::FormatMismatch { .. }
    ));

    let mut other = compressed.to_vec();
    other.push(0);
    let error = Decoder::default()
        .decode_from_index(&Arc::<[u8]>::from(other), &mut io::sink(), &index)
        .expect_err("size mismatch");
    assert!(matches!(error, IndexDecodeError::Index(_)));
}

#[test]
fn footer_corruption_is_rejected() {
    let plain = corpus(2 * 1024 * 1024);
    let mut bytes = gzip(&plain, 6);
    let clean: Arc<[u8]> = Arc::from(bytes.clone());
    let index = built_index(&clean, 1);
    let footer = bytes.len() - 8;
    bytes[footer] ^= 0x80;
    let error = Decoder::builder()
        .decoder_threads(4)
        .build()
        .expect("decoder")
        .decode_from_index(&Arc::<[u8]>::from(bytes), &mut io::sink(), &index)
        .expect_err("CRC mismatch");
    assert!(matches!(
        error,
        IndexDecodeError::Decode(rapidgzip_core::DecodeError::ChecksumMismatch { .. })
    ));
}

#[test]
fn indexed_reader_is_read_send_and_exposes_runtime_control() {
    fn assert_read_send<T: Read + Send>(_: &T) {}

    let plain = corpus(3 * 1024 * 1024);
    let mut bytes = Vec::new();
    for part in plain.chunks(256 * 1024) {
        bytes.extend_from_slice(&gzip(part, 6));
    }
    let compressed: Arc<[u8]> = bytes.into();
    let index = Arc::new(built_index(&compressed, 1));
    let decoder = Decoder::builder()
        .decoder_threads(8)
        .build()
        .expect("decoder");
    let mut reader = decoder
        .reader_from_index(Arc::clone(&compressed), index)
        .expect("indexed reader");
    assert_read_send(&reader);
    let handle = reader.handle();
    handle.set_worker_limit(2).expect("lower ceiling");
    let mut output = Vec::new();
    reader.read_to_end(&mut output).expect("read");
    let report = reader.finish().expect("finish");
    assert_eq!(output, plain);
    assert_eq!(report.decoder_threads, 8);
    assert_eq!(handle.stats().path, DecoderPath::IndexedParallel);
    assert_eq!(handle.stats().worker_limit, 2);
}

#[test]
fn output_limit_applies_before_the_offending_chunk_is_emitted() {
    let plain = corpus(2 * 1024 * 1024);
    let compressed: Arc<[u8]> = gzip(&plain, 6).into();
    let index = built_index(&compressed, 1);
    let limit = 700_000_u64;
    let decoder = Decoder::builder()
        .decoder_threads(4)
        .decoded_chunk_size(64 * 1024)
        .output_limit(Some(limit))
        .build()
        .expect("decoder");
    let mut output = Vec::new();
    let error = decoder
        .decode_from_index(&compressed, &mut output, &index)
        .expect_err("output limit");
    assert!(matches!(
        error,
        IndexDecodeError::Decode(rapidgzip_core::DecodeError::OutputLimitExceeded { .. })
    ));
    assert!(output.len() as u64 <= limit);
}

#[test]
fn declared_checkpoint_output_offsets_are_verified() {
    let plain = corpus(8 * 1024 * 1024);
    let compressed: Arc<[u8]> = gzip(&plain, 6).into();
    let options = IndexOptions {
        checkpoint_spacing: NonZeroU64::new(512 * 1024).expect("nonzero"),
        ..IndexOptions::default()
    };
    let original = Decoder::builder()
        .decoder_threads(1)
        .build()
        .expect("decoder")
        .decode_with_index(&compressed, &mut io::sink(), options)
        .expect("index build")
        .index;
    assert!(original.checkpoint_count() > 2);

    let mut changed = DeflateIndex::new();
    changed.set_kind(original.kind());
    changed.set_compressed_size(original.compressed_size());
    changed.set_uncompressed_size(original.uncompressed_size());
    changed.set_checkpoint_spacing(original.checkpoint_spacing());
    changed.set_total_line_count(original.total_line_count());
    for (position, checkpoint) in original.checkpoints().iter().copied().enumerate() {
        let window = original
            .windows()
            .get(checkpoint.compressed_offset_in_bits)
            .cloned()
            .unwrap_or_else(StoredWindow::empty);
        changed
            .push(
                Checkpoint {
                    uncompressed_offset_in_bytes: checkpoint
                        .uncompressed_offset_in_bytes
                        .saturating_add(u64::from(position == 1)),
                    ..checkpoint
                },
                window,
            )
            .expect("modified checkpoint");
    }
    changed.validate().expect("structurally valid index");

    let error = Decoder::builder()
        .decoder_threads(4)
        .build()
        .expect("decoder")
        .decode_from_index(&compressed, &mut io::sink(), &changed)
        .expect_err("wrong decompressed offset");
    assert!(matches!(
        error,
        IndexDecodeError::Decode(rapidgzip_core::DecodeError::IndexOutputMismatch { .. })
    ));
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
    writes: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes += buffer.len();
        self.writes += 1;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn internal_deflate_blocks_are_coalesced_into_output_chunks() {
    let plain = corpus(8 * 1024 * 1024);
    let compressed: Arc<[u8]> = gzip(&plain, 6).into();
    let options = IndexOptions {
        checkpoint_spacing: NonZeroU64::new(1024 * 1024).expect("nonzero"),
        ..IndexOptions::default()
    };
    let index = Decoder::builder()
        .decoder_threads(1)
        .build()
        .expect("decoder")
        .decode_with_index(&compressed, &mut io::sink(), options)
        .expect("index build")
        .index;
    let chunk_size = 256 * 1024;
    let decoder = Decoder::builder()
        .decoder_threads(4)
        .decoded_chunk_size(chunk_size)
        .build()
        .expect("decoder");
    let mut output = CountingWriter::default();
    decoder
        .decode_from_index(&compressed, &mut output, &index)
        .expect("indexed decode");

    assert_eq!(output.bytes, plain.len());
    let full_chunks = plain.len().div_ceil(chunk_size);
    assert!(
        output.writes <= full_chunks + index.checkpoint_count(),
        "{} writes exceeded the chunk-plus-span bound",
        output.writes
    );
}

/// Produces byte-aligned independent DEFLATE regions and records their starts.
fn full_flush_index(plain: &[u8], format: Format, pieces: usize) -> (Arc<[u8]>, DeflateIndex) {
    let window_bits = match format {
        Format::Gzip => 31,
        Format::Zlib => 15,
        Format::RawDeflate => -15,
        _ => panic!("unsupported test format"),
    };
    let mut stream = z::z_stream::default();
    // SAFETY: the stream and ABI arguments are valid and remain uniquely owned
    // until the matching `deflateEnd` below.
    let status = unsafe {
        z::deflateInit2_(
            &mut stream,
            6,
            z::Z_DEFLATED,
            window_bits,
            8,
            z::Z_DEFAULT_STRATEGY,
            z::zlibVersion(),
            size_of::<z::z_stream>() as i32,
        )
    };
    assert_eq!(status, z::Z_OK);
    let mut output = vec![0_u8; plain.len() + plain.len() / 8 + 4096];
    stream.next_out = output.as_mut_ptr();
    stream.avail_out = output.len() as u32;

    let mut index = DeflateIndex::new();
    index.set_kind(match format {
        Format::Gzip => IndexKind::Gzip,
        Format::Zlib => IndexKind::Zlib,
        Format::RawDeflate => IndexKind::RawDeflate,
        _ => panic!("unsupported test format"),
    });
    let first_kind = match format {
        Format::Gzip => CheckpointKind::GzipMemberHeader,
        Format::Zlib => CheckpointKind::ZlibHeader,
        Format::RawDeflate => CheckpointKind::RawDeflateStart,
        _ => panic!("unsupported test format"),
    };
    index
        .push(
            Checkpoint {
                compressed_offset_in_bits: 0,
                uncompressed_offset_in_bytes: 0,
                kind: first_kind,
                line_offset: None,
            },
            StoredWindow::empty(),
        )
        .expect("first checkpoint");

    let piece_size = plain.len().div_ceil(pieces);
    let mut uncompressed = 0_usize;
    for (position, piece) in plain.chunks(piece_size).enumerate() {
        stream.next_in = piece.as_ptr();
        stream.avail_in = piece.len() as u32;
        let last = position + 1 == plain.len().div_ceil(piece_size);
        // SAFETY: input and output pointers describe live slices for this call.
        let status = unsafe {
            z::deflate(
                &mut stream,
                if last { z::Z_FINISH } else { z::Z_FULL_FLUSH },
            )
        };
        assert_eq!(stream.avail_in, 0);
        assert_eq!(status, if last { z::Z_STREAM_END } else { z::Z_OK });
        uncompressed += piece.len();
        if !last {
            index
                .push(
                    Checkpoint {
                        compressed_offset_in_bits: stream.total_out.saturating_mul(8),
                        uncompressed_offset_in_bytes: uncompressed as u64,
                        kind: CheckpointKind::DeflateBlock,
                        line_offset: None,
                    },
                    StoredWindow::from_raw(vec![0; WINDOW_SIZE]).expect("window"),
                )
                .expect("interior checkpoint");
        }
    }
    let produced = stream.total_out as usize;
    // SAFETY: this initialized stream is ended exactly once.
    unsafe { z::deflateEnd(&mut stream) };
    output.truncate(produced);
    index.set_compressed_size(Some(output.len() as u64));
    index.set_uncompressed_size(Some(plain.len() as u64));
    index.validate().expect("full-flush index");
    (output.into(), index)
}

#[test]
fn interior_checkpoints_parallelize_all_supported_formats() {
    let plain = corpus(4 * 1024 * 1024);
    for format in [Format::Gzip, Format::Zlib, Format::RawDeflate] {
        let (compressed, index) = full_flush_index(&plain, format, 8);
        assert!(index.checkpoint_count() >= 8);
        assert_eq!(decode_from_index(&compressed, &index, 4, format), plain);
    }
}
