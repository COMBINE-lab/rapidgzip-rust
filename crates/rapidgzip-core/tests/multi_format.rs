//! Decoding zlib streams and raw DEFLATE.

mod common;

use common::{bgzf, corpus, gzip, raw_deflate, zlib};
use rapidgzip_core::{DecodeError, Decoder, DeflateErrorKind, Format, ZlibErrorKind};
use std::io::{Cursor, Read};

/// Decodes `compressed` with `format` and returns the output and report.
fn decode(
    compressed: &[u8],
    format: Format,
) -> Result<(Vec<u8>, rapidgzip_core::DecodeReport), DecodeError> {
    let decoder = Decoder::builder().format(format).build().expect("builder");
    let mut output = Vec::new();
    let report = decoder.decode(&compressed.to_vec(), &mut output)?;
    Ok((output, report))
}

#[test]
fn decodes_a_zlib_stream() {
    let plain = corpus(512 * 1024);
    let compressed = zlib(&plain, 6);

    let (output, report) = decode(&compressed, Format::Zlib).expect("decode");
    assert_eq!(output, plain);
    assert_eq!(report.format, Format::Zlib);
    assert_eq!(report.decompressed_bytes, plain.len() as u64);
    assert_eq!(report.compressed_bytes, compressed.len() as u64);
    assert_eq!(report.member_count, 1);
}

#[test]
fn decodes_raw_deflate() {
    let plain = corpus(512 * 1024);
    let compressed = raw_deflate(&plain, 6);

    let (output, report) = decode(&compressed, Format::RawDeflate).expect("decode");
    assert_eq!(output, plain);
    assert_eq!(report.format, Format::RawDeflate);
    assert_eq!(report.decompressed_bytes, plain.len() as u64);
}

#[test]
fn decodes_empty_streams() {
    for (compressed, format) in [
        (zlib(b"", 6), Format::Zlib),
        (raw_deflate(b"", 6), Format::RawDeflate),
    ] {
        let (output, report) = decode(&compressed, format).expect("decode");
        assert!(output.is_empty());
        assert_eq!(report.decompressed_bytes, 0);
    }
}

#[test]
fn auto_detects_zlib_and_gzip() {
    let plain = corpus(64 * 1024);

    let (output, report) = decode(&zlib(&plain, 6), Format::Auto).expect("zlib");
    assert_eq!(output, plain);
    assert_eq!(report.format, Format::Zlib);

    let (output, report) = decode(&gzip(&plain, 6), Format::Auto).expect("gzip");
    assert_eq!(output, plain);
    assert_eq!(report.format, Format::Gzip);

    let (output, report) = decode(&bgzf(&plain, 48 * 1024), Format::Auto).expect("bgzf");
    assert_eq!(output, plain);
    assert_eq!(report.format, Format::Gzip);
}

#[test]
fn auto_never_selects_raw_deflate() {
    let plain = corpus(64 * 1024);
    // Raw DEFLATE has no header, so auto-detection must reject it rather than
    // guess; the corpus starts with "record 0", which is neither container.
    let error = decode(&raw_deflate(&plain, 6), Format::Auto).expect_err("rejected");
    assert!(matches!(error, DecodeError::InvalidGzip { .. }), "{error}");
}

#[test]
fn a_zlib_stream_read_as_gzip_is_rejected() {
    let compressed = zlib(&corpus(4096), 6);
    let error = decode(&compressed, Format::Gzip).expect_err("rejected");
    assert!(matches!(error, DecodeError::InvalidGzip { .. }), "{error}");
}

#[test]
fn a_gzip_member_read_as_zlib_is_rejected() {
    let compressed = gzip(&corpus(4096), 6);
    let error = decode(&compressed, Format::Zlib).expect_err("rejected");
    assert!(
        matches!(
            error,
            DecodeError::InvalidZlib {
                reason: ZlibErrorKind::BadHeader | ZlibErrorKind::UnsupportedCompressionMethod(_),
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a_corrupt_adler32_is_rejected() {
    let plain = corpus(64 * 1024);
    let mut compressed = zlib(&plain, 6);
    let last = compressed.len() - 1;
    compressed[last] ^= 0xff;

    let error = decode(&compressed, Format::Zlib).expect_err("rejected");
    assert!(
        matches!(
            error,
            DecodeError::InvalidZlib {
                reason: ZlibErrorKind::ChecksumMismatch { .. },
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a_truncated_zlib_trailer_is_rejected() {
    let plain = corpus(64 * 1024);
    let mut compressed = zlib(&plain, 6);
    compressed.truncate(compressed.len() - 2);

    let error = decode(&compressed, Format::Zlib).expect_err("rejected");
    assert!(
        matches!(
            error,
            DecodeError::InvalidZlib {
                reason: ZlibErrorKind::Truncated,
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a_truncated_zlib_header_is_rejected() {
    let error = decode(&[0x78], Format::Zlib).expect_err("rejected");
    assert!(
        matches!(
            error,
            DecodeError::InvalidZlib {
                reason: ZlibErrorKind::Truncated,
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a_preset_dictionary_is_rejected() {
    // 0x78 0x20 is a legal header with FDICT set.
    let error = decode(&[0x78, 0x20, 0, 0, 0, 0], Format::Zlib).expect_err("rejected");
    assert!(
        matches!(
            error,
            DecodeError::InvalidZlib {
                reason: ZlibErrorKind::PresetDictionary,
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn trailing_bytes_are_rejected() {
    let plain = corpus(4096);

    let mut compressed = zlib(&plain, 6);
    compressed.extend_from_slice(b"extra");
    let error = decode(&compressed, Format::Zlib).expect_err("rejected");
    assert!(
        matches!(
            error,
            DecodeError::InvalidZlib {
                reason: ZlibErrorKind::TrailingGarbage,
                ..
            }
        ),
        "{error}"
    );

    let mut compressed = raw_deflate(&plain, 6);
    compressed.extend_from_slice(b"extra");
    let error = decode(&compressed, Format::RawDeflate).expect_err("rejected");
    assert!(
        matches!(
            error,
            DecodeError::InvalidDeflate {
                reason: DeflateErrorKind::TrailingGarbage,
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a_truncated_deflate_stream_is_rejected() {
    let plain = corpus(64 * 1024);
    let mut compressed = raw_deflate(&plain, 6);
    compressed.truncate(compressed.len() / 2);

    let error = decode(&compressed, Format::RawDeflate).expect_err("rejected");
    assert!(
        matches!(error, DecodeError::InvalidDeflate { .. }),
        "{error}"
    );
}

#[test]
fn an_expected_size_is_verified_for_raw_deflate() {
    let plain = corpus(64 * 1024);
    let compressed = raw_deflate(&plain, 6);

    let decoder = Decoder::builder()
        .format(Format::RawDeflate)
        .expected_uncompressed_size(Some(plain.len() as u64))
        .build()
        .expect("builder");
    let mut output = Vec::new();
    decoder
        .decode(&compressed, &mut output)
        .expect("matching size");
    assert_eq!(output, plain);

    let decoder = Decoder::builder()
        .format(Format::RawDeflate)
        .expected_uncompressed_size(Some(plain.len() as u64 + 1))
        .build()
        .expect("builder");
    let error = decoder
        .decode(&compressed, &mut Vec::new())
        .expect_err("mismatched size");
    assert!(
        matches!(error, DecodeError::UnexpectedOutputSize { .. }),
        "{error}"
    );
}

#[test]
fn an_expected_size_is_refused_for_other_formats() {
    for format in [Format::Auto, Format::Gzip, Format::Zlib] {
        assert!(
            Decoder::builder()
                .format(format)
                .expected_uncompressed_size(Some(10))
                .build()
                .is_err(),
            "{format} accepted an expected size"
        );
    }
}

#[test]
fn the_output_limit_still_applies() {
    let plain = corpus(64 * 1024);
    let decoder = Decoder::builder()
        .format(Format::Zlib)
        .output_limit(Some(1024))
        .build()
        .expect("builder");
    let error = decoder
        .decode(&zlib(&plain, 6), &mut Vec::new())
        .expect_err("limit");
    assert!(
        matches!(error, DecodeError::OutputLimitExceeded { limit: 1024 }),
        "{error}"
    );
}

#[test]
fn non_seekable_input_decodes_both_formats() {
    let plain = corpus(256 * 1024);

    for (compressed, format) in [
        (zlib(&plain, 6), Format::Zlib),
        (raw_deflate(&plain, 6), Format::RawDeflate),
    ] {
        let decoder = Decoder::builder().format(format).build().expect("builder");
        let mut output = Vec::new();
        let report = decoder
            .decode_stream(Cursor::new(compressed), &mut output)
            .expect("decode");
        assert_eq!(output, plain);
        assert_eq!(report.format, format);
    }
}

#[test]
fn a_non_seekable_zlib_stream_is_auto_detected() {
    let plain = corpus(128 * 1024);
    let decoder = Decoder::builder().build().expect("builder");
    let mut reader = decoder
        .stream_reader(Cursor::new(zlib(&plain, 6)))
        .expect("stream reader");
    let mut output = Vec::new();
    reader.read_to_end(&mut output).expect("read");
    assert_eq!(output, plain);
    assert_eq!(reader.finish().expect("report").format, Format::Zlib);
}

#[test]
fn an_owned_reader_decodes_zlib() {
    let plain = corpus(256 * 1024);
    let decoder = Decoder::builder()
        .format(Format::Zlib)
        .build()
        .expect("builder");
    let mut reader = decoder.reader(zlib(&plain, 6)).expect("reader");
    let mut output = Vec::new();
    reader.read_to_end(&mut output).expect("read");
    assert_eq!(output, plain);
    assert_eq!(reader.finish().expect("report").format, Format::Zlib);
}

/// Decodes with a worker budget that forces the parallel path.
fn decode_parallel(compressed: &[u8], format: Format) -> (Vec<u8>, rapidgzip_core::DecodeReport) {
    let decoder = Decoder::builder()
        .format(format)
        .decoder_threads(4)
        .build()
        .expect("builder");
    let mut output = Vec::new();
    let report = decoder
        .decode(&compressed.to_vec(), &mut output)
        .expect("parallel decode");
    (output, report)
}

#[test]
fn the_parallel_path_matches_the_sequential_one() {
    let plain = corpus(24 * 1024 * 1024);

    for (compressed, format) in [
        (zlib(&plain, 6), Format::Zlib),
        (raw_deflate(&plain, 6), Format::RawDeflate),
    ] {
        let (sequential, sequential_report) = decode(&compressed, format).expect("sequential");
        let (parallel, parallel_report) = decode_parallel(&compressed, format);

        assert_eq!(sequential, plain, "sequential output for {format}");
        assert_eq!(parallel, plain, "parallel output for {format}");
        assert_eq!(
            parallel_report.decompressed_bytes,
            sequential_report.decompressed_bytes
        );
        assert_eq!(
            parallel_report.compressed_bytes,
            sequential_report.compressed_bytes
        );
        assert_eq!(parallel_report.format, format);
    }
}

#[test]
fn the_parallel_path_still_verifies_the_adler32() {
    let plain = corpus(24 * 1024 * 1024);
    let mut compressed = zlib(&plain, 6);
    let last = compressed.len() - 1;
    compressed[last] ^= 0xff;

    let decoder = Decoder::builder()
        .format(Format::Zlib)
        .decoder_threads(4)
        .build()
        .expect("builder");
    let error = decoder
        .decode(&compressed, &mut Vec::new())
        .expect_err("rejected");
    assert!(
        matches!(
            error,
            DecodeError::InvalidZlib {
                reason: ZlibErrorKind::ChecksumMismatch { .. },
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn the_parallel_path_rejects_trailing_bytes() {
    let plain = corpus(24 * 1024 * 1024);
    let mut compressed = zlib(&plain, 6);
    compressed.extend_from_slice(b"extra");

    let decoder = Decoder::builder()
        .format(Format::Zlib)
        .decoder_threads(4)
        .build()
        .expect("builder");
    let error = decoder
        .decode(&compressed, &mut Vec::new())
        .expect_err("rejected");
    assert!(
        matches!(
            error,
            DecodeError::InvalidZlib {
                reason: ZlibErrorKind::TrailingGarbage,
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn an_index_is_built_and_seekable_for_both_formats() {
    use rapidgzip_core::IndexedReader;
    use std::io::{Seek, SeekFrom};

    let plain = corpus(24 * 1024 * 1024);

    for (compressed, format) in [
        (zlib(&plain, 6), Format::Zlib),
        (raw_deflate(&plain, 6), Format::RawDeflate),
    ] {
        let decoder = Decoder::builder()
            .format(format)
            .decoder_threads(4)
            .build_index(true)
            .build()
            .expect("builder");
        let mut reader = decoder.reader(compressed.clone()).expect("reader");
        std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
        let index = reader
            .finish()
            .expect("report")
            .index
            .expect("index was requested");

        index.validate().expect("invariants hold");
        assert!(
            index.checkpoint_count() >= 3,
            "{format} produced only {} checkpoints",
            index.checkpoint_count()
        );

        let mut random = IndexedReader::new(compressed, index).expect("indexed reader");
        for target in [0usize, 1000, 9_000_000, 20_000_000] {
            random.seek(SeekFrom::Start(target as u64)).expect("seek");
            let mut buffer = vec![0u8; 2048];
            random.read_exact(&mut buffer).expect("read");
            assert_eq!(
                buffer,
                &plain[target..target + 2048],
                "{format} at {target}"
            );
        }
    }
}
