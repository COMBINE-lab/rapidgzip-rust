//! Gzip, zlib, and raw-DEFLATE API behavior.

mod common;

use common::{corpus, gzip, raw_deflate, zlib};
use rapidgzip_core::{
    DecodeError, Decoder, DeflateErrorKind, Format, IndexKind, IndexOptions, IndexedReader,
    ZlibErrorKind,
};
use std::io::{self, Cursor, Read, Seek, SeekFrom};

fn decoder(format: Format) -> Decoder {
    Decoder::builder()
        .format(format)
        .decoder_threads(1)
        .build()
        .expect("valid decoder")
}

fn decode(
    compressed: &[u8],
    format: Format,
) -> Result<(Vec<u8>, rapidgzip_core::DecodeReport), DecodeError> {
    let mut output = Vec::new();
    let report = decoder(format).decode(&compressed.to_vec(), &mut output)?;
    Ok((output, report))
}

#[test]
fn explicit_formats_decode_and_report_concrete_framing() {
    let plain = corpus(512 * 1024);
    for (compressed, format) in [
        (gzip(&plain, 6), Format::Gzip),
        (zlib(&plain, 6), Format::Zlib),
        (raw_deflate(&plain, 6), Format::RawDeflate),
    ] {
        let (output, report) = decode(&compressed, format).expect("decode");
        assert_eq!(output, plain);
        assert_eq!(report.format, format);
        assert_eq!(report.member_count, 1);
        assert_eq!(report.compressed_bytes, compressed.len() as u64);
        assert_eq!(report.decompressed_bytes, plain.len() as u64);
    }
}

#[test]
fn auto_detects_gzip_and_zlib_but_never_raw() {
    let plain = corpus(64 * 1024);
    let decoder = Decoder::builder()
        .auto_detect_format()
        .decoder_threads(1)
        .build()
        .unwrap();
    for (compressed, format) in [
        (gzip(&plain, 6), Format::Gzip),
        (zlib(&plain, 6), Format::Zlib),
    ] {
        let mut output = Vec::new();
        let report = decoder.decode(&compressed, &mut output).unwrap();
        assert_eq!(output, plain);
        assert_eq!(report.format, format);
    }
    assert!(matches!(
        decoder.decode(&raw_deflate(&plain, 6), &mut Vec::new()),
        Err(DecodeError::UnrecognizedFormat)
    ));
}

#[test]
fn zlib_checksum_truncation_and_trailing_data_are_rejected() {
    let plain = corpus(64 * 1024);
    let mut corrupt = zlib(&plain, 6);
    *corrupt.last_mut().unwrap() ^= 0xff;
    assert!(matches!(
        decode(&corrupt, Format::Zlib),
        Err(DecodeError::InvalidZlib {
            reason: ZlibErrorKind::ChecksumMismatch { .. },
            ..
        })
    ));

    let mut truncated = zlib(&plain, 6);
    truncated.truncate(truncated.len() - 2);
    assert!(matches!(
        decode(&truncated, Format::Zlib),
        Err(DecodeError::InvalidZlib {
            reason: ZlibErrorKind::Truncated,
            ..
        })
    ));

    let mut trailing = zlib(&plain, 6);
    trailing.push(0);
    assert!(matches!(
        decode(&trailing, Format::Zlib),
        Err(DecodeError::InvalidZlib {
            reason: ZlibErrorKind::TrailingGarbage,
            ..
        })
    ));
}

#[test]
fn raw_trailing_data_and_exact_size_are_enforced_before_handoff() {
    let plain = corpus(64 * 1024);
    let compressed = raw_deflate(&plain, 6);
    let mut trailing = compressed.clone();
    trailing.push(0);
    assert!(matches!(
        decode(&trailing, Format::RawDeflate),
        Err(DecodeError::InvalidDeflate {
            reason: DeflateErrorKind::TrailingGarbage,
            ..
        })
    ));

    let decoder = Decoder::builder()
        .format(Format::RawDeflate)
        .expected_uncompressed_size(Some(plain.len() as u64 - 1))
        .build()
        .unwrap();
    let mut output = Vec::new();
    assert!(matches!(
        decoder.decode(&compressed, &mut output),
        Err(DecodeError::UnexpectedOutputSize { .. })
    ));
    assert!(output.is_empty(), "the overrun chunk must not be emitted");
}

struct OneByteReads<R> {
    inner: R,
    interrupt_next: bool,
}

impl<R: Read> Read for OneByteReads<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.interrupt_next {
            self.interrupt_next = false;
            return Err(io::ErrorKind::Interrupted.into());
        }
        self.interrupt_next = true;
        let length = output.len().min(1);
        self.inner.read(&mut output[..length])
    }
}

#[test]
fn auto_detection_survives_one_byte_reads_and_interruptions() {
    let plain = corpus(32 * 1024);
    let source = OneByteReads {
        inner: Cursor::new(zlib(&plain, 6)),
        interrupt_next: true,
    };
    let decoder = Decoder::builder()
        .auto_detect_format()
        .input_page_size(1)
        .build()
        .unwrap();
    let mut reader = decoder.stream_reader(source).unwrap();
    let mut output = Vec::new();
    reader.read_to_end(&mut output).unwrap();
    assert_eq!(output, plain);
    assert_eq!(reader.finish().unwrap().format, Format::Zlib);
}

#[test]
fn push_pull_and_indexed_seek_work_for_zlib_and_raw() {
    let plain = corpus(512 * 1024);
    for (compressed, format, kind) in [
        (zlib(&plain, 6), Format::Zlib, IndexKind::Zlib),
        (
            raw_deflate(&plain, 6),
            Format::RawDeflate,
            IndexKind::RawDeflate,
        ),
    ] {
        let decoder = decoder(format);
        let mut pull = decoder.reader(compressed.clone()).unwrap();
        let mut pulled = Vec::new();
        pull.read_to_end(&mut pulled).unwrap();
        assert_eq!(pulled, plain);
        assert_eq!(pull.finish().unwrap().format, format);

        let mut sink = Vec::new();
        let indexed = decoder
            .decode_with_index(&compressed, &mut sink, IndexOptions::default())
            .unwrap();
        assert_eq!(indexed.index.kind(), kind);
        assert_eq!(sink, plain);
        let mut random = IndexedReader::new(compressed, indexed.index).unwrap();
        random.seek(SeekFrom::Start(123_456)).unwrap();
        let mut sample = [0_u8; 4096];
        random.read_exact(&mut sample).unwrap();
        assert_eq!(&sample, &plain[123_456..123_456 + sample.len()]);
    }
}

#[test]
fn every_reader_remains_send() {
    fn assert_send<T: Send>(_: &T) {}
    fn assert_copy<T: Copy>() {}
    assert_copy::<rapidgzip_core::DecodeReport>();
    let reader = decoder(Format::RawDeflate)
        .stream_reader(Cursor::new(raw_deflate(b"send", 1)))
        .unwrap();
    assert_send(&reader);
}

#[test]
fn large_zlib_and_raw_streams_validate_after_adaptive_path_selection() {
    let plain = corpus(24 * 1024 * 1024);
    for (compressed, format) in [
        (zlib(&plain, 1), Format::Zlib),
        (raw_deflate(&plain, 1), Format::RawDeflate),
    ] {
        assert!(
            compressed.len() >= 2 * 1024 * 1024,
            "fixture must expose two tasks"
        );
        let decoder = Decoder::builder()
            .format(format)
            .decoder_threads(4)
            .build()
            .unwrap();
        let mut reader = decoder
            .reader_with_index(compressed.clone(), IndexOptions::default())
            .unwrap();
        let mut output = Vec::new();
        reader.read_to_end(&mut output).unwrap();
        assert_eq!(output, plain);
        assert!(matches!(
            reader.stats().path,
            rapidgzip_core::DecoderPath::Sequential | rapidgzip_core::DecoderPath::MarkerWindow
        ));
        let indexed = reader.finish().unwrap();
        assert_eq!(indexed.decode.format, format);
        assert!(!indexed.index.is_empty());

        let mut malformed = compressed.clone();
        match format {
            Format::Zlib => *malformed.last_mut().unwrap() ^= 0xff,
            Format::RawDeflate => malformed.push(0),
            _ => unreachable!("the fixture only covers zlib and raw DEFLATE"),
        }
        let error = decoder
            .decode(&malformed, &mut Vec::new())
            .expect_err("parallel decoding must validate the terminal framing");
        assert!(
            matches!(
                (format, error),
                (
                    Format::Zlib,
                    DecodeError::InvalidZlib {
                        reason: ZlibErrorKind::ChecksumMismatch { .. },
                        ..
                    }
                ) | (
                    Format::RawDeflate,
                    DecodeError::InvalidDeflate {
                        reason: DeflateErrorKind::TrailingGarbage,
                        ..
                    }
                )
            ),
            "unexpected terminal-validation error"
        );

        let mut random = IndexedReader::new(compressed, indexed.index).unwrap();
        random.seek(SeekFrom::Start(12_345_678)).unwrap();
        let mut sample = [0_u8; 4096];
        random.read_exact(&mut sample).unwrap();
        assert_eq!(&sample, &plain[12_345_678..12_345_678 + sample.len()]);
    }
}
