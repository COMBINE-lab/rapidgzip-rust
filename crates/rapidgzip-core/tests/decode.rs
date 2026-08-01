//! End-to-end decoder and paraseq integration tests.

use paraseq::{Record, fastq};
use rapidgzip_core::{
    DecodeError, Decoder, Format, GzipIndex, WindowCompression, ZlibErrorKind,
};
use std::io::{self, Cursor, Read};

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

fn hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn member_from_raw_deflate(deflate: &[u8], decoded: &[u8]) -> Vec<u8> {
    let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
    encoded.extend_from_slice(deflate);
    encoded.extend_from_slice(&crc32(decoded).to_le_bytes());
    encoded.extend_from_slice(&(decoded.len() as u32).to_le_bytes());
    encoded
}

fn stored_then_fixed_member(bytes: &[u8]) -> Vec<u8> {
    assert!(bytes.len() <= u16::MAX as usize);
    let length = bytes.len() as u16;
    let mut deflate = vec![0, length as u8, (length >> 8) as u8];
    deflate.extend_from_slice(&(!length).to_le_bytes());
    deflate.extend_from_slice(bytes);
    // Empty final fixed-Huffman block. The non-final stored block above keeps
    // this fixture off the fully-stored fast path.
    deflate.extend_from_slice(&[0x03, 0x00]);
    member_from_raw_deflate(&deflate, bytes)
}

fn dynamic_multiblock_fixture() -> (Vec<u8>, Vec<u8>) {
    let deflate = hex(
        "ecc3410900000804b06c870f0b5cff2c82393658661b5555555555555555555555555555555555555555555555555555555555555555555555555555f51f000000ffffedc3310d00000803306d640706f0af856336daa4b7195555555555555555555555555555555555555555555555555555555555555555555555555555b51f",
    );
    assert_eq!(deflate.len(), 129);
    let mut decoded = b"ACGT".repeat(10_000);
    decoded.extend_from_slice(&b"TGCA".repeat(10_000));
    (member_from_raw_deflate(&deflate, &decoded), decoded)
}

fn bgzf_member(bytes: &[u8]) -> Vec<u8> {
    let deflate = stored_deflate(bytes);
    bgzf_member_from_raw_deflate(&deflate, bytes)
}

fn bgzf_member_from_raw_deflate(deflate: &[u8], decoded: &[u8]) -> Vec<u8> {
    let total_size = 18 + deflate.len() + 8;
    assert!(total_size <= u16::MAX as usize + 1);
    let block_size = (total_size - 1) as u16;
    let mut encoded = b"\x1f\x8b\x08\x04\0\0\0\0\x00\xff\x06\x00BC\x02\x00".to_vec();
    encoded.extend_from_slice(&block_size.to_le_bytes());
    encoded.extend_from_slice(deflate);
    encoded.extend_from_slice(&crc32(decoded).to_le_bytes());
    encoded.extend_from_slice(&(decoded.len() as u32).to_le_bytes());
    encoded
}

fn bgzf_eof() -> Vec<u8> {
    vec![
        31, 139, 8, 4, 0, 0, 0, 0, 0, 255, 6, 0, 66, 67, 2, 0, 27, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]
}

fn member_with_optional_header(bytes: &[u8]) -> Vec<u8> {
    const FLAGS: u8 = 0x02 | 0x04 | 0x08 | 0x10;
    let mut header = vec![0x1F, 0x8B, 8, FLAGS, 0, 0, 0, 0, 0, 255];
    header.extend_from_slice(&6_u16.to_le_bytes());
    header.extend_from_slice(b"XY\x02\x00ok");
    header.extend_from_slice(b"reads.fastq\0");
    header.extend_from_slice(b"test fixture\0");
    let header_crc = crc32(&header) as u16;
    header.extend_from_slice(&header_crc.to_le_bytes());
    header.extend_from_slice(&stored_deflate(bytes));
    header.extend_from_slice(&crc32(bytes).to_le_bytes());
    header.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    header
}

fn padded_empty_member(total_size: usize) -> Vec<u8> {
    const FIXED_SIZE: usize = 10 + 2 + 5 + 8;
    assert!((FIXED_SIZE..=FIXED_SIZE + u16::MAX as usize).contains(&total_size));
    let extra_size = total_size - FIXED_SIZE;
    let mut encoded = b"\x1f\x8b\x08\x04\0\0\0\0\x00\xff".to_vec();
    encoded.extend_from_slice(&(extra_size as u16).to_le_bytes());
    encoded.resize(encoded.len() + extra_size, 0);
    encoded.extend_from_slice(&stored_deflate(b""));
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    assert_eq!(encoded.len(), total_size);
    encoded
}

#[test]
fn decodes_single_member() {
    let compressed = member(b"the quick brown fox");
    let mut decoded = Vec::new();
    let report = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, b"the quick brown fox");
    assert_eq!(report.member_count, 1);
    assert_eq!(report.compressed_bytes, compressed.len() as u64);
}

fn with_corrupt_crc(mut compressed: Vec<u8>) -> Vec<u8> {
    let crc_offset = compressed.len() - 8;
    compressed[crc_offset] ^= 1;
    compressed
}

fn with_corrupt_isize(mut compressed: Vec<u8>) -> Vec<u8> {
    let isize_offset = compressed.len() - 4;
    compressed[isize_offset] ^= 1;
    compressed
}

#[test]
fn valid_member_decodes_with_crc32_enabled_and_disabled() {
    let payload = b"the quick brown fox";
    let compressed = member(payload);

    for enabled in [true, false] {
        let decoder = Decoder::builder().crc32_enabled(enabled).build().unwrap();
        let mut decoded = Vec::new();
        let report = decoder.decode(&compressed, &mut decoded).unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(report.member_count, 1);
    }
}

#[test]
fn corrupt_crc_rejected_when_enabled_accepted_when_disabled() {
    let payload = b"crc footer check";
    let compressed = with_corrupt_crc(member(payload));

    let enabled = Decoder::builder().crc32_enabled(true).build().unwrap();
    let mut decoded = Vec::new();
    let error = enabled.decode(&compressed, &mut decoded).unwrap_err();
    assert!(matches!(
        error,
        DecodeError::ChecksumMismatch { member: 0, .. }
    ));

    let disabled = Decoder::builder().crc32_enabled(false).build().unwrap();
    let mut decoded = Vec::new();
    let report = disabled.decode(&compressed, &mut decoded).unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
}

#[test]
fn corrupt_isize_still_fails_when_crc32_disabled() {
    let payload = b"isize still checked";
    let compressed = with_corrupt_isize(member(payload));

    let decoder = Decoder::builder().crc32_enabled(false).build().unwrap();
    let mut decoded = Vec::new();
    let error = decoder.decode(&compressed, &mut decoded).unwrap_err();
    assert!(matches!(error, DecodeError::SizeMismatch { member: 0, .. }));
}

#[test]
fn decodes_concatenated_members_including_empty() {
    let mut compressed = member(b"first\n");
    compressed.extend(member(b""));
    compressed.extend(member(b"second\n"));

    let mut decoded = Vec::new();
    let report = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, b"first\nsecond\n");
    assert_eq!(report.member_count, 3);
}

#[test]
fn decode_read_matches_decode_on_single_member() {
    let compressed = member(b"the quick brown fox");
    let decoder = Decoder::default();

    let mut from_readat = Vec::new();
    let report_at = decoder.decode(&compressed, &mut from_readat).unwrap();

    let mut from_read = Vec::new();
    let report_read = decoder
        .decode_read(Cursor::new(compressed.as_slice()), &mut from_read)
        .unwrap();

    assert_eq!(from_read, from_readat);
    assert_eq!(report_read.member_count, report_at.member_count);
    assert_eq!(report_read.compressed_bytes, report_at.compressed_bytes);
    assert_eq!(
        report_read.decompressed_bytes,
        report_at.decompressed_bytes
    );
}

#[test]
fn decode_read_decodes_concatenated_members() {
    let mut compressed = member(b"first\n");
    compressed.extend(member(b""));
    compressed.extend(member(b"second\n"));

    let mut decoded = Vec::new();
    let report = Decoder::default()
        .decode_read(Cursor::new(compressed.as_slice()), &mut decoded)
        .unwrap();
    assert_eq!(decoded, b"first\nsecond\n");
    assert_eq!(report.member_count, 3);
    assert_eq!(report.compressed_bytes, compressed.len() as u64);
}

#[test]
fn decode_read_empty_input_errors_cleanly() {
    let mut decoded = Vec::new();
    let error = Decoder::default()
        .decode_read(Cursor::new(&[] as &[u8]), &mut decoded)
        .unwrap_err();
    assert!(matches!(
        error,
        DecodeError::InvalidGzip {
            offset: 0,
            reason: rapidgzip_core::GzipErrorKind::BadMagic,
        }
    ));
    assert!(decoded.is_empty());
}

/// Reader that yields at most one byte per `read` call (forces true streaming).
struct LimitedReader<'a> {
    data: &'a [u8],
    pos: usize,
    max_per_read: usize,
}

impl Read for LimitedReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.data.len() || buf.is_empty() || self.max_per_read == 0 {
            return Ok(0);
        }
        let n = self
            .max_per_read
            .min(buf.len())
            .min(self.data.len() - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[test]
fn decode_read_streams_gzip_multi_member_limited_reader() {
    let mut compressed = member(b"first\n");
    compressed.extend(member(b""));
    compressed.extend(member(b"second\n"));

    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(8) // still sequential on decode_read
        .build()
        .unwrap()
        .decode_read(
            LimitedReader {
                data: &compressed,
                pos: 0,
                max_per_read: 1,
            },
            &mut decoded,
        )
        .unwrap();
    assert_eq!(decoded, b"first\nsecond\n");
    assert_eq!(report.member_count, 3);
    assert_eq!(report.compressed_bytes, compressed.len() as u64);
}

#[test]
fn decode_read_streams_zlib() {
    let payload = b"streaming zlib payload";
    let compressed = zlib_member(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .format(Format::Zlib)
        .build()
        .unwrap()
        .decode_read(
            LimitedReader {
                data: &compressed,
                pos: 0,
                max_per_read: 3,
            },
            &mut decoded,
        )
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
    assert_eq!(report.compressed_bytes, compressed.len() as u64);
}

#[test]
fn decode_read_streams_raw_deflate() {
    let payload = b"streaming raw deflate";
    let compressed = stored_deflate(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .format(Format::RawDeflate)
        .build()
        .unwrap()
        .decode_read(
            LimitedReader {
                data: &compressed,
                pos: 0,
                max_per_read: 2,
            },
            &mut decoded,
        )
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
}

#[test]
fn decode_read_large_synthetic_stream() {
    // ~256 KiB payload, multi-member; correctness over full-buffer baseline.
    let chunk = b"ACGT".repeat(16 * 1024); // 64 KiB
    let mut expected = Vec::new();
    let mut compressed = Vec::new();
    for i in 0..4 {
        let mut payload = chunk.clone();
        payload.extend_from_slice(&[i as u8]);
        expected.extend_from_slice(&payload);
        compressed.extend(member(&payload));
    }

    let mut from_stream = Vec::new();
    let report_stream = Decoder::builder()
        .decoder_threads(4)
        .build()
        .unwrap()
        .decode_read(
            LimitedReader {
                data: &compressed,
                pos: 0,
                max_per_read: 4096,
            },
            &mut from_stream,
        )
        .unwrap();

    let mut from_decode = Vec::new();
    let report_decode = Decoder::default()
        .decode(&compressed, &mut from_decode)
        .unwrap();

    assert_eq!(from_stream, expected);
    assert_eq!(from_stream, from_decode);
    assert_eq!(report_stream.member_count, report_decode.member_count);
    assert_eq!(report_stream.compressed_bytes, report_decode.compressed_bytes);
    assert_eq!(
        report_stream.decompressed_bytes,
        report_decode.decompressed_bytes
    );
}

#[test]
fn decode_read_truncated_gzip_errors() {
    let mut compressed = member(b"hello");
    compressed.truncate(compressed.len() / 2);
    let mut decoded = Vec::new();
    let error = Decoder::default()
        .decode_read(Cursor::new(compressed.as_slice()), &mut decoded)
        .unwrap_err();
    assert!(
        matches!(
            error,
            DecodeError::InvalidDeflate { .. }
                | DecodeError::InvalidGzip {
                    reason: rapidgzip_core::GzipErrorKind::Truncated,
                    ..
                }
        ),
        "unexpected error for truncated gzip: {error:?}"
    );
}

#[test]
fn decode_read_keep_index_and_line_offsets() {
    let payload = b"line1\nline2\nline3\n";
    let compressed = member(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .keep_index(true)
        .gather_line_offsets(true)
        .decoder_threads(8)
        .build()
        .unwrap()
        .decode_read(Cursor::new(compressed.as_slice()), &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.line_count, Some(3));
    let index = report.index.expect("keep_index should yield an index");
    assert!(!index.checkpoints.is_empty());
    assert!(index.has_line_offsets);
}

#[test]
fn decodes_all_optional_header_fields_and_fhcrc() {
    let compressed = member_with_optional_header(b"optional metadata");
    let mut decoded = Vec::new();
    Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, b"optional metadata");
}

#[test]
fn decodes_bgzf_as_generic_multimember_gzip() {
    let mut compressed = bgzf_member(b"@r1\nACGT\n+\n!!!!\n");
    compressed.extend(bgzf_member(b"@r2\nTGCA\n+\n####\n"));
    compressed.extend(bgzf_eof());

    let mut decoded = Vec::new();
    let report = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, b"@r1\nACGT\n+\n!!!!\n@r2\nTGCA\n+\n####\n");
    assert_eq!(report.member_count, 3);
}

#[test]
fn mixed_bgzf_and_plain_members_remain_valid_generic_gzip() {
    let mut compressed = bgzf_member(b"bgzf");
    compressed.extend(member(b"plain"));
    let mut decoded = Vec::new();
    let report = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, b"bgzfplain");
    assert_eq!(report.member_count, 2);
}

#[test]
fn parallel_bgzf_preserves_block_order() {
    let mut compressed = Vec::new();
    let mut expected = Vec::new();
    for index in 0..100_u32 {
        let block = format!("{index:04}\n").into_bytes();
        compressed.extend(bgzf_member(&block));
        expected.extend(block);
    }
    compressed.extend(bgzf_eof());
    let decoder = Decoder::builder().decoder_threads(4).build().unwrap();
    let mut decoded = Vec::new();
    let report = decoder.decode(&compressed, &mut decoded).unwrap();
    assert_eq!(decoded, expected);
    assert_eq!(report.member_count, 101);
}

#[test]
fn parallel_bgzf_decodes_compressed_dynamic_blocks() {
    let deflate = hex(
        "edc3410900000804b06c870f0b5cff2c82393658661b5555555555555555555555555555555555555555555555555555555555555555555555555555f51f",
    );
    let block = b"ACGT".repeat(10_000);
    let mut compressed = bgzf_member_from_raw_deflate(&deflate, &block);
    compressed.extend(bgzf_member_from_raw_deflate(&deflate, &block));
    compressed.extend(bgzf_eof());

    let decoder = Decoder::builder().decoder_threads(4).build().unwrap();
    let mut decoded = Vec::new();
    let report = decoder.decode(&compressed, &mut decoded).unwrap();
    assert_eq!(decoded, [block.as_slice(), block.as_slice()].concat());
    assert_eq!(report.member_count, 3);
}

#[test]
fn speculative_marker_path_decodes_dynamic_multiblock_members() {
    let (member, expected_member) = dynamic_multiblock_fixture();
    let mut compressed = member.clone();
    compressed.extend_from_slice(&member);
    let mut expected = expected_member.clone();
    expected.extend_from_slice(&expected_member);

    let decoder = Decoder::builder()
        .decoder_threads(4)
        .input_page_size(32)
        .decoded_chunk_size(64 * 1024)
        .build()
        .unwrap();
    let mut decoded = Vec::new();
    let report = decoder.decode(&compressed, &mut decoded).unwrap();
    assert_eq!(decoded, expected);
    assert_eq!(report.member_count, 2);
}

#[test]
fn parallel_small_member_path_decodes_dense_dynamic_members() {
    let (member, expected_member) = dynamic_multiblock_fixture();
    let mut compressed = Vec::new();
    let mut expected = Vec::new();
    for _ in 0..64 {
        compressed.extend_from_slice(&member);
        expected.extend_from_slice(&expected_member);
    }

    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let mut decoded = Vec::new();
    let report = decoder.decode(&compressed, &mut decoded).unwrap();
    assert_eq!(decoded, expected);
    assert_eq!(report.member_count, 64);
}

#[test]
fn parallel_small_member_path_ignores_header_magic_inside_deflate() {
    let mut first_output = vec![0; 8];
    first_output.extend_from_slice(b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff");
    first_output.extend_from_slice(b"gzip magic inside stored payload");
    let first_member = stored_then_fixed_member(&first_output);
    let (member, expected_member) = dynamic_multiblock_fixture();

    let mut compressed = first_member;
    let mut expected = first_output;
    for _ in 0..32 {
        compressed.extend_from_slice(&member);
        expected.extend_from_slice(&expected_member);
    }

    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let mut decoded = Vec::new();
    let report = decoder.decode(&compressed, &mut decoded).unwrap();
    assert_eq!(decoded, expected);
    assert_eq!(report.member_count, 33);
}

#[test]
fn parallel_small_member_path_falls_back_at_a_corrupt_member() {
    let (member, expected_member) = dynamic_multiblock_fixture();
    let mut compressed = Vec::new();
    for index in 0..64 {
        let start = compressed.len();
        compressed.extend_from_slice(&member);
        if index == 20 {
            let footer = start + member.len() - 8;
            compressed[footer] ^= 1;
        }
    }

    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let mut decoded = Vec::new();
    let error = decoder.decode(&compressed, &mut decoded).unwrap_err();
    assert!(matches!(
        error,
        DecodeError::ChecksumMismatch { member: 20, .. }
    ));
    assert_eq!(decoded, expected_member.repeat(21));
}

#[test]
fn reader_streams_dense_small_members_in_order() {
    let (member, expected_member) = dynamic_multiblock_fixture();
    let compressed = member.repeat(64);
    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let mut reader = decoder.reader(compressed).unwrap();
    let mut decoded = Vec::new();
    reader.read_to_end(&mut decoded).unwrap();
    assert_eq!(decoded, expected_member.repeat(64));
    assert_eq!(reader.report().unwrap().member_count, 64);
}

#[test]
fn parallel_small_member_path_enforces_global_output_limit() {
    let (member, expected_member) = dynamic_multiblock_fixture();
    let compressed = member.repeat(64);
    let decoder = Decoder::builder()
        .decoder_threads(8)
        .output_limit(Some(expected_member.len() as u64 + 1))
        .build()
        .unwrap();
    let mut decoded = Vec::new();
    assert!(matches!(
        decoder.decode(&compressed, &mut decoded),
        Err(DecodeError::OutputLimitExceeded { .. })
    ));
    assert_eq!(decoded, expected_member);
}

#[test]
fn parallel_bridge_recognizes_a_final_block_beyond_the_last_grid_point() {
    const MIB: usize = 1024 * 1024;
    const MAX_PADDED_MEMBER: usize = 25 + u16::MAX as usize;
    let (member, expected_member) = dynamic_multiblock_fixture();
    let mut compressed = member.clone();

    // Place the final member's two-block DEFLATE payload across the ninth
    // 1 MiB grid point. Header-only padding keeps this regression fixture
    // deterministic and compact in source while reproducing issue #1's
    // position-dependent final-member transition.
    let grid = 10 + 9 * MIB;
    let final_header = grid - 80 - 10;
    while compressed.len() < final_header {
        let remaining = final_header - compressed.len();
        let mut member_size = remaining.min(MAX_PADDED_MEMBER);
        let tail = remaining - member_size;
        if tail != 0 && tail < 25 {
            member_size -= 25 - tail;
        }
        compressed.extend(padded_empty_member(member_size));
    }
    assert_eq!(compressed.len(), final_header);
    compressed.extend_from_slice(&member);

    let decoder = Decoder::builder().decoder_threads(4).build().unwrap();
    let mut decoded = Vec::new();
    let report = decoder.decode(&compressed, &mut decoded).unwrap();
    assert_eq!(
        decoded,
        [expected_member.as_slice(), expected_member.as_slice()].concat()
    );
    assert_eq!(report.member_count, 146);
}

#[test]
fn reader_handles_one_byte_consumer_buffers() {
    let compressed = member(&vec![42; 200_000]);
    let mut reader = Decoder::default().reader(compressed.clone()).unwrap();
    let mut output = Vec::new();
    let mut byte = [0_u8; 1];
    while reader.read(&mut byte).unwrap() != 0 {
        output.push(byte[0]);
    }
    assert_eq!(output, vec![42; 200_000]);
    assert_eq!(
        reader.report().unwrap().compressed_bytes,
        compressed.len() as u64
    );
}

#[test]
fn dropping_backpressured_reader_cancels_workers() {
    let compressed = member(&vec![7; 16 * 1024 * 1024]);
    let decoder = Decoder::builder()
        .decoder_threads(4)
        .in_flight_chunks(1)
        .build()
        .unwrap();
    let reader = decoder.reader(compressed).unwrap();
    drop(reader);
}

#[test]
fn reader_coerces_to_boxed_read_send() {
    let reader = Decoder::default().reader(member(b"hello")).unwrap();
    let mut boxed: Box<dyn Read + Send> = Box::new(reader);
    let mut output = String::new();
    boxed.read_to_string(&mut output).unwrap();
    assert_eq!(output, "hello");
}

#[test]
fn paraseq_consumes_decoder_reader_directly() {
    let fastq_data = b"@r1\nACGT\n+\n!!!!\n@r2\nTGCA\n+\n####\n";
    let decoded = Decoder::default().reader(member(fastq_data)).unwrap();
    let mut reader = fastq::Reader::new(decoded);
    let mut records = reader.new_record_set();
    let mut observed = Vec::new();
    while records.fill(&mut reader).unwrap() {
        for record in records.iter() {
            let record = record.unwrap();
            observed.push((
                record.id().to_vec(),
                record.seq_raw().to_vec(),
                record.qual().unwrap().to_vec(),
            ));
        }
    }
    assert_eq!(
        observed,
        vec![
            (b"r1".to_vec(), b"ACGT".to_vec(), b"!!!!".to_vec()),
            (b"r2".to_vec(), b"TGCA".to_vec(), b"####".to_vec()),
        ]
    );
}

#[test]
fn reports_corrupt_later_member_after_earlier_output() {
    let mut compressed = member(b"valid");
    let mut corrupt = member(b"corrupt");
    let footer = corrupt.len() - 8;
    corrupt[footer] ^= 1;
    compressed.extend(corrupt);

    let mut reader = Decoder::default().reader(compressed).unwrap();
    let mut output = Vec::new();
    let error = reader.read_to_end(&mut output).unwrap_err();
    assert_eq!(output, b"validcorrupt");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<DecodeError>())
            .is_some()
    );
}

#[test]
fn rejects_trailing_garbage() {
    let mut compressed = member(b"valid");
    compressed.extend_from_slice(b"garbage");
    let error = Decoder::default()
        .decode(&compressed, &mut io::sink())
        .unwrap_err();
    assert!(matches!(error, DecodeError::InvalidGzip { .. }));
}

#[test]
fn enforces_output_limit_before_emitting_excess() {
    let compressed = member(b"0123456789");
    let decoder = Decoder::builder().output_limit(Some(5)).build().unwrap();
    let mut output = Vec::new();
    assert!(matches!(
        decoder.decode(&compressed, &mut output),
        Err(DecodeError::OutputLimitExceeded { limit: 5 })
    ));
    assert!(output.is_empty());
}

#[test]
fn parallel_stored_path_enforces_global_output_limit() {
    let compressed = member(&vec![9; 10 * 1024 * 1024]);
    let decoder = Decoder::builder()
        .decoder_threads(4)
        .output_limit(Some(5 * 1024 * 1024))
        .build()
        .unwrap();
    let mut output = Vec::new();
    assert!(matches!(
        decoder.decode(&compressed, &mut output),
        Err(DecodeError::OutputLimitExceeded { .. })
    ));
    assert!(output.len() <= 4 * 1024 * 1024);
}

#[test]
fn keep_index_false_leaves_report_index_none() {
    let compressed = member(b"no index by default");
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert!(report.index.is_none());
}

#[test]
fn keep_index_sequential_builds_valid_index() {
    let payload = b"the quick brown fox jumps over the lazy dog";
    let compressed = member(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .checkpoint_spacing(16)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    let index = report.index.expect("index when keep_index");
    index.validate().expect("index validates");
    assert_eq!(index.compressed_size_in_bytes, report.compressed_bytes);
    assert_eq!(index.uncompressed_size_in_bytes, report.decompressed_bytes);
    // At least start + end (member start empty window and EOF).
    assert!(
        index.checkpoint_count() >= 2,
        "expected start+end, got {}",
        index.checkpoint_count()
    );
    assert_eq!(index.checkpoints[0].uncompressed_offset_in_bytes, 0);
    assert!(
        index
            .windows
            .get(index.checkpoints[0].compressed_offset_in_bits)
            .is_some_and(|w| w.is_empty())
    );
    let last = index.checkpoints.last().unwrap();
    assert_eq!(last.uncompressed_offset_in_bytes, report.decompressed_bytes);
    assert_eq!(
        last.compressed_offset_in_bits,
        report.compressed_bytes.saturating_mul(8)
    );
}

#[test]
fn keep_index_round_trips_through_gzidx_export() {
    let payload = vec![0xABu8; 64 * 1024];
    let compressed = member(&payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .checkpoint_spacing(8 * 1024)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    let index = report.index.expect("index");
    let mut exported = Vec::new();
    index.export_indexed_gzip(&mut exported).unwrap();
    let restored =
        GzipIndex::import_indexed_gzip(&mut Cursor::new(&exported), Some(compressed.len() as u64))
            .unwrap();
    assert_eq!(restored.checkpoint_count(), index.checkpoint_count());
    for (a, b) in index.checkpoints.iter().zip(restored.checkpoints.iter()) {
        assert_eq!(a.compressed_offset_in_bits, b.compressed_offset_in_bits);
        assert_eq!(
            a.uncompressed_offset_in_bytes,
            b.uncompressed_offset_in_bytes
        );
    }
}

#[test]
fn keep_index_multi_member_empty_windows_at_boundaries() {
    let m1 = b"first member payload";
    let m2 = b"second";
    let m3 = b"third and final member";
    let mut compressed = member(m1);
    compressed.extend(member(m2));
    compressed.extend(member(m3));

    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .checkpoint_spacing(1_000_000)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, [m1.as_slice(), m2.as_slice(), m3.as_slice()].concat());
    let index = report.index.expect("index");
    index.validate().unwrap();

    // Empty-window checkpoints at each member DEFLATE start (uncompressed offsets
    // at member boundaries) plus EOF.
    let empty_starts: Vec<_> = index
        .checkpoints
        .iter()
        .filter(|cp| {
            index
                .windows
                .get(cp.compressed_offset_in_bits)
                .is_some_and(|w| w.is_empty())
                && cp.compressed_offset_in_bits < report.compressed_bytes.saturating_mul(8)
        })
        .collect();
    assert!(
        empty_starts.len() >= 3,
        "expected empty-window member starts, got {}",
        empty_starts.len()
    );
    assert_eq!(empty_starts[0].uncompressed_offset_in_bytes, 0);
    assert_eq!(
        empty_starts[1].uncompressed_offset_in_bytes,
        m1.len() as u64
    );
    assert_eq!(
        empty_starts[2].uncompressed_offset_in_bytes,
        (m1.len() + m2.len()) as u64
    );
}

#[test]
fn keep_index_bgzf_parallel_empty_windows_at_blocks() {
    let b1 = b"@r1\nACGT\n+\n!!!!\n";
    let b2 = b"@r2\nTGCA\n+\n####\n";
    let b3 = b"@r3\nAAAA\n+\n$$$$\n";
    let mut compressed = bgzf_member(b1);
    compressed.extend(bgzf_member(b2));
    compressed.extend(bgzf_member(b3));
    compressed.extend(bgzf_eof());

    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(4)
        .keep_index(true)
        .checkpoint_spacing(1)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(
        decoded,
        [b1.as_slice(), b2.as_slice(), b3.as_slice()].concat()
    );
    let index = report.index.expect("index");
    index.validate().unwrap();
    // Four blocks (including empty EOF block) get empty-window starts + EOF.
    let empty = index
        .checkpoints
        .iter()
        .filter(|cp| {
            index
                .windows
                .get(cp.compressed_offset_in_bits)
                .is_some_and(|w| w.is_empty())
        })
        .count();
    assert!(
        empty >= 4,
        "expected empty-window points for each BGZF block, got {empty}"
    );

    let mut exported = Vec::new();
    index.export_indexed_gzip(&mut exported).unwrap();
    assert!(!exported.is_empty());
}

#[test]
fn keep_index_works_with_crc32_disabled() {
    let payload = b"crc optional with index";
    let compressed = member(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .crc32_enabled(false)
        .keep_index(true)
        .checkpoint_spacing(32)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    let index = report.index.expect("index");
    index.validate().unwrap();
    assert!(index.checkpoint_count() >= 2);
}

#[test]
fn keep_index_independent_members_records_empty_starts() {
    // Dense small members exercise the independent multi-member parallel path.
    let mut compressed = Vec::new();
    let mut expected = Vec::new();
    for i in 0..32u8 {
        let payload = vec![i; 64];
        expected.extend_from_slice(&payload);
        compressed.extend(member(&payload));
    }
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(4)
        .keep_index(true)
        .checkpoint_spacing(1_000_000)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, expected);
    let index = report.index.expect("index");
    index.validate().unwrap();
    assert!(
        index.checkpoint_count() >= 2,
        "got {}",
        index.checkpoint_count()
    );
    // First checkpoint is empty-window at uncompressed 0.
    assert_eq!(index.checkpoints[0].uncompressed_offset_in_bytes, 0);
    assert!(
        index
            .windows
            .get(index.checkpoints[0].compressed_offset_in_bits)
            .is_some_and(|w| w.is_empty())
    );
}

#[test]
fn gather_line_offsets_counts_newlines_without_index() {
    let payload = b"one\ntwo\nthree\n";
    let compressed = member(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .gather_line_offsets(true)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    assert!(report.index.is_none());
    assert_eq!(report.line_count, Some(3));
}

#[test]
fn gather_line_offsets_with_keep_index_stamps_checkpoints() {
    // Multi-line payload large enough for intermediate checkpoints.
    let mut payload = Vec::new();
    for i in 0..200 {
        payload.extend_from_slice(format!("line-{i:04}\n").as_bytes());
    }
    let expected_newlines = payload.iter().filter(|&&b| b == b'\n').count() as u64;
    let compressed = member(&payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .gather_line_offsets(true)
        .checkpoint_spacing(64)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.line_count, Some(expected_newlines));
    let index = report.index.expect("index");
    assert!(index.has_line_offsets);
    assert_eq!(index.total_line_count(), Some(expected_newlines));
    // First checkpoint at offset 0 has zero newlines before it.
    assert_eq!(index.checkpoints[0].line_offset, 0);
    // EOF checkpoint carries the full newline count.
    let last = index.checkpoints.last().unwrap();
    assert_eq!(last.uncompressed_offset_in_bytes, payload.len() as u64);
    assert_eq!(last.line_offset, expected_newlines);
    // Checkpoints are non-decreasing in line_offset and match the true
    // prefix newline count at each uncompressed offset.
    let mut prev = 0u64;
    for checkpoint in &index.checkpoints {
        assert!(checkpoint.line_offset >= prev);
        prev = checkpoint.line_offset;
        let actual = payload[..checkpoint.uncompressed_offset_in_bytes as usize]
            .iter()
            .filter(|&&b| b == b'\n')
            .count() as u64;
        assert_eq!(
            checkpoint.line_offset, actual,
            "line_offset mismatch at uncompressed {}",
            checkpoint.uncompressed_offset_in_bytes
        );
    }
    // EOF (or any late) checkpoint must see the multi-line payload's newlines.
    assert!(
        index.checkpoints.iter().any(|cp| cp.line_offset > 0),
        "expected a non-zero line_offset on a multi-line payload"
    );
}

#[test]
fn gather_line_offsets_empty_payload() {
    let compressed = member(b"");
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .gather_line_offsets(true)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert!(decoded.is_empty());
    assert_eq!(report.line_count, Some(0));
}

#[test]
fn gather_line_offsets_defaults_to_none() {
    let compressed = member(b"a\nb\n");
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(report.line_count, None);
}

// --- zlib (RFC 1950) wrapper support ----------------------------------------

fn adler32(bytes: &[u8]) -> u32 {
    // RFC 1950 / zlib initial value is 1.
    const BASE: u32 = 65521;
    let mut s1 = 1_u32;
    let mut s2 = 0_u32;
    for &byte in bytes {
        s1 = (s1 + u32::from(byte)) % BASE;
        s2 = (s2 + s1) % BASE;
    }
    (s2 << 16) | s1
}

/// zlib-wrapped stored DEFLATE (CMF/FLG 0x78 0x01 + payload + Adler-32 BE).
fn zlib_member(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = vec![0x78, 0x01];
    encoded.extend_from_slice(&stored_deflate(bytes));
    encoded.extend_from_slice(&adler32(bytes).to_be_bytes());
    encoded
}

fn with_corrupt_adler(mut compressed: Vec<u8>) -> Vec<u8> {
    let adler_offset = compressed.len() - 4;
    compressed[adler_offset] ^= 1;
    compressed
}

#[test]
fn decodes_single_zlib_stream_auto() {
    let payload = b"the quick brown fox";
    let compressed = zlib_member(payload);
    let mut decoded = Vec::new();
    let report = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
    assert_eq!(report.compressed_bytes, compressed.len() as u64);
}

#[test]
fn decodes_zlib_with_forced_format() {
    let payload = b"forced zlib";
    let compressed = zlib_member(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .format(Format::Zlib)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
}

#[test]
fn gzip_still_works_when_zlib_exists() {
    let payload = b"still gzip";
    let compressed = member(payload);
    let mut decoded = Vec::new();
    let report = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
}

#[test]
fn gzip_format_rejects_zlib_stream() {
    let compressed = zlib_member(b"not gzip");
    let mut decoded = Vec::new();
    let error = Decoder::builder()
        .format(Format::Gzip)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap_err();
    assert!(matches!(
        error,
        DecodeError::InvalidGzip {
            reason: rapidgzip_core::GzipErrorKind::BadMagic,
            ..
        }
    ));
}

#[test]
fn zlib_format_rejects_gzip_stream() {
    let compressed = member(b"not zlib");
    let mut decoded = Vec::new();
    let error = Decoder::builder()
        .format(Format::Zlib)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap_err();
    assert!(matches!(
        error,
        DecodeError::InvalidZlib {
            reason: ZlibErrorKind::UnsupportedCompressionMethod(15) | ZlibErrorKind::BadHeader,
            ..
        }
    ));
}

#[test]
fn decodes_concatenated_zlib_streams() {
    let mut compressed = zlib_member(b"first\n");
    compressed.extend(zlib_member(b""));
    compressed.extend(zlib_member(b"second\n"));

    let mut decoded = Vec::new();
    let report = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, b"first\nsecond\n");
    assert_eq!(report.member_count, 3);
}

#[test]
fn corrupt_adler_rejected_when_enabled_accepted_when_disabled() {
    let payload = b"adler footer check";
    let compressed = with_corrupt_adler(zlib_member(payload));

    let enabled = Decoder::builder().crc32_enabled(true).build().unwrap();
    let mut decoded = Vec::new();
    let error = enabled.decode(&compressed, &mut decoded).unwrap_err();
    assert!(matches!(
        error,
        DecodeError::ChecksumMismatch { member: 0, .. }
    ));

    let disabled = Decoder::builder().crc32_enabled(false).build().unwrap();
    let mut decoded = Vec::new();
    let report = disabled.decode(&compressed, &mut decoded).unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
}

#[test]
fn zlib_empty_payload() {
    let compressed = zlib_member(b"");
    let mut decoded = Vec::new();
    let report = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert!(decoded.is_empty());
    assert_eq!(report.member_count, 1);
}

#[test]
fn zlib_reader_and_decode_read() {
    let payload = b"reader path zlib";
    let compressed = zlib_member(payload);
    let decoder = Decoder::builder().decoder_threads(2).build().unwrap();

    let mut from_decode = Vec::new();
    decoder.decode(&compressed, &mut from_decode).unwrap();

    let mut reader = decoder.reader(compressed.clone()).unwrap();
    let mut from_reader = Vec::new();
    reader.read_to_end(&mut from_reader).unwrap();
    let report = reader.finish().unwrap();
    assert_eq!(from_reader, payload);
    assert_eq!(report.member_count, 1);

    let mut from_read = Vec::new();
    decoder
        .decode_read(Cursor::new(compressed.as_slice()), &mut from_read)
        .unwrap();
    assert_eq!(from_read, from_decode);
    assert_eq!(from_decode, payload);
}

#[test]
fn known_zlib_hello_fixture() {
    // Python: zlib.compress(b'hello') → x\x9c\xcbH\xcd\xc9\xc9\x07\x00\x06,\x02\x15
    let compressed = hex("789ccb48cdc9c90700062c0215");
    let mut decoded = Vec::new();
    let report = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, b"hello");
    assert_eq!(report.member_count, 1);
}

#[test]
fn zlib_trailing_garbage_errors() {
    let mut compressed = zlib_member(b"ok");
    compressed.extend_from_slice(b"garbage");
    let mut decoded = Vec::new();
    let error = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap_err();
    assert!(matches!(
        error,
        DecodeError::InvalidZlib {
            reason: ZlibErrorKind::TrailingGarbage
                | ZlibErrorKind::BadHeader
                | ZlibErrorKind::BadHeaderChecksum
                | ZlibErrorKind::UnsupportedCompressionMethod(_),
            ..
        }
    ));
}

#[test]
fn multi_thread_budget_still_decodes_zlib_sequentially() {
    // Parallel budget must not break zlib (no parallel zlib path).
    let payload = b"parallel budget zlib payload with enough text to matter";
    let compressed = zlib_member(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(8)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
}

// --- raw DEFLATE (RFC 1951) support ----------------------------------------

#[test]
fn decodes_raw_deflate_stored_block() {
    let payload = b"the quick brown fox";
    let compressed = stored_deflate(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .format(Format::RawDeflate)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
    assert_eq!(report.compressed_bytes, compressed.len() as u64);
    assert_eq!(report.decompressed_bytes, payload.len() as u64);
}

#[test]
fn decodes_raw_deflate_empty_payload() {
    let compressed = stored_deflate(b"");
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .format(Format::RawDeflate)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert!(decoded.is_empty());
    assert_eq!(report.member_count, 1);
    assert_eq!(report.compressed_bytes, compressed.len() as u64);
}

#[test]
fn gzip_format_rejects_raw_deflate() {
    let compressed = stored_deflate(b"not gzip");
    let mut decoded = Vec::new();
    let error = Decoder::builder()
        .format(Format::Gzip)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap_err();
    assert!(matches!(
        error,
        DecodeError::InvalidGzip {
            reason: rapidgzip_core::GzipErrorKind::BadMagic,
            ..
        }
    ));
}

#[test]
fn auto_does_not_select_raw_deflate() {
    // Raw has no magic; Auto tries gzip (or zlib if CMF/FLG matches) and fails.
    let compressed = stored_deflate(b"raw under auto");
    let mut decoded = Vec::new();
    let error = Decoder::default()
        .decode(&compressed, &mut decoded)
        .unwrap_err();
    assert!(
        matches!(
            error,
            DecodeError::InvalidGzip { .. }
                | DecodeError::InvalidZlib { .. }
                | DecodeError::InvalidDeflate { .. }
        ),
        "unexpected error for raw under Auto: {error:?}"
    );
}

#[test]
fn raw_deflate_reader_and_decode_read() {
    let payload = b"reader path raw deflate";
    let compressed = stored_deflate(payload);
    let decoder = Decoder::builder()
        .format(Format::RawDeflate)
        .decoder_threads(2)
        .build()
        .unwrap();

    let mut from_decode = Vec::new();
    decoder.decode(&compressed, &mut from_decode).unwrap();

    let mut reader = decoder.reader(compressed.clone()).unwrap();
    let mut from_reader = Vec::new();
    reader.read_to_end(&mut from_reader).unwrap();
    let report = reader.finish().unwrap();
    assert_eq!(from_reader, payload);
    assert_eq!(report.member_count, 1);

    let mut from_read = Vec::new();
    decoder
        .decode_read(Cursor::new(compressed.as_slice()), &mut from_read)
        .unwrap();
    assert_eq!(from_read, from_decode);
    assert_eq!(from_decode, payload);
}

#[test]
fn raw_deflate_trailing_garbage_errors() {
    let mut compressed = stored_deflate(b"ok");
    compressed.extend_from_slice(b"garbage");
    let mut decoded = Vec::new();
    let error = Decoder::builder()
        .format(Format::RawDeflate)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap_err();
    assert!(matches!(
        error,
        DecodeError::InvalidDeflate {
            reason: rapidgzip_core::DeflateErrorKind::InvalidData,
            ..
        }
    ));
}

#[test]
fn multi_thread_budget_still_decodes_raw_deflate_sequentially() {
    let payload = b"parallel budget raw deflate payload with enough text to matter";
    let compressed = stored_deflate(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .format(Format::RawDeflate)
        .decoder_threads(8)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
}

#[test]
fn raw_deflate_rejects_empty_source() {
    let compressed: Vec<u8> = Vec::new();
    let mut decoded = Vec::new();
    let error = Decoder::builder()
        .format(Format::RawDeflate)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap_err();
    assert!(matches!(
        error,
        DecodeError::InvalidDeflate {
            reason: rapidgzip_core::DeflateErrorKind::Truncated,
            ..
        }
    ));
}

#[test]
fn raw_deflate_external_crc32_match_ok() {
    let payload = b"raw deflate with external crc";
    let compressed = stored_deflate(payload);
    let expected = crc32(payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .format(Format::RawDeflate)
        .raw_crc32_list(vec![expected])
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
}

#[test]
fn raw_deflate_external_crc32_mismatch_errors() {
    let payload = b"raw deflate bad crc";
    let compressed = stored_deflate(payload);
    let mut decoded = Vec::new();
    let error = Decoder::builder()
        .format(Format::RawDeflate)
        .raw_crc32_list(vec![0xDEAD_BEEF])
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap_err();
    match error {
        DecodeError::ChecksumMismatch {
            member: 0,
            expected: 0xDEAD_BEEF,
            actual,
        } => {
            assert_eq!(actual, crc32(payload));
            assert_ne!(actual, 0xDEAD_BEEF);
        }
        other => panic!("expected ChecksumMismatch, got {other}"),
    }
}

#[test]
fn raw_crc32_list_ignored_for_gzip_and_zlib() {
    // Wrong list must not affect gzip/zlib (their trailers are authoritative).
    let payload = b"ignore external list";
    let gzip = member(payload);
    let mut decoded = Vec::new();
    Decoder::builder()
        .format(Format::Gzip)
        .raw_crc32_list(vec![0x1111_1111])
        .build()
        .unwrap()
        .decode(&gzip, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);

    let zlib = zlib_member(payload);
    decoded.clear();
    Decoder::builder()
        .format(Format::Zlib)
        .raw_crc32_list(vec![0x2222_2222])
        .build()
        .unwrap()
        .decode(&zlib, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
}

#[test]
fn raw_deflate_external_crc32_decode_read_path() {
    let payload = b"streaming raw with external crc";
    let compressed = stored_deflate(payload);
    let expected = crc32(payload);
    let mut decoded = Vec::new();
    Decoder::builder()
        .format(Format::RawDeflate)
        .raw_crc32_list(vec![expected])
        .build()
        .unwrap()
        .decode_read(std::io::Cursor::new(&compressed), &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);

    let error = Decoder::builder()
        .format(Format::RawDeflate)
        .raw_crc32_list(vec![0])
        .build()
        .unwrap()
        .decode_read(std::io::Cursor::new(&compressed), &mut Vec::new())
        .unwrap_err();
    assert!(matches!(
        error,
        DecodeError::ChecksumMismatch { member: 0, .. }
    ));
}

#[test]
fn keep_index_compress_windows_default_uses_zlib_when_smaller() {
    // Highly compressible payload so mid-stream 32 KiB windows shrink under zlib.
    let payload = vec![0xABu8; 128 * 1024];
    let compressed = member(&payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .checkpoint_spacing(16 * 1024)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    let index = report.index.expect("index");
    let mut saw_zlib = false;
    for (_bits, window) in index.windows.iter() {
        if window.is_empty() {
            assert_eq!(window.compression(), WindowCompression::None);
            continue;
        }
        let raw = window.decompressed().expect("decompressed window");
        assert!(!raw.is_empty());
        assert_eq!(raw.len(), window.len());
        if window.compression() == WindowCompression::Zlib {
            saw_zlib = true;
        }
    }
    assert!(
        saw_zlib,
        "expected at least one zlib-compressed non-empty window with default compress_index_windows"
    );

    // Export still works from zlib-stored windows (decompress then GZIDX raw).
    let mut exported = Vec::new();
    index.export_indexed_gzip(&mut exported).unwrap();
    let restored =
        GzipIndex::import_indexed_gzip(&mut Cursor::new(&exported), Some(compressed.len() as u64))
            .unwrap();
    assert_eq!(restored.checkpoint_count(), index.checkpoint_count());
    for (a, b) in index.checkpoints.iter().zip(restored.checkpoints.iter()) {
        assert_eq!(a.compressed_offset_in_bits, b.compressed_offset_in_bits);
        assert_eq!(
            a.uncompressed_offset_in_bytes,
            b.uncompressed_offset_in_bytes
        );
        let wa = index.windows.get(a.compressed_offset_in_bits).unwrap();
        let wb = restored.windows.get(b.compressed_offset_in_bits).unwrap();
        assert_eq!(
            wa.decompressed().unwrap().as_ref(),
            wb.decompressed().unwrap().as_ref()
        );
        // Import always stores raw.
        assert_eq!(wb.compression(), WindowCompression::None);
    }
}

#[test]
fn keep_index_compress_windows_off_keeps_none() {
    let payload = vec![0xCDu8; 96 * 1024];
    let compressed = member(&payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .compress_index_windows(false)
        .checkpoint_spacing(16 * 1024)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    let index = report.index.expect("index");
    for (_bits, window) in index.windows.iter() {
        assert_eq!(window.compression(), WindowCompression::None);
        let _ = window.decompressed().unwrap();
    }
}

#[test]
fn decode_read_multi_thread_matches_decode_on_multi_member_gzip() {
    // Parallel decode_read path: spill stream to temp + positional parallel decode.
    let mut compressed = member(b"first multi-member payload\n");
    compressed.extend(member(b""));
    compressed.extend(member(b"second multi-member payload\n"));
    compressed.extend(member(b"third\n"));

    let decoder = Decoder::builder().decoder_threads(4).build().unwrap();

    let mut from_readat = Vec::new();
    let report_at = decoder.decode(&compressed, &mut from_readat).unwrap();

    let mut from_read = Vec::new();
    let report_read = decoder
        .decode_read(Cursor::new(compressed.as_slice()), &mut from_read)
        .unwrap();

    assert_eq!(from_read, from_readat);
    assert_eq!(from_read, b"first multi-member payload\nsecond multi-member payload\nthird\n");
    assert_eq!(report_read.member_count, report_at.member_count);
    assert_eq!(report_read.member_count, 4);
    assert_eq!(report_read.compressed_bytes, report_at.compressed_bytes);
    assert_eq!(
        report_read.decompressed_bytes,
        report_at.decompressed_bytes
    );
}
