//! End-to-end decoder and paraseq integration tests.

use paraseq::{Record, fastq};
use rapidgzip_core::{DecodeError, Decoder};
use std::io::{self, Read};

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
