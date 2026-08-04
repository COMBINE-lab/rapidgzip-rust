//! End-to-end decoder and paraseq integration tests.

use paraseq::{Record, fastq};
use rapidgzip_core::{
    DecodeError, DecodeReport, Decoder, DecoderHandle, DecoderPath, DecoderPressure, ReadAt,
};
use std::io::{self, Read};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    condition()
}

#[derive(Clone)]
struct GatedReadAt {
    bytes: Arc<Vec<u8>>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl GatedReadAt {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Arc::new(bytes),
            gate: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn open(&self) {
        let (lock, signal) = &*self.gate;
        *lock.lock().unwrap() = true;
        signal.notify_all();
    }
}

impl ReadAt for GatedReadAt {
    fn len(&self) -> io::Result<u64> {
        Ok(self.bytes.as_slice().len() as u64)
    }

    fn read_at(&self, offset: u64, output: &mut [u8]) -> io::Result<usize> {
        // Integration-test threads and the named coordinator may classify the
        // input. Unnamed scoped decoder workers wait so the test can observe
        // their elastic population deterministically.
        if std::thread::current().name().is_none() {
            let (lock, signal) = &*self.gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = signal.wait(open).unwrap();
            }
        }
        self.bytes.as_slice().read_at(offset, output)
    }
}

/// Non-seekable byte source fed one chunk at a time by another thread.
///
/// This implements only [`Read`], never [`std::io::Seek`], so a test cannot
/// accidentally exercise positional reads the way a `Cursor` would. Reads block
/// until the producer supplies more bytes, which is what a real pipe does.
struct ChunkedPipe {
    receiver: mpsc::Receiver<Vec<u8>>,
    current: Vec<u8>,
    offset: usize,
}

impl Read for ChunkedPipe {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        while self.offset == self.current.len() {
            match self.receiver.recv() {
                Ok(chunk) => {
                    self.current = chunk;
                    self.offset = 0;
                }
                // Every sender was dropped, which is this pipe's end of input.
                Err(_) => return Ok(0),
            }
        }
        let count = (self.current.len() - self.offset).min(output.len());
        output[..count].copy_from_slice(&self.current[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}

fn pipe_pair() -> (SyncSender<Vec<u8>>, ChunkedPipe) {
    let (sender, receiver) = mpsc::sync_channel(1);
    (
        sender,
        ChunkedPipe {
            receiver,
            current: Vec::new(),
            offset: 0,
        },
    )
}

/// Feeds `bytes` through a pipe in `chunk_size` pieces, pausing between each.
fn pipe_from(bytes: &[u8], chunk_size: usize, pause: Duration) -> ChunkedPipe {
    let (sender, pipe) = pipe_pair();
    let chunks: Vec<Vec<u8>> = bytes
        .chunks(chunk_size.max(1))
        .map(<[u8]>::to_vec)
        .collect();
    thread::spawn(move || {
        for chunk in chunks {
            if !pause.is_zero() {
                thread::sleep(pause);
            }
            if sender.send(chunk).is_err() {
                return;
            }
        }
    });
    pipe
}

/// Decodes `compressed` positionally, the way every 0.1.0 entry point does.
fn decode_positional(
    decoder: &Decoder,
    compressed: &[u8],
) -> Result<(Vec<u8>, DecodeReport), DecodeError> {
    let mut output = Vec::new();
    decoder
        .decode(compressed, &mut output)
        .map(|report| (output, report))
}

/// Decodes `compressed` through a genuinely non-seekable source.
fn decode_streaming(
    decoder: &Decoder,
    compressed: &[u8],
) -> Result<(Vec<u8>, DecodeReport), DecodeError> {
    let mut output = Vec::new();
    decoder
        .decode_stream(pipe_from(compressed, 64, Duration::ZERO), &mut output)
        .map(|report| (output, report))
}

/// Asserts that both paths accept `compressed` and agree byte for byte.
fn assert_paths_agree(decoder: &Decoder, compressed: &[u8]) -> DecodeReport {
    let (positional, positional_report) = decode_positional(decoder, compressed).unwrap();
    let (streamed, streamed_report) = decode_streaming(decoder, compressed).unwrap();
    assert_eq!(streamed, positional);
    assert_eq!(streamed_report.member_count, positional_report.member_count);
    assert_eq!(
        streamed_report.decompressed_bytes,
        positional_report.decompressed_bytes
    );
    assert_eq!(
        streamed_report.compressed_bytes,
        positional_report.compressed_bytes
    );
    // Reports retain the configured worker budget even though telemetry shows
    // an effective target of one for this sequential path.
    assert_eq!(
        streamed_report.decoder_threads,
        positional_report.decoder_threads
    );
    streamed_report
}

fn assert_handle_traits<T: Clone + Send + Sync + Unpin>() {}

#[test]
fn decoder_handle_is_clone_send_sync_and_unpin() {
    assert_handle_traits::<DecoderHandle>();
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
fn reader_telemetry_survives_moving_and_finishing_the_reader() {
    let (member, expected_member) = dynamic_multiblock_fixture();
    let compressed = member.repeat(64);
    let expected_bytes = expected_member.len() * 64;
    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let mut reader = decoder.reader(compressed).unwrap();
    let handle = reader.handle();
    let mut decoded = Vec::new();
    reader.read_to_end(&mut decoded).unwrap();
    assert_eq!(decoded.len(), expected_bytes);

    let stats = handle.stats();
    assert_eq!(stats.path, DecoderPath::DenseMembers);
    assert_eq!(stats.configured_workers, 8);
    assert_eq!(stats.decompressed_bytes, expected_bytes as u64);
    assert_eq!(stats.consumed_bytes, expected_bytes as u64);
    assert_eq!(stats.member_count, 64);
    assert_eq!(stats.active_workers, 0);
    assert_eq!(stats.spawned_workers, 0);
    assert_eq!(stats.auxiliary_threads, 0);
    assert!(matches!(stats.pressure, DecoderPressure::Finished));
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(
        handle.stats().decode_throughput_bps,
        stats.decode_throughput_bps
    );
}

#[test]
fn telemetry_reports_specialized_and_sequential_paths() {
    let stored = member(&vec![1; 10 * 1024 * 1024]);
    let mut bgzf = Vec::new();
    for _ in 0..32 {
        bgzf.extend(bgzf_member(b"ACGT\n"));
    }
    bgzf.extend(bgzf_eof());
    let (marker, _) = dynamic_multiblock_fixture();

    for (compressed, workers, expected_path, expected_members) in [
        (stored, 4, DecoderPath::Stored, 1),
        (bgzf, 4, DecoderPath::Bgzf, 33),
        // This fixture is only 147 compressed bytes. It cannot expose two
        // complete grid-task waves and therefore stays sequential even with a
        // larger configured budget.
        (marker.clone(), 4, DecoderPath::Sequential, 1),
        (marker, 1, DecoderPath::Sequential, 1),
    ] {
        let decoder = Decoder::builder().decoder_threads(workers).build().unwrap();
        let mut reader = decoder.reader(compressed).unwrap();
        let handle = reader.handle();
        io::copy(&mut reader, &mut io::sink()).unwrap();
        assert_eq!(handle.stats().path, expected_path);
        assert_eq!(handle.stats().member_count, expected_members);
    }
}

#[test]
fn specialized_task_queue_publication_is_stable_under_repetition() {
    let stored = member(&vec![1; 10 * 1024 * 1024]);
    let mut bgzf = Vec::new();
    for _ in 0..64 {
        bgzf.extend(bgzf_member(b"ACGT\n"));
    }
    bgzf.extend(bgzf_eof());
    let (dense_member, _) = dynamic_multiblock_fixture();
    let dense = dense_member.repeat(64);

    for _ in 0..12 {
        for (compressed, expected_path, expected_members) in [
            (&stored, DecoderPath::Stored, 1),
            (&bgzf, DecoderPath::Bgzf, 65),
            (&dense, DecoderPath::DenseMembers, 64),
        ] {
            let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
            let mut reader = decoder.reader(compressed.clone()).unwrap();
            let handle = reader.handle();
            io::copy(&mut reader, &mut io::sink()).unwrap();
            let report = reader.finish().unwrap();
            assert_eq!(report.member_count, expected_members);
            assert_eq!(handle.stats().path, expected_path);
        }
    }
}

#[test]
fn runtime_limit_lazily_grows_and_retires_dense_workers() {
    let visible = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    if visible < 2 {
        return;
    }

    let (member, _) = dynamic_multiblock_fixture();
    let source = GatedReadAt::new(member.repeat(512));
    let decoder = Decoder::builder()
        .decoder_threads(8)
        .in_flight_chunks(1)
        .build()
        .unwrap();
    let reader = decoder.reader(source.clone()).unwrap();
    let handle = reader.handle();
    handle.set_worker_limit(1).unwrap();

    assert!(wait_until(Duration::from_secs(5), || {
        let stats = handle.stats();
        stats.path == DecoderPath::DenseMembers
            && stats.spawned_workers == 1
            && stats.busy_workers == 1
    }));

    let raised_limit = visible.min(4);
    handle.set_worker_limit(raised_limit).unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        handle.stats().spawned_workers == raised_limit
    }));

    handle.set_worker_limit(1).unwrap();
    source.open();
    assert!(wait_until(Duration::from_secs(5), || {
        handle.stats().spawned_workers <= 1
    }));
    drop(reader);
}

#[test]
fn final_reader_handoff_reports_consumer_backpressure() {
    let (member, _) = dynamic_multiblock_fixture();
    let decoder = Decoder::builder()
        .decoder_threads(8)
        .in_flight_chunks(1)
        .build()
        .unwrap();
    let reader = decoder.reader(member.repeat(512)).unwrap();
    let handle = reader.handle();
    assert!(wait_until(Duration::from_secs(5), || {
        matches!(
            handle.stats().pressure,
            DecoderPressure::ConsumerBound { .. }
        )
    }));

    // Backpressure must reduce observed decode activity. A worker that already
    // owns a completed result can remain parked on a bounded internal handoff
    // while the coordinator waits for the reader, so `spawned_workers` is not
    // required to fall until output advances or cancellation releases it.
    assert!(wait_until(Duration::from_secs(5), || {
        handle.stats().busy_workers <= 1
    }));
    drop(reader);
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
fn streaming_matches_positional_for_a_single_member() {
    let payload = b"ACGT".repeat(5_000);
    let compressed = member(&payload);
    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let report = assert_paths_agree(&decoder, &compressed);
    assert_eq!(report.member_count, 1);
    assert_eq!(report.decompressed_bytes, payload.len() as u64);
}

#[test]
fn streaming_matches_positional_for_concatenated_and_empty_members() {
    let mut compressed = member(b"first");
    compressed.extend_from_slice(&member(b""));
    compressed.extend_from_slice(&member(b"second"));
    let decoder = Decoder::builder().decoder_threads(4).build().unwrap();
    let report = assert_paths_agree(&decoder, &compressed);
    assert_eq!(report.member_count, 3);
    let (streamed, _) = decode_streaming(&decoder, &compressed).unwrap();
    assert_eq!(streamed, b"firstsecond");
}

#[test]
fn streaming_matches_positional_for_bgzf_with_the_eof_member() {
    // A dynamic-Huffman BGZF block, so the stream is not trivially stored.
    let deflate = hex(
        "edc3410900000804b06c870f0b5cff2c82393658661b5555555555555555555555555555555555555555555555555555555555555555555555555555f51f",
    );
    let block = b"ACGT".repeat(10_000);
    let mut compressed = Vec::new();
    for _ in 0..3 {
        compressed.extend(bgzf_member_from_raw_deflate(&deflate, &block));
    }
    compressed.extend(bgzf_eof());

    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let report = assert_paths_agree(&decoder, &compressed);
    // Three data blocks plus the conventional 28-byte EOF member.
    assert_eq!(report.member_count, 4);
    assert_eq!(report.decompressed_bytes, 3 * block.len() as u64);

    let (streamed, _) = decode_streaming(&decoder, &compressed).unwrap();
    assert_eq!(streamed, block.repeat(3));
}

#[test]
fn streaming_matches_positional_for_a_fully_stored_stream() {
    let payload: Vec<u8> = (0..10_u32 * 1024 * 1024).map(|value| value as u8).collect();
    let compressed = member(&payload);
    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    // Positionally this fixture is large enough to take the parallel stored
    // path, so the comparison below is against a genuinely different decoder.
    let reader = decoder.reader(compressed.clone()).unwrap();
    let handle = reader.handle();
    assert_eq!(reader.finish().unwrap().member_count, 1);
    assert_eq!(handle.stats().path, DecoderPath::Stored);

    let report = assert_paths_agree(&decoder, &compressed);
    assert_eq!(report.decompressed_bytes, payload.len() as u64);
}

#[test]
fn streaming_rejects_input_truncated_inside_deflate() {
    let compressed = member(&b"ACGT".repeat(5_000));
    let truncated = &compressed[..compressed.len() - 64];
    let decoder = Decoder::default();
    let mut output = Vec::new();
    let error = decoder
        .decode_stream(pipe_from(truncated, 64, Duration::ZERO), &mut output)
        .unwrap_err();
    assert!(
        matches!(error, DecodeError::InvalidDeflate { .. }),
        "expected a DEFLATE truncation error, got {error:?}"
    );
}

#[test]
fn streaming_rejects_input_truncated_inside_the_footer() {
    let compressed = member(b"payload");
    let truncated = &compressed[..compressed.len() - 3];
    let decoder = Decoder::default();
    let mut output = Vec::new();
    let error = decoder
        .decode_stream(pipe_from(truncated, 4, Duration::ZERO), &mut output)
        .unwrap_err();
    assert!(
        matches!(error, DecodeError::InvalidGzip { .. }),
        "expected a truncated-footer error, got {error:?}"
    );
}

#[test]
fn streaming_reports_the_member_with_a_corrupt_checksum() {
    let mut second = member(b"second");
    let crc_start = second.len() - 8;
    second[crc_start] ^= 0xFF;
    let mut compressed = member(b"first");
    compressed.extend_from_slice(&second);
    compressed.extend_from_slice(&member(b"third"));

    let decoder = Decoder::default();
    let mut output = Vec::new();
    let error = decoder
        .decode_stream(pipe_from(&compressed, 8, Duration::ZERO), &mut output)
        .unwrap_err();
    assert!(
        matches!(error, DecodeError::ChecksumMismatch { member: 1, .. }),
        "expected member 1 to fail its checksum, got {error:?}"
    );
}

#[test]
fn streaming_reports_the_member_with_a_corrupt_size() {
    let mut second = member(b"second");
    let size_start = second.len() - 4;
    second[size_start] ^= 0x0F;
    let mut compressed = member(b"first");
    compressed.extend_from_slice(&second);

    let decoder = Decoder::default();
    let mut output = Vec::new();
    let error = decoder
        .decode_stream(pipe_from(&compressed, 8, Duration::ZERO), &mut output)
        .unwrap_err();
    assert!(
        matches!(error, DecodeError::SizeMismatch { member: 1, .. }),
        "expected member 1 to fail its size check, got {error:?}"
    );
}

#[test]
fn streaming_rejects_trailing_garbage() {
    let mut compressed = member(b"valid");
    compressed.extend_from_slice(b"garbage");
    let decoder = Decoder::default();
    let mut output = Vec::new();
    let error = decoder
        .decode_stream(pipe_from(&compressed, 8, Duration::ZERO), &mut output)
        .unwrap_err();
    assert!(
        matches!(error, DecodeError::InvalidGzip { .. }),
        "expected trailing bytes to be rejected, got {error:?}"
    );
}

#[test]
fn streaming_rejects_input_that_is_not_gzip_at_all() {
    let decoder = Decoder::default();
    let mut output = Vec::new();
    let error = decoder
        .decode_stream(
            pipe_from(b"not gzip at all", 4, Duration::ZERO),
            &mut output,
        )
        .unwrap_err();
    assert!(
        matches!(error, DecodeError::InvalidGzip { .. }),
        "expected bad magic to be rejected, got {error:?}"
    );
    // With four bytes available immediately, best-effort constructor
    // validation can reject this before returning a reader.
    assert!(
        decoder
            .stream_reader(pipe_from(b"not gzip at all", 4, Duration::ZERO))
            .is_err()
    );
}

#[test]
fn streaming_enforces_output_limit_before_emitting_excess() {
    let compressed = member(b"0123456789");
    let decoder = Decoder::builder().output_limit(Some(5)).build().unwrap();
    let mut output = Vec::new();
    assert!(matches!(
        decoder.decode_stream(pipe_from(&compressed, 4, Duration::ZERO), &mut output),
        Err(DecodeError::OutputLimitExceeded { limit: 5 })
    ));
    // A stream is fed to zlib-rs in small windows, so unlike the positional
    // decode of this fixture the limit can be reached part way through the
    // member. What must hold either way is that nothing past the limit is
    // emitted.
    assert!(output.len() <= 5, "emitted {} bytes", output.len());
}

#[test]
fn streaming_reader_preserves_the_budget_but_uses_no_workers() {
    let compressed = member(&b"ACGT".repeat(20_000));
    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let reader = decoder
        .stream_reader(pipe_from(&compressed, 4096, Duration::ZERO))
        .unwrap();
    let handle = reader.handle();
    let initial = handle.stats();
    assert_eq!(initial.configured_workers, 8);
    assert_eq!(initial.worker_limit, 8);
    assert_eq!(initial.active_workers, 1);
    assert_eq!(initial.spawned_workers, 0);
    assert_eq!(initial.auxiliary_threads, 0);
    reader.set_worker_limit(7).unwrap();

    let report = reader.finish().unwrap();
    assert_eq!(report.member_count, 1);
    assert_eq!(report.decoder_threads, 8);

    let stats = handle.stats();
    assert_eq!(stats.path, DecoderPath::Sequential);
    assert_eq!(stats.configured_workers, 8);
    assert_eq!(stats.worker_limit, 7);
    assert_eq!(stats.spawned_workers, 0);
    assert_eq!(stats.auxiliary_threads, 0);
    assert_eq!(stats.pressure, DecoderPressure::Finished);
}

#[test]
fn streaming_reader_reports_the_calling_thread_while_blocked_on_input() {
    let (sender, pipe) = pipe_pair();
    // Best-effort construction can validate this complete fixed header. The
    // first read then consumes it and blocks waiting for DEFLATE input.
    sender
        .send(b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec())
        .unwrap();
    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let reader = decoder.stream_reader(pipe).unwrap();
    let handle = reader.handle();
    let consumer = thread::spawn(move || {
        let mut reader = reader;
        let mut byte = [0_u8; 1];
        let result = reader.read(&mut byte);
        (reader, result)
    });

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let stats = handle.stats();
        if stats.busy_workers == 1 {
            assert_eq!(stats.active_workers, 1);
            assert_eq!(stats.spawned_workers, 0);
            assert_eq!(stats.auxiliary_threads, 0);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "streaming read never became busy"
        );
        thread::sleep(Duration::from_millis(1));
    }

    drop(sender);
    let (_reader, result) = consumer.join().unwrap();
    assert!(result.is_err(), "header-only input must be truncated");
}

#[test]
fn streaming_reader_coerces_to_boxed_read_send_for_paraseq() {
    let mut records = Vec::new();
    for index in 0..64 {
        records.extend_from_slice(format!("@read{index}\nACGTACGT\n+\nIIIIIIII\n").as_bytes());
    }
    let compressed = member(&records);
    let decoder = Decoder::default();
    let reader: Box<dyn Read + Send> = Box::new(
        decoder
            .stream_reader(pipe_from(&compressed, 512, Duration::ZERO))
            .unwrap(),
    );

    let mut parsed = fastq::Reader::new(reader);
    let mut record_set = fastq::RecordSet::new(16);
    let mut seen = 0;
    while record_set.fill(&mut parsed).unwrap() {
        for record in record_set.iter() {
            assert_eq!(record.unwrap().seq().as_ref(), b"ACGTACGT");
            seen += 1;
        }
    }
    assert_eq!(seen, 64);
}

#[test]
fn dropping_a_streaming_reader_before_eof_releases_its_source() {
    let compressed = member(&b"ACGT".repeat(200_000));
    let (sender, pipe) = pipe_pair();
    let prefix = compressed[..1024].to_vec();
    sender.send(prefix).unwrap();

    let decoder = Decoder::default();
    let mut reader = decoder.stream_reader(pipe).unwrap();
    let mut byte = [0_u8; 1];
    assert_eq!(reader.read(&mut byte).unwrap(), 1);
    // Nothing claims the unread remainder was verified.
    assert!(reader.report().is_none());

    drop(reader);
    // The pipe receiver is owned by the source. A detached coordinator would
    // keep it alive; synchronous pull decoding drops it with DecoderReader.
    assert!(
        sender.send(vec![0]).is_err(),
        "dropping a streaming reader must release its source immediately"
    );
}

#[test]
fn streaming_tolerates_a_slow_producer_without_deadlock_or_spinning() {
    let payload = b"ACGT".repeat(4_000);
    let compressed = member(&payload);
    let chunk_size = compressed.len().div_ceil(8);
    let pause = Duration::from_millis(20);

    let decoder = Decoder::default();
    let start = Instant::now();
    let mut output = Vec::new();
    let report = decoder
        .decode_stream(pipe_from(&compressed, chunk_size, pause), &mut output)
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(output, payload);
    assert_eq!(report.member_count, 1);
    // The decoder waited on the producer rather than failing early, and did not
    // take pathologically longer than the producer's own schedule.
    assert!(elapsed >= pause * 8, "finished too early: {elapsed:?}");
    assert!(elapsed < pause * 8 * 10, "took far too long: {elapsed:?}");
}

#[test]
fn streaming_reader_drains_one_byte_consumer_buffers() {
    let payload = b"ACGT".repeat(5_000);
    let compressed = member(&payload);
    let decoder = Decoder::default();
    let mut reader = decoder
        .stream_reader(pipe_from(&compressed, 256, Duration::ZERO))
        .unwrap();

    let mut output = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte).unwrap() {
            0 => break,
            _ => output.push(byte[0]),
        }
    }
    assert_eq!(output, payload);
    assert_eq!(reader.report().unwrap().member_count, 1);
}

/// A path in the system temporary directory, unique to this process and `name`.
#[cfg(unix)]
fn scratch_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("rapidgzip-{}-{name}", std::process::id()))
}

#[cfg(unix)]
#[test]
fn streaming_decodes_a_real_operating_system_pipe() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let payload = b"ACGTTGCA".repeat(20_000);
    let compressed = member(&payload);

    // `cat` gives both a real pipe file descriptor and a producer that is not
    // this process, so nothing about the source can be read positionally.
    let mut child = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child_stdin = child.stdin.take().unwrap();
    let written = compressed.clone();
    thread::spawn(move || {
        let _ = child_stdin.write_all(&written);
    });
    let child_stdout = child.stdout.take().unwrap();

    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let mut streamed = Vec::new();
    let report = decoder.decode_stream(child_stdout, &mut streamed).unwrap();
    assert!(child.wait().unwrap().success());

    let (positional, _) = decode_positional(&decoder, &compressed).unwrap();
    assert_eq!(streamed, positional);
    assert_eq!(streamed, payload);
    assert_eq!(report.member_count, 1);
    assert_eq!(report.decoder_threads, 8);
}

#[cfg(unix)]
#[test]
fn open_routes_a_fifo_to_the_streaming_path() {
    use std::fs;
    use std::io::Write;
    use std::process::Command;

    let path = scratch_path("open-fifo.gz");
    let _ = fs::remove_file(&path);
    assert!(
        Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success(),
        "mkfifo failed"
    );

    let payload = b"ACGT".repeat(30_000);
    let compressed = member(&payload);
    let writer_path = path.clone();
    // Opening a FIFO for writing blocks until a reader opens it, so this thread
    // and the `open` below rendezvous.
    let writer = thread::spawn(move || {
        let mut file = fs::File::create(&writer_path).unwrap();
        file.write_all(&compressed).unwrap();
    });

    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let mut reader = decoder.open(&path).unwrap();
    let handle = reader.handle();
    let mut decoded = Vec::new();
    reader.read_to_end(&mut decoded).unwrap();
    let report = reader.finish().unwrap();
    writer.join().unwrap();
    let _ = fs::remove_file(&path);

    assert_eq!(decoded, payload);
    assert_eq!(report.member_count, 1);
    assert_eq!(report.decoder_threads, 8);
    let stats = handle.stats();
    assert_eq!(stats.path, DecoderPath::Sequential);
    assert_eq!(stats.configured_workers, 8);
}

#[cfg(unix)]
#[test]
fn open_still_uses_a_parallel_path_for_a_regular_file() {
    use std::fs;

    let path = scratch_path("open-regular.gz");
    let mut compressed = Vec::new();
    for index in 0..64 {
        compressed.extend(member(format!("member-{index}").as_bytes()));
    }
    fs::write(&path, &compressed).unwrap();

    let decoder = Decoder::builder().decoder_threads(8).build().unwrap();
    let reader = decoder.open(&path).unwrap();
    let handle = reader.handle();
    let report = reader.finish().unwrap();
    let _ = fs::remove_file(&path);

    assert_eq!(report.member_count, 64);
    assert_eq!(report.decoder_threads, 8);
    // Which parallel path wins depends on the fixture; what matters is that
    // routing a regular file did not divert it to the streaming decoder.
    let stats = handle.stats();
    assert_ne!(stats.path, DecoderPath::Sequential);
    assert_eq!(stats.configured_workers, 8);
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
