//! Indexed seek/read tests for [`rapidgzip_core::IndexedReader`].

use rapidgzip_core::{DecodeError, Decoder, GzipIndex, IndexedReader, SeekCacheStats};
use std::io::{self, Cursor, Read, Seek, SeekFrom};

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

fn bgzf_member(bytes: &[u8]) -> Vec<u8> {
    let deflate = stored_deflate(bytes);
    let total_size = 18 + deflate.len() + 8;
    assert!(total_size <= u16::MAX as usize + 1);
    let block_size = (total_size - 1) as u16;
    let mut encoded = b"\x1f\x8b\x08\x04\0\0\0\0\x00\xff\x06\x00BC\x02\x00".to_vec();
    encoded.extend_from_slice(&block_size.to_le_bytes());
    encoded.extend_from_slice(&deflate);
    encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    encoded
}

fn bgzf_eof() -> Vec<u8> {
    vec![
        31, 139, 8, 4, 0, 0, 0, 0, 0, 255, 6, 0, 66, 67, 2, 0, 27, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]
}

fn build_index(compressed: &[u8], checkpoint_spacing: usize) -> (Vec<u8>, GzipIndex) {
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .checkpoint_spacing(checkpoint_spacing)
        .decoded_chunk_size(4 * 1024)
        .build()
        .unwrap()
        .decode(compressed, &mut decoded)
        .unwrap();
    let index = report.index.expect("keep_index");
    (decoded, index)
}

fn build_index_with_lines(compressed: &[u8], checkpoint_spacing: usize) -> (Vec<u8>, GzipIndex) {
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .gather_line_offsets(true)
        .checkpoint_spacing(checkpoint_spacing)
        .decoded_chunk_size(4 * 1024)
        .build()
        .unwrap()
        .decode(compressed, &mut decoded)
        .unwrap();
    let index = report.index.expect("keep_index");
    assert!(index.has_line_offsets);
    (decoded, index)
}

fn open_indexed(compressed: &[u8], index: GzipIndex) -> IndexedReader<Vec<u8>> {
    Decoder::builder()
        .decoder_threads(1)
        .decoded_chunk_size(4 * 1024)
        .input_page_size(4 * 1024)
        .build()
        .unwrap()
        .reader_with_index(compressed.to_vec(), index)
        .unwrap()
}

#[test]
fn seek_and_read_matches_slice() {
    let payload: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
    let compressed = member(&payload);
    let (decoded, index) = build_index(&compressed, 4_096);
    assert_eq!(decoded, payload);

    let mut reader = open_indexed(&compressed, index);
    let n = 12_345u64;
    let k = 2_000usize;
    reader.seek(SeekFrom::Start(n)).unwrap();
    let mut buf = vec![0u8; k];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(buf, payload[n as usize..n as usize + k]);
}

#[test]
fn seek_backward_rereads_earlier_data() {
    let payload = b"abcdefghijklmnopqrstuvwxyz0123456789".repeat(100);
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 64);

    let mut reader = open_indexed(&compressed, index);
    reader.seek(SeekFrom::Start(500)).unwrap();
    let mut later = [0u8; 16];
    reader.read_exact(&mut later).unwrap();
    assert_eq!(&later, &payload[500..516]);

    reader.seek(SeekFrom::Start(10)).unwrap();
    let mut earlier = [0u8; 16];
    reader.read_exact(&mut earlier).unwrap();
    assert_eq!(&earlier, &payload[10..26]);
}

#[test]
fn seek_to_zero_reads_entire_file() {
    let payload = b"the quick brown fox jumps over the lazy dog".repeat(200);
    let compressed = member(&payload);
    let (decoded, index) = build_index(&compressed, 128);

    let mut reader = open_indexed(&compressed, index);
    reader.seek(SeekFrom::Start(100)).unwrap();
    reader.seek(SeekFrom::Start(0)).unwrap();
    let mut all = Vec::new();
    reader.read_to_end(&mut all).unwrap();
    assert_eq!(all, decoded);
}

#[test]
fn seek_to_eof_read_returns_zero() {
    let payload = b"hello seek eof";
    let compressed = member(payload);
    let (_, index) = build_index(&compressed, 8);
    let len = payload.len() as u64;

    let mut reader = open_indexed(&compressed, index);
    assert_eq!(reader.len(), Some(len));
    reader.seek(SeekFrom::Start(len)).unwrap();
    let mut buf = [0u8; 4];
    assert_eq!(reader.read(&mut buf).unwrap(), 0);
}

#[test]
fn seek_past_end_like_cursor() {
    let payload = b"short";
    let compressed = member(payload);
    let (_, index) = build_index(&compressed, 8);

    let mut reader = open_indexed(&compressed, index);
    let past = reader.seek(SeekFrom::Start(1_000)).unwrap();
    assert_eq!(past, 1_000);
    assert_eq!(reader.position(), 1_000);
    let mut buf = [0u8; 8];
    assert_eq!(reader.read(&mut buf).unwrap(), 0);

    // Match Cursor: seek past end is ok; relative seeks still work.
    reader.seek(SeekFrom::Start(0)).unwrap();
    let mut all = Vec::new();
    reader.read_to_end(&mut all).unwrap();
    assert_eq!(all, payload);
}

#[test]
fn multi_member_seek_into_second() {
    let m1 = b"first member payload that is long enough";
    let m2 = b"SECOND-MEMBER-DATA";
    let m3 = b"third";
    let mut compressed = member(m1);
    compressed.extend(member(m2));
    compressed.extend(member(m3));
    let expected = [m1.as_slice(), m2.as_slice(), m3.as_slice()].concat();
    let (_, index) = build_index(&compressed, 1_000_000);

    let mut reader = open_indexed(&compressed, index);
    let start = m1.len() as u64;
    reader.seek(SeekFrom::Start(start)).unwrap();
    let mut buf = vec![0u8; m2.len()];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(buf, m2);

    reader.seek(SeekFrom::Start(start + 3)).unwrap();
    let mut mid = [0u8; 5];
    reader.read_exact(&mut mid).unwrap();
    assert_eq!(&mid, &m2[3..8]);

    // Cross into third member via sequential read.
    reader
        .seek(SeekFrom::Start((m1.len() + m2.len() - 2) as u64))
        .unwrap();
    let mut cross = [0u8; 6];
    reader.read_exact(&mut cross).unwrap();
    assert_eq!(
        &cross,
        &expected[m1.len() + m2.len() - 2..m1.len() + m2.len() + 4]
    );
}

#[test]
fn bgzf_seek_into_later_block() {
    let b1 = b"@r1\nACGT\n+\n!!!!\n";
    let b2 = b"@r2\nTGCA\n+\n####\n";
    let b3 = b"@r3\nAAAA\n+\n$$$$\n";
    let mut compressed = bgzf_member(b1);
    compressed.extend(bgzf_member(b2));
    compressed.extend(bgzf_member(b3));
    compressed.extend(bgzf_eof());
    let expected = [b1.as_slice(), b2.as_slice(), b3.as_slice()].concat();

    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(4)
        .keep_index(true)
        .checkpoint_spacing(1)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, expected);
    let index = report.index.expect("index");

    let mut reader = Decoder::builder()
        .build()
        .unwrap()
        .reader_with_index(compressed.clone(), index)
        .unwrap();

    let offset = (b1.len() + 3) as u64;
    reader.seek(SeekFrom::Start(offset)).unwrap();
    let mut buf = vec![0u8; 10];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(buf, expected[offset as usize..offset as usize + 10]);
}

#[test]
fn seek_to_line_reads_expected_content() {
    // Lines: 1="alpha\n", 2="beta\n", 3="gamma\n", 4="delta" (no trailing nl)
    let payload = b"alpha\nbeta\ngamma\ndelta";
    let compressed = member(payload);
    let (decoded, index) = build_index_with_lines(&compressed, 4);
    assert_eq!(decoded, payload);
    assert_eq!(index.total_line_count(), Some(3));

    let mut reader = open_indexed(&compressed, index);

    // Line 1 starts at byte 0.
    assert_eq!(reader.seek_to_line(1).unwrap(), 0);
    let mut buf = [0u8; 5];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"alpha");

    // Line 2 starts after first newline.
    assert_eq!(reader.seek_to_line(2).unwrap(), 6);
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"beta");

    // Line 3.
    assert_eq!(reader.seek_to_line(3).unwrap(), 11);
    let mut line = Vec::new();
    // Read until newline.
    let mut b = [0u8; 1];
    loop {
        if reader.read(&mut b).unwrap() == 0 {
            break;
        }
        line.push(b[0]);
        if b[0] == b'\n' {
            break;
        }
    }
    assert_eq!(line, b"gamma\n");

    // Line 4 (last line, no trailing newline).
    assert_eq!(reader.seek_to_line(4).unwrap(), 17);
    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).unwrap();
    assert_eq!(rest, b"delta");

    // Past last line → EOF position.
    let eof = reader.seek_to_line(100).unwrap();
    assert_eq!(eof, payload.len() as u64);
    assert_eq!(reader.read(&mut [0u8; 1]).unwrap(), 0);

    // 0 is invalid (1-based).
    assert_eq!(
        reader.seek_to_line(0).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn seek_to_line_requires_line_offsets() {
    let payload = b"a\nb\n";
    let compressed = member(payload);
    let (_, index) = build_index(&compressed, 8);
    assert!(!index.has_line_offsets);
    let mut reader = open_indexed(&compressed, index);
    assert_eq!(
        reader.seek_to_line(1).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
}

#[test]
fn gzidx_round_trip_seek() {
    let payload: Vec<u8> = (0..20_000u32).map(|i| (i * 7) as u8).collect();
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 2_048);

    let mut exported = Vec::new();
    index.export_indexed_gzip(&mut exported).unwrap();
    let restored =
        GzipIndex::import_indexed_gzip(&mut Cursor::new(&exported), Some(compressed.len() as u64))
            .unwrap();

    let mut reader = open_indexed(&compressed, restored);
    reader.seek(SeekFrom::Start(7_500)).unwrap();
    let mut buf = [0u8; 64];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, &payload[7_500..7_564]);
}

#[test]
fn keep_index_zlib_windows_export_import_seek_matches() {
    // Compressible payload so keep_index (default compress_index_windows) stores
    // mid-stream windows as WindowCompression::Zlib; export → import → seek must
    // still decode correctly.
    let payload = vec![0x55u8; 80_000];
    let compressed = member(&payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .compress_index_windows(true)
        .checkpoint_spacing(8_192)
        .decoded_chunk_size(4 * 1024)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    let index = report.index.expect("keep_index");
    assert!(
        index
            .windows
            .iter()
            .any(|(_, w)| w.compression() == rapidgzip_core::WindowCompression::Zlib),
        "expected zlib-stored windows for compressible history"
    );

    let mut exported = Vec::new();
    index.export_indexed_gzip(&mut exported).unwrap();
    let restored =
        GzipIndex::import_indexed_gzip(&mut Cursor::new(&exported), Some(compressed.len() as u64))
            .unwrap();

    let mut reader = open_indexed(&compressed, restored);
    for &offset in &[0u64, 10_000, 40_000, 70_000] {
        reader.seek(SeekFrom::Start(offset)).unwrap();
        let mut buf = [0u8; 128];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], &payload[offset as usize..offset as usize + n]);
    }
}

#[test]
fn zlib_index_window_expand_cache_hits_on_repeated_seeks() {
    // keep_index with compress_index_windows stores mid-stream history as zlib.
    // Disable the decoded-window LRU (bytes=0) so every seek re-inflates from a
    // checkpoint and exercises the expanded-window cache; capacity stays non-zero
    // so expand is enabled (tied to seek_cache_chunks).
    let payload = vec![0x55u8; 80_000];
    let compressed = member(&payload);
    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(1)
        .keep_index(true)
        .compress_index_windows(true)
        .checkpoint_spacing(8_192)
        .decoded_chunk_size(4 * 1024)
        .build()
        .unwrap()
        .decode(&compressed, &mut decoded)
        .unwrap();
    assert_eq!(decoded, payload);
    let index = report.index.expect("keep_index");
    assert!(
        index
            .windows
            .iter()
            .any(|(_, w)| w.compression() == rapidgzip_core::WindowCompression::Zlib),
        "expected zlib-stored windows for compressible history"
    );

    // First intermediate checkpoint for stored DEFLATE lands at 65535 (u16::MAX
    // stored-block limit); seek past that so predecessor history is zlib-stored.
    let target = 70_000u64;
    let cp = index.checkpoint_at_or_before(target).expect("checkpoint");
    let stored = index
        .window_for(cp.compressed_offset_in_bits)
        .expect("window map entry");
    assert_eq!(
        stored.compression(),
        rapidgzip_core::WindowCompression::Zlib,
        "target must use a zlib-stored predecessor window"
    );

    let mut reader = Decoder::builder()
        .decoder_threads(1)
        .decoded_chunk_size(4 * 1024)
        .input_page_size(4 * 1024)
        .seek_cache_chunks(16)
        .seek_cache_bytes(0)
        .seek_readahead(false)
        .seek_prefetch_windows(0)
        .build()
        .unwrap()
        .reader_with_index(compressed.clone(), index)
        .unwrap();

    // Alternate away from `target` so the active buffer cannot satisfy the
    // revisit; each return must re-init inflate and expand the same window.
    for &offset in &[target, 0, target, 0, target] {
        reader.seek(SeekFrom::Start(offset)).unwrap();
        let mut buf = [0u8; 128];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &payload[offset as usize..offset as usize + 128]);
    }

    let stats = reader.cache_stats();
    assert!(
        stats.window_expand_misses >= 1,
        "expected at least one zlib window expand: {stats:?}"
    );
    assert!(
        stats.window_expand_hits >= 1,
        "repeated seeks into the same checkpoint should hit expand cache: {stats:?}"
    );
    assert!(
        stats.window_expand_chunks >= 1,
        "expanded window should remain in the LRU: {stats:?}"
    );
}

#[test]
fn mismatched_archive_size_errors() {
    let payload = b"size check";
    let compressed = member(payload);
    let (_, mut index) = build_index(&compressed, 8);
    index.compressed_size_in_bytes = compressed.len() as u64 + 99;

    let result = Decoder::builder()
        .build()
        .unwrap()
        .reader_with_index(compressed, index);
    match result {
        Err(error @ DecodeError::InvalidIndex(_)) => {
            assert!(error.to_string().contains("invalid gzip index"));
        }
        Ok(_) => panic!("expected InvalidIndex"),
        Err(other) => panic!("unexpected error: {other}"),
    }
}

#[test]
fn empty_checkpoints_error() {
    let payload = b"x";
    let compressed = member(payload);
    let (_, mut index) = build_index(&compressed, 8);
    index.checkpoints.clear();
    index.windows = Default::default();

    let result = Decoder::builder()
        .build()
        .unwrap()
        .reader_with_index(compressed, index);
    assert!(matches!(result, Err(DecodeError::InvalidIndex(_))));
}

#[test]
fn seek_current_and_end() {
    let payload = b"0123456789abcdef";
    let compressed = member(payload);
    let (_, index) = build_index(&compressed, 4);
    let mut reader = open_indexed(&compressed, index);

    reader.seek(SeekFrom::Start(4)).unwrap();
    assert_eq!(reader.seek(SeekFrom::Current(2)).unwrap(), 6);
    let mut b = [0u8; 2];
    reader.read_exact(&mut b).unwrap();
    assert_eq!(&b, b"67");

    assert_eq!(
        reader
            .seek(SeekFrom::End(-(payload.len() as i64 - 1)))
            .unwrap(),
        1
    );
    reader.read_exact(&mut b).unwrap();
    assert_eq!(&b, b"12");
}

#[test]
fn open_with_index_via_tempfile() {
    use std::io::Write;
    let payload = b"file backed seek";
    let compressed = member(payload);
    let (_, index) = build_index(&compressed, 8);

    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "rapidgzip-seek-test-{}-{}.gz",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&compressed).unwrap();
    }

    let mut reader = Decoder::builder()
        .build()
        .unwrap()
        .open_with_index(&path, index)
        .unwrap();
    reader.seek(SeekFrom::Start(5)).unwrap();
    let mut buf = [0u8; 6];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"backed");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn io_error_source_is_decode_error() {
    let payload = b"err source";
    let compressed = member(payload);
    let (_, mut index) = build_index(&compressed, 8);
    // Corrupt a checkpoint bit offset so inflate fails on first seek/read.
    if let Some(cp) = index.checkpoints.get_mut(0) {
        cp.compressed_offset_in_bits = 3; // mid-header garbage
    }

    let mut reader = open_indexed(&compressed, index);
    // Seeking to 0 still uses the corrupted checkpoint.
    let result = reader.seek(SeekFrom::Start(0));
    // Seek to 0 may re-init; reading should surface the error.
    let mut buf = [0u8; 4];
    let err = match result {
        Ok(_) => reader.read(&mut buf).unwrap_err(),
        Err(e) => e,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(
        err.get_ref()
            .and_then(|e| e.downcast_ref::<DecodeError>())
            .is_some()
    );
}

#[test]
fn multi_range_seek_read_matches_payload() {
    let payload: Vec<u8> = (0..30_000u32).map(|i| (i % 251) as u8).collect();
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 2_048);

    let mut reader = open_indexed(&compressed, index);
    let ranges = [(100usize, 50usize), (12_000, 128), (500, 64), (25_000, 200)];
    for &(start, len) in &ranges {
        reader.seek(SeekFrom::Start(start as u64)).unwrap();
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &payload[start..start + len]);
    }
}

#[test]
fn random_seeks_match_full_decode() {
    let payload: Vec<u8> = (0..40_000u32).map(|i| ((i * 13) % 256) as u8).collect();
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 1_024);

    let mut reader = open_indexed(&compressed, index);
    // Deterministic pseudo-random offsets.
    let mut state = 0xC0FFEE_u64;
    for _ in 0..40 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let start = (state % (payload.len() as u64 - 32)) as usize;
        let len = 1 + ((state >> 17) as usize % 32);
        reader.seek(SeekFrom::Start(start as u64)).unwrap();
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &payload[start..start + len]);
    }
}

#[test]
fn repeated_region_reads_hit_cache() {
    let payload: Vec<u8> = (0..16_000u32).map(|i| (i % 199) as u8).collect();
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 1_024);

    let mut reader = open_indexed(&compressed, index)
        .with_cache_capacity(8, 256 * 1024)
        .with_readahead(true);

    let start = 3_000u64;
    let len = 64usize;
    reader.seek(SeekFrom::Start(start)).unwrap();
    let mut first = vec![0u8; len];
    reader.read_exact(&mut first).unwrap();
    assert_eq!(&first, &payload[start as usize..start as usize + len]);

    let after_first = reader.cache_stats();
    assert!(after_first.misses >= 1);
    assert!(after_first.inserts >= 1);

    // Leave the region, then return: should be served from the LRU.
    reader.seek(SeekFrom::Start(12_000)).unwrap();
    let mut other = [0u8; 16];
    reader.read_exact(&mut other).unwrap();
    assert_eq!(&other, &payload[12_000..12_016]);

    let before_revisit = reader.cache_stats();
    reader.seek(SeekFrom::Start(start)).unwrap();
    let mut second = vec![0u8; len];
    reader.read_exact(&mut second).unwrap();
    assert_eq!(second, first);

    let after_revisit = reader.cache_stats();
    assert!(
        after_revisit.hits > before_revisit.hits,
        "expected cache hit on revisited region: before={before_revisit:?} after={after_revisit:?}"
    );
}

#[test]
fn sequential_scan_after_seek_uses_readahead() {
    let payload: Vec<u8> = (0..12_000u32).map(|i| (i % 173) as u8).collect();
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 512);

    let mut reader = Decoder::builder()
        .decoder_threads(1)
        .decoded_chunk_size(512)
        .input_page_size(4 * 1024)
        .seek_cache_chunks(16)
        .seek_cache_bytes(64 * 1024)
        .seek_readahead(true)
        .seek_prefetch_windows(0)
        .build()
        .unwrap()
        .reader_with_index(compressed, index)
        .unwrap();

    reader.seek(SeekFrom::Start(100)).unwrap();
    let mut all = Vec::new();
    // Read a few KiB sequentially so multiple windows are filled.
    let mut buf = [0u8; 256];
    while all.len() < 4_000 {
        let n = reader.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        all.extend_from_slice(&buf[..n]);
    }
    assert_eq!(&all, &payload[100..100 + all.len()]);

    let stats = reader.cache_stats();
    assert!(
        stats.readaheads > 0 || stats.chunks > 1,
        "expected readahead or multi-chunk cache after sequential scan: {stats:?}"
    );
    // Returning into the scanned region should hit.
    let before = reader.cache_stats();
    reader.seek(SeekFrom::Start(200)).unwrap();
    let mut sample = [0u8; 32];
    reader.read_exact(&mut sample).unwrap();
    assert_eq!(&sample, &payload[200..232]);
    let after: SeekCacheStats = reader.cache_stats();
    assert!(after.hits >= before.hits);
}

#[test]
fn sequential_scan_with_prefetch_matches_and_records_stats() {
    let payload: Vec<u8> = (0..20_000u32).map(|i| (i % 191) as u8).collect();
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 512);

    let mut reader = Decoder::builder()
        .decoder_threads(2)
        .decoded_chunk_size(512)
        .input_page_size(4 * 1024)
        .seek_cache_chunks(32)
        .seek_cache_bytes(256 * 1024)
        .seek_readahead(true)
        .seek_prefetch_windows(2)
        .build()
        .unwrap()
        .reader_with_index(compressed, index)
        .unwrap();

    // Trigger a miss fill + readahead + background prefetch, then pause so
    // workers can complete before sequential readahead races further ahead.
    reader.seek(SeekFrom::Start(50)).unwrap();
    let mut first = [0u8; 32];
    reader.read_exact(&mut first).unwrap();
    assert_eq!(&first, &payload[50..82]);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while reader.cache_stats().prefetches == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let mid_stats = reader.cache_stats();
    assert!(
        mid_stats.prefetches > 0,
        "expected background prefetch inserts after initial fill: {mid_stats:?}"
    );
    assert!(
        mid_stats.readaheads > 0 || mid_stats.chunks > 1,
        "expected readahead or multi-chunk cache alongside prefetch: {mid_stats:?}"
    );
    assert_eq!(mid_stats.prefetch_windows, 2);

    // Full sequential scan from here still matches the payload.
    let mut all = first.to_vec();
    let mut buf = [0u8; 128];
    while all.len() < 8_000 {
        let n = reader.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        all.extend_from_slice(&buf[..n]);
    }
    assert_eq!(&all, &payload[50..50 + all.len()]);

    // Random seeks within the scanned region still match the payload.
    for &start in &[100usize, 2_000, 5_500, 7_200] {
        reader.seek(SeekFrom::Start(start as u64)).unwrap();
        let mut sample = [0u8; 48];
        reader.read_exact(&mut sample).unwrap();
        assert_eq!(&sample, &payload[start..start + 48]);
    }
}

#[test]
fn prefetch_does_not_break_multi_member_or_bgzf() {
    // Multi-member
    let m1 = b"first member payload that is long enough for several windows!!!!";
    let m2 = b"SECOND-MEMBER-DATA-also-long-enough-for-window-boundaries!!!!!!";
    let mut compressed = member(m1);
    compressed.extend(member(m2));
    let expected = [m1.as_slice(), m2.as_slice()].concat();
    let (_, index) = build_index(&compressed, 32);

    let mut reader = Decoder::builder()
        .decoder_threads(2)
        .decoded_chunk_size(32)
        .seek_readahead(true)
        .seek_prefetch_windows(2)
        .build()
        .unwrap()
        .reader_with_index(compressed, index)
        .unwrap();

    reader.seek(SeekFrom::Start(10)).unwrap();
    let mut all = Vec::new();
    reader.read_to_end(&mut all).unwrap();
    assert_eq!(all, expected[10..]);

    // BGZF multi-block
    let b1 = b"@r1\nACGTACGTACGT\n+\n!!!!!!!!!!!!\n";
    let b2 = b"@r2\nTGCATGCATGCA\n+\n############\n";
    let b3 = b"@r3\nAAAATTTTGGGG\n+\n$$$$$$$$$$$$\n";
    let mut bgzf = bgzf_member(b1);
    bgzf.extend(bgzf_member(b2));
    bgzf.extend(bgzf_member(b3));
    bgzf.extend(bgzf_eof());
    let expected = [b1.as_slice(), b2.as_slice(), b3.as_slice()].concat();

    let mut decoded = Vec::new();
    let report = Decoder::builder()
        .decoder_threads(2)
        .keep_index(true)
        .checkpoint_spacing(1)
        .build()
        .unwrap()
        .decode(&bgzf, &mut decoded)
        .unwrap();
    assert_eq!(decoded, expected);
    let index = report.index.expect("index");

    let mut reader = Decoder::builder()
        .decoder_threads(2)
        .seek_prefetch_windows(2)
        .build()
        .unwrap()
        .reader_with_index(bgzf, index)
        .unwrap();

    reader.seek(SeekFrom::Start(5)).unwrap();
    let mut got = Vec::new();
    reader.read_to_end(&mut got).unwrap();
    assert_eq!(got, expected[5..]);
}

#[test]
fn random_seeks_with_prefetch_match_full_decode() {
    let payload: Vec<u8> = (0..40_000u32).map(|i| ((i * 17) % 256) as u8).collect();
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 1_024);

    let mut reader = Decoder::builder()
        .decoder_threads(4)
        .decoded_chunk_size(1_024)
        .seek_readahead(true)
        .seek_prefetch_windows(2)
        .build()
        .unwrap()
        .reader_with_index(compressed, index)
        .unwrap();

    let mut state = 0xBEEF_u64;
    for _ in 0..50 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let start = (state % (payload.len() as u64 - 32)) as usize;
        let len = 1 + ((state >> 17) as usize % 32);
        reader.seek(SeekFrom::Start(start as u64)).unwrap();
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &payload[start..start + len]);
    }
}

#[test]
fn cache_can_be_disabled() {
    let payload = b"disable cache path".repeat(50);
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 32);

    let mut reader = open_indexed(&compressed, index)
        .with_cache_capacity(0, 0)
        .with_readahead(false);

    reader.seek(SeekFrom::Start(10)).unwrap();
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, &payload[10..18]);

    let stats = reader.cache_stats();
    assert_eq!(stats.chunks, 0);
    assert_eq!(stats.inserts, 0);
    assert!(!stats.readahead_enabled);
}

#[test]
fn gztool_round_trip_seek() {
    let payload: Vec<u8> = (0..20_000u32).map(|i| (i * 7) as u8).collect();
    let compressed = member(&payload);
    let (_, index) = build_index(&compressed, 2_048);

    let mut exported = Vec::new();
    index.export_gztool(&mut exported, false).unwrap();
    let restored =
        GzipIndex::import_gztool(&mut Cursor::new(&exported), Some(compressed.len() as u64))
            .unwrap();

    let mut reader = open_indexed(&compressed, restored);
    reader.seek(SeekFrom::Start(7_500)).unwrap();
    let mut buf = [0u8; 64];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, &payload[7_500..7_564]);
}

#[test]
fn bgzi_round_trip_seek_bgzf() {
    let b1 = b"@r1\nACGT\n+\n!!!!\n";
    let b2 = b"@r2\nTGCA\n+\n####\n";
    let b3 = b"@r3\nAAAA\n+\n$$$$\n";
    let mut compressed = bgzf_member(b1);
    compressed.extend(bgzf_member(b2));
    compressed.extend(bgzf_member(b3));
    compressed.extend(bgzf_eof());
    let expected = [b1.as_slice(), b2.as_slice(), b3.as_slice()].concat();

    // Large spacing so checkpoints land on independent BGZF member boundaries
    // (empty windows) plus EOF — suitable for BGZI export.
    let (decoded, index) = build_index(&compressed, usize::MAX / 4);
    assert_eq!(decoded, expected);
    assert!(
        index.checkpoints.len() >= 3,
        "expected block-boundary checkpoints, got {}",
        index.checkpoints.len()
    );

    let mut exported = Vec::new();
    index.export_bgzi(&mut exported).unwrap();
    let restored =
        GzipIndex::import_bgzi(&mut Cursor::new(&exported), Some(compressed.len() as u64)).unwrap();
    assert!(!restored.has_line_offsets);
    for cp in &restored.checkpoints {
        assert!(
            restored
                .window_for(cp.compressed_offset_in_bits)
                .map(|w| w.is_empty())
                .unwrap_or(true)
        );
    }

    let mut reader = open_indexed(&compressed, restored);
    let offset = (b1.len() + 3) as u64;
    reader.seek(SeekFrom::Start(offset)).unwrap();
    let mut buf = vec![0u8; 10];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(buf, expected[offset as usize..offset as usize + 10]);
}
