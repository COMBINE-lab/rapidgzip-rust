//! On-disk index format round-trips and rejection tests.

use rapidgzip_core::index::WINDOW_SIZE;
use rapidgzip_core::{Checkpoint, GzipIndex, IndexError, StoredWindow};

/// An index with a member-boundary point and an interior point whose
/// compressed offset is deliberately not byte aligned.
fn sample_index() -> GzipIndex {
    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = 1_000_000;
    index.uncompressed_size_in_bytes = 8_000_000;
    index.checkpoint_spacing_in_bytes = 4 * 1024 * 1024;
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 0,
            uncompressed_offset_in_bytes: 0,
            line_offset: 0,
        },
        StoredWindow::empty(),
    );
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 8 * 4096 + 3,
            uncompressed_offset_in_bytes: 4 * 1024 * 1024,
            line_offset: 1234,
        },
        StoredWindow::from_raw(vec![0xa5u8; WINDOW_SIZE]),
    );
    index
}

fn assert_same_windows(left: &GzipIndex, right: &GzipIndex) {
    for checkpoint in left.checkpoints() {
        let key = checkpoint.compressed_offset_in_bits;
        let expected = left.windows().get(key).map(|window| {
            window
                .decompressed()
                .expect("expand expected window")
                .into_owned()
        });
        let actual = right.windows().get(key).map(|window| {
            window
                .decompressed()
                .expect("expand actual window")
                .into_owned()
        });
        assert_eq!(expected, actual, "window mismatch at bit offset {key}");
    }
}

fn assert_same_index(left: &GzipIndex, right: &GzipIndex) {
    assert_eq!(left.checkpoints(), right.checkpoints());
    assert_eq!(
        left.compressed_size_in_bytes,
        right.compressed_size_in_bytes
    );
    assert_eq!(
        left.uncompressed_size_in_bytes,
        right.uncompressed_size_in_bytes
    );
    assert_same_windows(left, right);
}

#[test]
fn native_round_trips() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    let restored = GzipIndex::read_native(&mut bytes.as_slice()).expect("read");
    assert_same_index(&index, &restored);
    assert_eq!(
        restored.checkpoint_spacing_in_bytes,
        index.checkpoint_spacing_in_bytes
    );
}

#[test]
fn native_round_trips_compressed_windows() {
    let mut index = GzipIndex::new();
    index.compressed_size_in_bytes = 4096;
    index.uncompressed_size_in_bytes = 1 << 20;
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 0,
            uncompressed_offset_in_bytes: 0,
            line_offset: 0,
        },
        StoredWindow::empty(),
    );
    index.push(
        Checkpoint {
            compressed_offset_in_bits: 1000,
            uncompressed_offset_in_bytes: 65536,
            line_offset: 0,
        },
        StoredWindow::from_raw_maybe_compress(vec![0x3cu8; WINDOW_SIZE], true).expect("compress"),
    );

    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    let restored = GzipIndex::read_native(&mut bytes.as_slice()).expect("read");
    assert!(
        restored
            .windows()
            .get(1000)
            .expect("window")
            .is_compressed()
    );
    assert_same_index(&index, &restored);
}

#[test]
fn native_round_trips_an_empty_index() {
    let index = GzipIndex::new();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    let restored = GzipIndex::read_native(&mut bytes.as_slice()).expect("read");
    assert_eq!(restored.checkpoint_count(), 0);
}

#[test]
fn native_rejects_bad_magic() {
    let bytes = vec![0u8; 64];
    assert!(matches!(
        GzipIndex::read_native(&mut bytes.as_slice()),
        Err(IndexError::BadMagic { .. })
    ));
}

#[test]
fn native_rejects_truncation_at_every_prefix() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    for length in 1..bytes.len() {
        assert!(
            GzipIndex::read_native(&mut &bytes[..length]).is_err(),
            "prefix of {length} bytes was accepted as a complete index"
        );
    }
}

#[test]
fn native_rejects_a_hostile_checkpoint_count() {
    let index = GzipIndex::new();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    let count_at = bytes.len() - 8;
    bytes[count_at..].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(matches!(
        GzipIndex::read_native(&mut bytes.as_slice()),
        Err(IndexError::ExcessiveLength { .. })
    ));
}

#[test]
fn native_rejects_an_unknown_version() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    bytes[8..10].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        GzipIndex::read_native(&mut bytes.as_slice()).unwrap_err(),
        IndexError::UnsupportedVersion(2)
    );
}

#[test]
fn zran_bit_packing_round_trips() {
    use rapidgzip_core::index::{decode_bit_offset, encode_bit_offset};

    for offset in [0u64, 1, 7, 8, 9, 4095, 4096, 8 * 4096 + 3] {
        let (byte_offset, bits) = encode_bit_offset(offset);
        assert_eq!(decode_bit_offset(byte_offset, bits), Ok(offset));
    }

    // Byte-aligned offsets store a zero bits field.
    assert_eq!(encode_bit_offset(64), (8, 0));
    // Three bits into byte 4096 stores the next byte with five bits remaining.
    assert_eq!(encode_bit_offset(8 * 4096 + 3), (4097, 5));
}

#[test]
fn zran_bit_packing_rejects_denormal_values() {
    use rapidgzip_core::index::decode_bit_offset;

    assert!(decode_bit_offset(0, 3).is_err());
    assert!(decode_bit_offset(10, 8).is_err());
}

#[test]
fn gzidx_round_trips() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");
    let restored = GzipIndex::read_gzidx(&mut bytes.as_slice(), Some(1_000_000)).expect("read");

    // GZIDX has no line counters, so only the two offsets survive.
    let expected: Vec<_> = index
        .checkpoints()
        .iter()
        .map(|point| {
            (
                point.compressed_offset_in_bits,
                point.uncompressed_offset_in_bytes,
            )
        })
        .collect();
    let actual: Vec<_> = restored
        .checkpoints()
        .iter()
        .map(|point| {
            (
                point.compressed_offset_in_bits,
                point.uncompressed_offset_in_bytes,
            )
        })
        .collect();
    assert_eq!(actual, expected);
    assert!(
        restored
            .checkpoints()
            .iter()
            .all(|point| point.line_offset == 0)
    );
    assert_same_windows(&index, &restored);
}

#[test]
fn gzidx_rejects_a_mismatched_archive_size() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");
    assert_eq!(
        GzipIndex::read_gzidx(&mut bytes.as_slice(), Some(42)).unwrap_err(),
        IndexError::ArchiveSizeMismatch {
            index_size: 1_000_000,
            archive_size: 42,
        }
    );
}

#[test]
fn gzidx_rejects_a_foreign_window_size() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");
    // magic(5) version(1) flags(1) compressed(8) uncompressed(8) spacing(4)
    // puts the window size at offset 27.
    bytes[27..31].copy_from_slice(&4096u32.to_le_bytes());
    assert_eq!(
        GzipIndex::read_gzidx(&mut bytes.as_slice(), None).unwrap_err(),
        IndexError::InvalidWindowSize(4096)
    );
}

#[test]
fn gzidx_rejects_a_hostile_checkpoint_count() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");
    // The checkpoint count follows the window size at offset 31.
    bytes[31..35].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        GzipIndex::read_gzidx(&mut bytes.as_slice(), None),
        Err(IndexError::ExcessiveLength { .. })
    ));
}

#[test]
fn gzidx_rejects_truncation_at_every_prefix() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");
    for length in 1..bytes.len() {
        assert!(
            GzipIndex::read_gzidx(&mut &bytes[..length], None).is_err(),
            "prefix of {length} bytes was accepted"
        );
    }
}
