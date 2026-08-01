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
