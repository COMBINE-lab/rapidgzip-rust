//! On-disk index format round-trips and rejection tests.

use rapidgzip_core::index::WINDOW_SIZE;
use rapidgzip_core::{
    Checkpoint, CheckpointKind, GzipIndex, IndexError, IndexKind, IndexReadOptions, StoredWindow,
};

/// An index with a member-boundary point and an interior point whose
/// compressed offset is deliberately not byte aligned.
fn sample_index() -> GzipIndex {
    let mut index = GzipIndex::new();
    index.set_compressed_size(Some(1_000_000));
    index.set_uncompressed_size(Some(8_000_000));
    index.set_checkpoint_spacing(Some(4 * 1024 * 1024));
    index.set_total_line_count(Some(1234));
    index
        .push(
            Checkpoint {
                compressed_offset_in_bits: 80,
                uncompressed_offset_in_bytes: 0,
                kind: CheckpointKind::MemberDeflate {
                    header_offset_in_bytes: 0,
                },
                line_offset: Some(0),
            },
            StoredWindow::empty(),
        )
        .expect("first checkpoint");
    index
        .push(
            Checkpoint {
                compressed_offset_in_bits: 8 * 4096 + 3,
                uncompressed_offset_in_bytes: 4 * 1024 * 1024,
                kind: CheckpointKind::DeflateBlock,
                line_offset: Some(1234),
            },
            StoredWindow::from_raw(vec![0xa5u8; WINDOW_SIZE]).expect("window"),
        )
        .expect("interior checkpoint");
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
    assert_eq!(left.compressed_size(), right.compressed_size());
    assert_eq!(left.uncompressed_size(), right.uncompressed_size());
    assert_same_windows(left, right);
}

#[test]
fn native_round_trips() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    let restored = GzipIndex::read_native(&mut bytes.as_slice()).expect("read");
    assert_same_index(&index, &restored);
    assert_eq!(restored.checkpoint_spacing(), index.checkpoint_spacing());
}

#[test]
fn native_round_trips_compressed_windows() {
    let mut index = GzipIndex::new();
    index.set_compressed_size(Some(4096));
    index.set_uncompressed_size(Some(1 << 20));
    index
        .push(
            Checkpoint {
                compressed_offset_in_bits: 0,
                uncompressed_offset_in_bytes: 0,
                kind: CheckpointKind::MemberHeader,
                line_offset: None,
            },
            StoredWindow::empty(),
        )
        .expect("origin");
    index
        .push(
            Checkpoint {
                compressed_offset_in_bits: 1000,
                uncompressed_offset_in_bytes: 65536,
                kind: CheckpointKind::DeflateBlock,
                line_offset: None,
            },
            StoredWindow::from_raw_maybe_compress(vec![0x3cu8; WINDOW_SIZE], true)
                .expect("compress"),
        )
        .expect("interior checkpoint");

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
fn native_rejects_unknown_flags_and_caller_bounded_counts() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");

    let mut unknown_flags = bytes.clone();
    unknown_flags[10..12].copy_from_slice(&0x8000_u16.to_le_bytes());
    assert!(matches!(
        GzipIndex::read_native(&mut unknown_flags.as_slice()),
        Err(IndexError::UnsupportedFlags { flags: 0x8000 })
    ));

    let options = IndexReadOptions {
        max_checkpoints: 1,
        ..IndexReadOptions::default()
    };
    assert!(matches!(
        GzipIndex::read_native_with_options(&mut bytes.as_slice(), options),
        Err(IndexError::ExcessiveLength {
            what: "checkpoint count",
            value: 2
        })
    ));
}

#[test]
fn native_applies_the_per_window_payload_limit_before_allocation() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_native(&mut bytes).expect("write");
    let options = IndexReadOptions {
        max_window_payload_bytes: 1024,
        ..IndexReadOptions::default()
    };
    assert!(matches!(
        GzipIndex::read_native_with_options(&mut bytes.as_slice(), options),
        Err(IndexError::ExcessiveLength {
            what: "window payload length",
            ..
        })
    ));
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
            .all(|point| point.line_offset.is_none())
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
fn gzidx_rejects_unknown_header_and_checkpoint_flags() {
    let index = sample_index();
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("write");

    let mut header_flags = bytes.clone();
    header_flags[6] = 1;
    assert!(matches!(
        GzipIndex::read_gzidx(&mut header_flags.as_slice(), None),
        Err(IndexError::UnsupportedFlags { flags: 1 })
    ));

    let mut point_flags = bytes;
    // Header is 35 bytes; the first version-1 point's data flag is byte 52.
    point_flags[52] = 2;
    assert_eq!(
        GzipIndex::read_gzidx(&mut point_flags.as_slice(), None).unwrap_err(),
        IndexError::InvalidCheckpoint("GZIDX window flag is not zero or one")
    );
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

/// Three independent BGZF-style block starts, none needing a window.
fn bgzf_style_index() -> GzipIndex {
    let mut index = GzipIndex::new();
    index.set_kind(IndexKind::Bgzf);
    index.set_compressed_size(Some(300));
    index.set_uncompressed_size(Some(3000));
    for block in 0..3u64 {
        index
            .push(
                Checkpoint {
                    compressed_offset_in_bits: block * 8 * 100,
                    uncompressed_offset_in_bytes: block * 1000,
                    kind: CheckpointKind::MemberHeader,
                    line_offset: None,
                },
                StoredWindow::empty(),
            )
            .expect("BGZF checkpoint");
    }
    index
}

#[test]
fn gzi_round_trips_block_starts_and_skips_the_origin() {
    let index = bgzf_style_index();
    let mut bytes = Vec::new();
    index.write_gzi(&mut bytes).expect("write");

    assert_eq!(bytes.len(), 8 + 2 * 16);
    assert_eq!(u64::from_le_bytes(bytes[..8].try_into().unwrap()), 2);

    let restored = GzipIndex::read_gzi(&mut bytes.as_slice(), Some(300)).expect("read");
    assert_eq!(restored.checkpoints(), index.checkpoints());
    assert_eq!(restored.uncompressed_size(), None);
}

#[test]
fn gzi_round_trips_an_empty_index() {
    let mut index = GzipIndex::new();
    index.set_kind(IndexKind::Bgzf);
    let mut bytes = Vec::new();
    index.write_gzi(&mut bytes).expect("write");
    assert_eq!(bytes.len(), 8);

    let restored = GzipIndex::read_gzi(&mut bytes.as_slice(), None).expect("read");
    assert_eq!(restored.checkpoint_count(), 1);
}

#[test]
fn gzi_refuses_interior_deflate_checkpoints() {
    let mut index = bgzf_style_index();
    index
        .push(
            Checkpoint {
                compressed_offset_in_bits: 8 * 250,
                uncompressed_offset_in_bytes: 2500,
                kind: CheckpointKind::DeflateBlock,
                line_offset: None,
            },
            StoredWindow::from_raw(vec![3u8; WINDOW_SIZE]).expect("window"),
        )
        .expect("interior point");
    let mut bytes = Vec::new();
    assert_eq!(
        index.write_gzi(&mut bytes).unwrap_err(),
        IndexError::InvalidCheckpoint("BGZF index requires member-boundary checkpoints")
    );
}

#[test]
fn gzi_refuses_unaligned_offsets() {
    let mut index = GzipIndex::new();
    index.set_kind(IndexKind::Bgzf);
    index.set_compressed_size(Some(300));
    index
        .push(
            Checkpoint {
                compressed_offset_in_bits: 0,
                uncompressed_offset_in_bytes: 0,
                kind: CheckpointKind::MemberHeader,
                line_offset: None,
            },
            StoredWindow::empty(),
        )
        .expect("origin");
    let error = index
        .push(
            Checkpoint {
                compressed_offset_in_bits: 8 * 100 + 1,
                uncompressed_offset_in_bytes: 1000,
                kind: CheckpointKind::MemberHeader,
                line_offset: None,
            },
            StoredWindow::empty(),
        )
        .expect_err("unaligned member checkpoint");
    assert_eq!(
        error,
        IndexError::InvalidCheckpoint("member-header checkpoint is not byte aligned")
    );
}

#[test]
fn gzi_rejects_a_hostile_pair_count() {
    let mut bytes = u64::MAX.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[0u8; 16]);
    assert!(matches!(
        GzipIndex::read_gzi(&mut bytes.as_slice(), None),
        Err(IndexError::ExcessiveLength { .. })
    ));
}

#[test]
fn gzi_rejects_truncation_at_every_prefix() {
    let index = bgzf_style_index();
    let mut bytes = Vec::new();
    index.write_gzi(&mut bytes).expect("write");
    for length in 1..bytes.len() {
        assert!(
            GzipIndex::read_gzi(&mut &bytes[..length], None).is_err(),
            "prefix of {length} bytes was accepted"
        );
    }
}

#[test]
fn gztool_round_trips_without_lines() {
    use rapidgzip_core::index::WithLines;

    let index = sample_index();
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::No)
        .expect("write");
    assert_eq!(&bytes[..8], &[0u8; 8]);
    assert_eq!(&bytes[8..16], b"gzipindx");

    let restored = GzipIndex::read_gztool(&mut bytes.as_slice(), Some(1_000_000)).expect("read");
    assert_eq!(restored.total_line_count(), None);
    assert!(
        restored
            .checkpoints()
            .iter()
            .all(|point| point.line_offset.is_none())
    );
    assert_same_windows(&index, &restored);
    assert_eq!(restored.uncompressed_size(), index.uncompressed_size());
}

#[test]
fn gztool_round_trips_with_lines() {
    use rapidgzip_core::index::WithLines;

    let index = sample_index();
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::Yes)
        .expect("write");
    assert_eq!(&bytes[8..16], b"gzipindX");

    let restored = GzipIndex::read_gztool(&mut bytes.as_slice(), Some(1_000_000)).expect("read");
    let restored_fields: Vec<_> = restored
        .checkpoints()
        .iter()
        .map(|point| {
            (
                point.compressed_offset_in_bits,
                point.uncompressed_offset_in_bytes,
                point.line_offset,
            )
        })
        .collect();
    let expected_fields: Vec<_> = index
        .checkpoints()
        .iter()
        .map(|point| {
            (
                point.compressed_offset_in_bits,
                point.uncompressed_offset_in_bytes,
                point.line_offset,
            )
        })
        .collect();
    assert_eq!(restored_fields, expected_fields);
    assert_eq!(restored.total_line_count(), Some(1234));
    assert_same_windows(&index, &restored);
}

#[test]
fn gztool_rejects_an_incomplete_index() {
    use rapidgzip_core::index::WithLines;

    let index = sample_index();
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::No)
        .expect("write");
    // `size` follows `have` after the 16-byte header.
    bytes[24..32].copy_from_slice(&99u64.to_be_bytes());
    assert_eq!(
        GzipIndex::read_gztool(&mut bytes.as_slice(), None).unwrap_err(),
        IndexError::InvalidCheckpoint("gztool index is incomplete")
    );
}

#[test]
fn gztool_rejects_an_excessive_window_length() {
    use rapidgzip_core::index::WithLines;

    let index = sample_index();
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::No)
        .expect("write");
    // First point: header(16) + have(8) + size(8) + out(8) + in(8) + bits(4).
    let length_at = 16 + 8 + 8 + 8 + 8 + 4;
    bytes[length_at..length_at + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        GzipIndex::read_gztool(&mut bytes.as_slice(), None),
        Err(IndexError::ExcessiveLength { .. })
    ));
}

#[test]
fn gztool_rejects_bad_magic() {
    let bytes = vec![0u8; 64];
    assert!(matches!(
        GzipIndex::read_gztool(&mut bytes.as_slice(), None),
        Err(IndexError::BadMagic { .. })
    ));
}

#[test]
fn gztool_rejects_truncation_at_every_prefix() {
    use rapidgzip_core::index::WithLines;

    let index = sample_index();
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::Yes)
        .expect("write");
    for length in 1..bytes.len() {
        assert!(
            GzipIndex::read_gztool(&mut &bytes[..length], None).is_err(),
            "prefix of {length} bytes was accepted"
        );
    }
}
