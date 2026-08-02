//! Shared fixtures for the index and seek tests.
//!
//! Compression goes through the `libz-rs-sys` deflate ABI, which the crate
//! already links, so the tests need no additional dependency.

use libz_rs_sys as z;

/// Compresses `bytes` into a single gzip member at `level`.
pub fn gzip(bytes: &[u8], level: i32) -> Vec<u8> {
    // `31` selects the gzip wrapper over a 32 KiB window.
    deflate_with(bytes, level, 31)
}

/// Builds a deterministic, moderately compressible corpus of `size` bytes.
pub fn corpus(size: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(size + 64);
    let mut state = 0x243f_6a88u32;
    while bytes.len() < size {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let line = format!("record {} value {}\n", bytes.len(), state >> 16);
        bytes.extend_from_slice(line.as_bytes());
    }
    bytes.truncate(size);
    bytes
}

fn deflate_with(bytes: &[u8], level: i32, window_bits: i32) -> Vec<u8> {
    let mut stream = z::z_stream::default();
    // SAFETY: `stream` is live and uniquely borrowed, `zlibVersion` returns a
    // static NUL-terminated string, and the reported size matches the exact
    // Rust ABI type passed.
    let status = unsafe {
        z::deflateInit2_(
            &mut stream,
            level,
            z::Z_DEFLATED,
            window_bits,
            8,
            z::Z_DEFAULT_STRATEGY,
            z::zlibVersion(),
            size_of::<z::z_stream>() as i32,
        )
    };
    assert_eq!(status, z::Z_OK, "deflateInit2_ failed");

    let mut output = vec![0u8; bytes.len() + bytes.len() / 2 + 1024];
    stream.next_in = bytes.as_ptr();
    stream.avail_in = u32::try_from(bytes.len()).expect("fixture fits in a u32");
    stream.next_out = output.as_mut_ptr();
    stream.avail_out = u32::try_from(output.len()).expect("fixture fits in a u32");

    // SAFETY: the pointers and counts above describe the live `bytes` and
    // `output` slices for the duration of the call.
    let status = unsafe { z::deflate(&mut stream, z::Z_FINISH) };
    assert_eq!(
        status,
        z::Z_STREAM_END,
        "deflate did not finish in one pass"
    );
    let produced = output.len() - stream.avail_out as usize;
    // SAFETY: the stream was initialized above and is ended exactly once.
    unsafe { z::deflateEnd(&mut stream) };
    output.truncate(produced);
    output
}

/// Compresses `bytes` into BGZF blocks and appends the standard empty EOF
/// block.
pub fn bgzf(bytes: &[u8], block_payload: usize) -> Vec<u8> {
    let mut encoded = Vec::new();
    for chunk in bytes.chunks(block_payload) {
        let deflate = deflate_with(chunk, 6, -15);
        let total = 18 + deflate.len() + 8;
        assert!(total <= u16::MAX as usize + 1, "BGZF block does not fit");
        let mut block = b"\x1f\x8b\x08\x04\0\0\0\0\x00\xff\x06\x00BC\x02\x00".to_vec();
        block.extend_from_slice(&((total - 1) as u16).to_le_bytes());
        block.extend_from_slice(&deflate);
        block.extend_from_slice(&crc32(chunk).to_le_bytes());
        block.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&block);
    }
    encoded.extend_from_slice(&[
        31, 139, 8, 4, 0, 0, 0, 0, 0, 255, 6, 0, 66, 67, 2, 0, 27, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]);
    encoded
}

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
