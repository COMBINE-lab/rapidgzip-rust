//! Deterministic benchmark-corpus construction.
//!
//! These helpers deliberately fix container headers and use a small local
//! pseudo-random generator. Equal arguments therefore produce byte-identical
//! streams across invocations on the same supported zlib-rs version.

use libz_rs_sys as z;
use std::mem::size_of;

/// Maximum uncompressed payload used in one BGZF block.
pub const BGZF_PAYLOAD_BYTES: usize = 60 * 1024;

/// Canonical empty BGZF terminator emitted by htslib-compatible tools.
pub const BGZF_EOF: [u8; 28] = [
    31, 139, 8, 4, 0, 0, 0, 0, 0, 255, 6, 0, 66, 67, 2, 0, 27, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

/// Computes the RFC 1952 CRC-32 used by deterministic fixture containers.
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut value = u32::MAX;
    for &byte in bytes {
        value ^= u32::from(byte);
        for _ in 0..8 {
            value = (value >> 1) ^ (0xEDB8_8320 & 0_u32.wrapping_sub(value & 1));
        }
    }
    !value
}

/// Produces repeatable FASTQ-shaped bytes of exactly `length` bytes.
pub fn fastq_like_bytes(length: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(length.saturating_add(256));
    let mut record = 0_u64;
    while output.len() < length {
        output.extend_from_slice(format!("@read-{record}\n").as_bytes());
        output.extend_from_slice(b"ACGTGCTAGCTAGGATCCGATCGATCGTAGCTAGCTAGCTACGATCGATCG\n+\n");
        output.extend_from_slice(b"IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\n");
        record += 1;
    }
    output.truncate(length);
    output
}

/// Produces repeatable pseudo-random bytes from a non-cryptographic seed.
pub fn pseudo_random_bytes(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x243f_6a88_85a3_08d3;
    if state == 0 {
        state = 1;
    }
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

/// Compresses one buffer using zlib-rs with explicit framing and level.
///
/// `window_bits` uses zlib's `deflateInit2` convention: `15` is zlib framing,
/// `-15` is raw DEFLATE, and `31` is gzip framing.
pub fn deflate_with_level(bytes: &[u8], window_bits: i32, level: i32) -> Result<Vec<u8>, String> {
    let input_length = u32::try_from(bytes.len())
        .map_err(|_| "one fixture compression call is limited to 4 GiB".to_owned())?;
    let output_length = bytes
        .len()
        .checked_add(bytes.len() / 16)
        .and_then(|length| length.checked_add(1024))
        .ok_or_else(|| "fixture output allocation overflow".to_owned())?;
    let output_avail = u32::try_from(output_length)
        .map_err(|_| "one fixture output buffer is limited to 4 GiB".to_owned())?;
    let mut output = vec![0_u8; output_length];
    let mut stream = z::z_stream::default();
    // SAFETY: `stream` is live and uniquely borrowed, `zlibVersion` returns a
    // static NUL-terminated string, and the declared structure size is exact.
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
    if status != z::Z_OK {
        return Err(format!("deflateInit2 failed with zlib status {status}"));
    }

    stream.next_in = bytes.as_ptr();
    stream.avail_in = input_length;
    stream.next_out = output.as_mut_ptr();
    stream.avail_out = output_avail;
    // SAFETY: input and output point to live, non-overlapping allocations for
    // the declared lengths, and no other call can access this local stream.
    let deflate_status = unsafe { z::deflate(&mut stream, z::Z_FINISH) };
    let produced = output.len() - stream.avail_out as usize;
    // SAFETY: initialization succeeded above and this local stream is ended
    // exactly once, after its sole compression call.
    let end_status = unsafe { z::deflateEnd(&mut stream) };
    if deflate_status != z::Z_STREAM_END {
        return Err(format!(
            "deflate did not finish (zlib status {deflate_status})"
        ));
    }
    if end_status != z::Z_OK {
        return Err(format!("deflateEnd failed with zlib status {end_status}"));
    }
    output.truncate(produced);
    Ok(output)
}

/// Creates one gzip member with fixed header fields and raw DEFLATE payload.
pub fn gzip_member(bytes: &[u8], level: i32) -> Result<Vec<u8>, String> {
    let deflate = deflate_with_level(bytes, -15, level)?;
    let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
    encoded.extend_from_slice(&deflate);
    encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    Ok(encoded)
}

/// Creates concatenated ordinary gzip members of a fixed maximum decoded size.
pub fn gzip_members(
    bytes: &[u8],
    member_payload_bytes: usize,
    level: i32,
) -> Result<Vec<u8>, String> {
    if member_payload_bytes == 0 {
        return Err("gzip member payload must be nonzero".to_owned());
    }
    if bytes.is_empty() {
        return gzip_member(bytes, level);
    }
    let mut encoded = Vec::new();
    for chunk in bytes.chunks(member_payload_bytes) {
        encoded.extend_from_slice(&gzip_member(chunk, level)?);
    }
    Ok(encoded)
}

/// Creates a deterministic gzip member containing only stored DEFLATE blocks.
pub fn stored_gzip_member(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
    if bytes.is_empty() {
        encoded.extend_from_slice(&[1, 0, 0, 255, 255]);
    } else {
        let chunk_count = bytes.len().div_ceil(u16::MAX as usize);
        for (index, chunk) in bytes.chunks(u16::MAX as usize).enumerate() {
            encoded.push(u8::from(index + 1 == chunk_count));
            let length = chunk.len() as u16;
            encoded.extend_from_slice(&length.to_le_bytes());
            encoded.extend_from_slice(&(!length).to_le_bytes());
            encoded.extend_from_slice(chunk);
        }
    }
    encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    encoded
}

/// Creates true BGZF framing with a `BC`/`BSIZE` field on every data block.
///
/// A canonical empty terminator is appended and therefore counts as a gzip
/// member even though it emits no decoded bytes.
pub fn bgzf(bytes: &[u8], block_payload_bytes: usize, level: i32) -> Result<Vec<u8>, String> {
    if block_payload_bytes == 0 || block_payload_bytes > BGZF_PAYLOAD_BYTES {
        return Err(format!(
            "BGZF payload must be in 1..={BGZF_PAYLOAD_BYTES} bytes"
        ));
    }
    let mut encoded = Vec::new();
    for chunk in bytes.chunks(block_payload_bytes) {
        let deflate = deflate_with_level(chunk, -15, level)?;
        let total = 18_usize
            .checked_add(deflate.len())
            .and_then(|length| length.checked_add(8))
            .ok_or_else(|| "BGZF block length overflow".to_owned())?;
        if total > 65_536 {
            return Err(format!("compressed BGZF block is {total} bytes"));
        }
        let mut block = b"\x1f\x8b\x08\x04\0\0\0\0\x00\xff\x06\x00BC\x02\x00".to_vec();
        block.extend_from_slice(&((total - 1) as u16).to_le_bytes());
        block.extend_from_slice(&deflate);
        block.extend_from_slice(&crc32(chunk).to_le_bytes());
        block.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
        debug_assert_eq!(block.len(), total);
        encoded.extend_from_slice(&block);
    }
    encoded.extend_from_slice(&BGZF_EOF);
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payloads_are_exact_and_repeatable() {
        assert_eq!(fastq_like_bytes(10_000), fastq_like_bytes(10_000));
        assert_eq!(fastq_like_bytes(10_000).len(), 10_000);
        assert_eq!(
            pseudo_random_bytes(10_000, 7),
            pseudo_random_bytes(10_000, 7)
        );
        assert_ne!(pseudo_random_bytes(64, 7), pseudo_random_bytes(64, 8));
    }

    #[test]
    fn known_crc_is_correct() {
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
    }

    #[test]
    fn bgzf_blocks_have_matching_sizes_and_eof() {
        let encoded = bgzf(&pseudo_random_bytes(200_000, 1), 60_000, 6).unwrap();
        let mut offset = 0;
        let mut blocks = 0;
        while offset < encoded.len() {
            assert_eq!(&encoded[offset..offset + 2], b"\x1f\x8b");
            assert_eq!(&encoded[offset + 12..offset + 16], b"BC\x02\x00");
            let bsize = u16::from_le_bytes([encoded[offset + 16], encoded[offset + 17]]);
            offset += usize::from(bsize) + 1;
            blocks += 1;
        }
        assert_eq!(offset, encoded.len());
        assert_eq!(blocks, 5);
        assert!(encoded.ends_with(&BGZF_EOF));
    }

    #[test]
    fn empty_stored_member_has_a_final_block() {
        let encoded = stored_gzip_member(&[]);
        assert_eq!(&encoded[10..15], &[1, 0, 0, 255, 255]);
    }
}
