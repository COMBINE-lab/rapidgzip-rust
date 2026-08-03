//! Peak-memory and elapsed-time diagnostic for dense BGZF line indexes.

use rapidgzip_core::{Decoder, IndexOptions};
use std::io;
use std::sync::OnceLock;
use std::time::Instant;

fn crc32(bytes: &[u8]) -> u32 {
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut table = [0_u32; 256];
        for (value, entry) in table.iter_mut().enumerate() {
            let mut crc = value as u32;
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
            }
            *entry = crc;
        }
        table
    });
    let mut value = u32::MAX;
    for &byte in bytes {
        value = table[((value as u8) ^ byte) as usize] ^ (value >> 8);
    }
    !value
}

fn bgzf_fixture(decoded_bytes: usize, block_payload: usize) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(decoded_bytes + decoded_bytes / block_payload * 31);
    let mut decoded_offset = 0_usize;
    while decoded_offset < decoded_bytes {
        let length = block_payload.min(decoded_bytes - decoded_offset);
        let mut chunk = vec![b'A'; length];
        for (position, byte) in chunk.iter_mut().enumerate() {
            if (decoded_offset + position + 1).is_multiple_of(80) {
                *byte = b'\n';
            }
        }
        let mut deflate = Vec::with_capacity(length + 5);
        deflate.push(1);
        let length = length as u16;
        deflate.extend_from_slice(&length.to_le_bytes());
        deflate.extend_from_slice(&(!length).to_le_bytes());
        deflate.extend_from_slice(&chunk);
        let total = 18 + deflate.len() + 8;
        let mut block = b"\x1f\x8b\x08\x04\0\0\0\0\x00\xff\x06\x00BC\x02\x00".to_vec();
        block.extend_from_slice(&((total - 1) as u16).to_le_bytes());
        block.extend_from_slice(&deflate);
        block.extend_from_slice(&crc32(&chunk).to_le_bytes());
        block.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&block);
        decoded_offset += chunk.len();
    }
    encoded.extend_from_slice(&[
        31, 139, 8, 4, 0, 0, 0, 0, 0, 255, 6, 0, 66, 67, 2, 0, 27, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]);
    encoded
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = std::env::args().collect();
    let mode = arguments.get(1).map_or("count", String::as_str);
    if !matches!(mode, "count" | "index") {
        return Err("usage: line_index [count|index] [threads] [decoded-MiB]".into());
    }
    let threads = arguments
        .get(2)
        .map_or(Ok(8), |value| value.parse::<usize>())?;
    let decoded_mib = arguments
        .get(3)
        .map_or(Ok(128), |value| value.parse::<usize>())?;
    let decoded_bytes = decoded_mib
        .checked_mul(1024 * 1024)
        .ok_or("decoded fixture size overflow")?;
    let compressed = bgzf_fixture(decoded_bytes, 4 * 1024);
    let decoder = Decoder::builder()
        .decoder_threads(threads)
        .count_lines(true)
        .build()?;
    let started = Instant::now();
    let (report, checkpoints) = if mode == "index" {
        let indexed = decoder.decode_with_index(
            compressed.as_slice(),
            &mut io::sink(),
            IndexOptions::default(),
        )?;
        (indexed.decode, indexed.index.checkpoint_count())
    } else {
        (decoder.decode(compressed.as_slice(), &mut io::sink())?, 0)
    };
    println!(
        "mode={mode} threads={threads} decoded={} compressed={} lines={} checkpoints={checkpoints} elapsed_seconds={:.6}",
        report.decompressed_bytes,
        report.compressed_bytes,
        report.line_count.unwrap_or(0),
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}
