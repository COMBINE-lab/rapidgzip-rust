//! Public-reader throughput benchmark.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rapidgzip_core::Decoder;
use std::io;
use std::sync::Arc;

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

fn stored_member(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
    let chunks = bytes.chunks(u16::MAX as usize);
    let count = chunks.len();
    for (index, chunk) in chunks.enumerate() {
        encoded.push(u8::from(index + 1 == count));
        let length = chunk.len() as u16;
        encoded.extend_from_slice(&length.to_le_bytes());
        encoded.extend_from_slice(&(!length).to_le_bytes());
        encoded.extend_from_slice(chunk);
    }
    encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    encoded
}

fn decode_reader(criterion: &mut Criterion) {
    let decoded = vec![0xA5; 16 * 1024 * 1024];
    let compressed: Arc<[u8]> = stored_member(&decoded).into();
    let mut group = criterion.benchmark_group("decoder_reader_stored");
    group.throughput(Throughput::Bytes(decoded.len() as u64));
    for threads in [1, 4, 16, 44] {
        let decoder = Decoder::builder().decoder_threads(threads).build().unwrap();
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |bencher, _| {
                bencher.iter(|| {
                    let mut reader = decoder.reader(Arc::clone(&compressed)).unwrap();
                    io::copy(&mut reader, &mut io::sink()).unwrap()
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, decode_reader);
criterion_main!(benches);
