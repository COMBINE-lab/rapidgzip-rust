//! Public-reader throughput benchmark.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use libz_rs_sys as z;
use rapidgzip_core::{Decoder, Format, IndexOptions};
use std::io;
use std::mem::size_of;
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

fn deflate_with(bytes: &[u8], window_bits: i32) -> Vec<u8> {
    let mut stream = z::z_stream::default();
    // SAFETY: `stream` is live and uniquely borrowed, the zlib-rs version
    // string is static and NUL-terminated, and the ABI structure size matches.
    let status = unsafe {
        z::deflateInit2_(
            &mut stream,
            1,
            z::Z_DEFLATED,
            window_bits,
            8,
            z::Z_DEFAULT_STRATEGY,
            z::zlibVersion(),
            size_of::<z::z_stream>() as i32,
        )
    };
    assert_eq!(status, z::Z_OK);
    let mut output = vec![0_u8; bytes.len() + bytes.len() / 16 + 1024];
    stream.next_in = bytes.as_ptr();
    stream.avail_in = u32::try_from(bytes.len()).unwrap();
    stream.next_out = output.as_mut_ptr();
    stream.avail_out = u32::try_from(output.len()).unwrap();
    // SAFETY: input and output point to live, non-overlapping slices matching
    // the counts above; this is the only active call on `stream`.
    let status = unsafe { z::deflate(&mut stream, z::Z_FINISH) };
    assert_eq!(status, z::Z_STREAM_END);
    let produced = output.len() - stream.avail_out as usize;
    // SAFETY: initialization succeeded and the stream is ended exactly once.
    let status = unsafe { z::deflateEnd(&mut stream) };
    assert_eq!(status, z::Z_OK);
    output.truncate(produced);
    output
}

fn pseudo_random_bytes(length: usize) -> Vec<u8> {
    let mut state = 0x243f_6a88_u32;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
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

    let mut group = criterion.benchmark_group("decoder_reader_stored_with_index");
    group.throughput(Throughput::Bytes(decoded.len() as u64));
    for threads in [1, 4, 16, 44] {
        let decoder = Decoder::builder().decoder_threads(threads).build().unwrap();
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |bencher, _| {
                bencher.iter(|| {
                    let mut reader = decoder
                        .reader_with_index(Arc::clone(&compressed), IndexOptions::default())
                        .unwrap();
                    io::copy(&mut reader, &mut io::sink()).unwrap();
                    reader.finish().unwrap()
                });
            },
        );
    }
    group.finish();

    let decoded = pseudo_random_bytes(16 * 1024 * 1024);
    let encoded: Vec<_> = [
        ("gzip", Format::Gzip, 31),
        ("zlib", Format::Zlib, 15),
        ("raw", Format::RawDeflate, -15),
    ]
    .into_iter()
    .map(|(name, format, window_bits)| {
        (
            name,
            format,
            Arc::<[u8]>::from(deflate_with(&decoded, window_bits)),
        )
    })
    .collect();
    let mut group = criterion.benchmark_group("decoder_reader_deflate_formats");
    group.throughput(Throughput::Bytes(decoded.len() as u64));
    for (name, format, compressed) in encoded {
        for threads in [1, 4, 16] {
            let decoder = Decoder::builder()
                .format(format)
                .decoder_threads(threads)
                .build()
                .unwrap();
            group.bench_with_input(BenchmarkId::new(name, threads), &threads, |bencher, _| {
                bencher.iter(|| {
                    let mut reader = decoder.reader(Arc::clone(&compressed)).unwrap();
                    io::copy(&mut reader, &mut io::sink()).unwrap()
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, decode_reader);
criterion_main!(benches);
