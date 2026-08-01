//! Sequential inflate throughput, for comparing the raw-inflate backends.
//!
//! The backend is chosen at compile time, so one run measures one backend.
//! Compare two runs through criterion's baselines, which is why the benchmark
//! identifiers do not mention which backend produced them:
//!
//! ```text
//! cargo bench -p rapidgzip-bench --bench inflate_backend -- --save-baseline zlib-rs
//! ISAL_INSTALL_PREFIX=$(brew --prefix isa-l) \
//!   cargo bench -p rapidgzip-bench --bench inflate_backend \
//!   --features rapidgzip-core/isal -- --baseline zlib-rs
//! ```
//!
//! Both cases run single-threaded, since the pluggable paths are the ones that
//! are not parallel. Of the three, this covers the two reachable from a byte
//! slice: sequential gzip members and a single raw DEFLATE stream. BGZF is
//! pluggable too and is not measured here.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use libz_rs_sys as z;
use rapidgzip_core::{Decoder, Format};
use std::ffi::c_ulong;
use std::hint::black_box;
use std::io;
use std::sync::Arc;

/// Bytes of test corpus, large enough to swamp per-decode setup.
const CORPUS_BYTES: usize = 16 * 1024 * 1024;

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

/// Builds text-like input that compresses about as well as real log data.
///
/// The fields carry pseudo-random values on purpose. Text that repeats
/// verbatim compresses into a few very long matches, which both backends copy
/// at memory speed, hiding the Huffman decoding where they actually differ.
fn corpus() -> Vec<u8> {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut bytes = Vec::with_capacity(CORPUS_BYTES);
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    let mut next = move || {
        // xorshift64, enough entropy for a benchmark corpus and reproducible.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    while bytes.len() < CORPUS_BYTES {
        let value = next();
        bytes.extend_from_slice(b"2026-08-01T12:00:00Z host-");
        for shift in 0..8 {
            bytes.push(ALPHABET[((value >> (shift * 4)) & 0xF) as usize]);
        }
        bytes.extend_from_slice(b" request id=");
        for shift in 8..16 {
            bytes.push(ALPHABET[((value >> (shift * 4)) & 0xF) as usize]);
        }
        bytes.extend_from_slice(format!(" status 200 latency {}ms\n", value % 997).as_bytes());
    }
    bytes.truncate(CORPUS_BYTES);
    bytes
}

/// Compresses `bytes` into a zlib stream at the default level.
fn zlib_compress(bytes: &[u8]) -> Vec<u8> {
    let source_length = c_ulong::try_from(bytes.len()).expect("corpus fits c_ulong");
    let bound = z::compressBound(source_length);
    let mut compressed = vec![0_u8; bound as usize];
    let mut compressed_length = bound;
    // SAFETY: the destination buffer holds `compressed_length` writable bytes,
    // the source holds `source_length` readable bytes, and both stay live and
    // unmoved across the call.
    let status = unsafe {
        z::compress2(
            compressed.as_mut_ptr(),
            &mut compressed_length,
            bytes.as_ptr(),
            source_length,
            6,
        )
    };
    assert_eq!(status, z::Z_OK, "compressing the benchmark corpus failed");
    compressed.truncate(compressed_length as usize);
    compressed
}

/// Strips the zlib header and Adler-32 trailer, leaving raw DEFLATE.
fn raw_deflate(zlib: &[u8]) -> Vec<u8> {
    zlib[2..zlib.len() - 4].to_vec()
}

/// Wraps raw DEFLATE in a single-member gzip container.
fn gzip(deflate: &[u8], decoded: &[u8]) -> Vec<u8> {
    let mut member = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
    member.extend_from_slice(deflate);
    member.extend_from_slice(&crc32(decoded).to_le_bytes());
    member.extend_from_slice(&(decoded.len() as u32).to_le_bytes());
    member
}

fn inflate_backend(criterion: &mut Criterion) {
    let decoded = corpus();
    let zlib = zlib_compress(&decoded);
    let deflate = raw_deflate(&zlib);
    let gzip_member: Arc<[u8]> = gzip(&deflate, &decoded).into();
    let deflate: Arc<[u8]> = deflate.into();

    let mut group = criterion.benchmark_group("inflate_backend");
    group.throughput(Throughput::Bytes(decoded.len() as u64));

    let expected = decoded.len() as u64;
    let sequential = Decoder::builder().decoder_threads(1).build().unwrap();
    group.bench_function("gzip_sequential", |bencher| {
        bencher.iter(|| {
            let mut reader = sequential.reader(Arc::clone(&gzip_member)).unwrap();
            let written = io::copy(&mut reader, &mut io::sink()).unwrap();
            // A benchmark that silently decoded nothing would look fast.
            assert_eq!(written, expected);
            black_box(written)
        });
    });

    let raw = Decoder::builder()
        .decoder_threads(1)
        .format(Format::RawDeflate)
        .build()
        .unwrap();
    group.bench_function("raw_deflate", |bencher| {
        bencher.iter(|| {
            let mut reader = raw.reader(Arc::clone(&deflate)).unwrap();
            let written = io::copy(&mut reader, &mut io::sink()).unwrap();
            assert_eq!(written, expected);
            black_box(written)
        });
    });

    group.finish();
}

criterion_group!(benches, inflate_backend);
criterion_main!(benches);
