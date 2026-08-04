//! Public-reader throughput benchmark.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rapidgzip_bench::corpus::{
    bgzf, deflate_with_level, fastq_like_bytes, pseudo_random_bytes, stored_gzip_member,
};
use rapidgzip_core::{Decoder, Format, IndexOptions};
use std::io;
use std::num::NonZeroU64;
use std::sync::Arc;

fn deflate_with(bytes: &[u8], window_bits: i32) -> Vec<u8> {
    deflate_with_level(bytes, window_bits, 1).unwrap()
}

fn decode_reader(criterion: &mut Criterion) {
    let decoded = vec![0xA5; 16 * 1024 * 1024];
    let compressed: Arc<[u8]> = stored_gzip_member(&decoded).into();
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

    let decoded = fastq_like_bytes(16 * 1024 * 1024);
    let compressed: Arc<[u8]> = deflate_with_level(&decoded, 31, 6).unwrap().into();
    let decoder = Decoder::builder().decoder_threads(1).build().unwrap();
    let mut group = criterion.benchmark_group("structural_analysis_fastq");
    group.throughput(Throughput::Bytes(decoded.len() as u64));
    group.bench_function("verified_decode", |bencher| {
        bencher.iter(|| decoder.decode(&compressed, &mut io::sink()).unwrap());
    });
    group.bench_function("analyze", |bencher| {
        bencher.iter(|| decoder.analyze(&compressed).unwrap());
    });
    group.finish();

    let decoded = fastq_like_bytes(32 * 1024 * 1024);
    let compressed: Arc<[u8]> = bgzf(&decoded, 4 * 1024, 0).unwrap().into();
    for (name, build_index) in [("count", false), ("count_with_index", true)] {
        let mut group = criterion.benchmark_group(format!("bgzf_line_{name}"));
        group.throughput(Throughput::Bytes(decoded.len() as u64));
        for threads in [1, 4, 16] {
            let decoder = Decoder::builder()
                .decoder_threads(threads)
                .count_lines(true)
                .build()
                .unwrap();
            group.bench_with_input(
                BenchmarkId::from_parameter(threads),
                &threads,
                |bencher, _| {
                    bencher.iter(|| {
                        if build_index {
                            decoder
                                .decode_with_index(
                                    &compressed,
                                    &mut io::sink(),
                                    IndexOptions::default(),
                                )
                                .unwrap()
                                .decode
                        } else {
                            decoder.decode(&compressed, &mut io::sink()).unwrap()
                        }
                    });
                },
            );
        }
        group.finish();
    }

    let decoded = fastq_like_bytes(32 * 1024 * 1024);
    let compressed: Arc<[u8]> = deflate_with_level(&decoded, 31, 6).unwrap().into();
    let index_options = IndexOptions {
        checkpoint_spacing: NonZeroU64::new(1024 * 1024).expect("nonzero"),
        ..IndexOptions::default()
    };
    let index = Decoder::builder()
        .decoder_threads(1)
        .build()
        .unwrap()
        .decode_with_index(&compressed, &mut io::sink(), index_options)
        .unwrap()
        .index;
    assert!(index.checkpoint_count() > 1);
    let mut group = criterion.benchmark_group("indexed_parallel_vs_ordinary");
    group.throughput(Throughput::Bytes(decoded.len() as u64));
    for threads in [1, 2, 4, 8, 16] {
        let decoder = Decoder::builder().decoder_threads(threads).build().unwrap();
        group.bench_with_input(
            BenchmarkId::new("from_index", threads),
            &threads,
            |bencher, _| {
                bencher.iter(|| {
                    decoder
                        .decode_from_index(&compressed, &mut io::sink(), &index)
                        .unwrap()
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("ordinary", threads),
            &threads,
            |bencher, _| {
                bencher.iter(|| decoder.decode(&compressed, &mut io::sink()).unwrap());
            },
        );
    }
    group.finish();

    let decoded = pseudo_random_bytes(16 * 1024 * 1024, 1);
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
        for threads in [1, 2, 3, 4, 16] {
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
