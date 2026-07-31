# Benchmarking

Build benchmark binaries with the same locked dependency graph:

```bash
cargo build --locked --release --workspace
cargo bench --locked -p rapidgzip-bench
```

The Criterion benchmark drains the public `DecoderReader`, so channel
handoff, ordered assembly, verification, and the unavoidable `Read` copy are
included.

For corpus and competitor runs, build the Rust release binary and use the
matrix driver:

```bash
cargo build --locked --release
RUNS=9 \
DECODED_BYTES=268435456 \
RAPIDGZIP_CPP=/path/to/rapidgzip \
GZIPPY=/path/to/gzippy \
benchmarks/run-matrix.sh corpus.fastq.gz > results.tsv
```

The driver runs 1, 4, 16, and 44 threads by default, records wall/user/system
time and peak RSS, and includes gzippy whenever its executable is available.
Set `THREAD_CELLS` or `RUNS` to override the matrix. The Rust CLI uses the push
API in `--test` mode; Criterion separately measures the paraseq-facing pull
API.

For release comparisons, record:

- CPU model, NUMA topology, physical-core pinning, governor, and microcode.
- Rust, C++, zlib-rs, rapidgzip, gzippy, and corpus revisions.
- Two warmups followed by nine measured runs.
- Median decoded bytes/s and peak RSS.
- CRC verification enabled and an identical output sink.

Required decoder-thread cells are 1, 4, 16, and 44. Keep separate matrices for
single-member gzip, ordinary concatenated gzip, and BGZF. The release gate is
95% of zlib-only C++ rapidgzip in every cell, at least 100% geometric mean, and
no more than 110% peak RSS.

FASTQ integration benchmarks should use paraseq with a fixed total physical-core
budget. Sweep decompressor/parser thread splits rather than assigning all cores
to both pools.

## gzippy

gzippy must remain in the comparison. Its current library exposes
`decompress_with_threads(&[u8], usize)` and a threaded writer API, and its
single-member route is a parallel marker pipeline. It therefore overlaps this
project’s decoder scope substantially. It does not replace the reason for this
API: `DecoderReader` yields incremental `Read + Send` output from a positional
source without requiring the complete compressed input or decoded result in
one caller-owned buffer.

## 2026-07-31 parity snapshot

This diagnostic run used a 256 MiB decoded, base64-like fixture in three forms:
a 207,704,008-byte single member, four concatenated ordinary gzip members, and
4,114 BGZF members. The host had two 22-core/44-thread Intel Xeon E5-2699 v4
sockets (44 physical, 88 logical CPUs). Binaries were not CPU-pinned, so this is
not a release result.

The Rust build used rustc 1.91.1, `libz-rs-sys`/zlib-rs 0.6.6, thin LTO, and one
codegen unit. Competitors were rapidgzip 0.16.0 built with zlib-ng and ISA-L
disabled, and gzippy 0.8.0 (`fa2862a`). Each entry is the median wall time in
seconds after two warmups and nine measured runs; the closest concatenated
44-worker cell was confirmed with three warmups and 15 measured Rust/C++ runs.
Every decoder verified CRCs and wrote to a discard sink.

### Single-member gzip

| decoder | 1 | 4 | 16 | 44 |
|---|---:|---:|---:|---:|
| rapidgzip-rust, zlib-rs | 0.969 | 0.407 | 0.141 | 0.130 |
| C++ rapidgzip, zlib-ng | 1.834 | 0.623 | 0.214 | 0.136 |
| gzippy | 0.764 | 0.289 | 0.118 | 0.096 |

Rust/C++ throughput ratios are 1.894, 1.531, 1.512, and 1.045, with a 1.463
geometric mean.

### Four ordinary gzip members

| decoder | 1 | 4 | 16 | 44 |
|---|---:|---:|---:|---:|
| rapidgzip-rust, zlib-rs | 0.936 | 0.434 | 0.152 | 0.157 |
| C++ rapidgzip, zlib-ng | 1.839 | 0.625 | 0.214 | 0.149 |
| gzippy | 0.789 | 0.786 | 0.192 | 0.198 |

Rust/C++ throughput ratios are 1.964, 1.439, 1.409, and 0.952, with a 1.395
geometric mean. The 44-worker concatenated cell is the closest gate result:
each newly discovered member requires an exact bridge to the next file-wide
grid point, while workers and useful later tasks remain live.

### BGZF

| decoder | 1 | 4 | 16 | 44 |
|---|---:|---:|---:|---:|
| rapidgzip-rust, zlib-rs | 0.812 | 0.243 | 0.086 | 0.072 |
| C++ rapidgzip, zlib-ng | 1.048 | 0.339 | 0.139 | 0.080 |
| gzippy | 0.813 | 0.378 | 0.213 | 0.353 |

Rust/C++ throughput ratios are 1.290, 1.395, 1.621, and 1.103, with a 1.339
geometric mean. Median Rust peak RSS remained below C++ in every matrix cell;
at 44 workers it was 174,608 KiB versus 225,592 KiB for single-member gzip,
190,072 versus 219,560 KiB for concatenated gzip, and 50,548 versus
143,420 KiB for BGZF. These measurements clear the stated throughput and
memory gates in every required cell.
