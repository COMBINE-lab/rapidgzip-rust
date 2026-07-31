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
RAPIDGZIP_CPP_ISAL=/path/to/rapidgzip-with-isal \
RAPIDGZIP_CPP_ZLIB_NG=/path/to/rapidgzip-with-zlib-ng \
GZIPPY=/path/to/gzippy \
benchmarks/run-matrix.sh corpus.fastq.gz > results.tsv
```

The driver runs 1, 4, 16, and 44 threads by default, records wall/user/system
time and peak RSS, and reports the ISA-L and zlib-ng rapidgzip builds separately.
It includes gzippy whenever its executable is available. `RAPIDGZIP_CPP` remains
an alias for `RAPIDGZIP_CPP_ZLIB_NG` for older invocations. The competitor is
named `gzippy`; references to "zippy" in benchmark discussions mean that same
program.
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
single-member gzip, ordinary concatenated gzip, and BGZF. The primary parity
target is 95% of ISA-L-enabled C++ rapidgzip in every cell, at least 100%
geometric mean, and no more than 110% peak RSS. A second C++ build with ISA-L
disabled and zlib-ng enabled remains a useful control. "ISA-L-enabled" is
deliberate: rapidgzip can use its custom two-stage decoder rather than ISA-L for
some speculative chunks.

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

## 2026-07-31 public FASTQ snapshot

This is the first real-data parity run. It used
[`SRR22403185_2.fastq.gz`](https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR224/085/SRR22403185/SRR22403185_2.fastq.gz),
the same public FASTQ file linked from
[upstream rapidgzip's performance report](https://github.com/mxmlnkn/rapidgzip#decompression-of-gzip-compressed-fastq-data).
The compressed file is 96,754,995 bytes with MD5
`b14880ba2d4dd091040baa6488eeae39`. It is one ordinary gzip member containing
1,080,230 valid FASTQ records and 361,815,302 decoded bytes. The decoded SHA-256
from all four programs was
`24c643955db72b1e737689db45ab7fea0eccd057c0ca645ecceb73e168d323a2`.

The host was a two-socket Intel Xeon E5-2699 v4 system with 22 physical cores
per socket and SMT enabled. Every process was pinned to physical CPUs 0--43;
SMT siblings 44--87 were excluded. The Linux `powersave` governor was active,
the reported microcode was `0xb000010`, and the kernel was 5.4.0-172-generic.
The 362 MB decoded corpus is intentionally realistic but too small to saturate
44 cores, so the high-thread results include material startup, allocation, and
NUMA overhead.

The Rust binary was the optimized source represented by this snapshot, built
with rustc 1.91.1, zlib-rs/libz-rs-sys 0.6.6, thin LTO, and one codegen unit.
Both C++ binaries were rapidgzip 0.16.0 at commit
`d2350e9c9ba54398cd64e45bfc8c631beec017f0`; one enabled its vendored ISA-L and
zlib-ng, and the control disabled ISA-L while retaining zlib-ng. gzippy was
0.8.0 at commit `fa2862a44af0c3123758c2d8990e934da9b55971` in its pure-Rust
configuration.

The measured matrix was produced with the driver under one inherited CPU
affinity mask:

```bash
taskset -c 0-43 env \
  RUNS=9 WARMUPS=2 THREAD_CELLS="1 4 16 44" \
  DECODED_BYTES=361815302 \
  RAPIDGZIP_CPP_ISAL=/path/to/rapidgzip-with-isal \
  RAPIDGZIP_CPP_ZLIB_NG=/path/to/rapidgzip-with-zlib-ng \
  GZIPPY=/path/to/gzippy \
  benchmarks/run-matrix.sh SRR22403185_2.fastq.gz > fastq-matrix.tsv
```

Each cell had two warmups and nine measured runs. Rust used `-t`; both C++
builds used `-t --verify`; gzippy used `-d -c` to a discard sink because its
`--test` path did not honor the requested one-thread budget in this build.
Normal gzippy decompression verifies the gzip footer. Thread columns are
requested worker budgets. The generic Rust marker pipeline treats that value
as a maximum. For the 44-budget cell, its affinity-aware empirical controller
started with 15 active worker ranks and compared the neighboring 16-worker
setting using ordered output throughput; other Rust paths may use the complete
budget. Median decoded throughput in MiB/s is:

| decoder | 1 | 4 | 16 | 44 |
|---|---:|---:|---:|---:|
| rapidgzip-rust, zlib-rs | 658.7 | 804.7 | 1,663.0 | 1,613.3 |
| C++ rapidgzip, ISA-L enabled | 718.3 | 600.9 | 1,591.0 | 1,603.1 |
| C++ rapidgzip, zlib-ng only | 288.3 | 549.3 | 1,429.9 | 1,617.5 |
| gzippy | 798.1 | 735.8 | 2,034.5 | 1,342.7 |

Median wall time in seconds, with the measured minimum--maximum in parentheses:

| decoder | 1 | 4 | 16 | 44 |
|---|---:|---:|---:|---:|
| rapidgzip-rust, zlib-rs | 0.524 (0.508--0.570) | 0.429 (0.416--0.462) | 0.207 (0.201--0.214) | 0.214 (0.204--0.227) |
| C++ rapidgzip, ISA-L enabled | 0.480 (0.467--0.526) | 0.574 (0.537--0.617) | 0.217 (0.200--0.224) | 0.215 (0.203--0.240) |
| C++ rapidgzip, zlib-ng only | 1.197 (1.177--1.243) | 0.628 (0.599--0.663) | 0.241 (0.218--0.250) | 0.213 (0.196--0.305) |
| gzippy | 0.432 (0.387--0.448) | 0.469 (0.460--0.520) | 0.170 (0.162--0.185) | 0.257 (0.200--0.300) |

The maximum peak RSS observed across the nine measured runs, in KiB, was:

| decoder | 1 | 4 | 16 | 44 |
|---|---:|---:|---:|---:|
| rapidgzip-rust, zlib-rs | 8,008 | 114,040 | 381,844 | 388,632 |
| C++ rapidgzip, ISA-L enabled | 50,072 | 210,364 | 500,428 | 728,012 |
| C++ rapidgzip, zlib-ng only | 55,432 | 213,252 | 497,052 | 745,024 |
| gzippy | 24,160 | 276,752 | 562,224 | 763,340 |

Against the zlib-ng-only C++ control, Rust reaches 228.5%, 146.5%, 116.3%, and
99.7% at budgets 1, 4, 16, and 44, for a 140.4% geometric mean. This clears
the intermediate FASTQ gate of at least 95% in every cell and at least 100%
geometric mean. Its maximum observed RSS is also lower in every cell.

Against ISA-L-enabled rapidgzip, Rust reaches 91.7%, 133.9%, 104.5%, and
100.6%, for a 106.6% geometric mean. The remaining ISA-L parity failure is now
confined to the one-worker cell. gzippy's 798.1 MiB/s one-worker result is
further evidence that closing it does not inherently require an ISA-L binding.
The 44-budget competitor results are variable, as expected for this corpus on
a two-socket host. Implementation evidence and the next optimization targets
are recorded in [PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md).

## 2026-07-31 synthetic parity snapshot

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
143,420 KiB for BGZF. These measurements clear the earlier provisional zlib-ng
throughput and memory gates in every required cell. They do not supersede the
real-FASTQ ISA-L target above.
