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

The Rust binary was commit `d0d2c0334ed6b18d5190986853c1eb9ef1e4065e`,
rustc 1.91.1, zlib-rs/libz-rs-sys 0.6.6, thin LTO, and one codegen unit. Both
C++ binaries were rapidgzip 0.16.0 at commit
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
Normal gzippy decompression verifies the gzip footer. Median decoded throughput
in MiB/s is:

| decoder | 1 | 4 | 16 | 44 |
|---|---:|---:|---:|---:|
| rapidgzip-rust, zlib-rs | 605.8 | 328.2 | 396.5 | 272.5 |
| C++ rapidgzip, ISA-L enabled | 711.8 | 621.2 | 1,578.4 | 1,489.9 |
| C++ rapidgzip, zlib-ng only | 289.3 | 556.0 | 1,467.7 | 1,348.5 |
| gzippy | 786.2 | 759.5 | 1,972.3 | 1,379.1 |

Median wall time in seconds, with the measured minimum--maximum in parentheses:

| decoder | 1 | 4 | 16 | 44 |
|---|---:|---:|---:|---:|
| rapidgzip-rust, zlib-rs | 0.570 (0.541--0.592) | 1.051 (0.976--1.155) | 0.870 (0.835--0.969) | 1.266 (1.199--1.342) |
| C++ rapidgzip, ISA-L enabled | 0.485 (0.475--0.520) | 0.556 (0.547--0.590) | 0.219 (0.203--0.242) | 0.232 (0.211--0.292) |
| C++ rapidgzip, zlib-ng only | 1.193 (1.178--1.253) | 0.621 (0.593--0.641) | 0.235 (0.222--0.248) | 0.256 (0.196--0.274) |
| gzippy | 0.439 (0.388--0.460) | 0.454 (0.444--0.488) | 0.175 (0.166--0.188) | 0.250 (0.199--0.401) |

Median peak RSS in KiB was:

| decoder | 1 | 4 | 16 | 44 |
|---|---:|---:|---:|---:|
| rapidgzip-rust, zlib-rs | 7,776 | 109,068 | 308,032 | 535,628 |
| C++ rapidgzip, ISA-L enabled | 49,916 | 170,732 | 474,528 | 697,276 |
| C++ rapidgzip, zlib-ng only | 55,292 | 170,608 | 486,892 | 698,032 |
| gzippy | 23,944 | 203,624 | 370,176 | 606,356 |

At one thread, Rust reaches 85.1% of ISA-L-enabled rapidgzip and is 2.09 times
the zlib-ng-only control. At 4, 16, and 44 workers it reaches only 52.8%, 25.1%,
and 18.3% of the ISA-L-enabled result. Median Rust user CPU rises from 0.54 s at
one worker to 4.44, 5.64, and 6.99 s respectively, so this is redundant or
inefficient decode work rather than output or storage throughput. Upstream
rapidgzip specifically identifies long LZ77 backreferences in this FASTQ as a
case where speculative chunks cannot readily fall back to ISA-L. The local
profile and proposed response are in
[PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md).

gzippy is the fastest program at 1, 4, and 16 workers on this corpus. That is
important evidence that an ISA-L-class result does not inherently require an
ISA-L binding. The 44-worker gzippy and both rapidgzip results are more variable,
as expected for such a small corpus on a two-socket host.

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
