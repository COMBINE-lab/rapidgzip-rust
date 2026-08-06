# Benchmarking

Build benchmark binaries with the same locked dependency graph:

```bash
cargo build --locked --release --workspace
cargo bench --locked -p rapidgzip-bench
```

The Criterion benchmark drains the public `DecoderReader`, so channel
handoff, ordered assembly, verification, and the unavoidable `Read` copy are
included. Its paired `decoder_reader_stored_with_index` group runs the same
thread cells through `reader_with_index` and finalizes the returned index; the
ratio between the two groups isolates indexing overhead on identical data.
Record checkpoint count, serialized native-index size, and peak RSS alongside
throughput when evaluating a release candidate.

Programmatic-reader changes use the alternating FASTQ A/B runner rather than
only a synthetic Criterion group. Generate a validated corpus once, build the
same `reader_decode` target from exact `main` and the candidate in separate
worktrees, and run:

```bash
benchmarks/run-reader-ab.sh \
  --corpus-dir target/bench-corpora \
  --baseline /path/to/main/target/release/reader_decode \
  --candidate target/release/reader_decode \
  --threads "1 4 16" \
  --buffers "8192 1048576" \
  --iterations 20 \
  --reader-mode ordinary \
  --cpus 0-21

# Actual FASTQ parsing; the parser owns its internal Read buffer.
benchmarks/run-reader-ab.sh \
  --corpus-dir target/bench-corpora \
  --baseline /path/to/main/target/release/reader_decode \
  --candidate target/release/reader_decode \
  --threads "1 4 16" \
  --buffers "1048576" \
  --iterations 20 \
  --reader-mode paraseq \
  --cpus 0-21
```

The default fixture set is valid FASTQ encoded as one gzip member, four sparse
members, 512 dense members, and BGZF. `reader_decode` validates decoded bytes
and members on every iteration; paraseq mode also parses every FASTQ record.
The runner records independent medians for inspection, but its Markdown report
uses the median of within-repetition candidate/`main` deltas as the primary A/B
statistic. Pairing before aggregation reduces bias from CPU-frequency and NUMA
state changes between neighboring runs. A reader optimization must preserve
all four shapes and both small and bulk ordinary reads; a single-member win
cannot justify a dense-member or BGZF regression.

Shared-pool work uses the concurrent `shared_reader_decode` target. The
following paired commands compare two equal readers under the same aggregate
16-worker budget: the private control gives each file the informed even split,
while the shared case gives each file broad headroom and lets the pool allocate
the same total dynamically.

```bash
cargo build --release --locked -p rapidgzip-bench --bin shared_reader_decode

target/release/shared_reader_decode \
  target/bench-corpora/fastq-single.gz \
  2 16 8 1048576 268435456 1 20 private read

target/release/shared_reader_decode \
  target/bench-corpora/fastq-single.gz \
  2 16 16 1048576 268435456 1 20 shared read

# Exercise the actual downstream FASTQ parser instead of bulk Read.
target/release/shared_reader_decode \
  target/bench-corpora/fastq-single.gz \
  2 16 16 1048576 268435456 1 20 shared paraseq

# Force every decoder to advertise its complete headroom. Pool fairness still
# enforces the same aggregate limit.
target/release/shared_reader_decode \
  target/bench-corpora/fastq-single.gz \
  2 16 16 1048576 268435456 1 20 growing paraseq
```

Replace the decoded size and following member count with the exact
`decoded_bytes` and `member_count` from `manifest.tsv`; the example above
assumes a 256 MiB single-member corpus. Repeat the paired cells for
`fastq-sparse-members`, `fastq-dense-members`, and `fastq-bgzf`, and alternate
invocation order across at least nine measured repetitions. Include one-reader
cells with equal `GLOBAL_WORKERS` and `DECODER_WORKERS` to isolate shared-pool
coordination overhead. Multi-reader controls should divide the global budget
evenly when inputs are equal, while the shared case should retain the global
budget as every decoder's headroom. The driver verifies decoded bytes and
member counts per reader and reports peak aggregate busy, active, spawned,
auxiliary, queued, attached, and waiting telemetry plus sampled mean busy and
active widths. A separate sampler observes the pool at 1 kHz so its sleep
interval is not charged as reader-completion latency. Shared runs start the
pool at one slot, wait for every reader to attach, and then raise the global
limit. This exercises reliable runtime growth without letting whichever format
scan finishes first spawn the whole budget.
`shared` retains empirical per-decoder demand; `growing` additionally calls
`request_workers(DECODER_WORKERS)` on every reader.

A shared-pool change fails its performance gate if it materially regresses the
one-reader private equivalent or an informed private split on equal inputs.
Capacity-borrowing wins on unequal inputs do not excuse a standard FASTQ-shape
regression. During an unchanged pool limit, peak `busy_workers` must not exceed
that limit; live `spawned_workers` may be higher because retired or zero-grant
representatives are OS-thread telemetry rather than occupied execution slots.

The initial shared-pool gate ran on 2026-08-06 with CPUs 0-21, a 16-worker
aggregate budget, five paired repetitions of five 128 MiB decodes, and private
controls of 16, 8, or 4 workers per file for 1, 2, or 4 equal readers. Values
below are median throughput deltas versus that private control; positive is
faster. `growing` deliberately requests the complete 16-worker per-file floor,
so its one-reader losses on BGZF and dense members measure an intentionally
overwide policy rather than shared-pool coordination overhead.

| FASTQ shape / consumer | shared 1 / 2 / 4 readers | growing 1 / 2 / 4 readers |
| --- | ---: | ---: |
| single gzip / `Read` | +1.01% / -0.37% / -1.06% | +0.96% / +0.68% / +0.74% |
| single gzip / paraseq | -1.59% / +6.38% / +3.47% | +0.59% / +0.39% / +0.55% |
| sparse members / `Read` | -0.91% / +0.06% / -1.85% | -0.67% / +0.28% / -0.48% |
| sparse members / paraseq | +0.86% / +7.83% / +5.19% | -0.10% / -2.37% / +0.77% |
| 512 dense members / `Read` | +1.04% / +86.10% / +134.69% | -8.90% / +99.98% / +162.61% |
| 512 dense members / paraseq | +0.79% / -0.80% / +10.86% | 0.00% / +0.50% / +10.54% |
| BGZF / `Read` | +10.64% / +7.07% / -0.35% | -6.32% / +14.58% / -0.82% |
| BGZF / paraseq | -1.15% / -0.85% / -1.00% | -5.10% / -0.56% / -0.75% |

Every shared/growing sample stayed at or below 16 simultaneously busy decode
regions, and every invocation verified the exact decoded byte and gzip-member
counts. The adaptive shared policy's worst median loss was 1.85%; its large
dense-member wins come from retaining broad per-file scheduling/buffer
headroom without overspending the aggregate execution budget.

The `structural_analysis_fastq` group compares one-worker verified zlib-rs
decode with the sequential structural walker on the same generated 16 MiB
FASTQ-like gzip member. Both paths validate the complete container and discard
decoded output; analysis additionally visits every symbol and retains its
bounded structured block result. Record the `analyze / verified_decode` time
ratio and peak RSS. This is an analyzer regression gate, not a parallel decode
parity cell, because the dependency chain between DEFLATE blocks makes the
structural walk intentionally sequential.

### 2026-08-03 structural-analysis diagnostic

The clean PR #12 replacement was checked on the dual-socket Xeon E5-2699 v4
host and public `SRR22403185_2.fastq.gz` described below. The file contains
96,754,995 compressed bytes and 361,815,302 decoded bytes. These were unpinned
implementation diagnostics, not parallel decode parity results.

The initial implementation used three release runs. The optimized build and
both C++ controls used five interleaved release runs of the complete CLI
report, with report output discarded. The medians were:

| analyzer | elapsed | decoded throughput | peak RSS |
|---|---:|---:|---:|
| rapidgzip-rust, initial clean implementation | 2.68 s | 128.7 MiB/s | 7,256 KiB |
| rapidgzip-rust, optimized | 1.70 s | 203.0 MiB/s | 7,292 KiB |
| rapidgzip 0.16.0 | 1.71 s | 201.8 MiB/s | 17,724 KiB |
| rapidgzip 0.16.0, ISA-L disabled | 1.74 s | 198.3 MiB/s | 8,812 KiB |

The optimized Rust analyzer matches the reference on this input while using
41% of the packaged ISA-L build's resident memory. Relative to the clean
implementation, an inlined bit-buffer fast path, one linear history/checksum
buffer, and output-limit specialization reduced elapsed time by 36.6% and the
retired instruction count from approximately 24.3 billion to 12.6 billion.
An analyzer-specific 11-bit Huffman/extra-bit cache was also tested and removed
because its 1.85-second median regressed this workload. Bounded bulk LZ77
overlap copies had previously reduced the Rust median from 3.20 s without
retaining decoded output. The optimized generated 16 MiB FASTQ-like quick
Criterion cell measured 8.54 ms, or 1.83 GiB/s, for analysis and 2.57 ms, or
6.08 GiB/s, for verified zlib-rs decode; its highly repetitive
sequence content makes bulk-copy gains larger than on the public file.

The shared generic Huffman layer was separately checked against an untouched
`main` build by alternating five complete 16-worker verified decodes of the
public FASTQ input. Both medians were 0.24 s. Median user CPU was 2.00 s for the
analysis branch and 2.05 s for `main`; wall time and noisy unpinned RSS show no
ordinary decoder regression from the monomorphized abstraction.

The paired `bgzf_line_count` and `bgzf_line_count_with_index` groups use a
32 MiB FASTQ-like stream split into 4 KiB BGZF blocks (more than eight thousand
retained checkpoints). Their ratio isolates checkpoint annotation from SIMD
newline counting, while peak RSS from the benchmark process exposes growth per
checkpoint. The index builder annotates its checkpoint vector in place and
must not recreate the former tree-node allocation per checkpoint.

For a larger peak-RSS diagnostic without Criterion's sampling allocations:

```bash
cargo build --release -p rapidgzip-bench --bin line_index
/usr/bin/time -v target/release/line_index count 8 128
/usr/bin/time -v target/release/line_index index 8 128
```

Both modes generate the same 128 MiB stored-BGZF fixture with 4 KiB blocks and
discard generation scratch before timing. The index mode retains more than
32,000 line-annotated checkpoints. Compare elapsed time and maximum resident
set size across repeated, alternating invocations.

### 2026-08-03 line-index diagnostic

On the dual-socket Xeon E5-2699 v4 host described below, five alternating
un-pinned runs of the command above at eight workers produced these medians:

| mode | decoded bytes | checkpoints | timed decode | peak RSS |
|---|---:|---:|---:|---:|
| line count | 134,217,728 | 0 | 48.362 ms | 136,700 KiB |
| line count + index | 134,217,728 | 32,768 | 51.518 ms | 138,088 KiB |

In-place annotation therefore added 6.5% elapsed time and 1,388 KiB peak RSS,
or about 43 bytes per retained checkpoint, on this deliberately dense index.
The larger quick Criterion sweep measured count-plus-index overhead of 11.9%,
11.9%, and 5.9% at 1, 4, and 16 workers respectively; those single-sample
figures are retained as a routing diagnostic rather than a portable speed
claim.

The `decoder_reader_deflate_formats` group compresses one identical payload as
gzip, zlib, and raw DEFLATE, then decodes each through the public reader at the
same worker budgets. It is a paired container-overhead regression check, not a
replacement for the retained FASTQ parity matrix.

The unpublished telemetry sampler drains the same reader while recording its
elastic worker state:

```bash
cargo run --release -p rapidgzip-bench --bin telemetry -- \
  corpus.fastq.gz 32 16

# Add a delay after each 1 MiB Read call to exercise consumer backpressure.
cargo run --release -p rapidgzip-bench --bin telemetry -- \
  corpus.fastq.gz 32 32 1000
```

Arguments after the path are the configured worker maximum, optional runtime
worker ceiling, and optional consumer delay in microseconds. Output includes
maximum active, busy, spawned, and auxiliary threads; pressure sample counts;
and time from sustained consumer backpressure to retirement of excess workers.

Existing-index full-stream decoding has a paired file-backed driver:

```bash
cargo run --release -p rapidgzip-bench --bin indexed -- \
  reads.fastq.gz 1,2,4,8,16 5 zlib 1 file
```

Arguments after the path are thread budgets, repetitions, predecessor-window
storage (`zlib` or `raw`), checkpoint spacing in MiB, and source mode (`file`
or `memory`). Index construction is always performed once with one worker and
reported separately. Each cell warms both paths, then reports the median for
`decode_from_index` and ordinary `decode` over the same source and decoder
configuration. The generated Criterion group
`indexed_parallel_vs_ordinary` provides a repository-local regression fixture;
the binary is the retained FASTQ check.

## Reproducible cross-tool runner

Release comparisons use `run-fair.sh`. The runner generates or accepts
corpora, verifies decoded size and SHA-256 before timing, prepares each tool's
own index outside timed cells, rotates tool order by repetition, and retains
every attempted command in `raw.tsv`:

```bash
RAPIDGZIP_CPP_ISAL=/path/to/rapidgzip-with-isal \
RAPIDGZIP_CPP_ZLIB_NG=/path/to/rapidgzip-with-zlib-ng \
GZIPPY=/path/to/gzippy \
benchmarks/run-fair.sh \
  --generate \
  --decoded-mib 256 \
  --threads "1 4 16 44" \
  --modes "verify stdout indexed stdin" \
  --cpus 0-43 \
  --runs 9 \
  --warmups 2
```

Competitors are opt-in and their backend labels come only from the explicit
environment variable names. The runner never scans a binary or library to
guess its backend. Configured tools must be executable, report a version, and
pass correctness preflight. The initial C++ command template deliberately
supports rapidgzip 0.16.x. Missing optional tools are recorded in
`environment.tsv`; a configured but broken tool fails before timing.
`RAPIDGZIP_RUST`, `GENERATE_CORPORA`, and `SUMMARIZE_RESULTS` may point to
prebuilt Rust binaries, which keeps release artifacts and generated data in a
maintainer-selected scratch directory.

`--generate` creates deterministic single-member, sparse/dense ordinary
multi-member, true BGZF, stored, low-compression, zlib, and raw-DEFLATE inputs
under `target/bench-corpora`. Every stream is decoded and checked internally
before `manifest.tsv` is published. In particular, every BGZF block carries a
matching `BC`/`BSIZE` field and the generated stream is required to select the
specialized BGZF decoder path. Generate a corpus separately with:

```bash
cargo run --release -p rapidgzip-bench --bin generate_corpora -- \
  --output target/bench-corpora --decoded-mib 256 --seed 1
```

Caller-supplied gzip files can be measured in the same workflow:

```bash
RAPIDGZIP_CPP_ZLIB_NG=/path/to/rapidgzip-with-zlib-ng \
benchmarks/run-fair.sh --modes "verify stdout" reads.fastq.gz
```

Normal gzip release runs require at least one independent decoder. Use
`--rust-only` only for a labeled harness smoke test; it cannot establish
cross-decoder correctness. zlib and raw-DEFLATE generated controls are also
explicitly labeled Rust-only because the registered competitors are
gzip-specific.

Each invocation writes `environment.tsv`, `corpora.tsv`, `commands.tsv`,
`parity.tsv`, `raw.tsv`, `summary.tsv`, `SUMMARY.md`, and retained failure logs
to a timestamped directory under `target/bench-results`. `raw.tsv` is the
source of truth. The standard-library-only `summarize_results` binary validates
the exact schema, rejects duplicates and inconsistent decoded sizes, includes
failed attempts in group counts, and computes deterministic medians from
successful rows. Hosted CI exercises a two-worker Rust-only matrix solely to
validate the harness; hosted timing is not a performance gate.

`run-matrix.sh` is a deprecated one-release compatibility wrapper. It
translates `RUNS`, `WARMUPS`, `THREAD_CELLS`, `RAPIDGZIP_CPP`, and the explicit
tool variables into a verify-only fair run, then prints the expanded raw TSV.
It no longer guesses that an arbitrary `rapidgzip` on `PATH` is a zlib-ng
build. The competitor is named `gzippy`; references to "zippy" in benchmark
discussions mean that same program. Criterion separately measures the
paraseq-facing pull API.

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

## 2026-08-04 v0.2.0 release qualification

The v0.2.0 candidate was qualified from decoder source commit `e8b2531` on the
dual-socket Xeon E5-2699 v4 host described below. Every process was pinned to
physical CPUs 0--43, excluding SMT siblings. Each cell decoded 256 MiB of the
same deterministic valid FASTQ payload after two warmups and reported the
median of nine runs. The four representations were one ordinary member, four
ordinary members, 1,024 ordinary members, and 4,371-member BGZF including its
canonical EOF member.

The Rust build used rustc 1.91.1, zlib-rs/libz-rs-sys 0.6.6, thin LTO, and one
codegen unit. Both C++ binaries were rapidgzip 0.16.0 at commit `d2350e9`, one
with ISA-L and zlib-ng and one with zlib-ng only. gzippy was 0.8.0 at commit
`fa2862a` using its default `parallel-sm+pure` decoder. All four tools produced
the same 268,435,456-byte output and SHA-256 for every representation. All 576
timed attempts succeeded.

Median decoded throughput in MiB/s was:

| corpus and decoder | 1 | 4 | 16 | 44 |
|---|---:|---:|---:|---:|
| single: rapidgzip-rust | 4,458.5 | 4,691.6 | 3,183.4 | 3,109.2 |
| single: C++ ISA-L | 1,426.0 | 1,069.5 | 1,495.1 | 1,517.3 |
| single: C++ zlib-ng | 836.4 | 1,016.0 | 1,368.1 | 1,317.6 |
| single: gzippy | 4,878.3 | 4,464.3 | 3,278.5 | 2,045.0 |
| four members: rapidgzip-rust | 4,516.3 | 2,289.6 | 2,251.0 | 2,248.5 |
| four members: C++ ISA-L | 1,486.0 | 1,089.6 | 1,532.8 | 1,493.3 |
| four members: C++ zlib-ng | 828.1 | 860.7 | 1,402.3 | 1,318.9 |
| four members: gzippy | 1,294.1 | 2,865.7 | 2,780.9 | 2,758.8 |
| 1,024 members: rapidgzip-rust | 4,344.7 | 5,730.9 | 7,758.5 | 4,563.0 |
| 1,024 members: C++ ISA-L | 1,305.0 | 816.5 | 710.4 | 709.4 |
| 1,024 members: C++ zlib-ng | 877.0 | 531.2 | 525.2 | 525.5 |
| 1,024 members: gzippy | 700.2 | 2,304.9 | 2,602.9 | 2,294.7 |
| BGZF: rapidgzip-rust | 2,209.2 | 5,912.4 | 7,934.5 | 4,386.0 |
| BGZF: C++ ISA-L | 1,102.6 | 2,352.6 | 3,573.1 | 3,249.6 |
| BGZF: C++ zlib-ng | 798.2 | 1,775.2 | 3,046.2 | 2,977.4 |
| BGZF: gzippy | 635.2 | 1,875.5 | 1,702.3 | 814.1 |

Rust exceeded zlib-ng C++ in every cell by 1.47x--14.77x, with a 3.71x
geometric mean. It exceeded ISA-L-enabled C++ in every cell by 1.35x--10.92x,
with a 2.87x geometric mean. Its worst per-cell maximum-RSS ratio was 45.1% of
zlib-ng C++ and 42.7% of ISA-L-enabled C++. This clears all throughput and
memory release gates; the deliberately short, highly compressible generated
payload should be read as a regression matrix rather than a universal speed
claim.

The public `DecoderReader` and actual paraseq consumer were separately drained
through 8 KiB and 1 MiB read buffers at 1, 4, and 16 configured workers. Each
of the 24 cells per mode completed all nine runs with correct byte and member
counts; paraseq additionally parsed every FASTQ record. Across the eight
shape/buffer combinations per worker count, median throughput ranges were:

| consumer | 1 | 4 | 16 |
|---|---:|---:|---:|
| ordinary `Read` | 1,760.6--3,055.0 | 2,300.2--5,758.6 | 2,033.6--5,804.9 |
| paraseq FASTQ parse | 1,259.4--2,066.3 | 1,355.1--1,985.9 | 1,168.1--1,849.9 |

Those reader runs used the same `e8b2531` runtime binary for both arms of the
alternating harness because the release-preparation changes affected only
documentation and release/benchmark scripts. The paired differences therefore
measure host noise, not a code delta; the absolute medians and full validation
are the release evidence.

## 2026-08-03 adaptive-admission diagnostic

PR #13 identified a low-thread crossover that varied by input. This diagnostic
uses the same unpinned dual-socket Xeon E5-2699 v4 host and public FASTQ corpus
described below. It is a controller-tuning run, not a replacement for the
pinned release parity matrix. The clean implementation was based on `main` at
`944b6c5`; it did not merge the stacked PR branch.

The unchanged `main` marker policy produced these seven-run medians:

| requested workers | terminal path | decoded MiB/s |
|---:|---|---:|
| 1 | Sequential | 658.9 |
| 2 | MarkerWindow | 475.9 |
| 3 | MarkerWindow | 643.2 |
| 4 | MarkerWindow | 801.1 |

After adaptive marker admission, a final nine-run matrix produced:

| requested workers | terminal paths | decoded MiB/s |
|---:|---|---:|
| 1 | 9 Sequential | 620.3 |
| 2 | 9 Sequential | 645.9 |
| 3 | 8 Sequential, 1 MarkerWindow | 594.5 |
| 4 | 9 MarkerWindow | 777.0 |

The decisive regression cell therefore moved from 475.9 to 645.9 MiB/s while
the four-worker marker cell retained 97.0% of its original throughput. The
one- and three-worker medians reflect two visibly different host-frequency
bands during these unpinned runs; path counts, user CPU time, and peak RSS were
used to distinguish controller decisions from that external variance.

The following three-run telemetry medians exercise other route shapes. The
32 MiB text stream has only thirteen normal compressed-grid tasks and skips
admission. The sparse archive contains four large ordinary gzip members. Dense
FASTQ and BGZF are classified before generic admission.

| workload | 1 worker | 2 workers | 4 workers | terminal route(s) |
|---|---:|---:|---:|---|
| 32 MiB text | 287.7 | 287.8 | 287.9 | Sequential |
| four sparse members | 581.3 | 604.1 | 573.9 | Sequential |
| dense FASTQ members | 603.5 | 878.3 | 1,569.5 | Sequential / DenseMembers |
| BGZF | 458.1 | 881.7 | 1,525.5 | Bgzf |

These figures validate routing and probe overhead, not portable speed claims.
The controller uses the useful machine/request/runtime width and observed input
service rates; the table is deliberately retained so later threshold changes
must explain both false-positive and false-negative path movement.

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
as a maximum. This historical snapshot used the first affinity-aware empirical
controller from commit `78a5fb2`. For the 44-budget cell, that controller
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

## 2026-07-31 dense-member regression diagnostic

This diagnostic targets [issue #2](https://github.com/COMBINE-lab/rapidgzip-rust/issues/2),
where BCL Convert-style gzip members are too short to reach the next regular
compressed grid point. It used one 380,928,000-byte random-DNA payload in a
single member and in members containing 8 MiB, 2 MiB, or 372,000 decoded bytes.
The smallest form had 1,024 members averaging 109 KiB compressed. This is a
member-scheduling control, not a replacement for the public FASTQ parity run.

Each result below is median wall-clock throughput after two warmups and five
measured runs on the same dual-Xeon host as the synthetic parity snapshot.
Every run verified all trailers and wrote to a discard sink.

| decoded bytes/member | members | 1 | 4 | 16 | 32 |
|---|---:|---:|---:|---:|---:|
| one member | 1 | 636 MB/s | 1,315 MB/s | 2,602 MB/s | 2,551 MB/s |
| 8 MiB | 46 | 636 MB/s | 1,648 MB/s | 2,762 MB/s | 2,944 MB/s |
| 2 MiB | 182 | 642 MB/s | 1,849 MB/s | 4,398 MB/s | 4,935 MB/s |
| 372,000 | 1,024 | 641 MB/s | 1,881 MB/s | 5,016 MB/s | 6,173 MB/s |

Before the dense-member path, single measured runs of the 372,000-byte-member
form took 0.63, 1.09, and 0.99 seconds at budgets 1, 16, and 32 respectively:
parallel decode was slower than the 605 MB/s one-worker result. The table's
corresponding medians are 641, 5,016, and 6,173 MB/s. Peak RSS was 18,908 KiB
at budget 16 and 38,348 KiB at budget 32. A SHA-256 comparison against GNU
gzip produced the same decoded digest.

### Elastic-worker and telemetry diagnostic

The initial elastic-worker implementation was measured on the 1,024-member,
380,928,000-byte decoded fixture above using the public `DecoderReader`. The
process was pinned to physical CPUs 0--43. Each cell below is the median of five
runs with a configured maximum of 32 workers and a fast discard consumer:

| runtime worker ceiling | peak spawned workers | median MiB/s |
|---:|---:|---:|
| 4 | 4 | 1,531 |
| 8 | 8 | 2,985 |
| 16 | 16 | 4,809 |
| 32 | 32 | 4,462 |

This verifies that the runtime ceiling controls actual OS-worker creation, not
only task admission. It also reproduces the workload's useful-concurrency knee:
16 workers exceeded the unconstrained 32-worker median on this short fixture.
The 32-worker run begins at a 12-worker budget-derived bootstrap and may probe
upward; the input is too short to guarantee a completed empirical search.

For the backpressure run, the same decoder used a 1,000-microsecond delay after
each 1 MiB `Read` request. Across five runs, maximum active and spawned workers
were 12 before backpressure. While consumer-bound, the active target was always
one; excess worker threads retired after a median 252.0 ms, close to the
configured 250 ms hysteresis, and the live worker count reached zero once
decoding got ahead of the throttled consumer. Median end-to-end throughput was
300.8 MiB/s. This controlled delay is a scheduling diagnostic, not a decoder
throughput result.

The same sampler was run five times on the public single-member FASTQ under the
same 44-CPU affinity mask. These are pull-API measurements and therefore do not
replace the competitor matrix above:

| configured budget | peak worker OS threads | median MiB/s |
|---:|---:|---:|
| 1 | 0 (coordinator decodes) | 650.0 |
| 4 | 4 | 793.0 |
| 16 | 16 | 1,583.2 |
| 44 | 14 | 1,845.3 |

The 44-budget stream was too short to amortize calibration, so it retained the
14-worker bootstrap instead of allocating all 44 workers. Against the published
zlib-ng C++ control values, the corresponding throughput ratios remain above
the 95% per-cell gate. A final nine-run follow-up at budget 16 selected 16
workers every time and produced a 1,583.2 MiB/s median.

### Adjacent-member result collation

A follow-up optimization groups at most four candidate headers per worker and
collates only separately authenticated, exactly adjacent members. It activates
only when the prefix averages at least 256 candidates per configured compressed
grid interval. A fresh comparison against the exact pre-collation commit on the
372,000-byte-member fixture found essentially neutral throughput: -0.3%, -1.2%,
and +1.0% at budgets 4, 16, and 32. This confirms that ordinary BCL
Convert-sized FASTQ members remain on the prior one-member task path.

To expose coordinator overhead directly, a separate stress fixture concatenated
one million valid one-byte gzip members (23 MiB compressed). Three alternating
runs of the pre-collation and collating binaries gave these median wall times:

| decoder-worker budget | pre-collation | collating | speedup |
|---:|---:|---:|---:|
| 4 | 3.01 s | 2.52 s | 1.19x |
| 16 | 4.85 s | 1.43 s | 3.39x |
| 32 | 7.11 s | 2.43 s | 2.93x |

This deliberately pathological fixture is a scheduling diagnostic rather than
a throughput or release-parity corpus. High-thread measurements were noisy on
the shared dual-Xeon host, but every run verified all one million trailers and
the decoded digest matched GNU gzip.

## 2026-08-02 multi-format implementation baseline

The first zlib/raw implementation was measured with the paired Criterion group
on the same dual-socket Xeon E5-2699 v4 host. The fixture was 16 MiB of
deterministic xorshift bytes, independently compressed at level 1 with gzip,
zlib, and raw wrappers. Ten samples used one-second warmup and at least three
seconds of measurement. Values are Criterion point estimates in MiB/s:

| configured worker budget | gzip | zlib | raw DEFLATE |
|---:|---:|---:|---:|
| 1 | 219.16 | 223.88 | 222.57 |
| 4 | 199.13 | 206.22 | 205.80 |
| 16 | 186.47 | 181.01 | 181.65 |

At each budget, the three wrappers are within roughly 4%, showing that zlib
Adler-32 and raw terminal handling add no material regression relative to the
same gzip payload. This short, high-entropy fixture does not benefit from the
marker pipeline—the multi-worker cells are slower than one worker—so these
numbers validate paired format cost rather than scalability. Large-stream path
selection and indexed seeking are separately asserted by the integration
suite; release performance still uses retained FASTQ and representative
compressible zlib/raw corpora.

## 2026-08-03 indexed full-stream FASTQ validation

The clean PR #14 successor was measured on the dual-socket Xeon E5-2699 v4
host (44 physical cores, 88 hardware threads, affinity CPUs 0-87) with the
file-backed `indexed` driver above. The input was
`SRR22403185_2.fastq.gz`: 96,754,995 compressed bytes, 361,815,302 decoded
bytes, SHA-256
`3794ae8a0cf3a2db49d9814a5243f8c2ff6397b33c0ce6f79adc0ddea4cec0f2`.
Large input data is not stored in this repository.

One-worker index construction at 1 MiB spacing retained 334 checkpoints in
1.068 seconds using the default compressed-window policy. After one warmup per
path, five repetitions produced these median MiB/s values:

| decoder-worker budget | from existing index | ordinary decode |
|---:|---:|---:|
| 1 | 553.9 | 687.8 |
| 2 | 982.2 | 502.8 |
| 4 | 1,657.1 | 834.1 |
| 8 | 2,137.9 | 1,291.5 |
| 16 | 2,964.3 | 1,768.9 |

These values validate scaling and are not a new cross-implementation parity
claim. The paired run caught an important structural regression before review:
the first implementation emitted output after every `Z_BLOCK` return. This
FASTQ contains many small internal DEFLATE blocks, so multi-worker throughput
fell below 350 MiB/s despite synthetic success. Accumulating internal block
results into configured output chunks restored scaling; a focused integration
test now enforces a chunk-plus-span bound on writer handoffs.

## 2026-08-03 line-counting FASTQ diagnostic

The line-aware CLI successor was checked on the same public FASTQ file and
dual-socket host described above. This was an unpinned implementation
diagnostic, not a release parity matrix. The release CLI used a 16-worker
budget, one warmup per mode, and five alternating `/usr/bin/time` runs. Both
modes decoded and verified the full stream to a sink; the counted mode reported
4,320,920 newline bytes.

| mode | five wall times (s) | median (s) | decoded MiB/s |
|---|---|---:|---:|
| counting disabled (`--test`) | 0.25, 0.27, 0.25, 0.25, 0.24 | 0.25 | 1,380.2 |
| counting enabled (`--count-lines`) | 0.28, 0.28, 0.28, 0.27, 0.27 | 0.28 | 1,232.3 |

The first scalar implementation measured 0.52 seconds in the counted mode on
the same warm cache, more than twice the 0.25-second control. Runtime-dispatched
AVX2 with SSE2 and NEON baselines reduced the measured optional cost to roughly
12%. The ordinary disabled path still performs no scan and only tests the
decode-local enabled flag at each ordered output chunk.
