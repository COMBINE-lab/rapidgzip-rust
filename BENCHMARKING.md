# Benchmarking

Build benchmark binaries with the same locked dependency graph:

```bash
cargo build --locked --release --workspace
cargo bench --locked -p rapidgzip-bench
```

The Criterion benchmark drains the public `DecoderReader`, so channel
handoff, ordered assembly, verification, and the unavoidable `Read` copy are
included.

## Fair comparative harness (Rust vs C++ rapidgzip)

Use the one-shot driver for reproducible, fair runs:

```bash
# CI-light: synthetic few-MiB corpora, threads 1 2, 2 runs; C++ optional
./benchmarks/run-fair.sh --ci --threads "1 2" --runs 2

# Full local compare (requires C++ rapidgzip on PATH or RAPIDGZIP_CPP*)
./benchmarks/run-fair.sh --threads "1 4 16 44" --runs 9

# Same entry point via scripts/
./scripts/bench-vs-rapidgzip.sh --ci
```

Results land in `target/bench-results/<UTC-timestamp>/` (`matrix-verify.tsv`,
`matrix-verify.json`, optional `parity.tsv` / `parity.json`, `SUMMARY.md`,
`env.txt`). Synthetic corpora are generated under `target/bench-corpora/` by
`benchmarks/gen-corpora.sh` (no network; deterministic given `SEED`).

### Fairness rules

| Rule | Detail |
|------|--------|
| Same thread budgets | `THREAD_CELLS` / `--threads` applied to every tool |
| CRC verify on | Rust `-t` (or default verify on `-c`); C++ `-t --verify` when accepted, else `-d --verify -c -f` |
| Matched chunk size | Both get `--chunk-size ${CHUNK_SIZE_KIB:-4096}` (KiB) |
| Same sink | Discard payload (`-t` / sink, or `-c` with stdout discarded by the harness) |
| Warmups + median | Default 2 warmups + 9 measured runs for release; CI uses fewer. Report **median** wall time, MiB/s, peak RSS |
| Corpus size | Non-CI default **32 MiB** uncompressed (`CORPUS_BYTES`); avoid sub-4 MiB cells for headlines |
| Affinity | Non-CI auto-sets `TASKSET=0-(nproc-1)` when unset; wraps every timed process |
| Fail closed | Timed commands that exit non-zero drop the row (no invented thrpt from flag mismatches) |
| Auto-detect | If `RAPIDGZIP_CPP*` unset: `target/bench-venv/bin/rapidgzip`, else `PATH`. ISA-L symbols → `rapidgzip-cpp-isal` |
| Rust tool label | Default zlib-rs build → `rapidgzip-rust`. Binary with ISA-L symbols (`isal_`/`libisal`, or `ldd` → libisal) → `rapidgzip-rust-isal` |
| Throughput | `DECODED_BYTES` or auto via `rapidgzip-rust --count` |

**Intentionally not equalized by default:** inflate backend. C++ manylinux
wheels embed ISA-L; Rust **defaults** to zlib-rs. That is an implementation
difference, not a methodology bug — report the backend label honestly and do
not claim ISA-L parity from a zlib-rs-only binary. For fair P=1 thrpt vs C++
ISA-L, build Rust with `--features isal` (system/prefix `libisal`; see
`ISAL_INSTALL_PREFIX` / `LD_LIBRARY_PATH`). Fair harnesses label that binary
`rapidgzip-rust-isal` when ISA-L symbols are present; the default remains
`rapidgzip-rust`. PyPI entrypoints are Python wrappers (baseline RSS includes
the interpreter).

### Installing C++ rapidgzip for comparison

```bash
# Preferred fair competitor: 0.16.x with ISA-L in the wheel
uv venv target/bench-venv
uv pip install --python target/bench-venv/bin/python 'rapidgzip==0.16.0'
export PATH="$PWD/target/bench-venv/bin:$PATH"
# or: python3 -m pip install --user 'rapidgzip==0.16.0'
# Optional separate builds:
#   RAPIDGZIP_CPP_ISAL=/path/to/rapidgzip-with-isal
#   RAPIDGZIP_CPP_ZLIB_NG=/path/to/rapidgzip-zlib-ng-only
```

`--ci` continues with Rust-only cells if C++ is missing (prints a note).
Non-CI `run-fair.sh` errors with the install hint above.

### Mode matrix (`benchmarks/parity-compare.sh`)

| Mode | Rust | C++ rapidgzip (0.16+) |
|------|------|----------------|
| `verify` | `-P N --chunk-size K -t` | `-P N -t --verify --chunk-size K` (fallback: `-d --verify -c -f`) |
| `stdout` | `-P N --chunk-size K -c` (stdout discarded) | `-d -P N --verify -c -f --chunk-size K` |
| `index` | export GZIDX; `-P N -c --import-index` | export; `-d --import-index --verify -c -f` |
| `stdin` | `rust -P N -c - < file` | `rapidgzip -d -P N -c -f < file` (no literal `-` arg) |

C++ 0.16 requires `-d`/`--decompress` for stdout actions (`-c` alone is not
an action). C++ may skip real decode when writing to `/dev/null` without `-f`
/ `-l` / `--count`; the harness always forces a real decode for sink modes.
Rust `--test` with `--import-index` does **not** use the index decode path (it
re-verifies via the parallel pipeline); index mode therefore uses `-c
--import-index` on Rust. Stdin is sequential on both tools — not a
parallel-scaling cell.

### CI-light vs full FASTQ parity

| Profile | How | Corpora |
|---------|-----|---------|
| CI-light | `./benchmarks/run-fair.sh --ci` | `gen-corpora.sh` defaults (~2–4 MiB uncompressed) |
| Local full synthetic | `./benchmarks/run-fair.sh --threads "1 4 16 44"` | larger `CORPUS_BYTES` / `LARGE_BYTES` |
| Release FASTQ | `run-matrix.sh` on a pinned public FASTQ | e.g. SRR22403185 (manual download; **not** default CI) |

Do **not** put multi-hundred-MB FASTQ downloads in default CI. Keep the public
FASTQ snapshot procedure below for release gates only.

### Script map

| Script | Role |
|--------|------|
| `benchmarks/run-fair.sh` | One-shot: build, corpora, matrix, parity, markdown summary |
| `benchmarks/run-matrix.sh` | Thread × tool TSV (+ optional JSON medians); multi-file / `--corpus-dir` |
| `benchmarks/parity-compare.sh` | verify / stdout / index / stdin modes |
| `benchmarks/gen-corpora.sh` | Deterministic single/multi/BGZF-like/zlib fixtures |
| `scripts/bench-vs-rapidgzip.sh` | Forwards to `run-fair.sh` |

### Low-level matrix driver

```bash
cargo build --locked --release -p rapidgzip-rust-cli
RUNS=9 \
DECODED_BYTES=268435456 \
RAPIDGZIP_CPP_ISAL=/path/to/rapidgzip-with-isal \
RAPIDGZIP_CPP_ZLIB_NG=/path/to/rapidgzip-with-zlib-ng \
GZIPPY=/path/to/gzippy \
benchmarks/run-matrix.sh corpus.fastq.gz > results.tsv
# or: --corpus-dir target/bench-corpora --json results.json --mode verify
```

The driver runs 1, 4, 16, and 44 threads by default, records wall/user/system
time and peak RSS, and reports the ISA-L and zlib-ng rapidgzip builds separately.
It includes gzippy whenever its executable is available. `RAPIDGZIP_CPP` remains
an alias for `RAPIDGZIP_CPP_ZLIB_NG` for older invocations; if no `RAPIDGZIP_CPP*`
variable is set, `rapidgzip` on `PATH` is used. The competitor is named
`gzippy`; references to "zippy" in benchmark discussions mean that same
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

## Local fair-harness snapshot

See [benchmarks/RESULTS-SNAPSHOT.md](benchmarks/RESULTS-SNAPSHOT.md) for a machine-local fair compare of release `rapidgzip-rust` vs C++ rapidgzip 0.15.2 on synthetic corpora (verify mode, median of 5 runs).
