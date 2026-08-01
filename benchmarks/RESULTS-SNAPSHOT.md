# Fair benchmark snapshot (2026-08-01, Rust ISA-L feature)

## Method

| Lever | This run |
|-------|----------|
| C++ version | **0.16.0** (`target/bench-venv`; wheel embeds ISA-L → `rapidgzip-cpp-isal`) |
| Rust | **0.2.0** release, **`--features isal`**, linked to `target/isal-prefix/lib/libisal.so.2` |
| Verify | Both: CRC on (`-t` / `-t --verify`), discard payload |
| Chunk size | **`--chunk-size 4096`** (KiB) both tools |
| Corpus size | **32 MiB** synthetic (`target/bench-corpora-fair/`) + large-single ~64 MiB uncompressed |
| Timing | Medians of **5** measured runs after **2** warmups; wall via `EPOCHREALTIME`; peak RSS via `/usr/bin/time -f %M` |
| Threads | `1 4 16 44` (44 oversubscribes 12 CPUs) |
| Affinity | `TASKSET=0-11` |
| Artifacts | `target/bench-results/20260801T040458Z/` |

**Equalized:** threads, verify, chunk size, sink, corpora, affinity, medians.  
**Not equalized:** C++ Python-wrapper RSS baseline; parallel algorithm differences; host load vs older snapshots.

> **Host note:** Absolute MiB/s varies with thermal/load. Compare tools **within this run**. Prior zlib-rs-only snapshot (same day, earlier) is not mixed into absolute rankings.

## Results (verify, median MiB/s / median peak RSS MiB)

### single-member.gz (~32 MiB uncompressed)

| tool | 1 thr | 4 thr | 16 thr | 44 thr |
|------|------:|------:|-------:|-------:|
| rapidgzip-rust (**ISA-L**) | **230** / 6.5 | **105** / 62 | 61 / 80 | **96** / 81 |
| rapidgzip-cpp-isal 0.16 | 134 / 39 | 85 / 55 | **64** / 76 | 72 / 105 |

### multi-member.gz

| tool | 1 thr | 4 thr | 16 thr | 44 thr |
|------|------:|------:|-------:|-------:|
| rapidgzip-rust (**ISA-L**) | **312** / 6.5 | **230** / 63 | **236** / 82 | **242** / 85 |
| rapidgzip-cpp-isal 0.16 | 206 / 32 | 151 / 53 | 208 / 76 | 218 / 103 |

### bgzf-like.gz

| tool | 1 thr | 4 thr | 16 thr | 44 thr |
|------|------:|------:|-------:|-------:|
| rapidgzip-rust (**ISA-L**) | 93 / **4.7** | **504** / **9.2** | **570** / **21** | **510** / **42** |
| rapidgzip-cpp-isal 0.16 | **106** / 36 | 310 / 34 | 316 / 55 | 282 / 83 |

### large-single.gz (~64 MiB uncompressed)

| tool | 1 thr | 4 thr | 16 thr | 44 thr |
|------|------:|------:|-------:|-------:|
| rapidgzip-rust (**ISA-L**) | **286** / 6.5 | **193** / 72 | 178 / 116 | 214 / 116 |
| rapidgzip-cpp-isal 0.16 | 222 / 35 | 172 / 100 | **179** / 138 | **244** / 168 |

### zlib-stream.zz (single long zlib)

| tool | 1 thr | 4 thr | 16 thr | 44 thr |
|------|------:|------:|-------:|-------:|
| rapidgzip-rust (**ISA-L**) | **127** / 6.5 | **65** / 56 | 81 / 74 | **118** / 73 |
| rapidgzip-cpp-isal 0.16 | 82 / 35 | 50 / 61 | **108** / 80 | 108 / 105 |

## Takeaways

1. **P=1 thrpt (primary ISA-L goal):** Rust with `--features isal` **beats** C++ ISA-L on 4/5 corpora at 1 thread:
   - single-member **1.71×**, multi-member **1.51×**, large-single **1.29×**, zlib **1.56×**, bgzf **0.87×**
   - **Geometric mean ≈ 1.35×** Rust/C++ at P=1
2. **P=1 RSS:** Rust stays ~**6.5 MiB** peak vs C++ ~**32–39 MiB** (Python entrypoint baseline).
3. **Multi-thread:** Rust still strong on BGZF (independent blocks) and multi-member; single-member / large-single / zlib scaling is mixed and host-sensitive at 16–44 on 12 CPUs.
4. **Backend honesty:** This binary links **libisal** (not zlib-rs). Default builds without `isal` remain zlib-rs-only.

## Reproduce

```bash
export ISAL_INSTALL_PREFIX="$PWD/target/isal-prefix"
export LD_LIBRARY_PATH="$PWD/target/isal-prefix/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PATH="$PWD/target/bench-venv/bin:$PATH"

cargo build --locked --release -p rapidgzip-rust-cli --features isal

./benchmarks/run-fair.sh \
  --threads "1 4 16 44" --runs 5 --warmups 2 \
  --modes verify --skip-parity \
  --corpus-dir target/bench-corpora-fair

# optional zlib cell (not in default fair .gz glob)
RUNS=5 WARMUPS=2 THREAD_CELLS="1 4 16 44" CHUNK_SIZE_KIB=4096 \
  benchmarks/run-matrix.sh --mode verify target/bench-corpora-fair/zlib-stream.zz
```
