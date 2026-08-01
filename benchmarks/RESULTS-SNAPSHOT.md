# Fair benchmark snapshot (2026-08-01, post parallel zlib/raw)

## What “fairer C++” means here

| Lever | Before (misleading) | After (this snapshot) |
|-------|---------------------|------------------------|
| C++ version | 0.15.2 ad-hoc venv | **0.16.0** (`uv` venv `target/bench-venv`) |
| Backend | mislabeled zlib-ng | Wheel **embeds ISA-L** (symbols in `.so`); harness labels `rapidgzip-cpp-isal` |
| Verify | mixed / unclear | Both: CRC on (`-t` / `-t --verify`), same sink |
| Chunk size | defaults only | Both pinned **`--chunk-size 4096`** (KiB) |
| Corpus size | ~4 MiB uncompressed | **32 MiB** (plus **64 MiB** large-single) |
| Timing TSV | broken `\t` in `/usr/bin/time -f` | Fixed (`%U %S %M` + parse) |
| Affinity | none | Full `taskset` over all `nproc` CPUs |
| C++ 0.16 stdout/index/stdin | missing `-d` (instant fail timed as “fast”) | **`-d --verify -c -f`**; stdin without literal `-`; failed runs **dropped** |
| ISA-L detect | `strings\|grep -q` under `pipefail` hid matches | `grep -a` on the `.so` |

**Still not equalized:** inflate library (ISA-L/C++ vs zlib-rs/Rust) and C++ Python entrypoint RSS baseline. Those are intentional product / packaging differences; report them, do not hide them.

## Method

- Host: local Linux x86_64 (ThinkPad), `nproc=12`, `TASKSET=0-11`
- Date: **2026-08-01** (UTC re-run after parallel zlib/raw + perf work)
- Rust: `target/release/rapidgzip-rust` 0.1.0 (release, locked build)
- C++: `target/bench-venv/bin/rapidgzip` **0.16.0** (PyPI wheel; ISA-L present → `rapidgzip-cpp-isal`)
- Mode: **verify** (discard payload, CRC on)
- Warmups: 2; measured runs: **5**; median wall time → MiB/s; median peak RSS
- Corpora: `target/bench-corpora-fair/` (deterministic synthetic; 32 MiB / 64 MiB large-single)
- Shared: `--chunk-size 4096`, thread ladder `1 4 16 44` (44 is oversubscribed on 12 CPUs)
- Artifacts: `/tmp/fair-matrix-new.tsv`, `/tmp/fair-matrix-new.json`

> **Host note:** Absolute thrpt on this pass is ~0.6–0.8× an earlier same-day fair matrix on the same machine for both Rust and C++ (thermal/load). Relative Rust vs C++ rankings are stable; do not mix absolute MiB/s across snapshot revisions without re-running both tools together.

## Results (verify, median MiB/s / median peak RSS MiB)

### single-member.gz (~32 MiB uncompressed)

| tool | 1 thr | 4 thr | 16 thr | 44 thr |
|------|------:|------:|-------:|-------:|
| rapidgzip-rust | 131 / 6.2 | **221** / 47 | **260** / 68 | **249** / 69 |
| rapidgzip-cpp-isal 0.16 | **133** / 38 | 190 / 54 | 235 / 73 | 228 / 104 |

### multi-member.gz (~32 MiB, 4 members)

| tool | 1 thr | 4 thr | 16 thr | 44 thr |
|------|------:|------:|-------:|-------:|
| rapidgzip-rust | **166** / 6.1 | **200** / 45 | **187** / 59 | **124** / 61 |
| rapidgzip-cpp-isal 0.16 | 159 / 32 | 153 / 52 | 131 / 78 | 98 / 102 |

### bgzf-like.gz (~32 MiB, many small members)

| tool | 1 thr | 4 thr | 16 thr | 44 thr |
|------|------:|------:|-------:|-------:|
| rapidgzip-rust | **191** / 4.4 | **536** / 8.8 | **646** / 20 | **628** / 34 |
| rapidgzip-cpp-isal 0.16 | 167 / 36 | 320 / 36 | 324 / 57 | 319 / 83 |

### large-single.gz (~64 MiB uncompressed)

| tool | 1 thr | 4 thr | 16 thr | 44 thr |
|------|------:|------:|-------:|-------:|
| rapidgzip-rust | **155** / 6.2 | **182** / 48 | **284** / 81 | **185** / 85 |
| rapidgzip-cpp-isal 0.16 | 149 / 35 | 146 / 100 | 265 / 139 | 166 / 174 |

### zlib-stream.zz (~32 MiB; **re-measured with parallel single-stream zlib**)

| tool | 1 thr | 4 thr | 16 thr | 44 thr |
|------|------:|------:|-------:|-------:|
| rapidgzip-rust | **198** / 6.2 | **222** / 44 | **262** / 64 | **174** / 62 |
| rapidgzip-cpp-isal 0.16 | 172 / 35 | 171 / 53 | 232 / 74 | 137 / 104 |

> **Zlib path (measured):** Rust multi-thread thrpt and RSS now **scale** on this large single-stream zlib corpus (P=1 RSS ~6.2 MiB sequential; P≥4 RSS ~44–64 MiB with higher thrpt). Peak multi-thread cell is P=16 at **262 MiB/s** vs C++ **232 MiB/s**. P=44 oversubscribes 12 CPUs and drops thrpt for both tools. Gates unchanged in spirit: large single-stream zlib parallelizes when `decoder_threads >= 4` and size amortizes ~2× the compressed grid (default 1 MiB cells); multi-stream zlib is stream-granularity parallel when `decoder_threads > 1`. P=1 (and small streams / P=2–3 marker skip) stay sequential on zlib-rs. Multi-thread `decode_read` (gzip/zlib/raw) still spills to temp then uses those gates. **Residual:** no ISA-L in Rust; P=1 inflater remains zlib-rs; Adler-32 follows `crc32_enabled` / verify flags.

## Fairer headline

On **gzip/BGZF/zlib-shaped** synthetic corpora with **0.16.0 + ISA-L label + CRC verify + matched chunk size + larger files + real C++ 0.16 flags** (this re-run):

1. **Throughput (gzip/BGZF):** Rust is **competitive to clearly faster** on multi-thread cells (often ~1.1–2× on this host; BGZF-like peaks highest). Single-thread is essentially tied or slightly favors either side within noise.
2. **Throughput (zlib):** Parallel single-stream zlib **closes the old sequential residual**. On `zlib-stream.zz`, Rust multi-thread thrpt scales with threads/RSS and **beats** C++ ISA-L at P=4/16 on this pass (P=1 still sequential zlib-rs, still competitive).
3. **Memory:** Rust remains **more memory efficient**, especially at low thread counts (~0.15–0.2× C++ RSS at 1 thread; often ~0.5–0.8× at high thread counts on large single-member work). C++ RSS includes the Python entrypoint baseline.
4. **Residuals still honest:** inflate backend is **zlib-rs** (no ISA-L); P=1 / small / low-thread marker-skip paths stay sequential; oversubscribing far past `nproc` (e.g. 44 on 12 CPUs) hurts both tools.

## How to reproduce

```bash
# One-time: rapidgzip 0.16 with ISA-L-capable wheel
uv venv target/bench-venv
uv pip install --python target/bench-venv/bin/python 'rapidgzip==0.16.0'

export PATH="$PWD/target/bench-venv/bin:$PATH"
export RAPIDGZIP_RUST=target/release/rapidgzip-rust
export CORPUS_BYTES=$((32*1024*1024))
export TASKSET="0-$(($(nproc)-1))"
export CHUNK_SIZE_KIB=4096
export RUNS=5
export WARMUPS=2
export THREAD_CELLS="1 4 16 44"

cargo build --locked --release -p rapidgzip-rust-cli
benchmarks/gen-corpora.sh target/bench-corpora-fair
LARGE_BYTES=$((64*1024*1024)) benchmarks/gen-corpora.sh target/bench-corpora-fair

benchmarks/run-matrix.sh --mode verify \
  --tsv /tmp/fair-matrix.tsv --json /tmp/fair-matrix.json \
  --corpus-dir target/bench-corpora-fair
```

For the full multi-mode driver: `./benchmarks/run-fair.sh --threads "1 4 16 44" --runs 9`.
