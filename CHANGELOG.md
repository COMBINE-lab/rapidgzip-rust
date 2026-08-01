# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - 2026-08-01

### Changed

- `raw_crc32_list`: fail-closed — more than one CRC value is rejected at
  `DecoderBuilder::build` (single whole-stream CRC only).
- ISA-L backend (`isal` feature):
  - Lazy zlib-rs `Block` fallback; pending prime/dictionary so NoFlush seek
    resume does not allocate zlib-rs.
  - `InflateFlush::Finish` multi-steps `isal_inflate` until STREAM_END (or hard
    error / stall / `max_produce` / spare exhaustion) so BGZF one-shots that
    leave data in `tmp_out` after `INPUT_DONE` complete in one public call.
    `NoFlush` remains single-step.
  - Skip full ~87 KiB `inflate_state` zero on create (`isal_inflate_init` only);
    use ISA-L `crc32_gzip_refl` for gzip CRC when `isal` is enabled; BGZF with
    `decoder_threads == 1` (or a single task) runs inline without worker/channel
    reordering.
- CLI version reports `+isal` when built with the feature (e.g. `0.2.2+isal`).
- Fair harness labels linked ISA-L builds as `rapidgzip-rust-isal` (symbol/`ldd`
  detect) in `run-matrix.sh` / `parity-compare.sh`.
- CI: optional Ubuntu `isal` job with `libisal-dev`.

### Docs

- Fair bench snapshot and residual notes for optional ISA-L; monomorphization
  docs use `ActiveInflater` (zlib-rs default, ISA-L with `isal`).

### Known residual

- Without `isal`, P=1 and small / low-thread marker budgets stay zlib-rs-limited.
- **No compression** (decoder-only).
- Random-access indexes remain **gzip/BGZF-oriented** (not raw DEFLATE).

## [0.2.1] - 2026-08-01

### Added

- Optional **`isal`** feature on `rapidgzip-core` and `rapidgzip-rust-cli`:
  sequential inflate uses Intel ISA-L (`IsalInflater` as `ActiveInflater`) for
  a faster single-thread path when a shared `libisal` is available.
  - Default builds remain **zlib-rs** only (no system library required).
  - Build: install `libisal-dev` (or a prefix with `lib/libisal.so`) and set
    `ISAL_INSTALL_PREFIX` if needed; runtime may need `LD_LIBRARY_PATH`.
  - `InflateFlush::Block` (keep_index / analyze) still uses zlib-rs inside the
    ISA-L backend (ISA-L has no zlib-compatible `Z_BLOCK` contract).
  - Prefetch / `tmp_out` drain quirks mapped to zlib-compatible consume and
    `STREAM_END` semantics (refund at `INPUT_DONE`/`FINISH`; only `FINISH` is
    stream end).
- Fair re-bench snapshot vs C++ rapidgzip 0.16 ISA-L with Rust `--features isal`
  ([benchmarks/RESULTS-SNAPSHOT.md](benchmarks/RESULTS-SNAPSHOT.md)): P=1 thrpt
  geometric mean about **1.35×** vs C++ on the synthetic fair corpora; P=1 RSS
  remains far lower than the C++ Python entrypoint.

### Known residual

- Without `isal`, P=1 and small / low-thread marker budgets stay zlib-rs-limited.
- **No compression** (decoder-only).
- Random-access indexes remain **gzip/BGZF-oriented** (not raw DEFLATE).

## [0.2.0] - 2026-08-01

First **feature-complete** release of `rapidgzip-core` and `rapidgzip-rust-cli`.
Decoder-only; intentional non-goals are listed under
[Known residual](#known-residual).

**crates.io note:** `rapidgzip-core` **0.1.0** was an incomplete early publish
with a much smaller public API (no `Format`, `GzipIndex`, `decode_read`, index
on `DecodeReport`, and related surface). **0.2.0** is a **breaking** upgrade
relative to that stub: dependents must use `0.2` (or path/git), not
`0.1.0` from the registry. Do not treat 0.1.0 as the current library contract.

### Added

#### Formats and parallel decode

- Parallel **gzip** via rapidgzip-style **marker/window** estimated grid (zlib-rs
  authoritative fallback and single-thread path).
- Direct parallel paths for **BGZF**, fully **stored** gzip, and dense
  **independent multi-member** archives (candidate-header discovery with
  verified adjacency).
- Parallel **zlib** (RFC 1950):
  - multi-stream: stream-granularity parallel when ≥2 concatenated CMF/FLG…Adler
    frames and `decoder_threads > 1`;
  - single long stream: estimated marker/window when `decoder_threads >= 4` and
    size amortizes ~2× the compressed grid (plus CMF/FLG + Adler).
- Parallel **raw DEFLATE** (RFC 1951, explicit format only): same estimated
  marker/window gates as long single-stream zlib (`decoder_threads >= 4`, ~2×
  grid); optional whole-stream CRC via `raw_crc32_list`.
- Format auto-detect for gzip vs zlib; raw DEFLATE is never auto-selected.
- Public **`Format`** and related decode configuration for explicit format
  selection and reporting.

#### Streaming and non-seekable input

- **`Decoder::decode_read`**: single-thread page-at-a-time streaming (gzip /
  zlib / raw); multi-thread **spill to a private tempfile** then the same
  positional parallel backend for all formats.
- Push API (`Decoder::decode` → `Write`) and owned `Read + Send` stream
  (`Decoder::open` → `DecoderReader`) for parsers such as paraseq.
- CLI stdin (`-` / pipe) uses the same sequential-or-spill policy; `--analyze`,
  `--import-index`, and `--ranges` on stdin also spill rather than buffering
  the full archive only in RAM.

#### Indexes, seek, and analyze

- Random-access **index** import/export: **GZIDX** (indexed_gzip), **gztool**
  (`gzipindx` / `gzipindX`), and **BGZI** / htslib `.gzi` (BGZF block index).
- Public **`GzipIndex`** and index metadata on **`DecodeReport`**.
- **Seek** / range extraction (`--ranges`, library `IndexedReader`) with LRU
  decoded windows, sequential read-ahead, and optional background prefetch.
- Line counting and optional per-checkpoint line offsets for line-oriented seek.
- **`--analyze`**: structure dump for gzip / BGZF / zlib / raw (no payload).

#### CLI (`rapidgzip-rust`)

- Decompress, test, count, count-lines, ranges, analyze, index import/export.
- Thread budget (`-P`), chunk size, verify / no-verify, format force, quiet /
  verbose, tee (`-c` + `-o`), gzip-compatible default naming on TTY.

#### Benchmarking

- Fair harness vs **C++ rapidgzip 0.16** (ISA-L / zlib-ng labeled builds) and
  gzippy: `benchmarks/run-fair.sh`, `run-matrix.sh`, `parity-compare.sh`; see
  [BENCHMARKING.md](BENCHMARKING.md) and [PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md).

### Changed

- Workspace / package version **0.2.0** so path and registry dependency
  resolution no longer collide with the incomplete crates.io **0.1.0** stub
  when packaging `rapidgzip-rust-cli`.

### Known residual (as of 0.2.0)

Architectural / product gaps **not** shipped in 0.2.0:

- **No ISA-L** in this tag (added optionally in **0.2.1**); inflate is zlib-rs.
- **P=1** and small / low-thread marker budgets remain zlib-rs-limited (ordinary
  single-member gzip, single-stream zlib, and raw stay sequential for
  `decoder_threads` 1–3 and for streams below ~2× grid amortization).
- **No compression** (decoder-only).
- Random-access indexes remain **gzip/BGZF-oriented** (not raw DEFLATE).

Intentional sequential gates above are product policy, not unfinished parallel
paths. Residual performance work is tracked in [PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md).

### Safety and platform

- No unsafe public API; private unsafe limited to zlib-rs ABI, checked SIMD,
  and proven buffer ops ([SAFETY.md](SAFETY.md)).
- MSRV 1.87, Rust edition 2024; Unix and Windows positional file sources;
  runtime-dispatched x86-64 SIMD and AArch64 NEON where applicable.

<!-- After tagging, use: [0.2.0]: https://github.com/COMBINE-lab/rapidgzip-rust/releases/tag/v0.2.0 -->
