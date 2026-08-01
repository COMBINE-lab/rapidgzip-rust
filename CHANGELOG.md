# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

### Known residual

Architectural / product gaps **not** shipped in 0.2.0:

- **No real ISA-L** (or other second inflate backend); inflate is **zlib-rs**
  only. That is the remaining **architectural** inflate gap. A crate-private
  `InflateBackend` trait already covers sequential gzip/zlib/raw,
  `stream_decode`, `--analyze` (`Block` flush), BGZF `Finish`, independent-
  member workers (`inflate_capped`), multi-stream zlib index/workers,
  estimated-path residual continue / `inflate_tail` (`inflate_capped`), and
  seek / indexed_decode via `inflate_into_slice`. Unsafe `z::inflate` is
  confined to `inflate_backend.rs`; lifecycle ABI to `RawInflater` (see
  [PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md) coverage table and
  [ARCHITECTURE.md](ARCHITECTURE.md) / [SAFETY.md](SAFETY.md)).
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
