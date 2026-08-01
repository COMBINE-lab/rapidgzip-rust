# CLI Parity Implementation Plan

> Execution checklist for `docs/superpowers/specs/2026-08-01-cli-parity-design.md`. Boxes are ticked as each step lands, one commit per step.

**Goal:** Accept every rapidgzip 0.16.0 option except `--analyze`, and add the line counting three of them need.

**Branch:** `cli-parity`, stacked on `inflate-backends` (PR #10).

## How line offsets are assigned

Checkpoints are offered by the decode paths, which on the parallel path hold
unresolved marker symbols and therefore cannot count newlines. The coordinator
sees every byte, resolved and in order, so it does the counting.

The coordinator keeps a running decompressed offset and a running newline
count. Whenever the running offset passes the offset of a checkpoint already
recorded, that checkpoint's `line_offset` is set to the running count. This is
a merge of two sorted streams and is exact.

A checkpoint offered after its bytes were already emitted could not be filled.
That does not happen today: the sequential loop offers a member start at the
current total, BGZF pre-scans all block starts before decoding, and marker
workers offer before the coordinator emits their chunk. Rather than trust that,
`IndexBuilder` tracks whether every checkpoint received a count and
`into_index` sets `total_line_count` to `Some` only when all did. Export to
gztool-with-lines fails when it is `None`, which is what turns the current
silent-zero bug into a refusal.

## Steps

- [x] **1. Line counting in the core library**
  `Config::count_lines`, `DecoderBuilder::count_lines`,
  `DecodeReport::line_count`. `RuntimeState` gains a line counter and the
  merge described above; `Output::emit` call sites route through it.
  `IndexBuilder` records whether every checkpoint was annotated.
  Verify: a new `tests/line_count.rs` asserting the sequential, parallel,
  BGZF, and streaming paths report the same count on the same corpus, that a
  corpus with no trailing newline counts correctly, and that an index built
  with counting carries strictly increasing line offsets.

- [x] **2. Line-addressed seeking**
  `IndexedReader::seek_to_line(u64) -> io::Result<u64>`, returning the
  decompressed byte offset of that zero-based line. Errors when the index
  carries no line counts.
  Verify: tests in `tests/indexed_seek.rs` seeking to first, middle, last, and
  one-past-last lines, and the error case.

- [x] **3. gztool-with-lines export refuses an unannotated index**
  Export returns an error when `total_line_count` is `None` instead of writing
  zeros.
  Verify: a test in `tests/index_formats.rs`, and the interop job gains a case
  where real gztool reads an index we exported with line counters.

- [x] **4. CLI module split, no behaviour change**
  `main.rs` keeps arguments and dispatch; `source.rs` takes input
  classification and output destination. Nothing else moves yet.
  Verify: existing behaviour unchanged, plus a new `tests/cli.rs` covering the
  five options that exist today, which is the regression net for steps 5 to 8.

- [x] **5. The option surface**
  Every option in the spec's table, with output path derivation, overwrite
  rules, the terminal check, `-q`/`-v`, the accepted no-ops, and `--no-verify`
  rejected. `attributions.rs` holds the license text.
  Verify: `tests/cli.rs` cases for derived names, overwrite refusal and
  `-f`, the terminal check, each no-op accepted, and `--no-verify` rejected.

- [x] **6. Index import and export**
  `index.rs` with the format enum covering `indexed_gzip`, `gztool`,
  `gztool-with-lines`, `native`, and `gzi`.
  Verify: `tests/cli.rs` exporting in every format and reimporting, decoding
  to identical bytes.

- [x] **7. Ranges**
  `ranges.rs` with the `SIZE@OFFSET` parser and extraction through
  `IndexedReader`.
  Verify: unit tests for parsing, including units, `L`, `inf`, and malformed
  input; `tests/cli.rs` for byte, line, mixed, overlapping, and the two
  failure cases.

- [x] **8. Counting, reporting, and documentation**
  `report.rs` for `--count`, `--count-lines`, `--test`, and verbose output.
  `README.md`, crate docs, `ARCHITECTURE.md`, and `CHANGELOG.md`.

## Global constraints

- MSRV 1.87, Rust 2024, no let-chains.
- No new dependencies. CLI tests use `CARGO_BIN_EXE_rapidgzip-rust`.
- `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  and the full suite pass before each commit.
- No em dashes in code, comments, or commit messages.
