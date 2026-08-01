# Multi-Format Decode Implementation Plan

> Execution checklist for `docs/superpowers/specs/2026-08-01-multi-format-decode-design.md`. Boxes are ticked as each step lands, one commit per step.

**Goal:** Decode zlib streams and raw DEFLATE with the crate's verification discipline, sequentially and through the parallel estimated-grid path.

**Branch:** `multi-format`, stacked on `index-and-seek` (PR #8).

## Steps

- [x] **1. Format detection and zlib framing primitives**
  `format.rs` with the public `Format` enum and prefix detection, `zlib.rs`
  with header validation, Adler-32, and trailer verification, plus
  `ZlibErrorKind` and `DecodeError::InvalidZlib`.
  Verify: `cargo test -p rapidgzip-core --lib`.

- [x] **2. Sequential single-stream decode**
  `single_stream.rs` decoding one zlib or raw DEFLATE stream over an
  `InputCursor`, so positional and non-seekable sources share it. Adds
  `DeflateErrorKind::TrailingGarbage` and
  `DecodeError::UnexpectedOutputSize`.
  Verify: `cargo test -p rapidgzip-core --test multi_format`.

- [x] **3. Configuration and dispatch**
  `DecoderBuilder::format` and `expected_uncompressed_size`, the latter
  rejected for any format but raw DEFLATE, `DecodeReport::format`, and the
  detection and dispatch in `decode_source` and `decode_stream`.
  Verify: detection matrix and explicit-format mismatch tests.

- [x] **4. Parallel decode through the estimated grid**
  zlib and raw DEFLATE enter `decode_rapidgzip_estimated`, starting at the
  format's DEFLATE offset and verifying the format's trailer at the end
  instead of a gzip footer.
  Verify: parallel output equals sequential output on the same corpora.

- [x] **5. Indexing coverage**
  Confirm `build_index` produces interior checkpoints for zlib and raw, and
  that `IndexedReader` seeks into both.
  Verify: index and seek tests for the two formats.

- [ ] **6. Documentation**
  Crate docs, `ARCHITECTURE.md`, `README.md`, `CHANGELOG.md`.
  Verify: `cargo test --doc`, `cargo doc` with `-D warnings`.

## Global constraints

- No new runtime dependencies.
- New `unsafe` only where the crate already drives zlib-rs, each block with a
  `// SAFETY:` comment.
- `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  and the full test suite pass before each commit.
- No em dashes in code, comments, or commit messages.
