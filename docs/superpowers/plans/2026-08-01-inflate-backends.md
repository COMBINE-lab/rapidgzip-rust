# Inflate Backends Implementation Plan

> Execution checklist for `docs/superpowers/specs/2026-08-01-inflate-backends-design.md`. Boxes are ticked as each step lands, one commit per step.

**Goal:** Plug an alternative inflate implementation into the whole-stream paths, provide ISA-L behind an off-by-default feature, and measure the difference.

**Branch:** `inflate-backends`, stacked on `multi-format` (PR #9).

## Steps

- [x] **1. Backend trait and the zlib-rs implementation**
  `inflate_backend.rs` with `InflateBackend`, `InflateStep`, `InflateOutcome`,
  implemented by the existing `RawInflater`.
  Verify: `cargo test -p rapidgzip-core --lib`.

- [x] **2. Switch the whole-stream call sites**
  Sequential gzip members, single-stream zlib and raw DEFLATE, and BGZF blocks
  go through `ActiveInflater`. The marker/window path and `IndexedReader` stay
  on `RawInflater`, with a comment saying why.
  Verify: full suite unchanged.

- [ ] **3. ISA-L backend behind the `isal` feature**
  `isal_backend.rs` implementing the trait over `isal-sys`, feature off by
  default.
  Verify: `cargo test -p rapidgzip-core --features isal` locally against
  Homebrew's isa-l 2.32.1.

- [ ] **4. CI job**
  A job installing `libisal-dev` and running the suite with `--features isal`.

- [ ] **5. Benchmark and documentation**
  A criterion benchmark comparing the backends on the same corpus, plus crate
  docs, `ARCHITECTURE.md`, `README.md`, and `CHANGELOG.md`. The measured
  numbers go in the pull request.

## Global constraints

- The default build keeps its current dependency set and behaviour.
- New `unsafe` only in the backend implementations, each block with a
  `// SAFETY:` comment.
- `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  and the full suite pass before each commit, with and without the feature.
- No em dashes in code, comments, or commit messages.
