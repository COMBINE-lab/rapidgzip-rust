# Index-Driven Parallel Decoding Implementation Plan

> Execution checklist for `docs/superpowers/specs/2026-08-01-indexed-parallel-design.md`. Boxes are ticked as each step lands, one commit per step.

**Goal:** When an index is available, put every worker on plain zlib over its own span, with no speculation and no marker resolution.

**Branch:** `indexed-parallel`, stacked on `marker-threshold` (PR #13).

## The question the design left open

Spans are delimited by checkpoints, which fall wherever the index put them.
Gzip members are delimited by footers, which fall wherever the archive put
them. The two do not line up, so a member's CRC32 is generally split across
several spans, and no single worker can compute it.

`libz_rs_sys::crc32_combine64(crc_a, crc_b, len_b)` returns the CRC of the
concatenation. It is a safe function and already a dependency.

So each worker returns, for its span, the CRC of the whole span plus the
decompressed offsets at which it observed a member end, which zlib reports as
`Z_STREAM_END`. The coordinator, which sees spans in order, folds those into
per-member CRCs by combining the trailing part of one member with the leading
part of the next. ISIZE per member follows from the same offsets. Verification
is therefore exactly as strict as every other path, which is the bar this crate
has set.

A span that ends mid-member contributes its whole CRC to the member in
progress. A span containing a member end contributes the part before it to that
member and starts the next one after it.

## Steps

- [x] **1. Supplying and validating the index**
  `DecoderBuilder::index(Option<GzipIndex>)`, stored in `Config`. A helper
  decides whether the indexed path applies: an index is present, it validates,
  its recorded compressed size matches the source, it holds at least three
  checkpoints, and the worker budget is at least two.
  Verify: unit tests for each rejection, and that a rejected index leaves the
  existing dispatch untouched.

- [x] **2. Span planning**
  Consecutive checkpoints become spans carrying the compressed byte range to
  read, the resume bit offset, the window, and the exact decompressed length.
  A span longer than `decoded_chunk_size` is decoded in several passes inside
  one task rather than buffered whole, so a sparse index does not decide
  memory.
  Verify: planning tests over dense, sparse, and single-member indexes,
  asserting spans tile the output exactly and no span exceeds the budget.

- [x] **3. The worker**
  Reads its compressed range, resumes with `prime` and `set_dictionary_bytes`
  exactly as `IndexedReader::resume` does, inflates to the span's known length,
  and returns the output, its CRC32, and the offsets where members ended.
  Verify: a worker decoding one span produces bytes identical to the
  sequential decoder over the same range.

- [x] **4. Ordered coordination and verification**
  A bounded worker pool modelled on `decode_bgzf_parallel`, emitting spans in
  order. The coordinator folds span CRCs with `crc32_combine64` into per-member
  CRC32 and ISIZE and checks both against each member's footer.
  Verify: a corrupted member fails with `ChecksumMismatch` naming the right
  member, and a corrupted span fails before its bytes are emitted.

- [x] **5. Dispatch, telemetry, and the CLI**
  `DecoderPath::Indexed`, dispatched before the speculative grid.
  `--import-index` stops erroring without `--ranges`.
  Verify: the path-selection test gains the indexed case; a CLI test decodes
  with an imported index and compares against decoding without one.

- [x] **6. Measurement and documentation**
  A benchmark comparing the indexed path against the speculative one at equal
  worker counts. `README.md`, `ARCHITECTURE.md`, `CHANGELOG.md`, crate docs.
  The measured numbers go in the pull request, including the case where the
  prediction of roughly 1300 MiB/s at two workers does not hold.

## Global constraints

- MSRV 1.87, Rust 2024, no let-chains, no new dependencies.
- Verification stays exactly as strict: every member's CRC32 and ISIZE checked
  before its bytes are accepted.
- `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  and the full suite pass before each commit.
- No em dashes. Commits end with the Claude co-author trailer.
