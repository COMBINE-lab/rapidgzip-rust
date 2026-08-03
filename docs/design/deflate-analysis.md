# Bounded structural DEFLATE analysis

Status: implemented as the clean replacement for pull request #12

Scope: gzip, concatenated gzip, BGZF, zlib, and raw DEFLATE

Reference: rapidgzip 0.16.0 `--analyze`

## Goals

- Expose deterministic framing and DEFLATE structure to library callers rather
  than making a human-readable report the only interface.
- Walk each compressed symbol once, verify the complete container, and avoid
  retaining decompressed output.
- Treat every gzip member as a separate stream, including empty members and
  BGZF's conventional EOF member.
- Share format detection, gzip parsing, and native Huffman primitives with the
  decoder so analysis does not develop a second interpretation of accepted
  input.
- Bound result growth for adversarial block/member counts and optional header
  metadata.
- Make detailed predecessor-window references opt-in without weakening exact
  aggregate statistics.
- Provide a rapidgzip-compatible CLI report and differential tests without
  putting presentation quirks or timings in the core model.
- Add no dependency, no unsafe public API, and no new unsafe implementation.

## Non-goals

- Compression, index construction, or decoded-output seeking.
- Parallel analysis. Later blocks depend on output and history from all earlier
  blocks in the same stream, and the useful output of this operation is the
  ordered dependency structure itself.
- Replacing zlib-rs as the normal decode backend. The analyzer must inspect
  symbols that the backend API does not expose, so it uses the crate's native
  RFC 1951 primitives and independently authenticates the result.
- Retaining every individual LZ77 reference by default.
- Encoding rapidgzip's text layout into public Rust types.
- Reproducing a known unstable-sort defect in rapidgzip's merged-reference
  presentation.

## Public API

Positional callers use `ReadAt`:

```rust,no_run
use rapidgzip_core::{AnalyzeOptions, Decoder};
use std::fs::File;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let source = File::open("reads.fastq.gz")?;
let result = Decoder::default().analyze_with_options(
    &source,
    AnalyzeOptions::default()
        .maximum_streams(250_000)
        .maximum_blocks(500_000)
        .maximum_header_bytes(2 * 1024 * 1024)
        .maximum_retained_backreferences(20_000),
)?;
assert_eq!(result.compressed_size_in_bytes, source.metadata()?.len());
# Ok(())
# }
```

`Decoder::analyze` applies defaults. `Decoder::analyze_stream` and
`analyze_stream_with_options` accept forward-only `Read` sources. The latter
use the same structural engine; they do not spool or seek.

`Analysis` owns ordered `StreamAnalysis` and `BlockAnalysis` collections. A
stream identifies its range in the block vector and contains a typed
`StreamHeader` and verified `StreamFooter`. A block contains exact bit/byte
offsets and sizes, encoding type, dynamic alphabet shapes, symbol counts, and
predecessor-window facts. The input-wide reference-length histogram has fixed
size because RFC 1951 lengths are bounded by 258.

The owned collections mean `Analysis` is deliberately `Clone + Eq`, not
`Copy`. This does not affect `DecodeReport: Copy`: analysis is a separate
operation and result type, as index construction is. Public enums and structs
are non-exhaustive so future format facts can be added without treating the
current field set as a closed wire format.

## One authoritative framing path

Both positional and stream sources implement the internal `InputCursor`.
`AnalysisCursor` adds a little-endian bit buffer and an exact absolute bit
counter to that interface. Auto-detection calls the existing bounded
gzip/zlib resolver and retains its prefix for the real parse.

The gzip header parser has one implementation with an optional detail builder.
Ordinary decoding receives only `MemberHeader`; analysis additionally retains
FLG, MTIME, XFL, OS, FEXTRA, FNAME, FCOMMENT, verified FHCRC, and any BGZF
`BC/BSIZE` value. Reserved flags, truncation, malformed subfields, and header
CRC rules are therefore identical. Every member gets a fresh 32 KiB history
and checksum state.

Zlib analysis uses the normal CMF/FLG parser, enforces the declared CINFO
history limit, rejects FDICT as the decoder does, and checks Adler-32. Raw
DEFLATE has no framing checksum; acceptance still requires a real final block,
zero-to-byte padding consumption, and exact source end.

## Block and symbol walk

For each block the analyzer records the offset before BFINAL/BTYPE. Stored
blocks check LEN/NLEN, then copy bytes through the output state. Fixed blocks
use the process-wide fixed trees. Dynamic blocks retain the nineteen precode
length slots, the physically declared precode count, and the declared
literal/length and distance shapes after applying repeat symbols.

The production native decoder and analyzer implement one internal
`DeflateBits` trait. Huffman construction and table lookup are generic and
monomorphized. The production slice reader therefore keeps its current inline
word peek and has no dynamic dispatch; the analysis implementation refills
from `InputCursor`. Ordinary dynamic decoding asks not to retain code lengths,
so the new result work exists only on the explicit analysis path.

Decoded literals and matches update:

- a 32 KiB circular history;
- an 8 KiB staging buffer for CRC32 or Adler-32;
- exact literal, match-symbol, and copied-byte counters; and
- the configured total-output constraints.

No decoded chunk collection is created. A distance is rejected if it exceeds
the produced history or the zlib-declared maximum. Output-limit and exact-size
builder options have the same meaning as for decode.

## Predecessor-window semantics

A match is a predecessor-window reference when its distance is greater than
the number of bytes already emitted by the current block. To match rapidgzip's
definition, the recorded distance is the remaining reach before the block's
first output byte, and the reported length is capped at the original copy
distance.

Every such reference updates exact state even when no detail is retained:

- total reference count;
- input-wide length histogram;
- farthest predecessor reach;
- coverage of the 32 KiB predecessor window; and
- number of disjoint covered intervals.

The interval count is computed from a fixed coverage bitmap. It is deterministic
for equal-start or containing intervals. rapidgzip 0.16.0 sorts equal distances
unstably and can shorten a containing interval during its report-only merge;
the Rust core intentionally reports the true interval union. Used-window byte
coverage is exposed for blocks producing at least 32 KiB, matching the
reference's reporting condition.

`maximum_retained_backreferences` is an input-wide allowance. Once exhausted,
analysis continues, increments `omitted_backreference_count` on each affected
block, and preserves every aggregate above. `Analysis::has_complete_backreference_details`
distinguishes a complete detail set from a summary-only result.

## Resource and failure model

`AnalyzeOptions::default` permits:

- 100,000 streams;
- 100,000 DEFLATE blocks;
- 1 MiB of retained optional gzip metadata across all members; and
- zero retained individual predecessor-window references.

The fixed 32 KiB history, checksum staging buffer, and per-block 32 KiB
coverage scratch do not scale with input or decoded size. Dynamic alphabet
storage is bounded by RFC 1951. Vector growth for streams, blocks, header
metadata, alphabet copies, and retained references uses fallible reservation.
All externally meaningful bit, byte, and structural counters use checked
arithmetic.

`DecodeError::Analysis` contains `AnalysisErrorKind`, distinguishing a named
resource limit, allocation failure, and counter overflow. Invalid framing,
DEFLATE, checksum, size, and I/O errors retain their existing variants. A
partial `Analysis` is not returned after failure, and no validated footer is
published before its checksum and size agree.

The optional-header budget is aggregate rather than per member. This matters
for multi-member and BGZF inputs: combining a large per-header allowance with a
large member allowance would be mathematically bounded but permit impractical
aggregate memory. Callers analyzing unusually metadata-heavy archives can
raise the explicit input-wide value.

## CLI compatibility layer

`rapidgzip-rust --analyze` renders the structured result after successful
verification. `--verbose` retains reference details, bounded by
`--analysis-reference-limit`; stream, block, and optional-header limits also
have explicit flags. Decoder parallelism and handoff chunk size are rejected
for this operation because they cannot change a causal single-threaded walk.
Standard input is supported through `analyze_stream_with_options`.

The CLI owns C++-compatible general/scientific number formatting, bit and byte
units, histograms, OS/XFL labels, and report ordering. It writes incrementally
rather than constructing one report string. Locally measured time is kept in a
separate CLI `Timings` value; it is deliberately absent from `Analysis` and
from deterministic equality.

Differential tests compare the report with rapidgzip 0.16.0 while masking
timings and the merged-reference count affected by the reference defect. CI
pins that version so upstream presentation changes cannot silently redefine
the compatibility target.

## Safety

The analyzer is safe Rust. History indices are used only after distance checks,
fixed arrays carry the DEFLATE window and checksum staging, and cursor bit
positions are checked. Sharing the generic Huffman layer does not move unsafe
code into analysis or alter the production reader's existing unaligned-load
safety proof. No caller must uphold a new unsafe contract.

## Validation and performance gates

Focused tests cover all three framing formats, detection through one-byte
reads, stored/fixed/dynamic blocks, dynamic alphabet metadata, concatenated and
empty members, BGZF, optional headers and aggregate metadata limits, reference
retention, exact summaries, block/stream limits, paged positional reads,
output constraints, trailing bytes, and corrupt checksums.

The complete workspace still runs formatting, warning-free Clippy, every unit
and integration target, Rustdoc with warnings denied, and MSRV CI. Ignored
differential tests exercise large, tiny, concatenated, and stored gzip inputs
against the pinned C++ tool.

The `structural_analysis_fastq` Criterion group compares analysis with verified
one-worker decode over the same generated 16 MiB FASTQ-like gzip member. It is
not a parallel throughput target. The gate is stable bounded overhead and no
regression in ordinary decoding; the normal decoder remains monomorphized and
does not retain analysis metadata.
