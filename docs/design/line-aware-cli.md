# Line-aware decoding and CLI expansion

## Status

Implemented as the clean successor to pull request #11. The implementation is
based on the current `DeflateIndex`, explicit indexing operations, multi-format
decoder, adaptive worker runtime, and strict indexed full-stream decoder. It
does not include #11's stacked backend or superseded index changes.

## Goals

- Count newline bytes through every verified decode interface without changing
  the default data path.
- Preserve `DecodeReport: Copy`.
- Annotate retained index checkpoints exactly when counting and indexing are
  both requested.
- Seek to zero-based lines without scanning from decoded offset zero.
- interoperate correctly with gztool's line-aware version 1 index format.
- Provide practical rapidgzip-compatible CLI operations for decoding, testing,
  counting, index import/export, and byte or line ranges.
- Make imported indexes strict inputs to full-stream decoding rather than
  advisory metadata.
- Reject command-line options whose semantics are not implemented.

## Non-goals

- Compression or index construction without decoding.
- Sparse predecessor-window transformation.
- Alternate shared-cursor or locked-read input implementations.
- Disabling container verification.
- `--analyze`, which belongs to the separately designed DEFLATE analyzer.
- Claiming complete command-line parity with every rapidgzip release.

## Public library API

`DecoderBuilder::count_lines(bool)` controls line counting. It is off by
default. A successful operation returns the result through:

```rust
pub struct DecodeReport {
    // Existing scalar fields.
    pub line_count: Option<u64>,
}
```

The optional scalar preserves `Copy`, equality, and cheap report access. An
owned line table does not belong in `DecodeReport`; index metadata remains in
`IndexedDecodeReport::index`.

`DeflateIndex::checkpoint_at_or_before_line(u64)` returns a checkpoint only if
the index has a total line count and every checkpoint is annotated.
`IndexedReader::seek_to_line(u64)` returns the decoded byte offset at which the
requested zero-based line begins.

Line count means the number of byte values equal to `b'\n'`. This is independent
of UTF-8 or other text encodings. A final unterminated line contributes no
newline to the count. The convention matches an offset metric: checkpoint
`line_offset` is the number of newline bytes strictly before its decoded byte
offset.

## Ordered counting

Speculative marker output cannot be counted in a worker. Its 16-bit symbols may
refer to an unknown predecessor byte, including a newline. Counting therefore
happens only at the final ordered `Output` handoff, after marker replacement.

Push decoding wraps its destination in a decode-local `LineCountingOutput`.
The wrapper scans each final chunk immediately before forwarding it. The
pull-driven non-seekable reader has no `Output` adapter, so it applies the same
`LineCounter` before publishing each final chunk to the reader. Strict indexed
parallel decoding applies it at the ordered span coordinator.

When counting is disabled, `LineCounter::note_output` returns after one branch.
It allocates nothing and leaves `line_count` as `None`. No crate dependency is
required. Enabled counting uses runtime-dispatched AVX2 with an x86-64 SSE2
baseline, AArch64 NEON, and a scalar fallback. All vector loads are slice-bound
checked and the safety argument is recorded in `SAFETY.md`.

## Checkpoint annotation

Index construction stays decode-local and separate from `RuntimeState`.
`IndexCollector` receives an `annotate_lines` flag only for an operation that
requested both indexing and line counting.

The collector merges two monotonically ordered inputs without auxiliary search
trees:

1. retained checkpoint decoded offsets; and
2. final emitted byte ranges plus the line count preceding each range.

One cursor advances through the retained checkpoint vector. For a checkpoint
inside `[start, end)`, the collector scans from the preceding checkpoint within
that same output slice and records the exact prefix count in place. A
checkpoint at decoded EOF is resolved during finalization. Multiple empty gzip
members can have distinct compressed offsets but the same decoded offset; they
correctly reuse the same resolved line count.

The collector returns the newline count from that same scan to the outer
counter, including the suffix after the final checkpoint. Counting plus index
construction therefore traverses each output byte once rather than rescanning
checkpoint-bearing chunks.

The decoder paths currently offer authoritative checkpoints before output
passes them. The collector nevertheless treats this as a checked invariant. A
late checkpoint remains pending. If any retained checkpoint is unresolved at
successful decode finalization, all per-checkpoint line offsets and the total
are cleared. This all-or-none rule prevents a serializer or reader from
mistaking a partial table for complete metadata.

Index validation rejects line offsets greater than their decoded offsets,
decreasing line offsets, checkpoint counts greater than the recorded total, and
a total line count greater than the known decoded size.

## Seeking by line

Line seeking chooses the latest checkpoint proven to precede the requested
line's start. For targets after line zero, that means a checkpoint with a
strictly smaller line offset: a checkpoint with the same count may already sit
inside a very long line. Line zero uses a checkpoint at decoded offset zero.
`IndexedReader` resumes there using the same framing and predecessor-window
rules as byte seeking. It scans decoded bytes until it consumes the remaining
newlines, then seeks back over any overshoot in the final scratch buffer.

For an in-range line, work is bounded by the distance to the previous retained
checkpoint. Seeking beyond the final line consumes to decoded EOF and returns
that byte position. An index without complete line metadata returns
`io::ErrorKind::Unsupported`; silently scanning the full stream would defeat
the API's indexed-access contract.

## gztool convention

gztool version 1 stores one-based line numbers at checkpoints. The Rust model
stores preceding newline counts and is therefore zero-based. Its codec writes
`line_offset + 1` and reads `external_line - 1`. External zero is rejected as
invalid, and internal `u64::MAX` cannot be represented in the shifted format.
The total newline count is not shifted.

Version 1 export requires a total and a line offset for every checkpoint.
Missing metadata is an error; the writer never fills absent values with zero.
Version 0 export remains available for indexes without line metadata.

## CLI structure

The binary is divided by responsibility:

- `main.rs`: argument contract, validation, and operation dispatch;
- `source.rs`: regular-file versus streaming classification and safe output
creation;
- `index.rs`: bounded streaming format detection, strict import, and
  transactional export;
- `ranges.rs`: `SIZE@OFFSET` parsing and ordered extraction;
- `report.rs`: test, count, line-count, and verbose output; and
- `attributions.rs`: linked dependency licenses.

The CLI defaults to gzip/zlib auto-detection, while the library retains strict
gzip as its compatibility default. Raw DEFLATE must be selected explicitly.
`-P`/`--decoder-parallelism` is a maximum budget; `--threads` is an alias and
zero retains the machine-derived builder default. `--chunk-size` maps to the
decoded handoff size in KiB, matching the meaning of rapidgzip's decoded chunk
rather than the internal compressed marker-grid spacing.

Without `-c` or `-o`, redirected stdout receives decoded bytes. At an
interactive terminal, a regular input derives an output filename by removing a
known compression suffix case-insensitively or adding `.out`. Existing output
is preserved unless `--force` is supplied. Validation and count actions use a
sink unless combined with explicit output. When decoded payload occupies
stdout, byte and line count results are routed to stderr so the payload remains
unmodified.

## Index operations

Without `--import-index`, `--export-index` selects an explicit indexing decode.
The exported `DeflateIndex` is written after complete stream verification.
Supported outputs are native version 1, indexed_gzip GZIDX, gztool versions 0
and 1, and BGZF `.gzi`; format provenance checks remain in the core serializers.
Native is the format-neutral CLI default. Serialization uses a completed
same-directory temporary file and only then installs the destination, so a
conversion or write failure never truncates the previous index.

With `--import-index`, a full-stream operation calls `Decoder::decode_from_index`.
Every checkpoint boundary and decoded span size must match, and errors never
fall back to an ordinary decoder. An imported index may be re-exported only
after this strict decode verifies the source. Line-aware re-export requires the
imported index itself to contain complete line metadata. Line counting during
strict indexed decoding recomputes every imported checkpoint annotation and
the total; a mismatch is an error.

Range extraction requires a regular positional file. Without an imported
index, the CLI performs one verified sink decode to build one. A line-addressed
range enables checkpoint annotation automatically. With an imported index, a
line range requires complete line metadata before extraction begins. An
interior checkpoint cannot authenticate the skipped prefix. `--verify` therefore
runs a complete strict indexed validation pass before imported extraction;
without it, decoder format, size, thread, and chunk options that cannot affect
random access are rejected instead of ignored.

## Range grammar

`--ranges` accepts comma-separated `SIZE@OFFSET` elements. Each finite quantity
is independently one of:

- bare bytes or a `B` suffix;
- binary `KiB`, `MiB`, `GiB`, `TiB`, `PiB`, or `EiB`; or
- a zero-based line quantity with `L`.

`inf` is accepted only as a size and means all remaining output. Arithmetic is
checked. Ranges are executed in input order, are not merged, and may overlap.
A line size ends just after its requested newline or at decoded EOF.

## Compatibility boundaries

The CLI accepts options that describe behavior already in force:

- `-d` / `--decompress`;
- `-k` / `--keep`, because inputs are never deleted;
- `--verify`; it requests a complete pass for imported ranges and is already
  satisfied by ordinary complete decode actions;
- `--no-sparse-windows`, because complete windows are retained; and
- `--io-read-method pread` for regular files.

It rejects options that would otherwise lie about behavior:

- `--no-verify`;
- `--sparse-windows`;
- `--io-read-method sequential`; and
- `--io-read-method locked-read`.

## Correctness and performance validation

Core tests cover disabled counting, trailing and unterminated lines, empty
output, sequential and marker-parallel gzip, concatenated and empty members,
BGZF, every positional and streaming pull-index surface, zlib, raw DEFLATE,
strict indexed decoding, imported line-metadata authentication, exact
checkpoint annotations, line seeks, and missing-metadata rejection.

Format tests pin gztool's one-based field encoding and reject zero external
line numbers. Optional interoperability tests let real gztool extract a line
through an index written here and compare the result with `seek_to_line`.

CLI integration tests execute the built binary and cover output and alias
safety, combined output/count actions, broken pipes, quiet/verbose reporting,
every index family, strict bounded imported-index parsing, transactional export
failure, zlib/raw native indexes, full indexed decoding, verified and partial
range semantics, byte/line/mixed/overlapping ranges, stdin, compatibility
aliases, semantic rejections, corrupt input, and complete attributions.

The newline scanner adds narrowly scoped SIMD intrinsics. Runtime AVX2
detection, the x86-64 SSE2 baseline, the AArch64 NEON baseline, slice-bounded
unaligned loads, and scalar differential tests form its safety argument; the
same argument is recorded in `SAFETY.md`. No other unsafe code is introduced.
