# Random-access index and seeking

Date: 2026-08-01
Status: approved design, not yet implemented
Scope: sub-project 1 of 4 replacing PR #5 (COMBINE-lab/rapidgzip-rust)

## Background

PR #5 ("Most features completed") is a 23k-line single-commit-series contribution
that adds random-access indexes, multi-format decode, alternative inflate
backends, and a full CLI at once. It is not reviewable in that shape: it grows
`backend.rs` from 3806 to 8261 lines, mixes format parsing with decode
scheduling, and cannot be validated feature by feature.

The decision is to reimplement its useful parts as four independent
sub-projects, each on its own branch with its own pull request:

1. Random-access index and seeking (this document), including the streaming
   (non-seekable input) work already on the `streaming-input` branch.
2. Multi-format decode: zlib streams, raw DEFLATE, BGZF fast paths.
3. Alternative inflate backends (ISA-L FFI, pure-Rust inflate).
4. Full CLI parity with rapidgzip, including block analysis.

PR #5 stays open as a reference only; its format-level knowledge (on-disk
layouts, zran bit packing) is worth reusing, its structure is not.

## Goals

- Build a random-access index as a by-product of a normal decode, on the
  parallel, sequential, BGZF, and streaming paths alike.
- Persist and load that index in four formats: a native versioned format,
  indexed_gzip `GZIDX`, htslib BGZF `.gzi`, and gztool.
- Provide random access to decompressed data through a reader implementing
  `Read + Seek`.
- Keep new code out of `backend.rs`. No new `unsafe`.

## Non-goals

- Parallel indexed decode (using the index to split a range across workers).
  That is a follow-up sub-project once this one is stable.
- Writing or re-compressing gzip data.
- Line-oriented seeking beyond what a gztool index already stores.

## Architecture

```
crates/rapidgzip-core/src/
  index/
    mod.rs        Checkpoint, StoredWindow, WindowMap, GzipIndex, IndexError
    build.rs      IndexSink trait and IndexBuilder: collect, order, validate
    native.rs     native versioned serialization
    gzidx.rs      indexed_gzip GZIDX import/export
    gzi.rs        htslib BGZF .gzi import/export
    gztool.rs     gztool import/export
  indexed/
    mod.rs        IndexedReader<R: ReadAt>: Read + Seek
    window.rs     bounded LRU of expanded predecessor windows
```

Constraints on the layout:

- `index/` depends only on `std` plus the `libz-rs-sys` binding already in the
  crate, whose deflate side covers the zlib-wrapped window payloads that
  gztool and the native format use. No new dependency. `index/` has no
  knowledge of the decoder, so every format can be tested against synthetic
  indexes with no decode involved.
- `indexed/` depends on `index/` and on the existing sequential inflate path.
- `backend.rs` gains only calls to `IndexSink` at boundaries it already
  computes (`Chunk::start_bit`, `Chunk::end_bit`, and the resolved
  `backend_continuation` window). No index logic lives there.
- No file in `index/` or `indexed/` exceeds roughly 600 lines. When a format
  module approaches that, its reader and writer split into sibling files.

### Index construction

`IndexSink` is an internal trait with one significant method: accept a
candidate checkpoint carrying a compressed bit offset, an uncompressed byte
offset, and a predecessor window. Decode paths call it; `IndexBuilder`
implements it and is the only implementation initially.

Checkpoints from the parallel path are only valid once markers are resolved,
so they are emitted in the existing output-ordering step rather than at chunk
completion. This means index emission follows the same order as decoded bytes
and needs no post-hoc sorting, though `IndexBuilder` still asserts ordering.

Checkpoint spacing is configurable through the builder and defaults to the
chunk size, not one checkpoint per DEFLATE block. A member boundary always
produces a checkpoint with an empty window.

The streaming (non-seekable) path emits checkpoints identically. A streaming
decode can therefore produce an index that is useful for later random access
over the same data once it is available on disk.

### Index semantics

A `Checkpoint` holds:

- `compressed_offset_in_bits: u64`, absolute from the start of the source.
- `uncompressed_offset_in_bytes: u64`, absolute across all members.
- an optional predecessor window of exactly 32768 bytes, or empty.
- `line_offset: u64`, only meaningful for gztool indexes; zero otherwise.

An empty window means no history is required at that point: a member boundary,
the start of the stream, or an independent BGZF block. A non-empty window is
the 32 KiB of decompressed history that must be installed as the inflate
dictionary before resuming at that bit offset.

Windows may be held zlib-compressed in memory when the builder is configured
that way, which matters for large files where 32 KiB per checkpoint dominates
resident memory. Export always decompresses first, then applies the
format-specific encoding.

`GzipIndex::validate()` enforces: strictly increasing compressed and
uncompressed offsets, a window present whenever the point is not a member
boundary, window length of exactly 32768 when non-empty, and offsets within
the recorded source bounds when those are known.

### Random access

`IndexedReader<R: ReadAt>` owns a source and an index. A seek picks the last
checkpoint at or before the target uncompressed offset, installs its window as
the inflate dictionary, resumes inflate at the checkpoint bit offset, and
discards output up to the target. It is single-threaded and sequential by
design; throughput remains the job of `DecoderReader`.

Expanded windows are cached in a bounded LRU keyed by compressed bit offset so
that repeated nearby seeks do not re-inflate the same window. The cache has a
hard byte budget, not just an entry count.

`IndexedReader` is deliberately a separate type from `DecoderReader`. Bolting
`Seek` onto the parallel reader would make its ordering and cancellation
semantics conditional on whether an index exists.

Line-based seeking is exposed only when the index carries line counters, and
returns an explicit error otherwise rather than a silent fallback.

## Public API sketch

```rust
// build during a normal decode
let decoder = Decoder::builder().build_index(true).build()?;
let mut reader = decoder.open("reads.fastq.gz")?;
io::copy(&mut reader, &mut io::sink())?;
let report = reader.finish()?;
let index = report.index.expect("build_index was enabled");

// persist and reload
index.write_native(&mut file)?;
index.write_gzidx(&mut file)?;
index.write_gzi(&mut file)?;
index.write_gztool(&mut file, WithLines::No)?;
let index = GzipIndex::read_gzidx(&mut file, Some(archive_size))?;

// random access
let mut reader = IndexedReader::new(File::open(path)?, index)?;
reader.seek(SeekFrom::Start(4_000_000_000))?;
reader.read_exact(&mut buffer)?;
```

`DecodeReport` gains an `index: Option<GzipIndex>` field. Existing callers are
unaffected because index building is opt-in and defaults to off.

## On-disk formats

All four are implemented from their reference implementations, with the
constants and packing verified against them.

### Native

Versioned, little-endian, magic plus a `u16` version. Stores compressed and
uncompressed sizes, checkpoint spacing, checkpoint count, then checkpoints
with raw or zlib-compressed windows and an explicit payload length per window.
The only format under our control, so it stores everything the in-memory type
holds and round-trips exactly.

### indexed_gzip (GZIDX)

Magic `GZIDX`, `u8` version, `u8` flags, then little-endian: `u64` compressed
size, `u64` uncompressed size, `u32` checkpoint spacing, `u32` window size
(must be 32768), `u32` checkpoint count. Then one record per checkpoint:
`u64` compressed byte offset, `u64` uncompressed offset, `u8` bits field,
`u8` data flag (version 1 only). Window payloads follow as a contiguous block,
one fixed 32768-byte payload per checkpoint whose data flag is set.

Bit offsets use the zran packing: with `bits = offset % 8`, a zero remainder
stores `offset / 8` and a bits field of 0; otherwise it stores
`offset / 8 + 1` and a bits field of `8 - bits`. Decoding rejects a bits field
of 8 or more and rejects a non-zero bits field at byte offset 0.

Version 0 is accepted on import (no data flag; every checkpoint after the
first carries a window). Export always writes version 1.

### htslib BGZF `.gzi`

Little-endian `u64` pair count followed by that many
`(compressed_offset, uncompressed_offset)` `u64` pairs, listing block starts
after the first (the origin is implicit). No windows, no total uncompressed
size.

Export refuses any checkpoint with a non-empty window or a non-byte-aligned
compressed offset, because re-import would install empty windows and produce
wrong seeks. Import marks the uncompressed size as unknown. The pair count is
capped before allocation.

### gztool

Big-endian throughout. Header is eight zero bytes plus magic `gzipindx`
(version 0) or `gzipindX` (version 1, with line counters); version 1 adds a
`u32` line-number format field. Then the point count written twice (`have`
and `size`, which must be equal; growing indexes are rejected), then per
point: `u64` uncompressed offset, `u64` compressed byte offset, `u32` bits
field, `u32` compressed window length, the zlib-wrapped window payload, and,
in version 1, a `u64` line counter. The file ends with the `u64` uncompressed
size and, in version 1, the total line count.

Window payload length is validated against a maximum before allocation.

## Errors

`IndexError` is a new public, `non_exhaustive` error type, separate from
`DecodeError`, with variants for bad magic, unsupported version, invalid
window size, invalid or inconsistent checkpoint, truncated input, and I/O
failure. `io::ErrorKind::UnexpectedEof` maps to the truncated variant so that
short files report a format problem rather than a raw I/O error.

Index files are treated as untrusted input. Every count and length read from a
file is bounds-checked against an explicit maximum before it is used to
allocate or to reserve capacity.

## Testing

Test-driven: each unit gets its failing test before its implementation.

- Per format: round-trip of synthetic indexes, including empty indexes, single
  checkpoints, empty and non-empty windows, and non-byte-aligned offsets.
- Per format: rejection tests for bad magic, unsupported version, truncation
  at every header field, hostile counts, and inconsistent sizes.
- Golden files: short binary fixtures under `tests/data/`, produced by the
  reference tools, parsed byte for byte.
- Equivalence: for each corpus file, decode fully, then for a set of offsets
  seek with `IndexedReader` and compare against the full output slice.
  Corpora cover single-member gzip, concatenated members, BGZF, and streams
  whose checkpoints fall on non-byte-aligned bit offsets.
- Interop, ignored by default and enabled by a CI job that installs the tools:
  indexes we write are read by `indexed_gzip`, `bgzip`, and `gztool`, and
  indexes those tools write are read by us and produce identical seeks.
- Streaming: an index built from a non-seekable source equals the index built
  from the same bytes read positionally.

## Delivery

One branch, `index-and-seek`, based on the current `streaming-input` commits,
producing one pull request titled around random-access indexing, seeking, and
non-seekable input. Commits are atomic and ordered so the diff reads as:
index types, then each format, then the sink wiring, then `IndexedReader`,
then documentation.

Documentation updates in the same PR: the crate-level docs currently state
that index persistence and decoded-output seeking are out of scope, and
`ARCHITECTURE.md` needs the two new modules described.

Sub-projects 2 through 4 each get their own design document once this one
lands.
