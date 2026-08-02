# Random-access DEFLATE indexing and seeking

Status: implemented by PR #8 and generalized by PR #9

Scope: gzip, BGZF, zlib, and raw-DEFLATE decoding and seeking

Companion: `multi-format-decode.md`

## Goals

- Collect a random-access index as an explicit by-product of a verified decode.
- Keep ordinary decode APIs and `DecodeReport` source-compatible, `Copy`, and
  free of index-construction work.
- Resume decoded reads through a `Read + Seek` adapter without decoding from
  the beginning of the archive.
- Correctly cross ordinary concatenated gzip members, empty members, and BGZF
  blocks, and seek within single zlib or raw-DEFLATE streams.
- Support the native rapidgzip-rust format, indexed_gzip GZIDX, htslib `.gzi`,
  and gztool indexes with bounded parsing of untrusted data.
- Preserve the pull-driven, zero-background-thread implementation for
  non-seekable input introduced by PR #7.
- Keep indexing overhead out of runtime telemetry and adaptive scheduling.
- Add no crate dependency and no unsafe public API.

## Non-goals

- Encoding or modifying gzip data.
- Parallel decoding *from* an existing index. `IndexedReader` is deliberately
  single-threaded; parallel indexed range decoding is separate work.
- Authenticating output resumed inside a member when the imported format does
  not contain the checksum state of the skipped prefix.
- Treating line counters as present when they were not supplied by an imported
  index. Line-oriented seeking/export is deferred until it has an end-to-end
  API and tests.

## Public result types

`DecodeReport` remains the small scalar result returned by every existing
decode operation:

```rust
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DecodeReport {
    pub compressed_bytes: u64,
    pub decompressed_bytes: u64,
    pub member_count: u64,
    pub decoder_threads: usize,
    pub format: Format,
}
```

An indexing operation returns a distinct owning result:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedDecodeReport {
    pub decode: DecodeReport,
    pub index: DeflateIndex,
}
```

`IndexedDecodeReport` provides:

- `report(&self) -> &DecodeReport`;
- `index(&self) -> &DeflateIndex`;
- `into_parts(self) -> (DecodeReport, DeflateIndex)`;
- `AsRef<DecodeReport>`.

`DecodeReport` also implements `AsRef<DecodeReport>`. Generic consumers that
only need scalar decode statistics can therefore accept
`T: AsRef<DecodeReport>`:

```rust
fn record_decode<T: AsRef<DecodeReport>>(value: &T) {
    let report = value.as_ref();
    eprintln!("decoded {} bytes", report.decompressed_bytes);
}
```

No new common project-specific trait is introduced initially. `AsRef` covers
the useful generic operation without expanding the semver surface. `Deref` is
not implemented because field/method forwarding would obscure that the index
belongs only to one result. `Borrow` is not implemented because its equality
and hashing equivalence convention does not describe the two result types.
There is no lossy `From<IndexedDecodeReport> for DecodeReport`; callers use
`into_parts` when they intentionally dispose of the index.

Code that chooses indexing at run time must use its own enum or branch before
the call, because Rust functions have one concrete return type. This is not a
cost for the normal API: callers choose `decode` or `decode_with_index` at
compile time.

## Public decode API

Index construction is an operation, not a persistent decoder-builder boolean:

```rust
let options = IndexOptions::default();
let result = decoder.decode_with_index(&source, &mut output, options)?;

let mut reader = decoder.reader_with_index(source, options)?;
io::copy(&mut reader, &mut output)?;
let result = reader.finish()?;
```

The full surface is:

- `Decoder::decode_with_index` for positional push decoding;
- `Decoder::reader_with_index` for positional `Read + Send` output;
- `Decoder::decode_stream_with_index` for non-seekable push decoding;
- `Decoder::stream_reader_with_index` for pull-driven non-seekable input.

`IndexingDecoderReader` delegates `Read`, `handle`, `stats`, and
`set_worker_limit` to the same internal reader core as `DecoderReader`. Its
`finish` and post-EOF `report` return `IndexedDecodeReport`. Ordinary reader
construction cannot accidentally create an indexed completion, and indexed
reader construction cannot produce a plain completion.

`IndexOptions` is `Copy` and contains:

- a non-zero target decompressed checkpoint spacing, defaulting to 4 MiB;
- a window storage policy (`Raw` or `Zlib`), defaulting to zlib compression.

Indexing failures are represented by `IndexingError::{Decode, Index}`. A
window-codec or index-finalization failure is never mislabeled as source I/O.
If index finalization fails after decoding succeeds, already-emitted output is
still a completely verified decode.

## In-memory index model

### Index and checkpoint kinds

`IndexKind` describes source/container provenance:

- `Gzip`: ordinary gzip, including concatenated members;
- `Bgzf`: a stream proven to consist of BGZF blocks;
- `Zlib`: one RFC 1950 zlib stream;
- `RawDeflate`: one unwrapped RFC 1951 stream.

Only `Bgzf` indexes may be exported as `.gzi`.

Every `Checkpoint` explicitly carries a `CheckpointKind`:

- `GzipMemberHeader`: the compressed offset is the first gzip magic byte. It must
  be byte-aligned and requires no predecessor window. Resume parses the header
  before raw inflate.
- `GzipMemberDeflate`: the compressed offset is the byte-aligned raw-DEFLATE
  payload, and the checkpoint separately retains its member-header byte
  offset. This preserves full CRC32/ISIZE verification while allowing export
  to formats that store raw-DEFLATE positions.
- `ZlibHeader`: the compressed offset is the zlib CMF byte at source origin;
- `RawDeflateStart`: the compressed offset is the raw stream's first bit at
  source origin;
- `DeflateBlock`: the compressed offset is the first bit of a DEFLATE block.
  It may be bit-aligned and requires a 32 KiB predecessor window unless the
  external format or decoder proves the block independent.

This removes gzip-magic sniffing. Legal DEFLATE bytes that resemble a gzip
header cannot cause the reader to skip input.

The checkpoint fields are:

- absolute compressed bit offset;
- absolute decompressed byte offset across all members;
- checkpoint kind;
- optional preceding line count.

Known compressed size, known decompressed size, target spacing, checkpoint
line offset, and total line count use `Option` rather than conflicting numeric
sentinels. On-disk formats may have sentinel encodings, but codecs normalize
them at the boundary.

### Windows

A non-empty `StoredWindow` represents exactly 32 KiB of predecessor history.
It is stored raw or as a complete zlib stream. Constructors reject every other
expanded length. Validation expands compressed windows, checks the exact
length, and requires the zlib payload to be consumed completely.

The absence of a window is valid for every explicit framing start and an
independently proven `DeflateBlock`. Interior checkpoints in zlib and raw
streams require a stored predecessor window. The model records the point kind;
absence alone is never used to infer how to resume.

### Ordering and empty members

Compressed offsets are strictly increasing. Decompressed offsets are
non-decreasing so that empty gzip members and BGZF EOF blocks remain
representable. A lookup at an offset shared by multiple checkpoints chooses
the last checkpoint, avoiding needless traversal of preceding empty members.

All offsets must fit known source bounds. The index's known compressed size is
compared with `ReadAt::len` when opening `IndexedReader`.

## Index construction

An `IndexCollector` is separate from `RuntimeState`. Runtime telemetry and the
adaptive controller never own windows or perform window compression.

Decode paths submit only boundaries that have become authoritative in output
order:

- sequential positional and streaming paths submit every parsed member's raw
  DEFLATE offset together with its header offset, or the single zlib/raw stream
  start;
- BGZF submits every proven non-empty block with both offsets; its conventional
  empty EOF member is decoded and verified but need not be a seek target;
- dense-member decoding submits a member header only when its preceding chain
  has been authenticated by the coordinator;
- stored-stream decoding submits member headers and independent stored-block
  starts whose decompressed offsets are exact;
- the marker/window path submits resolved chunk starts with their predecessor
  windows, plus exact member headers.

Member and independent-block boundaries are always retained. Interior points
are thinned online before their window is copied or compressed. A candidate is
kept when its decompressed distance from the most recently retained point is
at least the requested spacing. This bounds work and resident memory by the
final index rather than by every discovered DEFLATE boundary.

The collector has a small synchronization boundary only because push,
background positional reader, and pull-driven streaming ownership differ. All
offers for one decode are committed in output order. Expensive window copying
and compression happen outside the collector lock; the lock protects only the
ordered index state and a deferred error.

## `IndexedReader` integrity model

`IndexedReader<R: ReadAt>` owns a stable positional source, a validated index,
a raw inflater, and a byte-bounded expanded-window cache.

On seek it selects the last checkpoint at or before the target, resumes
according to `CheckpointKind`, installs the predecessor dictionary for a raw
DEFLATE block, and discards decoded bytes until the requested position.

Compressed input exhaustion before `Z_STREAM_END` is always truncated
DEFLATE. `Z_BUF_ERROR` with no progress at source EOF is never clean EOF.

At every `Z_STREAM_END`, the reader applies the index kind's terminal rule:
eight gzip footer bytes, four zlib Adler-32 bytes, or exact raw source end.
Short trailers and trailing bytes are errors.

Integrity depends on the resume point:

- From `GzipMemberHeader` or `GzipMemberDeflate`, the reader parses the header and
  hashes every produced byte (including bytes discarded to reach the seek
  target), tracking member output modulo 2^32. CRC32 and ISIZE must match
  before the member is accepted.
- From an interior `DeflateBlock`, foreign indexes do not supply the skipped
  prefix checksum state. The reader parses the footer structurally but cannot
  authenticate that member's whole CRC32 or ISIZE and documents this limit.
- After crossing the next member header, full verification resumes for every
  later member.
- From `ZlibHeader`, the reader validates CMF/FLG, enforces CINFO, and checks
  Adler-32. `RawDeflateStart` has no checksum to verify.

The native format reserves room for future prefix-checksum state, but that is
not required for version 1 interoperability.

## Native format version 1

The native format is finalized before its first release. It is little-endian
and begins with `RGZIDX01`, a `u16` version, and a `u16` known-flags field.
Flags encode optional aggregate fields and gzip/BGZF/zlib/raw provenance.
Unknown or mutually conflicting source-kind flags are rejected.

Each checkpoint stores checkpoint kind, optional member-header offset, and
window kind. An explicit checkpoint flag records whether line metadata is
present. Payload lengths use checked arithmetic and are bounded by read
options. Writers validate the whole index before writing anything.

## External formats

### indexed_gzip GZIDX

Versions 0 and 1 are read; version 1 is written. Flags and per-point data flags
must use supported values. Spacing that does not fit `u32` is an export error,
not a saturated value. Imported offsets are normalized as raw DEFLATE resume
points according to indexed_gzip's format semantics.

### htslib `.gzi`

Pairs are BGZF member-header offsets, with origin implicit. Import produces an
`IndexKind::Bgzf` index with `GzipMemberHeader` points. Export accepts
`GzipMemberDeflate` points only when their retained header offsets are available,
and otherwise requires member-header points, BGZF provenance, byte alignment,
and absent windows.

### gztool

Both versions are read. Unsupported line-number formats are rejected. Version
0 is written initially. Version-1 writing is rejected unless every checkpoint
and aggregate line counter is present; it is not synthesized with zeroes.

## Bounded untrusted parsing

Every reader accepts `IndexReadOptions`. Defaults are useful for ordinary
large indexes while bounding failure:

- maximum checkpoint count;
- maximum aggregate stored-window bytes;
- maximum individual payload bytes.

All count-to-byte calculations use checked arithmetic. Allocation uses
`try_reserve`; allocation failure becomes `IndexError`. Aggregate budgets are
checked before reading or allocating payloads. Convenience `read_*` methods
use defaults, while `read_*_with_options` allows trusted applications to raise
limits deliberately.

Writers call `validate` before emitting. Format flags, versions, counts,
payload lengths, size conversions, and trailing compressed-window bytes are
validated rather than ignored or truncated.

## Tests and performance gates

Correctness tests cover:

- full and scattered reads from gzip-member, zlib/raw-start, and interior
  checkpoints;
- truncated DEFLATE, truncated footer, CRC32 mismatch, and ISIZE mismatch;
- concatenated and empty members, BGZF including its EOF block, stored-only
  streams, dense members, and marker/window streams;
- false gzip magic at a raw DEFLATE checkpoint;
- source-size/index mismatch;
- exact window invariants and compressed-payload trailing bytes;
- equal decompressed offsets;
- hostile counts, checked-size overflow, allocation limits, flags, versions,
  and every truncated format prefix;
- native and external round trips plus ignored real-tool interoperability.

Performance validation reports indexing disabled and enabled for retained
FASTQ, multi-member, and BGZF inputs at representative worker counts. It
records throughput, peak RSS when available, checkpoint count, index size, and
seek latency. Indexing disabled must show no material regression relative to
current `main`.

## Delivery

Implementation is based directly on current `main`, not the pre-PR-#7 stream
coordinator in the original PR branch. Reusable index/format code may be
transplanted, but pull-driven streaming, telemetry semantics, and current path
selection remain authoritative.

The work is committed in reviewable layers on PR #8's existing head branch:

1. specification and in-memory model;
2. native and bounded external codecs;
3. indexed reader and integrity tests;
4. operation-specific push/pull APIs and all-path index collection;
5. docs, interoperability, and performance evidence.
