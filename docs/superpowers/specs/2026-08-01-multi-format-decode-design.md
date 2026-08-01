# Multi-format decode: zlib and raw DEFLATE

Date: 2026-08-01
Status: approved design, not yet implemented
Scope: sub-project 2 of 4 replacing PR #5 (COMBINE-lab/rapidgzip-rust)

## Background

The crate decodes gzip, concatenated gzip, and BGZF. PR #5 also accepted zlib
streams (RFC 1950) and raw DEFLATE (RFC 1951) behind a `Format` enum, but only
on its sequential path, so those formats lost every benefit of the parallel
decoder.

Sub-project 1 (random-access indexing) is in review as PR #8. This branch
stacks on it, so zlib and raw DEFLATE support indexing and seeking from the
start rather than acquiring it in a later pass.

## Goals

- Decode zlib streams and raw DEFLATE with the same verification discipline the
  crate applies to gzip.
- Run both through the parallel estimated-grid path, not just the sequential
  one, since each is a single DEFLATE stream, which is exactly what that path
  splits.
- Detect gzip against zlib automatically; require an explicit choice for raw
  DEFLATE, which has no magic bytes.
- Support both formats on non-seekable input and in the random-access index.

## Non-goals

- Concatenated zlib streams. A zlib stream is decoded as one stream; bytes
  after its trailer are an error, as trailing bytes after a gzip member are.
- Preset dictionaries (`FDICT`). A zlib header requesting one is rejected.
- Encoding, and the CLI flags that expose these formats, which belong to
  sub-project 4.

## Formats

### zlib (RFC 1950)

Two header bytes, `CMF` and `FLG`. The header is valid when the compression
method is 8, `CINFO` is at most 7 (a 32 KiB window), and `(CMF << 8 | FLG)` is
a multiple of 31. Bit 5 of `FLG` is `FDICT`, which this crate rejects.

The DEFLATE stream follows immediately. The trailer is a four-byte big-endian
Adler-32 of the decompressed output, always verified.

### Raw DEFLATE (RFC 1951)

No header, no trailer, no checksum. The stream is the DEFLATE data alone,
ending at a final block. After byte-aligning past that block, any remaining
byte is trailing garbage and an error.

Because nothing in the format is verifiable beyond DEFLATE's own structure, the
builder accepts an optional expected decompressed size, checked when supplied.

## Public API

```rust
/// Container framing of the compressed input.
pub enum Format { Auto, Gzip, Zlib, RawDeflate }

let decoder = Decoder::builder()
    .format(Format::Zlib)
    .build()?;

let decoder = Decoder::builder()
    .format(Format::RawDeflate)
    .expected_uncompressed_size(Some(4_000_000))
    .build()?;

let report = decoder.decode(&source, &mut output)?;
assert_eq!(report.format, Format::Zlib);
```

`Format::Auto` is the default and preserves today's behavior for gzip and BGZF
input. It reads the first two bytes: `1f 8b` selects gzip, otherwise a valid
`CMF`/`FLG` pair selects zlib, and anything else is a gzip magic error, as
today. Auto never selects raw DEFLATE, because every raw stream would otherwise
be indistinguishable from corrupt input.

`DecodeReport` gains `format: Format`, always a concrete variant, reporting
what was actually decoded.

`expected_uncompressed_size` applies to raw DEFLATE only. Supplying it with
another format is a `ConfigError`, because gzip and zlib carry their own
trailers and a second, weaker check would be misleading.

## Errors

A new `ZlibErrorKind`, reached through a new `DecodeError::InvalidZlib`
variant, covers the framing failures the gzip variants cannot express:

- `BadHeader`: the `CMF`/`FLG` pair is not a legal zlib header.
- `UnsupportedCompressionMethod(u8)`: the method is not DEFLATE.
- `UnsupportedWindowSize(u8)`: `CINFO` is above 7.
- `PresetDictionary`: `FDICT` is set.
- `Truncated`: the header or the Adler-32 trailer is incomplete.
- `ChecksumMismatch { expected, actual }`: the trailer disagrees with the
  output.
- `TrailingGarbage`: bytes follow a complete stream.

Raw DEFLATE reuses the existing `DeflateErrorKind` for stream failures, the
gzip `TrailingGarbage` kind for bytes after the final block, and a new
`DecodeError::SizeMismatch`-style check for a supplied expected size, reusing
the existing `SizeMismatch` variant with member zero.

## Architecture

```
crates/rapidgzip-core/src/
  format.rs      Format enum, detection from the source prefix
  zlib.rs        zlib framing: header parse, Adler-32, trailer verification
  backend.rs     framing-aware entry points (call sites, not new logic)
```

### Framing abstraction

The decode paths currently assume gzip framing in three places: the initial
header parse, the per-stream checksum accounting, and the end-of-stream footer
verification. Each becomes a small internal enum rather than a trait, because
there are exactly three cases and they are all in this crate:

- `Framing::{Gzip, Zlib, RawDeflate}` decides where DEFLATE starts, whether
  another stream may follow, and how the end is verified.
- `StreamChecksum::{Crc32, Adler32, None}` replaces the fixed CRC32 in
  `MemberAccounting`, which becomes `StreamAccounting`.

`Crc32` and `Adler32` keep their existing shapes; the Adler-32 implementation
uses `libz_rs_sys::adler32_z`, which the crate already links, rather than a
hand-rolled loop.

### Path selection

`decode_source_inner` detects the format first, then dispatches:

- gzip: unchanged, including the BGZF, stored-stream, dense-member, and
  estimated-grid paths.
- zlib and raw DEFLATE: the estimated-grid path when the input is large enough
  to expose parallelism, the sequential path otherwise, exactly as gzip
  chooses today. Neither format can be BGZF or multi-member, so the member
  probes are skipped.

The estimated-grid path already decodes one long DEFLATE stream between member
boundaries. Supporting these formats means starting it at the format's DEFLATE
offset and, at the end of the stream, verifying the format's trailer instead of
a gzip footer, then stopping instead of looking for another member.

### Indexing

Indexing works unchanged. The parallel path offers interior checkpoints with
resolved windows exactly as it does for gzip, and the stream start is offered
as a checkpoint at the DEFLATE offset with no window. That matches what
indexed_gzip records for gzip, and the reader added in sub-project 1 already
detects whether a gzip header sits at a window-less checkpoint rather than
assuming one, so seeking into a zlib or raw stream needs no reader change.

### Non-seekable input

`decode_stream` gains the same framing switch. Detection reads the first two
bytes of the stream cursor, which already buffers a page, so nothing is
spooled.

## Testing

Test-driven, one failing test before each unit.

- Fixtures come from `libz-rs-sys` in `tests/common`, which already produces
  gzip and BGZF: `zlib` uses window bits 15 and `raw` uses -15.
- Framing units: header validity across the `CMF`/`FLG` matrix, `FDICT`
  rejection, Adler-32 over known vectors.
- Round-trips: zlib and raw streams of several sizes decode to the original
  bytes, sequentially and in parallel, with the parallel result asserted equal
  to the sequential one.
- Rejection: corrupt Adler-32, truncated header, truncated trailer, trailing
  bytes after a complete stream, raw stream whose expected size disagrees.
- Detection: gzip, zlib, BGZF, and garbage each reach the expected outcome
  under `Format::Auto`, and an explicit format that disagrees with the input
  is an error rather than a silent reinterpretation.
- Non-seekable: the same corpora through `stream_reader`.
- Index: a zlib stream decoded with `build_index` produces interior
  checkpoints, and `IndexedReader` seeks into it correctly.

## Delivery

One branch, `multi-format`, stacked on `index-and-seek`, and one pull request.
It will show sub-project 1's commits until PR #8 merges. Commits are ordered
as: format detection, zlib framing, the framing switch in the sequential path,
the parallel path, indexing coverage, then documentation.
