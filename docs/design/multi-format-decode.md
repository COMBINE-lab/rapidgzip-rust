# Multi-format DEFLATE decoding

Status: implementation specification

Target: PR #9, rebuilt on `main` after PR #8

Scope: decoding, verification, indexing, and decoded-output seeking

## Goals

- Decode gzip (including concatenated members and BGZF), zlib, and raw
  DEFLATE through the same public push and pull APIs.
- Preserve strict gzip behavior for existing callers unless automatic format
  detection is explicitly requested.
- Run sufficiently large zlib and raw-DEFLATE streams through the same
  adaptive marker/window algorithm used for ordinary gzip.
- Apply every check carried by the selected container and clearly distinguish
  structural raw-DEFLATE validation from checksum-authenticated decoding.
- Allow an optional exact expected output size for every format, enforcing an
  overrun before bytes beyond the expectation are emitted.
- Build and consume format-aware random-access indexes without inferring
  checkpoint framing from source bytes.
- Keep `DecodeReport` small and `Copy`, and keep all reader variants `Read +
  Send`.

## Non-goals

- Encoding.
- Guessing raw DEFLATE during automatic detection.
- Concatenated zlib streams.
- zlib preset dictionaries in the initial implementation.
- Authenticating the skipped prefix when a seek resumes at an interior block
  whose index record carries no running checksum state.
- Caller-supplied checksums for raw DEFLATE. An exact size is implemented now;
  optional CRC32/Adler-32 expectations can be added independently later.

## Dependency decision

The implementation retains explicit `Display`, `Error`, conversion, and
`io::ErrorKind` code rather than adding `thiserror` 2.0.19. The public errors
are cloneable, non-exhaustive, and contain shared I/O errors and nested framing
reasons; derive macros would remove little of the behavior that must remain
hand-written. Avoiding the proc-macro dependency also keeps the core crate's
dependency graph small.

## Public format API

`Format` contains only formats that can appear in a completed report:

```rust
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum Format {
    #[default]
    Gzip,
    Zlib,
    RawDeflate,
}
```

`DecoderBuilder::format(Format)` explicitly selects framing. Strict gzip
remains the builder default. `DecoderBuilder::auto_detect_format()` opts into
two-byte detection between gzip and zlib. Raw DEFLATE is never detected.

The selection state is private, so `DecodeReport::format` cannot contain an
impossible `Auto` value. Adding the public report field is a source-breaking
change to the 0.1 public-field struct and therefore makes the next release
0.2.0.

An automatic-detection failure is `DecodeError::UnrecognizedFormat`, not a
gzip-specific missing-magic error. Explicit selections retain format-specific
header errors.

## Prefix detection

Authoritative detection uses an exact, non-consuming two-byte peek. For a
non-seekable source it:

- tolerates one-byte and other short reads;
- retries `Interrupted`;
- works when the configured input page is one byte;
- distinguishes EOF before two bytes;
- leaves the prefix available to the framing decoder.

Reader construction may perform best-effort fail-fast validation when a full
prefix is already available, but it never commits a format from a partial
prefix. The resumable decoder performs the authoritative resolution on its
first `Read` call.

## Framing and verification

### gzip

Existing behavior remains unchanged: optional headers are parsed, every
member reaches a final DEFLATE block, CRC32 and modulo-2^32 ISIZE are checked,
concatenated members are decoded, and trailing non-gzip bytes are rejected.

### zlib

The two-byte CMF/FLG header must specify DEFLATE, satisfy FCHECK, and declare a
window at most 32 KiB. FDICT is rejected as an unsupported preset dictionary.
The declared CINFO window applies to all sequential, speculative, bridge,
tail, and indexed inflation; a distance larger than that window is invalid.
The four-byte big-endian Adler-32 trailer is always verified. Bytes after the
trailer are rejected.

### raw DEFLATE

Raw input begins directly at a DEFLATE block and ends after the byte containing
the final block. It has no checksum or length. Successful decoding proves only
that the bitstream is structurally valid and has no trailing bytes. It must not
be described as authenticated or checksum-verified.

### Output expectations

`DecoderBuilder::expected_uncompressed_size(Option<u64>)` applies to every
format. Before each output handoff, the decoder rejects a total larger than
the expectation without emitting the offending bytes. At the verified or
structurally complete end, it rejects a smaller total. `output_limit` remains
an independent maximum; when both bounds would be crossed, the lower bound
determines the error.

## Sequential architecture

The current pull-driven `SequentialDecoder` is generalized rather than adding
a second synchronous implementation. Its state machine resolves framing once,
then enters gzip-member, zlib-stream, or raw-stream states. Positional push,
non-seekable push, and non-seekable pull APIs all drive this same machine.

Checksum accounting is explicit:

```rust
enum StreamChecksum {
    Crc32(Crc32),
    Adler32(Adler32),
    None,
}
```

`None` performs no checksum pass. Borrowed zlib input/output pointers are
cleared before the resumable state can yield or move, preserving the existing
`Send` safety argument.

## Parallel architecture

gzip retains its BGZF, stored-block, dense-member, and estimated-grid paths.
zlib and raw DEFLATE use the estimated grid only when the compressed payload
can expose at least two useful grid tasks; otherwise they remain sequential.
The existing empirical adaptive controller owns worker admission.

The estimated-grid implementation receives a framing descriptor containing:

- concrete format;
- first DEFLATE bit offset;
- maximum LZ77 distance from CINFO or 32 KiB;
- checksum policy;
- terminal trailer behavior.

Native speculative decoding rejects distances above the descriptor's limit.
All raw-inflate bridges and tails use matching negative `windowBits` through
the existing RAII wrapper. Resolved output is accounted in order; gzip uses
CRC32, zlib uses Adler-32, and raw uses no checksum.

## Index model

PR #8's index API has not appeared in a crates.io release, so it is generalized
before its first public release:

- `DeflateIndex` is the canonical in-memory type;
- `IndexKind` is `Gzip`, `Bgzf`, `Zlib`, or `RawDeflate`;
- checkpoint kinds explicitly distinguish a gzip member header, gzip member
  DEFLATE start, zlib header, raw-DEFLATE stream start, and interior DEFLATE
  block;
- index validation rejects checkpoint kinds incompatible with provenance;
- start checkpoints carry no predecessor window;
- interior checkpoints retain the fixed 32 KiB index-window representation,
  while decoders expose only the suffix permitted by the container window.

The native version-1 format is still unreleased and is finalized with source
kind and explicit checkpoint tags. External formats remain container-specific:

- `.gzi` requires BGZF;
- GZIDX and gztool require gzip or BGZF;
- importing those formats records gzip-family provenance;
- incompatible export fails rather than silently dropping framing semantics.

## Indexed reader

`IndexedReader` continues to avoid magic sniffing. Resume behavior is chosen
from `IndexKind` plus `CheckpointKind`:

- gzip starts parse a member header and verify CRC32/ISIZE;
- a zlib-header start validates CMF/FLG and verifies Adler-32;
- a raw start performs structural inflation only;
- an interior block installs its predecessor window and cannot authenticate
  output skipped before the checkpoint.

For zlib and raw DEFLATE there is one stream, so end-of-stream handling checks
the applicable trailer and exact source end. gzip continues across later
members, fully verifying any member entered through its header.

## Telemetry and reports

`DecodeReport::member_count` and runtime `verified_members` are documented as
completed framing units: gzip members for gzip and one completed stream for
zlib/raw. A raw unit is structurally complete, not checksum-authenticated.
`DecodeReport::format` records the concrete format. Worker telemetry and
runtime controls are shared unchanged across formats.

## Errors

- `UnrecognizedFormat` covers automatic detection failure.
- `InvalidZlib` carries a `ZlibErrorKind` for header, dictionary, truncation,
  checksum, and trailing-data failures.
- `UnexpectedOutputSize` is format-independent.
- `InvalidDeflate` remains the raw payload error and gains trailing-data
  reporting.
- Error-to-`io::ErrorKind` mapping remains explicit and tested.

## Validation gates

- short-read, one-byte, interrupted, positional, and non-seekable inputs;
- every legal CINFO value plus an over-window distance fixture;
- empty, corrupt, trailing, and every-prefix-truncated inputs;
- sequential/parallel differential tests and randomized corpora;
- all push/pull and indexed/non-indexed APIs;
- `Read + Send` compile assertions;
- native index round trips and incompatible external export refusal;
- seek correctness and the documented interior-checksum limitation;
- paired gzip/zlib/raw benchmarks at representative thread counts;
- unchanged gzip FASTQ performance gate;
- fmt, clippy, rustdoc warnings, Rust 1.87 MSRV, package verification, and all
  platform/interoperability CI jobs.
