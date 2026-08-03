# Parallel full-stream decoding from an existing index

Status: implemented as the clean replacement for PR #14

Scope: gzip, concatenated gzip, BGZF, zlib, and raw DEFLATE

Companion: `random-access-index.md`

## Goals

- Use a caller-supplied `DeflateIndex` to split one complete decode into exact,
  independently resumable spans.
- Preserve byte order and all container integrity checks, including across
  ordinary concatenated gzip members and empty members.
- Keep `DecodeReport` small and `Copy`; accepting an index changes the
  operation, not the report type or decoder configuration.
- Provide both a calling-thread `Write` API and an owned `Read + Send` API.
- Treat the supplied index as a strict contract. Invalid, incomplete, or
  source-mismatched metadata must fail instead of silently selecting ordinary
  decoding.
- Share adaptive worker control and telemetry with the existing parallel paths.
- Bound queued decoded data independently of checkpoint spacing.
- Add no crate dependency and no unsafe public API.

## Non-goals

- Parallel range reads or a parallel `Read + Seek` implementation.
  `IndexedReader` remains the random-seek API and intentionally owns one
  inflater.
- Repairing, completing, or heuristically trusting a malformed foreign index.
- Non-seekable input. Concurrent spans require stable positional reads.
- Encoding or index construction in the same operation. Call
  `decode_with_index` first when an index does not already exist.

## Public API

Full-stream push decoding borrows the index:

```rust,no_run
use rapidgzip_core::{Decoder, DeflateIndex};
use std::fs::File;
use std::io;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut serialized = File::open("reads.fastq.gz.rgzidx")?;
let index = DeflateIndex::read_native(&mut serialized)?;
let source = File::open("reads.fastq.gz")?;
let decoder = Decoder::builder().decoder_threads(16).build()?;
let report = decoder.decode_from_index(&source, &mut io::sink(), &index)?;
assert!(report.member_count >= 1);
# Ok(())
# }
```

The owned reader takes `Arc<DeflateIndex>` so a large set of stored windows is
shared with its background coordinator rather than cloned:

```rust,no_run
use rapidgzip_core::{Decoder, DeflateIndex};
use std::fs::File;
use std::io::{self, Read};
use std::sync::Arc;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut serialized = File::open("reads.fastq.gz.rgzidx")?;
let index = Arc::new(DeflateIndex::read_native(&mut serialized)?);
let decoder = Decoder::builder().decoder_threads(16).build()?;
let mut reader = decoder.reader_from_index(File::open("reads.fastq.gz")?, index)?;
let control = reader.handle();
control.set_worker_limit(8)?;
io::copy(&mut reader, &mut io::sink())?;
let report = reader.finish()?;
assert_eq!(control.stats().path, rapidgzip_core::DecoderPath::IndexedParallel);
assert!(report.member_count >= 1);
# Ok(())
# }
```

Both operations return the ordinary `DecodeReport`. `decode_from_index`
returns `IndexDecodeError`, which distinguishes an invalid index, a selected
format/provenance mismatch, and a decode or verification failure. Initial
reader construction returns the same typed error; failures discovered after
construction are preserved by `DecoderReader::finish` and appear as
`std::io::Error` from `Read::read`, consistently with the other reader paths.

## Strict preflight

`IndexedPlan::build` completes before workers are created. It:

1. runs full structural index validation;
2. snapshots `ReadAt::len` and compares any recorded compressed size;
3. resolves index provenance to one concrete container and compares an
   explicitly configured decoder format;
4. requires the first checkpoint and decompressed offset to describe source
   origin;
5. parses the actual gzip or zlib header and confirms the first checkpoint;
6. requires a known final output size, except for BGZF `.gzi`, whose format
   does not store it; and
7. turns every checkpoint into one ordered span without dropping equal-output
   boundaries.

The final source length is checked again after decoding. `ReadAt` implementors
must keep content stable for the operation, as required by the safe trait
contract.

## Exact span execution

A span begins at one `Checkpoint` and ends at the next checkpoint, or at the
recorded source end. A worker resets a reusable raw zlib-rs inflater, primes a
non-byte-aligned first block when necessary, expands and installs the stored
32 KiB predecessor history, and parses any framing start described by the
checkpoint kind.

Inflation uses `Z_BLOCK` because an end checkpoint is a compressed *bit*
boundary. Success requires both:

- the inflater landing on the exact next bit offset; and
- the span producing exactly the difference between the two recorded
  decompressed offsets.

Internal DEFLATE blocks are not output boundaries. Workers accumulate their
results into `DecoderBuilder::decoded_chunk_size` buffers and flush only when a
buffer fills or a span/member ends. This is important for common gzip writers
that emit many small blocks: handing off every `Z_BLOCK` result serializes the
coordinator and destroys multi-worker scaling.

Each active span has a one-event channel. The coordinator admits at most the
effective worker target, consumes spans in index order, and schedules the next
span only after one completes. A sparse index can therefore increase work per
span without allocating the whole span, while later spans can retain at most
one completed output chunk each. Direct `Write` decoding recycles returned
chunk allocations; reader decoding transfers ownership through its existing
bounded final channel.

## Multi-member and container verification

Span boundaries and framing boundaries are independent. One span may cross
several gzip members, and adjacent checkpoints may have the same decompressed
offset when an empty member or BGZF EOF block intervenes.

Workers emit ordered checksum fragments and framing events rather than
declaring a member valid themselves. The coordinator:

- combines CRC32 fragments in output order and checks every gzip footer's CRC32
  and ISIZE;
- combines Adler-32 fragments and checks the zlib trailer;
- requires raw DEFLATE to end exactly at the source boundary;
- parses every gzip header, including FHCRC and reserved-flag rules;
- requires `BC`/`BSIZE` on every BGZF member and checks each declared block end;
  and
- counts empty members even though they produce no data event.

Because a full-stream decode covers every span from source origin, an interior
checkpoint does not weaken whole-member authentication: prefix and suffix
checksum fragments are recombined before its footer is accepted. This differs
from seeking directly into the middle with `IndexedReader`, where the skipped
prefix is unavailable.

## Scheduling and telemetry

The maximum worker count is the minimum of the configured worker budget,
configured in-flight chunk budget, affinity-visible parallelism, and number of
spans. The shared empirical controller derives its machine- and request-aware
bootstrap, creates worker ranks lazily, probes upward or downward when enough
spans remain, and honors `DecoderHandle::set_worker_limit` while decoding.
Workers that remain above a lowered ceiling retire after the normal grace
period and can be recreated after a later increase.

Telemetry reports `DecoderPath::IndexedParallel`, live/busy/queued workers,
verified members, byte progress, and the same pressure states as other
positional readers. `DecodeReport::decoder_threads` continues to mean the
configured maximum, not the number eventually admitted.

## Index construction needed for reuse

The sequential indexing path now drives zlib-rs with `Z_BLOCK` only when an
index was requested. It retains a rolling 32 KiB history and offers exact
interior block checkpoints at the requested decompressed spacing. As a result,
an index built with a one-worker decoder can later expose parallel spans for a
single-member gzip, zlib, or raw-DEFLATE stream. Ordinary non-indexing decode
continues to use `Z_NO_FLUSH` and pays none of this work.

## Safety

The feature adds no public unsafe interface. Its direct zlib-rs call uses the
same initialized, uniquely owned `RawInflater` invariant as existing paths.
Before each call, input points into a cursor page that cannot move during the
call and output points at exactly the advertised spare vector capacity. The
vector length is increased only by zlib-rs's checked produced-byte count. All
other indexing, scheduling, checksum, and framing logic is safe Rust.

## Validation

The focused suite covers gzip, zlib, raw DEFLATE, native, GZIDX, gztool, BGZF
`.gzi` without a known output total, single and multiple spans, concatenated
and empty members, checkpoint-output mismatches, source/format mismatches,
corrupt footers, output limits, `Read + Send`, runtime controls, and coalescing
of small internal DEFLATE blocks.

`rapidgzip-bench` includes a reproducible generated-Criterion comparison and
an `indexed` binary for a caller-supplied FASTQ gzip. The latter reports index
construction separately and compares indexed and ordinary full-stream decode
at identical thread budgets.
