# Architecture

## Data flow

`rapidgzip-core` accepts an immutable positional `ReadAt` source. A decode
snapshots its length, parses gzip framing itself, and routes the raw DEFLATE
payload through one of five bounded paths:

1. Standard zlib-rs raw inflate is the authoritative fallback and the
   single-thread path.
2. A fully stored stream is indexed from its exact block headers and copied by
   ordered worker tasks.
3. A consistently formed BGZF stream is indexed from `BC/BSIZE` and its
   independently verified members are decoded by ordered worker tasks.
4. Ordinary streams with densely spaced members use candidate-header
   discovery and independently verified member workers.
5. Other streams use a file-wide estimated grid and rapidgzip's marker/window
   path, with zlib-rs fallback from the last authoritative boundary.

All paths return ordered owned chunks to one coordinator. The coordinator alone
updates member accounting and calls the user's `Write`, so a writer need not be
`Send`. `DecoderReader` substitutes a bounded synchronous channel at this final
edge and therefore implements `Read + Send` without changing the decoder core.

## Non-seekable input

`rapidgzip-core` also accepts a plain `std::io::Read`. Such a source cannot be
indexed, probed, or revisited, so paths 2 through 5 are all unreachable: each
one begins by reading headers or block boundaries scattered across the file
before it decodes anything. Path 1 is reachable, because it only ever moves
forward.

Both cursors implement one internal `InputCursor` trait, which is the exact set
of forward operations path 1 and the member-header parser use: current offset,
end of input, the readable window, consume, and confirm the source did not
change. `SourceCursor` implements it over `ReadAt`; `StreamCursor` implements it
over `Read` with a single window that compacts consumed bytes on refill. Member
framing, footer verification, trailing-garbage detection, per-member history
reset, and the output limit are therefore not reimplemented for streams: path 1
is generic over the cursor and is the same code either way.

The length snapshot does not exist for a non-seekable source rather than
becoming mutable. A positional decode snapshots `len()` when it creates its
cursor and re-checks it at the end, which is what `verify_source_unchanged`
does. A stream has nothing to snapshot: end of input is whatever the reader
reports, and the framing loop refuses to stop anywhere except at a verified
member boundary, so trailing bytes after the last member are parsed as another
header and rejected as trailing garbage. Reaching end of input therefore still
means the whole input was verified, and `verify_source_unchanged` is a no-op.
The `ReadAt` contract itself is unchanged, and no parallel path observes any of
this.

Sequencing follows the positional entry points. The first member header is
validated against the initial window, without consuming it, before any
coordinator is spawned. The runtime is then configured with a single worker so
`DecoderStats` and `DecodeReport` report the concurrency actually in use.

## Line counting

`DecoderBuilder::count_lines` counts newlines in the `Output` implementations,
on the thread that emits. That is the only place the bytes are final: the
marker path's chunks hold 16-bit symbols until the coordinator resolves them,
and a marker can resolve to a newline. Counting in the workers would therefore
be wrong on the path that matters most.

Checkpoint line offsets come from merging two ordered streams. The index
builder keeps the offsets it has been offered but not yet passed, and each run
of output resolves the ones it covers in a single scan. Offers always precede
the emit of the bytes at their offset, so nothing is passed before it is known.
The builder tracks whether every checkpoint was resolved and claims a total
line count only when they all were, so a future path that offers late degrades
to an index without counters rather than one full of zeros.

## Inflate backends

Raw inflate sits behind one crate-internal trait, `InflateBackend`, with three
operations: create, reset, and inflate a buffer into the spare capacity of an
output vector. Its result is an enum, `Progress`, `StreamEnd`, or `Blocked`,
rather than a zlib status code, so an implementation that does not speak zlib's
numbering does not have to fake one. A type alias, `ActiveInflater`, picks the
implementation at compile time: zlib-rs by default, ISA-L under the `isal`
feature. Because the alias is a concrete type, the default build compiles to
what it compiled to before the trait existed.

Only the paths that decode a whole stream from its start go through the alias:
sequential gzip members, the single-stream zlib and raw DEFLATE loop, and BGZF
blocks. The marker/window path and `IndexedReader` name `RawInflater`
directly. Both resume at arbitrary bit offsets, which needs `inflatePrime`, and
the marker path locates DEFLATE block boundaries through zlib's `Z_BLOCK`
contract. ISA-L exposes neither, so it must not be reachable from them, and the
split is enforced by the type rather than by convention.

The trait carries no dictionary call for the same reason. Every stream the
pluggable paths inflate starts at its own beginning, with no predecessor
history; installing a window belongs to the paths that resume mid-stream, and
those keep the concrete type.

Two properties of ISA-L shape its implementation. `inflate_state` embeds a
64 KiB scratch buffer, so it is boxed rather than held inline. More
importantly, ISA-L reads ahead into a bit buffer, so at the end of a stream it
has consumed input the stream does not own; whole bytes still in that buffer
are given back, or the gzip footer and the next member would be read from the
wrong offset.

## Marker/window algorithm

The implementation follows rapidgzip 0.16.0 at upstream commit
`d2350e9c9ba54398cd64e45bfc8c631beec017f0`, principally:

- `blockfinder/DynamicHuffman.hpp`
- `chunkdecoding/GzipChunk.hpp`
- `DecodedData.hpp`
- `MarkerReplacement.hpp`

A speculative chunk begins with a 32 KiB history containing 16-bit symbols
`32768..65535`. Each symbol names the corresponding byte in the unknown
predecessor window. Literal symbols remain `0..255`. LZ77 copies operate on
these symbols exactly as they do on bytes, including overlap, so marker chains
collapse naturally to their original predecessor-window reference.

The block finder searches for non-final dynamic headers and validates the
precode plus complete literal/distance trees. Stored streams have a separate
exact-header route. Each worker starts near a 1 MiB compressed grid point,
finds the first structurally valid dynamic boundary in a bounded search
region, and decodes to the first complete-block boundary at or beyond the next
grid point. The predecessor independently lands on the same boundary. A false
candidate is rejected by complete DEFLATE decoding or by the coordinator's
exact predecessor/successor boundary check; decoding then resumes with zlib-rs
from the last authoritative position.

While unknown history can propagate, output remains 16-bit marker symbols. The
native unknown-history decoder uses that output itself as its LZ77 history:
cross-boundary references become predecessor markers, and in-chunk matches use
bulk `Vec::extend_from_within` copies with geometric doubling to preserve
overlap semantics. It does not maintain a second per-byte 32 KiB symbol ring.

Once a complete block leaves a marker-free 32 KiB window, the rest of that
independent chunk is decoded by zlib-rs using `inflatePrime`,
`inflateSetDictionary`, and `Z_BLOCK`. Successful speculative output is not
decoded again. To advance dependencies, the coordinator resolves only the
final 32 KiB needed for the successor window. Full marker replacement is a
separate ordered task in the same bounded worker pool, overlapped with later
native decode. Large full-window buffers use a branch-free 16-bit lookup table;
small buffers retain runtime-dispatched SSE4.1 on x86-64, baseline NEON on
AArch64, and a scalar fallback. Exact member starts are decoded directly by
zlib-rs. False boundaries and chunks exceeding their speculative allowance
fall back to zlib-rs from the last authoritative position.

## Members and BGZF

Member ends come only from an actual `BFINAL`, byte alignment, and a verified
eight-byte footer. Optimized raw inflate can read several bytes beyond the
DEFLATE end, so footer recovery examines at most the preceding 16 bytes and
accepts only a location matching both the already computed CRC32 and ISIZE.
With `Z_BLOCK`, zlib may return `Z_OK` at the final end-of-block before a later
call would return `Z_STREAM_END`; the decoder recognizes zlib's last-block
`data_type` flag and byte-aligns that boundary instead of treating the footer
as another resumable DEFLATE block.
The next header is parsed at that verified offset; gzip magic inside compressed
bytes is never accepted as a member boundary.

For dense ordinary multi-member streams, an 8 MiB prefix probe selects a
member-parallel path only when it exposes at least four plausible headers with
an average spacing no larger than four compressed grid intervals. A scanner
then runs concurrently with bounded decode work.
The common fixed ten-byte header is found with runtime-dispatched AVX2 on
x86-64, NEON on AArch64, and a scalar fallback; optional headers still go
through the complete gzip parser. A scanned header is only a candidate: a
worker must reach an actual `Z_STREAM_END` and verify CRC32 and ISIZE, and the
coordinator accepts the result only when its start is the exact end reported by
the preceding verified member. Thus plausible gzip bytes inside DEFLATE are
discarded. If a candidate chain is incomplete, corrupt, or exceeds its
per-member bound, the decoder continues sequentially from the first member it
has not emitted.

When the prefix averages at least 256 candidates per configured compressed
grid interval, candidate scheduling groups up to four nearby headers into one
worker task. Each candidate is still inflated and authenticated independently.
A worker collates output only while the verified end of one member is exactly
the next candidate's start, and it records every member's decoded length so the
coordinator can preserve per-member output-limit and accounting semantics.
Task span is seeded from the probe size and requested worker budget to expose
at least two initial waves of work, capped by the configured compressed grid.
Less-dense streams retain one task and result per member; this prevents larger
FASTQ members from losing parallelism or inflating result-buffer residency.

CRC32 and modulo-2^32 ISIZE are tracked and checked per member. History resets
to empty after every footer. Empty members, concatenated gzip, and BGZF EOF
members therefore use the same semantics.

The BGZF route is selected only when every declared `BSIZE` leads exactly to
another `BC` header or EOF. Mixed BGZF/plain streams and gzip members with an
incidental or inconsistent `BC` subfield fall back to generic gzip decoding.
BGZF workers decode eight independently framed blocks per task directly into
one aggregate output allocation, verify every block's CRC32 and ISIZE, and
reuse their initialized zlib-rs stream with `inflateReset`.

## Containers

Framing is resolved before any path is selected. An explicit format is taken as
given, since the framing checks report a mismatch far better than a prefix
sniff; `Format::Auto` reads two bytes and falls back to reporting missing gzip
magic, which is what it did before zlib was supported.

gzip keeps every existing path. zlib and raw DEFLATE are each exactly one
DEFLATE stream, so they skip the BGZF and multi-member probes and go to either
`single_stream.rs`, which runs the sequential loop over an `InputCursor` and
therefore serves positional and non-seekable sources identically, or the
estimated-grid path when a worker budget exists.

Supporting the parallel path costs three parameters rather than a second
implementation: where DEFLATE starts, which checksum accumulates over the
output, and what the end of the stream verifies. zlib validates its `CMF`/`FLG`
header, accumulates Adler-32 through `libz-rs-sys`, and checks the trailer
where a gzip footer would be read; raw DEFLATE starts at bit zero, accumulates
nothing, and only refuses trailing bytes and, when the caller supplied one, a
size that disagrees.

Index checkpoints for these containers record the DEFLATE start rather than a
header start, which is what indexed_gzip records, so `IndexedReader` resumes
there directly.

## Random-access index

`index/` holds the index data model and the on-disk formats and knows nothing
about decoding, so every format is testable against synthetic indexes. A
checkpoint pairs a compressed bit offset with a decompressed byte offset and,
unless it sits where no history is needed, the 32 KiB predecessor window that
must become the inflate dictionary before resuming there. Windows are held
zlib-compressed in memory by default, which matters once a large file
accumulates thousands of them.

Decode paths do not build the index themselves. They offer checkpoints to
`RuntimeState`, which is already shared with every worker, and `IndexBuilder`
orders, deduplicates, and thins the offers when the decode finishes. Offers may
therefore arrive in any order, which is what lets concurrent workers contribute
without coordination.

What each path can offer differs. The estimated-grid path offers every chunk
start together with the resolved predecessor window, so its checkpoints are
interior points, usually not byte aligned. The BGZF path offers every non-empty
block start with no window, reading each block's ISIZE footer for the
decompressed offset, which is why a BGZF index exports as a complete htslib
`.gzi`. The sequential and streaming paths offer member starts only: zlib does
not report DEFLATE block boundaries, so a forward-only source yields a coarse
but valid index.

`indexed/` consumes an index. `IndexedReader` picks the last checkpoint at or
before the target, primes the inflater with the straddled bits when the
checkpoint is not byte aligned, installs the window, and discards output up to
the target. A checkpoint with no window is not assumed to be a member start:
indexed_gzip records its first point after the gzip header, so the reader
checks for the magic bytes and skips a header only when one is there. Expanded
windows are cached in a byte-bounded LRU so nearby seeks do not re-inflate the
same history.

## Scheduling and memory

The BGZF, stored, dense-member, and native paths use a
`crossbeam_deque::Injector`, scoped workers, and bounded result channels. The
dense-member scanner is held to sixteen result windows of candidate work so
its header queue cannot grow with archive size. A collated dense-member result
contains at most four independently verified members and is also bounded by
the configured decoded/compressed work sizes.

All parallel paths treat the configured thread count as an immutable maximum,
not an eager allocation. The bootstrap budget is the minimum of that maximum
and affinity-visible parallelism, and its initial target is the ceiling of twice
the square root of that budget. The mutable application ceiling further caps
the effective target. This preserves a larger configured request as headroom
without immediately multiplying worker stacks, inflaters, buffers, and
speculative output by every processor.

Streams with enough work empirically probe upward from that conservative
bootstrap in bootstrap-sized steps while throughput materially improves, then
probe downward around the best setting and prefer a lower count within a 3%
noise margin. Each candidate uses the median of five intervals. Work carries a
controller generation, and completions begun under an earlier limit do not
inflate a new candidate's byte count. The search may reach the configured
budget when every increase remains useful; streams with fewer than eight
initial worker-waves retain the bootstrap because an optimum found near EOF
cannot repay calibration overhead. Modest budgets of at most twice their
bootstrap grow to their requested ceiling after the initial sample; on these
small pools, noisy interior calibration costs more than the few threads it
could release.

The adaptive target is combined with the application ceiling exposed by
`DecoderHandle::set_worker_limit`. Workers have stable ranks and are created
lazily only when the effective target grows. A worker above a reduced target
finishes its current task, stops taking queue work, and exits after a 250 ms
hysteresis interval if the reduction persists. The coordinator observes that
exit and can recreate the missing rank if demand later returns. Thread creation
and retirement therefore remain bounded by the same scoped lifetime as the
source borrowed by a decode.

`DecoderReader` supplies an additional feedback signal at the only queue that
unambiguously represents its consumer: the final synchronous output handoff.
When that queue fills, task admission falls to one. A successful send that had
to wait does not immediately clear the condition; a later send must complete
without encountering a full queue. This small hysteresis prevents a slow parser
from repeatedly restoring the full adaptive target for one available slot.
Sustained backpressure consequently retires excess workers, while transient
handoff jitter normally ends before their retirement timeout.

The active limit also controls the decode/resolve scheduling horizon because
each speculative result commonly owns several MiB of `u16` symbols. Enabled
generic workers dynamically take marker-resolution work first and
boundary-decode work second. Result channels retain two slots of scheduling
slack so every worker cannot block publishing native results while an exact
member bridge awaits resolution.

`DecoderHandle::stats` reads relaxed atomics only. It distinguishes configured,
application-limited, active, busy, and live-worker counts; live auxiliary
coordinator/scanner threads are reported separately. Produced and consumed
bytes distinguish decoder progress from parser progress. These snapshots are
approximate task telemetry, not OS scheduler or per-thread CPU accounting.

Native workers and their estimated grid persist across all members in a file.
At each ordinary gzip member transition, the coordinator resets
history/accounting and decodes an exact bridge from the new header to the first
later file-wide grid point; already-running tasks beyond that point remain
useful. Results and resolved buffers are reordered by ordinal before being
committed. No speculative worker calls the user's output object.

Input is paged with positional reads. Speculative output is capped per task;
oversized regions continue through zlib-rs instead. `DecoderReader` adds at most
the configured in-flight chunk count plus its currently partially consumed
chunk. Dropping it closes the consumer edge, sets cancellation, and joins the
coordinator.

A non-seekable source holds one input window instead of a positional page and
spools nothing, so its memory is independent of the input length. Backpressure
reaches the producer without any new mechanism: the bounded final handoff blocks
the coordinator, the coordinator therefore stops reading, and the pipe fills.
Dropping such a reader closes the consumer edge and sets cancellation as usual,
but does not join. Its coordinator can be parked inside a read against a
producer that never writes again, and a drop that could block forever is worse
than a thread that exits at its next read or send boundary.
