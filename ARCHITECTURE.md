# Architecture

## Data flow

`rapidgzip-core` accepts an immutable positional `ReadAt` source. A decode
snapshots its length, resolves explicit or automatic container selection,
parses gzip or zlib framing itself, and routes the raw DEFLATE payload through
one of six bounded paths:

1. Standard zlib-rs raw inflate is the authoritative fallback and the
   single-thread path.
2. A fully stored stream is indexed from its exact block headers and copied by
   ordered worker tasks.
3. A consistently formed BGZF stream is indexed from `BC/BSIZE` and its
   independently verified members are decoded by ordered worker tasks.
4. Gzip streams with densely spaced members use candidate-header
   discovery and independently verified member workers.
5. Other sufficiently large gzip, zlib, and raw-DEFLATE streams use a
   bounded empirical admission screen, then either authoritative zlib-rs or a
   file-wide estimated grid and rapidgzip's marker/window path.
6. A caller-supplied validated index partitions a full gzip, BGZF, zlib, or
   raw-DEFLATE decode into exact independently resumable spans.

Positional paths return ordered owned chunks to one coordinator. The
coordinator updates member accounting and calls the user's `Write`, so a writer
need not be `Send`. A positional `DecoderReader` substitutes a bounded
synchronous channel at this final edge. The non-seekable path uses the same
sequential core as a resumable state machine, described below.

## Non-seekable input

`rapidgzip-core` also accepts a plain `std::io::Read`. Such a source cannot be
pre-indexed, probed, or revisited, so paths 2 through 6 are all unreachable:
each one begins by reading headers or block boundaries scattered across the
file before it decodes anything. Path 1 is reachable, because it only ever
moves forward. An explicit indexing operation can nevertheless record member
boundaries as they are decoded; that coarser index is useful later with a
stable positional copy of the same compressed bytes.

Both cursors implement one internal `InputCursor` trait, which is the exact set
of forward operations path 1 and the framing parsers use: current offset, end
of input, an exact non-consuming two-byte peek, the readable window, consume,
and confirm the source did not change. `SourceCursor` implements it over
`ReadAt`; `StreamCursor` implements it over `Read` with a single window that
compacts consumed bytes on refill. Format resolution, framing, trailer
verification, trailing-data detection, history reset, and output bounds are
therefore not reimplemented for streams: path 1 is generic over the cursor and
is the same code either way.

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

`decode_stream` drives the resumable engine to completion on the calling thread.
`stream_reader` performs one initial read for best-effort fail-fast header
validation, then advances the engine only from the consumer's `Read::read`.
This avoids both a permanently allocated streaming thread and the possibility
of detaching one blocked inside an arbitrary `Read`. The state machine owns the
selected concrete format, current framing state, zlib-rs inflater,
CRC32/Adler-32/none checksum policy, output size, and input cursor between
chunks, so dropping the reader drops the source immediately.

Runtime telemetry retains the builder's immutable configured-worker budget and
application worker limit. The sequential path sets its adaptive target to one,
spawns no decoder workers or auxiliary coordinator, and returns the configured
budget in `DecodeReport::decoder_threads`, preserving the published field
semantics while still exposing the concurrency actually in use.

`Decoder::open` classifies only regular files as positional. Seekability alone
is insufficient: block and character devices can implement seek while lacking
the stable, known-length, concurrently readable snapshot required by `ReadAt`.
Every non-regular path accepted by `File::open` uses the streaming engine.

## Structural analysis

Structural analysis is an explicit operation over the same positional and
streaming input cursors. `AnalysisCursor` layers exact bit positions and a
small look-ahead buffer over `InputCursor`; it does not materialize compressed
input. The gzip analyzer calls the same header parser as ordinary decode in a
detailed-retention mode, and format detection, reserved flags, FHCRC, BGZF
metadata, zlib CMF/FLG, and trailing-data rules therefore have one source of
truth.

The block walker is deliberately sequential. It decodes literal and
length/distance symbols in causal order into a 32 KiB ring, feeds an 8 KiB
checksum buffer, and discards output after it can no longer be referenced.
gzip CRC32/ISIZE and zlib Adler-32 are checked before a stream result is
published. Each gzip member resets its history and becomes one `StreamAnalysis`;
empty members and BGZF's EOF member remain visible instead of being collapsed.

The speculative native decoder and analyzer share monomorphized bit/Huffman
primitives. The production `BitReader` keeps its slice-specialized unaligned
word loads, while the analyzer supplies a bounded cursor implementation. There
is no trait object or added branch in the ordinary hot loop. Dynamic code
lengths are copied into the result only for analysis; ordinary marker decoding
does not retain them.

Result memory is explicit. `AnalyzeOptions` limits total streams, total blocks,
input-wide optional gzip metadata, and input-wide detailed predecessor-window
references. Collection growth uses fallible reservation and structural
counters use checked arithmetic. Once the detailed-reference allowance is
exhausted, the walker continues to compute exact counts, reference-length
histograms, farthest reach, predecessor-window coverage, and deterministic
interval-union counts. It records omitted detail per block.

The core `Analysis` model contains deterministic facts and derives equality;
it contains no wall-clock measurements or formatted text. The CLI measures its
own invocation and renders a rapidgzip 0.16.0-compatible report in a separate
module. Compatibility formatting does not constrain the library API, and the
CLI intentionally fixes rapidgzip's unstable equal-distance interval merge.
The complete design is recorded in `docs/design/deflate-analysis.md`.

## Random-access index construction

Indexing is selected by operation (`decode_with_index` or
`reader_with_index`), not stored as decoder configuration. Existing calls do
not allocate an index, copy predecessor windows, or change their `Copy`
`DecodeReport` result. Indexed calls own a decode-local collector that is
separate from `RuntimeState`, so telemetry and adaptive scheduling do not
retain or compress checkpoint data.

Each path publishes only boundaries it can prove in output order. Sequential
decoding records parsed member payload/header pairs. BGZF records every
non-empty independently framed block. Stored streams additionally record
zero-history stored-block starts. Dense-member workers expose a member only
after its footer is authenticated and exact adjacency is accepted by the
coordinator. The marker/window coordinator records a raw DEFLATE block only
after resolving its complete 32 KiB predecessor history. Interior candidates
are thinned by requested decompressed spacing before window compression.

`CheckpointKind` distinguishes gzip-header positions, raw member-payload
positions with retained header provenance, zlib headers, raw-stream starts, and
interior DEFLATE blocks. `IndexKind` records matching container provenance, so
`IndexedReader` never guesses framing from source bytes. It fully checks gzip
CRC32/ISIZE or zlib Adler-32 when it resumes at a framing start; an interior
index cannot authenticate the skipped prefix. Raw DEFLATE has no checksum.
Index parsers have explicit count and aggregate-window limits, and source-size
metadata is checked before indexed reads begin.

An existing index can separately drive complete parallel decoding through
`decode_from_index` or `reader_from_index`. Preflight validates source size,
container provenance, origin, actual framing, and final-size metadata before
workers exist. Every checkpoint, including equal-output empty-member points,
becomes one ordered span. A worker installs its exact predecessor window and
must reach the next compressed bit and decompressed byte offsets. It accumulates
small internal `Z_BLOCK` results into ordinary decoded chunks; checkpoint
spacing therefore controls task size without turning every DEFLATE block into
an ordered handoff. One-slot span channels and the normal adaptive admission
target bound reordering. The coordinator combines CRC32 or Adler-32 fragments
across interior checkpoints, validates every trailer, and reports
`DecoderPath::IndexedParallel`. The complete design is recorded in
`docs/design/indexed-parallel-decode.md`.

## Ordered line counting and line seeking

Newline counting is decoder configuration, while index construction remains an
explicit per-operation choice. `DecoderBuilder::count_lines(true)` adds one
scalar scan over final ordered output and places the result in
`DecodeReport::line_count`; the field is `Option<u64>`, so `DecodeReport`
remains `Copy`. The disabled path keeps no counter and does not scan output.

Counting occurs at the `Output` boundary after predecessor markers have been
resolved. Worker-local or speculative buffers are not valid inputs because an
unresolved symbol may later become a newline. Push decodes wrap their final
output sink in a decode-local counter. Pull-driven streaming uses the same
counter immediately before publishing each final chunk. Strict indexed
parallel decoding counts its ordered span handoff.

When indexing and counting are both requested, `IndexCollector` advances one
cursor through the retained checkpoint vector as ordered byte ranges arrive.
It annotates checkpoints in place, avoiding duplicate tree nodes and logarithmic
lookup work per point. Checkpoints at decoded EOF are resolved during
finalization. Empty gzip members may produce multiple checkpoints at the same
output offset; they correctly share one line count. If any retained checkpoint
was offered too late to resolve, finalization clears every checkpoint line
field and the total rather than publishing partial metadata.

`IndexedReader::seek_to_line` selects the latest complete annotated checkpoint
proven to precede the requested zero-based line's start, resumes through its
exact predecessor window, and scans forward to the terminating newline. It
uses a strictly smaller line count for nonzero targets because an equal-count
checkpoint can sit inside a long line. The scan is bounded by the actual
decoded distance to the previous retained checkpoint for an in-range target;
external, streaming-built, or thinned indexes may have gaps larger than their
recorded target spacing. A request beyond the final line lands at
decoded EOF. The method rejects indexes without a total and per-point line
counters. gztool version 1 is translated at the codec boundary because it
stores one-based checkpoint line numbers, while the Rust API stores preceding
newline counts.

The complete design and CLI integration are recorded in
`docs/design/line-aware-cli.md`.

For strict full-stream decoding through an imported index, enabling line
counting also authenticates every supplied checkpoint line offset and the
total against final ordered bytes. Random-access seeking alone must trust those
annotations because it intentionally does not scan the skipped prefix.

## Marker/window algorithm

Before the generic marker grid starts, path admission derives a useful worker
width from the configured budget, current application ceiling,
affinity-visible processors, the adaptive machine bootstrap, and available
grid work. Inputs shorter than sixteen normal grid tasks, inputs without two
complete waves, and effective one-worker runs stay sequential. Specialized
stored, BGZF, and dense-member paths are classified first and never pay for
this screen.

Eligible inputs compare the best of three exact zlib-rs service samples over a
128 KiB compressed prefix with one concurrent adjacent 128 KiB marker task per
useful worker. The speculative sample includes boundary validation, ordered
predecessor-window propagation, and worker-parallel full marker resolution.
Its worker-width-adjusted gate accounts for fixed marker/search setup that a
normal 1 MiB task amortizes. Ambiguous, invalid, discontinuous, cancelled, or
too-small samples restart the authoritative sequential decoder; passing
samples discard the bounded screen and start the unchanged normal marker grid.
The transient state is reported as `DecoderPath::MarkerAdmission`.

Admission chooses an algorithm; the separate steady-state concurrency
controller still tunes the active worker count after the marker path wins.
Index checkpoints are published only after this decision, so discarded probe
work can never enter an index.

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

Once a complete block leaves a marker-free 32 KiB index window, the rest of
that independent chunk is decoded by zlib-rs using `inflatePrime`,
`inflateSetDictionary`, and `Z_BLOCK`. Successful speculative output is not
decoded again. To advance dependencies, the coordinator resolves only the
final 32 KiB needed for the successor window. Full marker replacement is a
separate ordered task in the same bounded worker pool, overlapped with later
native decode. Large full-window buffers use a branch-free 16-bit lookup table;
small buffers retain runtime-dispatched SSE4.1 on x86-64, baseline NEON on
AArch64, and a scalar fallback. Exact member starts are decoded directly by
zlib-rs. False boundaries and chunks exceeding their speculative allowance
fall back to zlib-rs from the last authoritative position.

For zlib, CINFO supplies an 8--15 bit maximum history window. Both the native
distance decoder and every zlib-rs bridge/tail enforce that limit; a persisted
32 KiB predecessor window is sliced to the permitted suffix before it is
installed. Ordered accounting selects CRC32 for gzip, Adler-32 for zlib, and no
checksum pass for raw DEFLATE. The terminal coordinator then applies the
container-specific trailer and exact-end rule.

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
spools nothing, so its memory is independent of the input length. There is no
final handoff or streaming coordinator: the consumer directly advances one
decoded chunk at a time. A slow consumer therefore stops calling the compressed
source, naturally propagating backpressure to a pipe or socket. Dropping the
reader drops the cursor and source synchronously, so no OS thread, stack,
channel, or input resource can remain detached behind it.
