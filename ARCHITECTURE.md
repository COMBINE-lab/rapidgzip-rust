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
