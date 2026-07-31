# Architecture

## Data flow

`rapidgzip-core` accepts an immutable positional `ReadAt` source. A decode
snapshots its length, parses gzip framing itself, and routes the raw DEFLATE
payload through one of four bounded paths:

1. Standard zlib-rs raw inflate is the authoritative fallback and the
   single-thread path.
2. A fully stored stream is indexed from its exact block headers and copied by
   ordered worker tasks.
3. A consistently formed BGZF stream is indexed from `BC/BSIZE` and its
   independently verified members are decoded by ordered worker tasks.
4. Other streams use a file-wide estimated grid and rapidgzip's marker/window
   path, with zlib-rs fallback from the last authoritative boundary.

All paths return ordered owned chunks to one coordinator. The coordinator alone
updates member accounting and calls the user's `Write`, so a writer need not be
`Send`. `DecoderReader` substitutes a bounded synchronous channel at this final
edge and therefore implements `Read + Send` without changing the decoder core.

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

The BGZF, stored, and native paths use a `crossbeam-deque::Injector`, a sliding
task window, scoped workers, and bounded result channels. For generic native
decoding, the configured thread count is a maximum budget rather than a fixed
active count. The controller reads `available_parallelism`, which respects the
process affinity mask on supported platforms. Small pools and budgets no
larger than half the visible machine start fully enabled. A larger machine-wide
budget starts at roughly one third of visible parallelism, then compares that
baseline with one additional worker using warmup and measured intervals of
ordered decoded output. It keeps the neighbor only when throughput improves by
more than a 3% noise margin. Thus the active limit is derived from both the
machine and the current input instead of a compiled-in constant.

The active limit also controls the decode/resolve scheduling horizon because
each speculative result commonly owns several MiB of `u16` symbols. Workers
have stable ranks: only the enabled prefix touches per-worker buffers, while
the remaining requested workers sleep on a separate condition variable and do
not wake for ordinary queue activity. All enabled workers dynamically take
marker-resolution work first and boundary-decode work second. BGZF and stored
paths retain their format-specific worker counts.

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
