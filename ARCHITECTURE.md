# Architecture

## Data flow

`rapidgzip-core` accepts an immutable positional `ReadAt` source. A decode
snapshots its length, detects gzip (`1f 8b`) vs zlib (RFC 1950 CMF/FLG) from
the prefix (or an explicit `DecoderBuilder::format`), then routes the payload.
Auto never selects raw DEFLATE (no magic). Non-seekable `Read` sources
(`Decoder::decode_read`, CLI stdin decompress) use a buffered pull reader:

- **Sequential streaming** when `decoder_threads == 1`: zlib-rs inflate only; no
  parallel workers and no full-archive buffer (O(input page) compressed-side
  memory). Applies to gzip, zlib, and raw DEFLATE.
- **Spill + positional backend** when `decoder_threads > 1` on non-seekable
  `decode_read` (any resolved format: gzip, zlib, or raw DEFLATE): the stream is
  spilled to a private temporary file (secure temp-dir defaults; deleted on
  drop), then the usual positional path below runs on that file. Peak cost is
  compressed size on disk plus the decoder working set. After spill, gzip, zlib,
  and raw DEFLATE use the same parallel gates as file/`ReadAt` input.
- **Parallel multi-stream zlib** on positional `ReadAt` (including after spill)
  when `decoder_threads > 1` and the input is ≥2 concatenated independent
  CMF/FLG…Adler frames: two-pass discard-index of stream boundaries, then ordered
  worker re-decode with zlib-rs (stream granularity). Skipped when a large single
  stream will take the marker path (avoids a full discard re-inflate of solitary
  long streams).
- **Parallel single-stream zlib** on positional `ReadAt` (including after spill)
  when `decoder_threads >= 4` and the compressed length is at least about two grid
  cells (`2 × compressed_chunk_size` plus header/trailer): same estimated
  marker/window path as ordinary gzip, with Adler-32 trailer checks. Concatenated
  streams after the first are multi-stream-parallel when ≥1 remain (else
  sequential).
- **Parallel single-stream raw DEFLATE** on positional `ReadAt` (including after
  spill) when `decoder_threads >= 4` and compressed length is at least about two
  grid cells (`2 × compressed_chunk_size`; no wrapper bytes): same estimated
  marker/window path; leftover compressed bytes after stream end are an error.

CLI paths that need a known length / `ReadAt` on stdin (`--analyze`,
`--import-index`, `--ranges`) likewise spill stdin to a tempfile rather than
holding the full archive only in RAM.

Positional routes after format resolution:

0. **zlib wrapper**: parse CMF/FLG, raw-inflate, verify the big-endian Adler-32
   trailer (gated by `crc32_enabled`). Supports concatenated zlib streams.
   Routing when `decoder_threads > 1`: (a) multi-stream stream-granularity
   parallel if a discard index finds ≥2 frames (unless a large single stream is
   preferred for the marker path); (b) single long streams use the estimated
   marker/window path when `decoder_threads >= 4` and size amortizes the grid;
   (c) multi-stream tails after a parallel first stream use stream-granularity
   parallel (or sequential for a single remainder); else sequential zlib-rs.
0b. **raw DEFLATE** (RFC 1951, explicit `Format::RawDeflate` only; Auto never
   selects it): single long streams use the estimated marker/window path when
   `decoder_threads >= 4` and size amortizes ~2× the compressed grid; otherwise
   sequential zlib-rs raw inflate (`windowBits = -15`) from offset 0 to
   `Z_STREAM_END`. Leftover compressed bytes are an error. No container integrity
   trailer and no random-access index (`keep_index` rejected at build time);
   optional whole-stream CRC via `raw_crc32_list`.
1. Standard zlib-rs raw inflate is the authoritative gzip fallback and the
   default single-thread gzip path. Paths that go through the crate-private
   `InflateBackend` trait (generic, monomorphized to `ActiveInflater` — zlib-rs
   `RawInflater` by default, or ISA-L `IsalInflater` with the optional `isal`
   feature; see `inflate_backend.rs` / `isal_backend.rs`):
   - sequential positional multi-member gzip, sequential zlib, sequential raw
     DEFLATE, and streaming gzip/zlib/raw via `stream_decode` (`create` /
     `reset` / trait `inflate`);
   - structure analysis (`Decoder::analyze` / CLI `--analyze`) with
     `InflateFlush::Block` (block spans + `last_block`; ISA-L backend delegates
     Block flush to an internal zlib-rs inflater);
   - parallel BGZF one-shot block inflate via trait `inflate`
     (`InflateFlush::Finish`; ISA-L multi-steps `isal_inflate` until
     STREAM_END within that public call when `tmp_out` still drains);
   - parallel independent-member workers: trait `create`/`reset`/
     `inflate_capped` so the per-member output budget cannot overshoot spare
     capacity (no raw `stream.avail_out` at the call site);
   - multi-stream zlib index discard-inflate and parallel zlib stream workers
     via trait `create`/`reset`/`inflate`/`inflate_capped`;
   - mid-stream bit resume first-class on the trait (`prime`,
     `set_dictionary`, default `install_bit_resume` /
     `prepare_at_bit_offset`): estimated-path setup uses trait
     `prime`/`set_dictionary`, and source-backed seeks /
     `inflate_from_block` build streams via
     `prepare_inflater_at_bit_offset` (generic over `I: InflateBackend`);
   - estimated-path residual continue (`inflate_from_block`) and
     `inflate_tail` via trait `inflate_capped` (`NoFlush` / `Block`;
     block-end bit state from `InflateStep`);
   - seek sessions and indexed segment decode via trait
     `inflate_into_slice` (fixed caller `out` slices; member restarts use
     trait `create`/`set_dictionary`).

   All sequential/block inflate call sites go through `InflateBackend`
   (monomorphized to `ActiveInflater`). Raw `z::inflate` lives in the zlib-rs
   implementor in `inflate_backend.rs`; ISA-L `isal_inflate` lives in
   `isal_backend.rs`. Build the `isal` feature only when a shared `libisal` is
   available (`ISAL_INSTALL_PREFIX` / system packages).
2. A fully stored gzip stream is indexed from its exact block headers and
   copied by ordered worker tasks.
3. A consistently formed BGZF stream is indexed from `BC/BSIZE` and its
   independently verified members are decoded by ordered worker tasks.
4. Ordinary gzip streams with densely spaced members use candidate-header
   discovery and independently verified member workers.
5. Other gzip streams use a file-wide estimated grid and rapidgzip's
   marker/window path, with zlib-rs fallback from the last authoritative boundary.

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
members therefore use the same semantics. Sequential multi-member gzip and
zlib paths (positional and streaming) reuse one raw-inflate stream across
members via `inflateReset`, which clears window history at each boundary.

The BGZF route is selected only when every declared `BSIZE` leads exactly to
another `BC` header or EOF. Mixed BGZF/plain streams and gzip members with an
incidental or inconsistent `BC` subfield fall back to generic gzip decoding.
BGZF workers decode eight independently framed blocks per task directly into
one aggregate output allocation, verify every block's CRC32 and ISIZE, and
reuse their initialized zlib-rs stream with `inflateReset`.

## Scheduling and memory

The BGZF, stored, dense-member, and native paths use a
`crossbeam-deque::Injector`, scoped workers, and bounded result channels. The
dense-member scanner is held to sixteen result windows of candidate work so
its header queue cannot grow with archive size. A collated dense-member result
contains at most four independently verified members and is also bounded by
the configured decoded/compressed work sizes. For generic native
decoding, the configured thread count is a maximum budget rather than a fixed
active count. The controller reads `available_parallelism`, which respects the
process affinity mask on supported platforms and bounds active ranks by that
value. A budget below the machine seed starts fully enabled. Larger budgets
share a starting point at the ceiling of twice the square root of visible
parallelism, so increasing the requested maximum never causes an abrupt jump
or drop in bootstrap concurrency.
This grows with the machine without multiplying speculative memory linearly by
every processor. Streams with fewer than sixteen initial worker-waves retain
that machine-derived bootstrap: calibrating a short decode costs more than an
optimum found near EOF can recover.

Longer streams measure native worker completions before ordered output handoff,
so `DecoderReader` backpressure or parser speed cannot bias the decoder limit.
Each candidate uses the median of three intervals. Work carries a controller
generation, and completions begun under an earlier limit do not inflate the new
candidate's byte count. The search probes downward first, preferring a lower
setting within a 3% noise margin, or climbs in quarter-bootstrap steps while
throughput improves by more than 5% (stricter than the down-probe tolerance so
noise does not keep adding ranks). Its empirical search extent is at most 1.5×
the bootstrap and never exceeds the configured budget. This bounds calibration
and speculative-memory exposure without a compiled-in worker cap.

Ordinary single-member gzip **and** large single-stream zlib enter the
estimated/marker pipeline only when the requested budget is at least four
workers; at two or three threads sequential zlib-rs is faster end-to-end
(measured). Zlib also requires enough compressed bytes for at least two grid
cells (see zlib route above). BGZF, stored-block, and independent multi-member
gzip paths still parallelize for any budget greater than one.

The active limit also controls the decode/resolve scheduling horizon because
each speculative result commonly owns several MiB of `u16` symbols. Workers
have stable ranks and are created lazily as upward probes require them. Ranks
disabled by a later downward decision sleep on a separate condition variable
and do not wake for ordinary queue activity. All enabled workers dynamically
take marker-resolution work first and boundary-decode work second. Result
channels retain two slots of scheduling slack so every worker cannot block
publishing native results while an exact member bridge awaits resolution. BGZF
and stored paths retain their format-specific worker counts.

Native workers and their estimated grid persist across all members in a file.
At each ordinary gzip member transition, the coordinator resets
history/accounting and decodes an exact bridge from the new header to the first
later file-wide grid point; already-running tasks beyond that point remain
useful. Results and resolved buffers are reordered by ordinal before being
committed. No speculative worker calls the user's output object. After a
successful resolve+emit, empty `Vec<u8>` capacity (marked resolved bytes,
clean, backend tail) is returned to a soft-capped free-list
(`2 × worker_count`) so workers can reuse it for the next task's clean/tail
scratch. After resolve on an estimated worker, emptied `Vec<Symbol>` capacity
is recycled into that worker's `marked_scratch` (resolve clears symbols and
returns the allocation; never while the buffer is still on the resolve queue).

Input is paged with positional reads. Speculative output is capped per task;
oversized regions continue through zlib-rs instead. `DecoderReader` adds at most
the configured in-flight chunk count plus its currently partially consumed
chunk, and shares a soft-capped reader-local free-list (`2 × in_flight_chunks`,
`buffer_pool::ByteBufferFreeList`) with `ChannelOutput`: fully-consumed channel
chunks are recycled so `emit_reusable` can return capacity for the next fill.
That pool is separate from the estimated-path free-list. Dropping the reader
closes the consumer edge, sets cancellation, and joins the coordinator.

## Structure analysis

`Decoder::analyze` (CLI `--analyze`) walks the archive sequentially via
`InflateBackend` with `InflateFlush::Block` (monomorphized to `RawInflater`).
Format follows `DecoderBuilder::format` (default auto-detect): gzip/BGZF, zlib
(RFC 1950), or explicit raw DEFLATE (RFC 1951). It reports per-member (or per
stream) compressed ranges, uncompressed sizes, footer check status, and
per-block type (stored/fixed/dynamic), final bit, and bit spans. For zlib it
also records CMF/FLG and Adler-32 status (`crc32_enabled` gates Adler the same
way as gzip CRC). Concatenated zlib streams appear as separate members. No
payload is emitted and no index is required.

Raw DEFLATE (`Format::RawDeflate`, never auto-selected) walks a single stream
from bit 0 to `Z_STREAM_END`, reports `ArchiveKind::RawDeflate` with
`crc32_ok: None` (no integrity trailer), and treats trailing bytes after EOS as
an error (same policy as decode).

## Random access and indexes

When `DecoderBuilder::keep_index` is enabled, the coordinator records
checkpoints (compressed bit offset, uncompressed offset, optional line
offset) and predecessor 32 KiB windows at resolved boundaries. BGZF and
independent member starts use empty windows. By default
(`compress_index_windows`, on), non-empty windows may be held
zlib-compressed in memory when smaller than raw (decompress on demand for
seek/export; `IndexedReader` caches expanded zlib windows in a small LRU
aligned with seek-cache chunk budget). The in-memory `GzipIndex`
exports/imports **indexed_gzip** (`GZIDX`), **gztool** (`gzipindx` /
`gzipindX`), and htslib **BGZI** (`.gzi` block index: little-endian pair
list of compressed/uncompressed block starts after the first). BGZI export
emits only empty-window member boundaries (no synthetic EOF pair, no
mid-stream windows). Import leaves uncompressed size unknown (`u64::MAX`);
full decode schedules an open-ended final segment for the last block.
`read_gzip_index` auto-detects the format (GZIDX magic, then gztool magic,
then exact-length BGZI).

`Decoder::decode_with_index` splits the archive on consecutive checkpoints and
inflates each span with zlib-rs (no marker speculation). Segments may start
mid-member, so this path does **not** verify member CRC32/ISIZE (same policy
as seek). Self-built indexes append an EOF checkpoint; imported indexes that
omit one still get a final tail segment when the declared uncompressed size is
known. Empty-window checkpoints that land on gzip magic (BGZI header starts)
skip the member header so inflate begins at the DEFLATE payload; marker-path
fallbacks never perform that skip.

`IndexedReader` (`Read` + `Seek`) restarts inflate at the nearest preceding
checkpoint, discards skip bytes, and serves sequential traffic from an LRU of
decoded windows with optional single-threaded readahead into the next window
plus best-effort parallel background prefetch of further windows
(`DecoderBuilder::seek_prefetch_windows`, default 2; workers inflate from
independent checkpoint resumes and never share the consumer session). Far seeks
invalidate in-flight prefetch inserts via a generation counter. `seek_to_line`
requires an index with line offsets (`gather_line_offsets` or a
gztool-with-lines import).
