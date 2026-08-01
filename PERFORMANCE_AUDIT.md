# Performance audit

## Follow-up optimizations (2026-08-01)

Shipped in-tree after a wall-clock + code profile on 128 MiB synthetic corpora
(kernel `perf` unavailable on the host):

1. **Bulk clean/marked LZ77 copy** in `parallel/deflate.rs` — overlap-aware
   `extend_from_within` + geometric doubling (same strategy as speculative
   `copy_match_unknown`), replacing per-byte ring loops once markers drain.
2. **Marker-path threshold** — ordinary single-member estimated pipeline only
   for `decoder_threads >= 4`; P=2/3 stay on sequential zlib-rs. BGZF / stored /
   independent multi-member still parallelize at `> 1`.
3. **Tighter coordination waits** — worker idle/full-channel parks 100 µs
   (native full-channel remains 25 µs); coordinator `recv_timeout` 2 ms.
4. **Adaptive plateau** — upward search needs **+5%** thrpt to climb; search
   ceiling **1.5×** bootstrap (was 2×).
5. **Parallel single-stream zlib** — positional `Format::Zlib` (and Auto that
   resolves to zlib) reuses the estimated marker/window path when
   `decoder_threads >= 4` and compressed length is at least
   `2 × compressed_chunk_size + 6` (CMF/FLG + Adler). Adler-32 still gated by
   `crc32_enabled`.
6. **Parallel multi-stream zlib** — concatenated independent CMF/FLG…Adler
   frames (≥2) with `decoder_threads > 1` use two-pass discard-index + ordered
   stream-granularity zlib-rs workers. Large solitary streams prefer the
   marker path (skip discard index); multi-stream tails after a parallel first
   stream also use stream-granularity parallel. Small/low-thread single streams
   and single-thread `decode_read` zlib remain sequential. Multi-thread
   `decode_read` zlib spills to a tempfile then uses the positional parallel
   gates.
7. **Parallel single-stream raw DEFLATE** — positional `Format::RawDeflate`
   reuses the estimated marker/window path when `decoder_threads >= 4` and
   compressed length is at least `2 × compressed_chunk_size` (no wrapper).
   Optional whole-stream CRC via `raw_crc32_list`. Small/low-thread streams
   stay sequential zlib-rs raw inflate.
8. **Multi-thread `decode_read` spill** — for gzip, zlib, and raw when
   `decoder_threads > 1`, non-seekable input is copied to a private tempfile
   then routed through the same positional backend gates (so parallel raw and
   zlib apply after spill without a second streaming parallel path).
9. **Post-emit byte free-list + post-resolve symbol recycle (estimated/marker
   path)** — after the coordinator emits resolved marked/clean/backend_tail
   parts via `emit_reusable`, empty `Vec<u8>` capacity is pushed to a shared
   free-list (`Arc` + `crossbeam_deque::Injector`, soft-capped at
   `2 × worker_count`; type lives in `buffer_pool.rs`). Estimated workers
   `try_steal` into `clean_scratch` before each new task. Only byte buffers
   exclusively owned after successful emit are recycled (never while live in
   the resolve queue). After a worker steals a resolve task,
   `resolve_parts` / `MarkerBuffer::resolve_with_symbols` clears the symbol
   vec and returns it; the worker `prefer_capacity`s into `marked_scratch`
   (success or error) before sending `ResolvedParts` to the coordinator — no
   cross-thread `Vec<Symbol>` free-list. Resolve still allocates a fresh
   `Vec<u8>` for marked bytes (those join the post-emit byte free-list after
   emit).
10. **DecoderReader channel buffer pool** — `DecoderReader::spawn` creates a
    separate reader-local `Arc<ByteBufferFreeList>` soft-capped at
    `2 × in_flight_chunks`. Fully-consumed (or `finish`-discarded) channel
    chunks are `recycle`d into that pool. `ChannelOutput::emit_reusable` sends
    the payload on the channel, then `try_steal`s an empty buffer (or returns
    a zero-capacity `Vec`) for the coordinator’s next fill. This does **not**
    merge with the estimated-path free-list: the estimated pool still only
    sees capacity returned from `emit_reusable`. With `DirectOutput`, that is
    the same cleared chunk; with `ChannelOutput`, steals come from the reader
    pool after the consumer finishes reading, so sequential estimated emit
    loops can recover capacity on the `Decoder::reader` / `open` path as well.
    Default `emit_reusable` (no free-list) still returns zero-capacity vecs.

Optional **faster single-thread inflater**: feature **`isal`** wires Intel
ISA-L (`IsalInflater` in `isal_backend.rs`) as `ActiveInflater`. Default
builds stay zlib-rs-only (no `libisal` required). Build with a system or
prefix shared library (`ISAL_INSTALL_PREFIX`, `LD_LIBRARY_PATH` at runtime).
Block-accurate flush still uses zlib-rs inside the ISA-L backend.

`InflateBackend` coverage today (`crates/rapidgzip-core/src/inflate_backend.rs`,
monomorphized to `ActiveInflater` — zlib-rs by default, ISA-L with `isal`):

| Path | Trait surface used |
|------|--------------------|
| Sequential positional gzip / zlib / raw | `create` / `reset` / `inflate` (`NoFlush` / `Block` for `keep_index`) |
| Streaming `stream_decode` (gzip / zlib / raw) | same |
| Structure analysis (`analyze` / `--analyze`) | `create` / `inflate` (`Block`; `last_block` from `data_type`) |
| Parallel BGZF Finish | `create` / `reset` / `inflate` (`Finish`) |
| Parallel independent-member workers | `create` / `reset` / `inflate_capped` (per-member output budget) |
| Multi-stream zlib index + workers | `create` / `reset` / `inflate` / `inflate_capped` |
| Mid-stream bit resume (estimated setup, seeks) | `prime` / `set_dictionary` / `prepare_at_bit_offset` |
| Estimated residual continue (`inflate_from_block`) | `prepare_at_bit_offset` + `inflate_capped` (`NoFlush`) |
| Estimated `inflate_tail` (`Z_BLOCK` walk) | `create` / `reset` / `prime` / `set_dictionary` / `inflate_capped` (`Block`; `unused_bits` / `at_block_end` / `last_block`) |
| Seek sessions + indexed segment decode | source `prepare_at_bit_offset` + `create` / `set_dictionary` / `inflate_into_slice` (fixed caller `out` slices) |

**Unsafe inflate ABI inventory** (grep `z::inflate*` / `isal_inflate*` under
`crates/rapidgzip-core/src`):

- **`z::inflate` only** in `inflate_backend.rs` (`RawInflater` implementor of
  `inflate_capped` / `inflate_into_slice`; trait `inflate` defaults to
  unlimited `inflate_capped`). No residual direct call sites in `backend.rs`
  sequential/estimated paths, `stream_decode`, `analyze`, `seek`, or
  `indexed_decode`.
- **Lifecycle only** on `RawInflater` in `backend.rs`: `inflateInit2_`,
  `inflateReset`, `inflatePrime`, `inflateSetDictionary`, `inflateEnd`.
  Trait `create` / `reset` / `prime` / `set_dictionary` thin-wrap those.
- **ISA-L** (`isal` feature): `isal_inflate*` only in `isal_backend.rs`.

Default product residual without `isal`: P=1 and small / low-thread budgets
remain zlib-rs-limited. With `isal`, sequential paths use ISA-L.

**2026-08-01 (0.2.1):** the optional **`isal`** feature now ships. Fair re-bench
of Rust `--features isal` vs C++ rapidgzip 0.16 ISA-L is in
[benchmarks/RESULTS-SNAPSHOT.md](benchmarks/RESULTS-SNAPSHOT.md) (~**1.35×**
P=1 thrpt geometric mean on the synthetic fair corpora; do not treat those
cells as a re-measure of the older public-FASTQ matrix below). Default builds
stay zlib-rs-only.

Shipped parallel paths and format coverage are listed in
[CHANGELOG.md](CHANGELOG.md); intentional sequential product gates (P=1 stream
`decode_read` without `isal`, P=1–3 marker skip, small streams under ~2× grid
amortization) are not unfinished work.

## Current conclusion

The optimized generic marker pipeline now clears the intermediate public-FASTQ
gate against zlib-ng-only C++ rapidgzip. At requested worker budgets 1, 4, 16,
and 44, its median throughput is 228.5%, 146.5%, 116.3%, and 99.7% of that
control, with a 140.4% geometric mean and lower observed peak RSS in every
cell. The reproducible matrix is in [BENCHMARKING.md](BENCHMARKING.md).

Against ISA-L-enabled rapidgzip **on default (zlib-rs) builds**, the historical
public-FASTQ matrix reached 91.7%, 133.9%, 104.5%, and 100.6%, with a 106.6%
geometric mean. The remaining formal ISA-L failure was therefore narrow and
specific: the one-worker path at 658.7 MiB/s versus 718.3 MiB/s. That path is
authoritative sequential inflate rather than the native marker decoder, so
further multi-worker marker tuning does not close it.

**Status (0.2.1):** that one-worker residual is **narrowed / closed as a product
gate** by the optional `isal` feature (sequential inflate → ISA-L
`IsalInflater`). Fair synthetic re-bench: [benchmarks/RESULTS-SNAPSHOT.md](benchmarks/RESULTS-SNAPSHOT.md)
(~1.35× P=1 geo mean vs C++ ISA-L). The FASTQ numbers above are **not**
re-measured with `isal` here; keep them as the zlib-rs-era record only.

Pure-Rust gzippy reaches 798.1 MiB/s at one worker on the same file. This was
strong evidence that an ISA-L binding is not the only way to beat zlib-rs on
throughput, although its library/output design differs from this project's
incremental `Read + Send` contract.

## What changed

The original FASTQ profile showed that the native path was doing useful work,
but doing it byte by byte. Temporary counters found 93 scheduled tasks, 93
successful decodes, 93 committed chunks, 92 structural candidates, and no
candidate failure or authoritative fallback. The problem was not discarded
speculation. It was native decode cost, serial marker resolution, and excessive
memory pressure at high worker counts.

The accepted changes are:

- Unknown-history LZ77 copies use the chunk's `Vec<Symbol>` as their history.
  Cross-boundary bytes remain predecessor markers; in-chunk matches use bulk
  `extend_from_within` plus geometric doubling for overlap. The old second
  32 KiB `u16` history ring is no longer maintained in this hot path.
- Output-limit checks and reserves are hoisted to the match level instead of
  repeated for every copied byte.
- The coordinator resolves only the final 32 KiB required to derive the next
  predecessor window. Full marker replacement runs concurrently as ordered
  work in the same bounded pool.
- Large full-window marker buffers use a branch-free `u16 -> u8` lookup table.
  Small buffers retain runtime SSE4.1 dispatch on x86-64, NEON on AArch64, and
  the scalar oracle.
- Huffman leaves are packed into `u16`, halving the large direct tables. The bit
  reader has a proven eight-byte fast path and retains checked safe-Rust tail
  handling.
- Native decode/resolve concurrency is empirically controlled rather than
  capped at a compiled-in value. A process-affinity-aware square-root bootstrap
  is capped monotonically by the requested worker budget and avoids linear
  speculative-memory growth. Long inputs search both downward and upward using
  generation-tagged native worker throughput; short inputs do not pay a
  calibration cost they cannot amortize. The scheduling horizon follows the
  selected limit, and worker ranks are created lazily. BGZF and stored paths
  keep their format-specific parallelism.

Correct overlapping-copy behavior is covered across predecessor references,
short-distance overlap, and 32 KiB wraparound. Suffix-only window derivation is
tested against full resolution. The existing single-member, concatenated gzip,
BGZF, corruption, cancellation, output-limit, and paraseq-facing `Read + Send`
tests remain the release gate.

## Current profile

A diagnostic 44-budget profile before bounding active workers attributed
approximately 60.0% of user cycles to native compressed-block decode, 13.6% to
marker replacement, 11.0% to `memcpy`, 4.7% to structural-candidate search,
4.0% to Huffman-table construction, and 2.1% to PCLMULQDQ CRC32. The controller
does not reduce the work per byte; it prevents that work from becoming slower
and more memory-intensive when spread across this host's two sockets.

The public FASTQ matrix originally confirmed the benefit of a bounded native
window, but its first empirical controller overfit that 44-core case: it could
only compare a one-third-machine bootstrap with one additional worker. On an
88-CPU host a 64-budget request therefore began around 30, could not search
downward, created all 64 requested threads, and regressed FASTQ median and tail
performance while materially increasing RSS.

The replacement controller resolves that failure mode. In a same-host
diagnostic on the public FASTQ, the 64-budget median improved from 1,491 MiB/s
with the first controller to 1,747 MiB/s, and median RSS fell from roughly
585 MiB to 426 MiB. The parent fixed-16 implementation reached 1,662 MiB/s in
the comparison run. Under the release 44-CPU affinity mask, the replacement's
14-rank short-input bootstrap reached 1,602 MiB/s, retaining parity with the
published C++ ISA-L and zlib-ng cells. A low-compression 256 MiB fixture reached
1,762 MiB/s at a 64-worker budget versus 1,567 MiB/s for the fixed-16 parent.

## Rejected experiments

Measured regressions were removed rather than retained behind optimistic
dispatch:

- 2 MiB and 4 MiB compressed grid spacing increased per-task allocations and
  substantially reduced 44-budget throughput.
- Native task windows of 12, 17, 18, and 32, and several fixed decode/resolve
  worker splits, were inferior to the 16-task shared pool on this corpus.
- Resolving markers into the existing `Vec<Symbol>` allocation reduced RSS but
  caused cache read/modify/write traffic and lowered throughput.
- A two-level nine-bit-root Huffman table reduced construction size but made the
  much hotter symbol decode branchier and slower.
- An AVX2 kernel that packed 16 literals and patched only marker lanes was
  slower than the branch-free scalar lookup on this Broadwell CPU. AVX2 is not
  automatically advantageous for irregular marker loads.
- A conventional bit reservoir and eager multi-megabyte output preallocation
  also regressed this workload.

These results answer the earlier AVX2 question: runtime AVX2 remains valuable
inside zlib-rs, but the AVX2 marker design tested here was not a winner. Shipped
x86-64 code continues to use runtime feature detection; no `target-cpu=native`
assumption is required.

## Closing the remaining ISA-L gap

> **Update 2026-08-01 (0.2.1):** optional feature **`isal`** ships; sequential
> inflate can use ISA-L as `ActiveInflater`. Fair re-bench (~**1.35×** P=1
> thrpt geo mean vs C++ rapidgzip ISA-L on synthetic corpora):
> [benchmarks/RESULTS-SNAPSHOT.md](benchmarks/RESULTS-SNAPSHOT.md). The
> zlib-rs-era FASTQ profiling and priority list below remain historical
> context for default (no-`isal`) builds; they are not a claim that the
> optional backend is unfinished.

The paired one-worker follow-up attributed the gap more precisely:

| counter | Rust/zlib-rs | C++/ISA-L | gzippy |
|---|---:|---:|---:|
| task-clock | 546.8 ms | 478.5 ms | 396.9 ms |
| cycles | 1.906 billion | 1.661 billion | 1.370 billion |
| instructions | 4.183 billion | 3.363 billion | 3.535 billion |
| branches | 668.5 million | 511.0 million | 575.4 million |
| branch misses | 26.9 million | 19.0 million | 16.6 million |

In Rust, 88.8% of sampled cycles are in
[zlib-rs](https://github.com/trifectatechfoundation/zlib-rs)'s runtime-dispatched
`inflate_fast_help_avx2`, 4.2% in its PCLMULQDQ CRC, 3.1% in output zero-fill,
2.6% in Huffman table construction, and less than 1% in the surrounding
inflate state machine. The C++ profile is dominated by ISA-L's assembly decode
and copy loops. gzippy's measured binary uses its default
[Linux/x86-64 whole-fast-loop assembly kernel](https://github.com/jackdanger/gzippy/blob/fa2862a44af0c3123758c2d8990e934da9b55971/src/decompress/parallel/asm_kernel.rs)
with multi-literal Huffman decoding; its result is not produced by a portable
scalar Rust inflater.

Removing the redundant output zero-fill and recycling the direct writer's
allocation lowered Rust to roughly 1.84 billion cycles. In a subsequent
portable 15-run interleaved comparison, Rust reached a 0.510 s median and
676.3 MiB/s versus ISA-L's 0.464 s and 743.2 MiB/s, or 91.0%. Larger input and
output buffers, fat LTO, and a diagnostic `target-cpu=native` build were neutral
within run variance; the latter improved by less than 1%, confirming that the
shipped runtime dispatch is already selecting the important hardware path.

The next architectural work, in priority order, is:

1. Keep zlib-rs as the default and prototype an upstreamable multi-symbol fast
   loop or more efficient match-copy path in zlib-rs. The profile says changes
   around its Rust API or our ABI adapter cannot recover the remaining 9%.
2. Evaluate a faster CRC fold. Even eliminating CRC entirely would only just
   cross the 95% threshold, so CRC work must accompany an inflater improvement
   and remain verified on every member.
3. ~~If a second backend is acceptable, evaluate it behind an explicit feature
   without changing the default.~~ **Done in 0.2.1** as optional `isal` /
   `IsalInflater` (default remains zlib-rs). The observed gzippy path still
   cannot be adopted as a drop-in crate call: it owns the complete input/output
   and its fastest kernel is Linux/x86-64-specific.
4. Otherwise, build an in-tree whole-loop x86-64 kernel with runtime dispatch,
   plus safe scalar and AArch64 paths. This is likely capable of closing the
   gap, but it is a substantially larger unsafe surface than the current
   localized loads and marker replacement.

The formal remaining gate **for default zlib-rs builds** was at least 95% of
ISA-L-enabled rapidgzip in the one-worker FASTQ cell while preserving at least
100% geometric mean across all four budgets, correctness, the streaming API,
and memory behavior. With **`isal`** enabled, that P=1 product residual is
addressed via a second backend rather than further zlib-rs-only tuning (fair
synthetic evidence in [benchmarks/RESULTS-SNAPSHOT.md](benchmarks/RESULTS-SNAPSHOT.md);
no new FASTQ cell is published here).

## Does ISA-L support NEON?

Yes, with an important qualification: the ISA-L fork vendored by rapidgzip has
an AArch64 implementation, not an x86-only implementation. At the audited
rapidgzip submodule commit, its CMake build recognizes `aarch64` and `arm64`;
its runtime dispatcher checks Linux `HWCAP_ASIMD`; and its AArch64 inflate
assembly uses Advanced SIMD/NEON instructions including `ld1` and `tbl`. The
dispatcher selects its optimized AArch64 stateless Huffman decoder when the Arm
CRC32 feature is available, otherwise retaining a base fallback.

Relevant upstream source:

- [AArch64 dispatcher](https://github.com/mxmlnkn/isa-l/blob/6a7c87e34293f427600e37f702d8a4d73391e48d/igzip/aarch64/igzip_multibinary_aarch64_dispatcher.c)
- [AArch64 Huffman decoder assembly](https://github.com/mxmlnkn/isa-l/blob/6a7c87e34293f427600e37f702d8a4d73391e48d/igzip/aarch64/igzip_decode_huffman_code_block_aarch64.S)
- [Architecture selection in CMake](https://github.com/mxmlnkn/isa-l/blob/6a7c87e34293f427600e37f702d8a4d73391e48d/CMakeLists.txt)

Our marker replacement similarly has an AArch64 NEON implementation. Future
architecture-specific kernels should preserve runtime multiversioning on
x86-64, baseline NEON on AArch64, and scalar reference paths for unsupported
targets and differential testing.

## Unsafe-code conditions

Unsafe remains limited to measured kernels and FFI boundaries. Every site must
have a safe wrapper and written argument covering feature dispatch, bounds,
alignment, aliasing, initialization, and overlap. Scalar implementations remain
the oracle. Wider SIMD or unchecked Huffman access is not justified merely to
remove bounds checks; a representative profile and differential tests are
required first.
