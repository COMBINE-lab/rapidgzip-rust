# Performance audit

## Current conclusion

The optimized generic marker pipeline now clears the intermediate public-FASTQ
gate against zlib-ng-only C++ rapidgzip. At requested worker budgets 1, 4, 16,
and 44, its median throughput is 216.7%, 138.8%, 121.4%, and 114.4% of that
control, with a 143.0% geometric mean and lower observed peak RSS in every
cell. The reproducible matrix is in [BENCHMARKING.md](BENCHMARKING.md).

Against ISA-L-enabled rapidgzip, Rust reaches 88.8%, 125.0%, 107.7%, and
107.3%, with a 106.4% geometric mean. The remaining formal ISA-L failure is
therefore narrow and specific: the one-worker path is 631.4 MiB/s versus
710.9 MiB/s. That path is authoritative zlib-rs inflate rather than the native
marker decoder, so further multi-worker marker tuning will not close it.

Pure-Rust gzippy reaches 800.2 MiB/s at one worker on the same file. This is
strong evidence that an ISA-L binding is not inherently required, although its
library/output design differs from this project's incremental `Read + Send`
contract.

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
- The combined native decode/resolve window and active generic worker count are
  capped at 16. Each task commonly owns 3--4 MiB of `u16` output on this FASTQ;
  allowing a 44-task active window crossed NUMA boundaries, raised system CPU
  and peak RSS, and reduced wall throughput. The public thread setting is now
  documented as a maximum worker budget. BGZF and stored paths keep their
  format-specific parallelism.

Correct overlapping-copy behavior is covered across predecessor references,
short-distance overlap, and 32 KiB wraparound. Suffix-only window derivation is
tested against full resolution. The existing single-member, concatenated gzip,
BGZF, corruption, cancellation, output-limit, and paraseq-facing `Read + Send`
tests remain the release gate.

## Current profile

A diagnostic 44-budget profile before applying the active-worker cap attributed
approximately 60.0% of user cycles to native compressed-block decode, 13.6% to
marker replacement, 11.0% to `memcpy`, 4.7% to structural-candidate search,
4.0% to Huffman-table construction, and 2.1% to PCLMULQDQ CRC32. The cap does
not reduce the work per byte; it prevents that work from becoming slower and
more memory-intensive when spread across this host's two sockets.

The latest matrix confirms the distinction. At budget 16, Rust uses median
1.91 s user and 0.46 s system CPU and finishes in 0.200 s. At budget 44, where
the native active count remains 16, it uses 1.94 s user and 0.50 s system CPU
and finishes in 0.208 s. The earlier unrestricted 44-worker experiments used
more CPU and memory while taking longer.

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
3. If a second backend is acceptable, evaluate it behind an explicit feature
   without changing the default. The observed gzippy path cannot be adopted as
   a drop-in crate call: it owns the complete input/output and its fastest
   kernel is Linux/x86-64-specific. Reusing its algorithm would require design
   work for resumable output and non-x86 fallbacks.
4. Otherwise, build an in-tree whole-loop x86-64 kernel with runtime dispatch,
   plus safe scalar and AArch64 paths. This is likely capable of closing the
   gap, but it is a substantially larger unsafe surface than the current
   localized loads and marker replacement.

The formal remaining gate is at least 95% of ISA-L-enabled rapidgzip in the
one-worker FASTQ cell while preserving at least 100% geometric mean across all
four budgets, correctness, the streaming API, and memory behavior.

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
