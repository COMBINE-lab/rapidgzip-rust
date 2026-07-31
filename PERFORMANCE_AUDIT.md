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

The next work should start with a paired one-worker profile of Rust, C++
rapidgzip with ISA-L, and gzippy. In priority order:

1. Attribute the approximately 80 MiB/s gap among DEFLATE decode, CRC32, input
   paging/copying, zlib ABI calls, and process/setup overhead. Compare cycles,
   instructions, branch misses, and bytes per backend call.
2. Tune the existing zlib-rs route's input and output buffer sizes and reuse
   inflater/output state where the profile supports it. This requires no new
   dependency.
3. Evaluate the safe zlib-rs Rust API against the current zlib-compatible ABI
   only if it exposes meaningful state or buffer advantages. That is an API and
   crate-surface choice and must be discussed before changing dependencies.
4. Compare one native worker against authoritative zlib-rs. The native decoder
   is now competitive in parallel, but its scalar symbol loop may still lose at
   one worker; measurements should decide whether a hybrid route is useful.
5. Only after those changes, revisit wider/multi-symbol Huffman decode with a
   microbenchmark representative of FASTQ codes. The rejected generic two-level
   design is not a basis for shipping more unsafe code.

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
