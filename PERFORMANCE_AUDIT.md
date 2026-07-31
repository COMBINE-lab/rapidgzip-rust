# Performance audit

## Scope and conclusion

This audit asks how `rapidgzip-rust` could approach ISA-L-enabled C++
rapidgzip without linking ISA-L. It records observations and proposed work; it
does not implement any of the optimizations.

The public FASTQ run in [BENCHMARKING.md](BENCHMARKING.md) changes the priority
order. The one-thread zlib-rs route is already reasonably competitive at
605.8 MiB/s, 85.1% of ISA-L-enabled rapidgzip and more than twice the zlib-ng
control on this host. The immediate problem is that selecting multiple workers
switches generic streams to the speculative native decoder and makes this file
slower while consuming much more CPU.

This is also a useful feasibility result: pure-Rust gzippy reaches 786.2 MiB/s
at one thread and 1,972.3 MiB/s at 16 threads on the same bytes. Direct ISA-L
binding is therefore not a prerequisite for ISA-L-class performance, although
matching it across formats and machines will require substantial native-decoder
work.

## Evidence

The one-worker path uses authoritative zlib-rs from the member start. The
multi-worker path divides the compressed stream at a 1 MiB grid, scans up to
512 KiB for a structurally plausible dynamic-Huffman boundary, decodes from an
unknown 32 KiB marker window, and retains up to another 512 KiB of compressed
lookahead. Each task may produce up to 20 times its compressed-grid size.

One diagnostic `perf stat`/`perf record` run at one and four workers, on the same
pinned CPUs and corpus as the measured matrix, produced:

| counter | 1 worker | 4 workers | multiplier |
|---|---:|---:|---:|
| wall seconds | 0.598 | 1.156 | 1.93x |
| task-clock seconds | 0.597 | 5.035 | 8.43x |
| cycles | 1.89 billion | 14.32 billion | 7.58x |
| instructions | 4.18 billion | 27.99 billion | 6.69x |
| branches | 0.67 billion | 5.00 billion | 7.47x |
| branch misses | 26.4 million | 250.0 million | 9.48x |
| cache misses | 1.16 million | 15.56 million | 13.36x |

At one worker, 85.2% of sampled cycles were in zlib-rs's runtime-dispatched
`inflate_fast_help_avx2`, plus 3.6% in its PCLMULQDQ CRC implementation. At four
workers, 75.8% were in our scalar `decode_compressed_block`, 15.6% in SSE4.1
marker resolution, 1.6% in structural-candidate scanning, and 1.6% rebuilding
Huffman tables. These are single diagnostic profiles rather than confidence
intervals, but they agree with the nine-run user-CPU medians and identify the
hot code unambiguously.

The source explains the amplification:

- Both marked and clean LZ77 matches are copied one symbol or byte at a time
  through a 32 KiB `u16` ring. Each byte rechecks output length; marked output
  also updates and tests the marker count.
- Huffman decoding performs one table lookup per symbol. A structural candidate
  builds and validates complete trees before the candidate is decoded, so false
  candidates repeat nontrivial work.
- Marker resolution uses runtime SSE4.1 dispatch on x86-64 and a NEON kernel on
  AArch64, but it handles only all-literal groups of eight. Any marker in the
  group sends all eight symbols through scalar resolution. There is no AVX2
  marker kernel today.
- zlib-rs is used only after a complete block leaves a marker-free history.
  Long-distance FASTQ backreferences keep marker dependencies alive and delay
  that handoff. [Upstream rapidgzip documents this exact corpus](https://github.com/mxmlnkn/rapidgzip#decompression-of-gzip-compressed-fastq-data)
  as requiring its costly two-stage decode for almost all data.
- When the coordinator rejects a speculative result and falls back from the last
  authoritative boundary, already-running tasks do not see the stop flag until
  the whole member fallback is complete. Their output is then discarded. The
  bounded task window limits this work but does not make it useful.

## Does ISA-L support NEON?

Yes, with an important qualification: the ISA-L fork vendored by rapidgzip has
an AArch64 implementation, not an x86-only implementation. At the exact
rapidgzip submodule commit audited here, its CMake build recognizes `aarch64`
and `arm64`; its runtime dispatcher checks Linux `HWCAP_ASIMD`; and its AArch64
inflate assembly uses Advanced SIMD/NEON instructions including `ld1` and
`tbl`. The dispatcher selects its optimized AArch64 stateless Huffman decoder
when the Arm CRC32 feature is available, otherwise retaining a base fallback.

Relevant upstream source:

- [AArch64 dispatcher](https://github.com/mxmlnkn/isa-l/blob/6a7c87e34293f427600e37f702d8a4d73391e48d/igzip/aarch64/igzip_multibinary_aarch64_dispatcher.c)
- [AArch64 Huffman decoder assembly](https://github.com/mxmlnkn/isa-l/blob/6a7c87e34293f427600e37f702d8a4d73391e48d/igzip/aarch64/igzip_decode_huffman_code_block_aarch64.S)
- [Architecture selection in CMake](https://github.com/mxmlnkn/isa-l/blob/6a7c87e34293f427600e37f702d8a4d73391e48d/CMakeLists.txt)

Our marker replacement similarly has an AArch64 NEON implementation. The
performance work below should preserve runtime multiversioning on x86-64 and
compile an AArch64 baseline NEON implementation, with scalar reference paths
for testing and unsupported targets.

## Recommended optimization order

### 1. Make speculation self-limiting

Add internal counters for candidate attempts, successful boundary joins,
marker lifetime, zlib-rs handoffs, output produced before failure, and bytes
discarded after authoritative fallback. Use those measurements to make the
following scheduler changes:

1. Set the worker stop flag as soon as the coordinator chooses whole-member
   authoritative fallback, before entering sequential inflate.
2. Start with a small speculative window and expand only after early chunks
   join successfully. Do not immediately schedule `threads + 2` expensive
   chunks on an unclassified stream.
3. If markers remain live for most of the sampled output, candidates repeatedly
   fail, or speculation consumes more CPU per committed byte than zlib-rs,
   reduce concurrency or use the sequential backend for that member.

This should eliminate the current outcome where asking for more threads makes
FASTQ decompression two times slower than one thread. It is a guardrail, not the
eventual parity mechanism: sequential fallback would restore roughly
605.8 MiB/s here but would not reach 1.5--2.0 GiB/s.

### 2. Remove byte-at-a-time DEFLATE work

Implement bulk overlapping match-copy kernels for the clean-history phase and
specialized marker propagation for the unknown-history phase. Separate marked
and marker-free phases outside the per-byte hot loop so literal emission does
not repeatedly sum two vector lengths or test marker state. Then evaluate a
multi-symbol or wider two-level Huffman decoder that consumes several literals
per refill and reuses validated tables.

These changes attack the 75.8% scalar decoder hotspot. Correct overlapping-copy
semantics for distances shorter than the match length are essential; tests
must cover every distance class, wraparound at 32 KiB, maximum length 258, and
arbitrary predecessor markers.

### 3. Extend SIMD beyond literal packing

Benchmark AVX2 and NEON marker kernels that classify 16 or more `u16` symbols,
resolve marker indices, and pack literals without dropping an entire vector to
scalar work because one lane is marked. SIMD is also worth evaluating for
structural-candidate prefiltering and bulk history updates. Runtime dispatch is
required for shipped x86-64 binaries; `target-cpu=native` is useful only as a
diagnostic. The current zlib-rs profile demonstrates that its AVX2 and PCLMUL
runtime paths are already active.

AVX2 is not automatically better than two 128-bit operations on every CPU, and
wide instructions can reduce clock frequency. Keep scalar, SSE4.1, AVX2, and
NEON microbenchmarks and select only measured winners per operation.

### 4. Reduce duplicate parsing and inflater setup

Carry a validated candidate's parsed Huffman trees into decoding instead of
building them again. Reuse zlib-rs inflater state in generic workers as the BGZF
path already does, and minimize dictionary materialization at marker-free block
handoffs. Investigate whether using zlib-rs's Rust API rather than its zlib ABI
would expose safe state reuse or more direct continuation; this is an API and
dependency choice, so it should be decided with the maintainer before changing
crates or relying on non-public internals.

### 5. Reduce allocation, transfer, and NUMA costs

The current per-task allowance can reach 20 MiB and median 44-worker RSS is
535 MiB. Use pooled segmented output with smaller evidence-based initial
reserves, and avoid moving large marker buffers between sockets where possible.
After the hot-loop work, benchmark CPU affinity and NUMA-aware queues on
multi-socket hosts. These changes matter at high worker counts but cannot
explain the sevenfold instruction increase by themselves.

### 6. Consider pure-Rust inflater designs, after measuring the above

gzippy proves that another pure-Rust implementation is competitive on this
corpus. We should audit whether its decoding design can be reused, adapted, or
compared at a finer granularity. Other Rust inflate implementations are also
possible. There is more than one viable crate/code-source choice, with API,
maintenance, and licensing tradeoffs, so no dependency should be selected
without an explicit maintainer decision.

Only after stable algorithmic wins should we measure PGO or post-link layout
optimization. Thin LTO and one codegen unit are already enabled; compiler-only
tuning will not erase the current redundant work.

## Unsafe-code conditions

Bulk SIMD, unchecked table access, and direct writes into spare vector capacity
may justify small unsafe kernels. Each must have a safe wrapper and a written
safety argument covering CPU-feature dispatch, input/output bounds, alignment,
aliasing, initialized length, and overlap semantics. Scalar implementations
remain the reference oracle. Differential property tests should compare every
kernel against that oracle across random windows, bit offsets, malformed input,
and all vector tails; architecture CI should exercise x86-64 baseline,
SSE4.1/AVX2, and AArch64 NEON. Unsafe is not justified merely to remove a bounds
check until a profile shows that check is material.

## Validation gates for each stage

Every stage should preserve the existing gzip, concatenated-member, and BGZF
correctness suites, CRC/ISIZE verification, `Read + Send` behavior, fuzzing,
Miri coverage of safe orchestration, and sanitizer runs around unsafe kernels.
Performance validation should include:

- this public FASTQ at 1, 4, 16, and 44 physical cores;
- a much larger FASTQ on a single NUMA node and across sockets;
- ordinary single-member, concatenated-member, and BGZF corpora with varied
  compression ratios and compressors;
- median wall time, total CPU time, committed/discarded speculative bytes,
  successful backend handoffs, and peak RSS;
- ISA-L-enabled C++ rapidgzip, zlib-ng-only C++ rapidgzip, and gzippy from
  pinned revisions.

The scheduler stage passes only if additional workers never cause a major
regression on this FASTQ. Final parity remains the target in
[BENCHMARKING.md](BENCHMARKING.md): at least 95% of ISA-L-enabled C++ rapidgzip
in every required cell and at least 100% geometric mean, without sacrificing
the streaming API or correctness contract.
