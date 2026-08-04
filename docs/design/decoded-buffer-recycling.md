# Bounded decoded-buffer recycling

Status: phase one implemented; additional decode paths remain separately
benchmark-gated

Origin: the decode-local byte-buffer free lists in PR #5

## Decision

The current push path already returns a cleared output allocation to sequential
decode, and strict indexed-parallel decode has its own recycled-vector queue.
The positional `DecoderReader` handoff does not: ownership moves through the
bounded channel and the coordinator receives a new zero-capacity vector for
its next fill. A fast consumer eventually drops each fully read allocation.

Recycling those reader-channel buffers is a small, well-defined optimization
and is warranted. Sharing recycled capacity with marker resolution may also
help the primary FASTQ path, but it touches more hot code and must earn its
complexity independently. Dense-member, BGZF, stored, analysis, indexing, and
random-access paths must not all be converted merely because a pool exists.

The replacement is a private, decode-local, size-classed pool bounded by both
entry count and retained capacity. It adds no public option, dependency, or
unsafe API.

## Current ownership map

Understanding which allocations already survive is more important than
introducing a general allocator abstraction.

### Already reused

- `DirectOutput::emit_reusable` writes a chunk, clears it, and returns the same
  allocation.
- Sequential decoding threads the returned vector through `next_chunk`.
- Strict indexed-parallel workers steal completed `Vec<u8>` values from their
  decode-local `Injector` before reserving another output chunk.
- `IndexedReader` retains its foreground input and output vectors across reads.
- zlib-rs inflater state is worker-local and reset rather than reconstructed in
  the persistent parallel paths.

### Currently discarded

- `ChannelOutput` sends a positional reader chunk and falls back to the default
  `emit_reusable`, which returns `Vec::new()`.
- `DecoderReader` drops a fully consumed `current` vector when the next message
  replaces it.
- resolved marker `Vec<u8>` parts are emitted in order and dropped after a
  reader handoff; their capacity is not made available to later resolve jobs;
  and
- several authoritative fallback and specialized worker results allocate a
  fresh exact or target-sized vector per task.

The first two bullets form one ownership round-trip and should be fixed
together. The later bullets have different sizes and contention patterns and
are not automatically part of phase one.

## Why the PR #5 pool should not be copied unchanged

PR #5 uses a count-capped `Injector<Vec<u8>>`. Count alone does not bound
retained capacity: one unusual large buffer can occupy the same slot as a
normal chunk, and `2 * in_flight_chunks` entries can retain much more memory
than their name suggests. A single unsized free list can also repeatedly hand
a small buffer to a large request or retain oversized capacity for tiny work.

The pool is also threaded through the marker coordinator, reader, and worker
scratch in one change. That makes a throughput difference difficult to
attribute and increases the chance that a win in one route hides an RSS or
contention regression in another.

The clean implementation must therefore have exact byte accounting, size
classes, path-local rollout, and paired reader/push controls.

## Internal pool contract

`ByteBufferPool` is crate-private and owned by one decode. It contains a small
fixed set of capacity buckets backed by the already-depended-on
`crossbeam_deque::Injector` type. There is no process-global pool and no
cross-decode retention.

```rust,ignore
struct ByteBufferPool {
    buckets: Box<[Injector<Vec<u8>>]>,
    retained_bytes: AtomicUsize,
    retained_entries: AtomicUsize,
    maximum_bytes: usize,
    maximum_entries: usize,
    minimum_capacity: usize,
    maximum_capacity: usize,
}
```

The operations are:

```rust,ignore
fn take(&self, minimum_capacity: usize) -> Vec<u8>;
fn recycle(&self, buffer: Vec<u8>);
```

`recycle` clears the vector, rejects zero, too-small, and too-large capacities,
then reserves the vector's actual capacity against `maximum_bytes` with a
compare/exchange loop. Entry capacity is reserved separately before the vector
is published. If either reservation fails, every earlier reservation is rolled
back before the vector is dropped.

`take` starts with the smallest class capable of satisfying the request and
checks larger classes only. A successful steal subtracts the actual vector
capacity and one entry before returning it. A spurious retry spins only around
the lock-free deque operation; an empty pool returns `Vec::new()` immediately.

Buckets use powers of two around the configured decoded chunk size. The pool
does not round or resize a vector merely to classify it. Actual capacity
remains the memory-accounting unit.

The pool invariants are:

- every stored vector has length zero;
- a vector has one owner at all times;
- retained capacity never exceeds the configured byte ceiling;
- retained entries never exceed the entry ceiling;
- capacity is charged before queue publication and released after a successful
  steal; and
- all retained vectors are dropped with the decode-local pool.

These are resource and ownership invariants in safe Rust. No unsafe block is
needed for the pool.

## Phase one: positional reader handoff

The pool is created with the positional reader's coordinator and shared with
its `ChannelOutput`. Streaming readers remain synchronous and keep threading
their one existing vector through `next_chunk`; they do not need a pool.

Reusable channel emissions transport a private `PooledChunk` rather than a
bare vector:

```rust,ignore
struct PooledChunk {
    bytes: Option<Vec<u8>>,
    pool: Arc<ByteBufferPool>,
}
```

`PooledChunk::drop` clears and recycles its vector. This covers every ownership
exit, including normal read completion, `finish`, reader cancellation, a
queued message dropped after receiver shutdown, and a terminal error. The
reader holds the wrapper as its current chunk and reads the byte slice without
changing message or public-reader semantics.

`Output::emit`, which gives the producer no replacement allocation, remains an
unpooled channel message in phase one. Otherwise marker and specialized paths
could fill the retained pool without having any code that calls `take`. Only
`emit_reusable` opts into the ownership round-trip.

After `ChannelOutput::emit_reusable` sends one `PooledChunk`, it calls
`pool.take(configured_chunk_size)` and returns that distinct empty allocation
to the coordinator. It never reclaims the just-sent vector while the reader can
observe it.

Pool ceilings are derived from the existing handoff configuration rather than
being public settings. The initial retained-byte ceiling is two decoded chunks
and the entry ceiling is `min(in_flight_chunks + 1, 4)`. These deliberately
small values should cover the common producer/consumer exchange without
turning idle capacity into another copy of the full channel bound. The exact
constants remain benchmark-tunable internal policy.

Direct push decoding continues returning its own allocation and does not use
the pool. This provides an otherwise-identical allocation-light control.

This phase therefore benefits sequential positional readers and strict
indexed-parallel reader handoff, both of which call `emit_reusable`. The generic
marker path currently calls `emit` for resolved parts and is intentionally
unchanged until phase two. A one-worker FASTQ win must not be presented as a
multi-worker marker win.

## Phase two: marker-resolution output

Phase two proceeds only if allocation profiles still show material marker
output allocation after phase one and a paired benchmark improves.

The marker pipeline already separates `marked`, `clean`, and backend-tail
vectors. It should not concatenate them merely to make pooling easier. Instead:

1. extend the private marker resolver with a method accepting an empty
   `Vec<u8>` scratch allocation;
2. obtain that scratch from the same decode-local size-classed pool;
3. retain the existing scalar/SSE4.1/NEON/lookup-table resolution choices;
4. emit each resolved part through `emit_reusable`; and
5. recycle the returned empty allocation for later worker or resolver work.

The initial implementation may safely `resize` reused output before the SIMD
resolver writes it. Avoiding that initialization would require a new
`MaybeUninit<u8>`-aware kernel contract and a larger safety proof; allocation
recycling must demonstrate a remaining zero-fill bottleneck before that unsafe
surface is considered.

Worker-local `Vec<Symbol>` storage is not byte-buffer-compatible and remains
worker-local. Compressed input pages and predecessor windows also stay out of
the pool. Mixing semantically different buffers would make capacity demand
less predictable and increase cache pollution.

## Later paths are opt-in experiments, not part of the design promise

Dense-member and BGZF workers often know exact output sizes and can retain one
result per task. Stored streams are commonly limited by copy bandwidth rather
than allocation. Analysis intentionally retains only one history window and
has already reached its C++ performance target. Random-access caching has a
separate lifetime and memory policy.

Each path therefore needs its own allocation profile and paired benchmark
before adopting `ByteBufferPool`. No generic "all `Vec<u8>` values come from
the pool" rule is introduced.

## Memory and telemetry

Phase-one peak retained memory is bounded by:

```text
existing channel payload bound
+ at most two retained decoded-chunk capacities
+ current reader chunk
+ coordinator/worker scratch already present before this change
```

In normal steady state the pool replaces a future allocation rather than
adding to this maximum, but the worst-case formula is retained honestly for
RSS review.

No public `DecoderStats` field is added initially. Pool hits, misses, rejected
bytes, retained bytes, and high-water marks are exposed under tests and by the
unpublished benchmark binary. If applications later need memory telemetry, it
should be designed across all decoder allocations rather than exposing one
internal pool in isolation.

## Error, cancellation, and panic behavior

Recycling is an optimization and never changes decode success:

- allocation remains fallible in the same places as today;
- an empty pool falls back to ordinary vector growth;
- an over-budget buffer is dropped rather than blocking;
- recycling does not occur until the last owner releases a chunk;
- cancellation drops queued `PooledChunk` messages and therefore returns or
  frees their allocations; and
- pool poisoning is impossible because the proposed hot path has no mutex.

If a worker panics, ordinary ownership drop releases its local vectors. The
coordinator retains the current worker-panic error behavior.

## Implementation plan

1. Add the private size-classed pool with deterministic unit tests for byte and
   entry accounting, capacity classes, replacement, concurrent steal/recycle,
   and rejection of abnormal capacities.
2. Add benchmark-only counters and an allocation/RSS baseline for direct push,
   a fast `DecoderReader` consumer, a one-byte consumer, and `finish` without
   payload consumption.
3. Introduce `PooledChunk` and phase-one reader recycling. Exercise normal EOF,
   early drop, errors, cancellation, full channels, `read_vectored`, and
   paraseq's `Read + Send` usage.
4. Re-run the full FASTQ thread matrix and slow-consumer telemetry. Retain phase
   one only if it reduces allocation churn or RSS without a statistically
   meaningful throughput or latency regression.
5. Profile marker resolution after phase one. Add scratch-accepting resolution
   and coordinator recycling only if allocation remains material.
6. Evaluate every other path separately and leave it unchanged absent a clear
   paired win.
7. Document the final memory formula and benchmark evidence in architecture,
   performance audit, changelog, and rustdocs where ownership behavior is
   relevant.

## Acceptance gates

Correctness requires the complete existing suite plus focused tests for reader
EOF/report semantics, multi-member gzip, BGZF, zlib, raw DEFLATE, indexed
readers, output limits, corrupt input, worker panic, cancellation, and short
consumer buffers.

Performance evaluation uses the public FASTQ file and generated single-member,
multi-member, BGZF, stored, zlib, and raw fixtures at the established worker
cells. Report:

- decoded throughput and latency;
- allocation count and allocated bytes when an instrumented allocator is
  available;
- pool hit/miss/rejection counts and retained-byte high-water mark;
- peak RSS;
- live worker counts; and
- direct push versus `DecoderReader` ratios.

Phase one is accepted if it materially reduces reader allocation churn on the
FASTQ workload, does not regress median throughput beyond normal run variance,
does not increase peak RSS beyond the documented retained bound, and preserves
the dynamic worker/backpressure behavior. Phase two must independently improve
the marker-reader path; a phase-one win is not evidence for merging it.

## Phase-one outcome

The implementation is intentionally narrower than a general allocator. A
private `ByteBufferPool` owns three capacity classes around the configured
decoded chunk size and charges actual capacity and entry count atomically before
publishing an empty vector. A private RAII reader chunk returns capacity after
normal consumption and also covers disconnected sends, dropped queued output,
early reader cancellation, `finish`, and terminal failures. Safe Rust expresses
the complete ownership protocol; this work adds no unsafe block or dependency.

The efficacy probe used a deterministic 128 MiB FASTQ-like single-member gzip,
a one-worker positional `DecoderReader`, and the default 4 MiB decoded chunk.
Valgrind DHAT measured:

| revision | allocated bytes | allocation blocks | bytes at global heap peak |
|---|---:|---:|---:|
| exact pre-change `main` | 145,808,062 | 86 | 23,120,080 |
| phase one | 70,317,510 | 74 | 23,127,000 |

That is 51.8% fewer allocated bytes and 14.0% fewer allocations, while the
6,920-byte sampled heap-peak difference is negligible relative to the explicit
two-chunk retention bound. A paired 20-sample Criterion run of the reusable
one-worker gzip reader was statistically unchanged: 218.06 MiB/s after versus
217.35 MiB/s before. Five runs of the intentionally unchanged 44-budget stored
reader had medians of 2,646.9 and 2,649.2 MiB/s, a 0.1% difference.

Phase one therefore passes its allocation, throughput, and memory gates.
Marker resolution is not evidence-backed by this result and remains deferred;
it must pass its own profile and paired benchmark before it may use the pool.
