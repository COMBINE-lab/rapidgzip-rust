# Shared decoder pool and runtime growth

## Problem and compatibility boundary

A process may decode many FASTQ gzip files under one CPU budget. Before this
feature, each `Decoder` owned an independent elastic worker population and the
caller had to divide the budget before it knew which files exposed useful
parallel work. A low early `DecoderHandle::set_worker_limit` could also affect
marker/window admission and permanently select the sequential path, so raising
the limit later did not reliably restore parallelism.

The new behavior is additive:

- a decoder without `DecoderBuilder::decoder_pool` retains private scheduling;
- `set_worker_limit` remains a hard per-decoder ceiling and existing shrinking
  code compiles unchanged;
- `request_workers` adds an explicit persistent growth floor;
- pool and new per-decoder telemetry fields are opt-in observations on
  non-exhaustive snapshots; and
- the only general path-selection change is that transient runtime throttles no
  longer make the irreversible marker/sequential admission decision.

`bon` 3.9.3 supplies the new pool's named, validated typestate builder. The
existing concrete `DecoderBuilder` is intentionally unchanged: migrating that
published builder would change its concrete type and repeatable setter
semantics for no runtime benefit.

## Public API

```rust
let pool = DecoderPool::builder()
    .workers(24)
    .initial_worker_limit(8)
    .build()?;

let decoder = Decoder::builder()
    .decoder_threads(24)
    .decoder_pool(pool.clone())
    .build()?;

let reader = decoder.open("reads.fastq.gz")?;
let control = reader.handle();
control.request_workers(12)?;
control.set_worker_limit(16)?;
pool.set_worker_limit(20)?;
```

The three widths are deliberately distinct:

- `decoder_threads` is immutable per-operation headroom and bounds requests;
- `DecoderHandle::set_worker_limit` is a mutable hard ceiling for one decoder;
- `DecoderHandle::request_workers` is a persistent floor under adaptive demand;
  and
- `DecoderPool::set_worker_limit` is the mutable aggregate execution ceiling.

The effective desired width is:

```text
min(per-decoder hard ceiling, max(adaptive target, explicit request))
```

Consumer backpressure temporarily changes the local effective desire to one.
It does not immediately redistribute stable spawn reservations: short parser
stalls would otherwise create and retire path-local OS threads repeatedly.
Shared-pool grants and available work can reduce actual concurrency further. A
request above the current hard ceiling is retained, so a later ceiling increase
does not require another request. `clear_worker_request` returns demand to the
adaptive controller.

`DecoderPoolStats` exposes configured and live pool limits, occupied execution
slots, distributed active allowances, live worker and auxiliary threads,
queued tasks, attached and runnable decoders, and decoders waiting for a slot.
`DecoderStats` adds the persistent request, pre-pool desired width, and a
`pool_limited` signal. Snapshots remain `Copy`; aggregate queue and allowance
fields briefly lock the member registry while other values use atomic loads.
They are scheduling feedback, not transactional accounting.

## Why the pool allocates execution slots

The existing fast paths use scoped threads borrowing concrete `ReadAt` sources,
configuration, queues, and task arrays. Their tasks have different static types
and keep path-specific inflaters and scratch buffers warm across tasks. Moving
all work into permanent process-wide OS threads would require one of:

- type-erasing every path's tasks and owned inputs;
- forcing borrowed push decoding to become `'static`;
- unsafe lifetime extension across a long-lived executor; or
- copying source/configuration state and losing scratch locality.

Those costs are unnecessary to enforce a CPU budget. `DecoderPool` therefore
allocates execution slots. Each path retains its scoped worker loop, but every
CPU-intensive task acquires a shared permit. This keeps the current borrowed
push API, `Read + Send` readers, concrete task dispatch, scoped cancellation,
and inflater reuse. It also leaves the default private path with no pool CAS or
mutex.

The design does not promise that `worker_limit == number of OS threads`.
Workers above a newly lowered grant retire asynchronously, and a decoder with
runnable work but no current grant may retain one parked representative to
enter fair admission. `spawned_workers` makes that distinction observable. The
hard property is the number of concurrently executing decode regions.

## Stable allocation and fairness

Each attached operation has a `PoolMemberState` containing atomic queued,
busy, waiting, desired, granted, and runnable state.

A member is runnable only while it has a nonzero desire and at least one queued,
executing, or pool-waiting task; that state is telemetry and permit-pressure
feedback. Spawn-reservation redistribution instead uses round-robin max-min
allocation across attached members and their declared demand:

1. give each attached demand-bearing member one allowance up to its desire;
2. repeat while aggregate limit remains; and
3. rotate the first member considered at the next rebalance.

This distinction deliberately stabilizes path-local OS-thread counts across
short queue gaps and parser stalls. A terminal member or explicit demand
decrease releases its reservation. Growth made available only because a peer
finished is delayed until 128 subsequent queue publications demonstrate enough
remaining work to amortize more threads; explicit demand changes and pool-limit
raises grow immediately. Queue-depth publications never take the scheduler
mutex. Pool snapshots sum queue depth and active allowances under a brief
member-registry lock.

An execution permit has two paths:

- if no decoder is waiting, an atomic compare-exchange claims a free slot;
- under contention, tasks receive monotonically increasing tickets and wait in
  FIFO order under one mutex and condition variable.

The FIFO is per waiting task. Distributed spawn allowances prevent a single
decoder from eagerly creating every representative, while FIFO admission makes
eventual execution independent of which decoder currently owns a grant.
Permits are RAII values, so errors and panics release slots. Tiny BGZF and dense
gzip-member tasks may reuse a worker-local permit across a successful
nonblocking handoff and an immediately available next task; the lease is always
released before a queue or channel wait, cancellation, or grant loss.

Lowering a pool limit is nonblocking. Existing holders finish, which means a
snapshot can temporarily report `busy_workers > worker_limit`; no new permit is
issued until occupancy falls below the new limit. Raising the limit rebalances
grants and wakes every waiter.

## What consumes a slot

The pool covers more than ordinary inflater worker calls. Accounted regions
include:

- stored, dense-member, BGZF, marker/window, and imported-index worker tasks;
- authoritative sequential inflate chunks;
- bounded marker-admission exact and speculative work;
- dense-member header scanning in bounded scan chunks;
- initial stored/BGZF classification scans;
- coordinator-side exact bridges and fallback inflation; and
- ordered CRC32/Adler-32 accounting.

Small framing transitions, queue manipulation, reordering, telemetry, and
channel operations do not hold a slot. In particular, a task releases its
permit before a bounded worker-result or final-reader handoff would block.
Ordered checksum work releases its own permit before calling the user's `Write`
or waiting for a reader. This is the central deadlock and utilization
invariant: a slow consumer must never retain global decode capacity.

Broad per-file worker headroom also implies a broad default reader channel.
Shared positional readers retain that configured physical maximum. When a
decoder owns less than the whole live pool, batch sampling activates a logical
high watermark at its current allowance plus two chunks and clears the limiter
below the allowance. A decoder that owns the whole pool bypasses the limiter.
On a high watermark the coordinator yields for 250 microseconds so a fast
reader can drain transient backlog, then parks between checks for a sustained
slow consumer. The inactive limiter samples in batches; once active, it checks
every handoff until the low watermark clears. This bounds persistent per-file
parser buffering without putting shared-counter traffic on the unconstrained
hot path.

The dense-member scanner acquires per scan chunk and releases before its
bounded scan-ahead callback can wait for workers. Holding one permit around the
whole scanner would deadlock a one-slot pool after its queue reached the
scan-ahead bound.

## Admission and reliable growth

Algorithm admission answers whether the marker/window path could be useful at
configured capacity. It now uses:

```text
min(decoder configured maximum, pool configured maximum,
    affinity-visible processors, available grid work)
```

It intentionally excludes both mutable runtime ceilings. The application
ceiling still limits simultaneous admission-screen execution: the prospective
tasks run in bounded batches, and any shared-pool permits impose the aggregate
limit. Service timing begins after permit acquisition. The admission model uses
per-task service time to estimate the configured-width path without temporarily
overspending the live CPU budget.

Once a parallel path exists, `AdaptiveWorkers::worker_pool_limit` preserves the
complete useful configured extent rather than truncating it to a short-input
bootstrap. Ranks remain lazy; this only ensures an explicit growth request can
create them later. Runtime limit epochs pause empirical samples taken across an
application or pool control change so stale completions cannot bias a new
target.

## Lifetime and cancellation

The reusable `Decoder` owns a clone of `DecoderPool`; each decode operation
registers a distinct member in its `RuntimeState`. Scoped workers and
coordinators retain that runtime, so a member cannot disappear while a permit
or waiter borrows it.

Reader terminal handling first marks desire and queued work as zero, cancels or
joins worker threads, and then detaches aggregate membership. A separately
retained `DecoderHandle` continues to expose the frozen terminal snapshot but
does not keep `attached_decoders` elevated or remain in future rebalance scans.
Detachment is idempotent, and member drop is the fallback for synchronous
operations and construction failures.

A pool limit is always at least one. Waiters therefore progress as current
CPU regions release their RAII permits. Cancellation closes bounded channels
and wakes path-specific worker loops; because no permit is held during channel
waits, cancellation cannot create a pool/channel cycle.

## Validation and performance gates

Correctness tests cover:

- the hard simultaneous-execution bound and live resizing;
- stable grant sharing and delayed growth after one member completes;
- persistent requests under a lower hard ceiling;
- two concurrent dense-member readers growing from one aggregate slot;
- a consumer-blocked reader sharing a one-slot pool with a progressing reader;
- terminal detachment with a retained telemetry handle; and
- single-member gzip, concatenated/dense members, and BGZF under a pool.

`shared_reader_decode` is the retained multi-reader benchmark. It opens the
same validated corpus concurrently through private scheduling, shared adaptive
scheduling, or shared scheduling with explicit growth requests, and supports
both bulk `Read` and actual paraseq FASTQ parsing. It reports aggregate
throughput plus peak and sampled-mean pool telemetry.
The 1 kHz telemetry sampler runs independently of worker joining, so polling
granularity cannot become artificial reader-completion latency.

Required comparisons use single-member FASTQ gzip, sparse and dense ordinary
members, and BGZF; one, two, and multiple concurrent readers; and budgets that
exercise one worker, a modest pool, and a machine-scale pool. The important
paired cells are:

- one reader with equal private/shared widths, isolating pool overhead;
- equal-shaped readers with an even private split versus the same global shared
  budget, testing scheduler efficiency; and
- unequal or backpressured readers, testing capacity borrowing.

The feature is acceptable only if the one-reader shared path is near the
private path and an informed even per-file split is not materially faster on
equal workloads. Peak shared `busy_workers` must not exceed the configured
limit absent an intentional live decrease; bytes, member counts, and paraseq
records must remain identical.

The first retained gate met those conditions across 24 single/sparse/dense/
BGZF and `Read`/paraseq cells at one, two, and four readers. The adaptive shared
policy's worst median loss against the informed private control was 1.85%, and
all observed peak busy counts respected the 16-slot limit. The complete table,
including the deliberately maximum-forced `growing` policy, is retained in
`BENCHMARKING.md`.
