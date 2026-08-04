# Observable indexed-reader caching and hinted prefetch

Status: proposed; implementation is intentionally gated by the benchmarks in
this document

Origin: the decoded-window cache and background seek prefetch in PR #5

## Decision

Decoded-output caching is useful for repeated and overlapping random reads.
Parallel prefetch is also useful when a caller already knows several upcoming
ranges, as the CLI does for a `--ranges` request. The feature is therefore
warranted, but PR #5's automatic two-window background policy is not the right
contract for this project.

The replacement must be explicit, bounded, observable, and dynamically
controllable. `IndexedReader::new` remains a zero-background-thread reader.
Merely configuring a large decoder budget must not create seek workers, and a
linear read must continue to use the existing live inflater rather than
independently decoding guessed future positions.

## Why the PR #5 design should not be copied

PR #5 adds a decoded-window LRU, synchronous read-ahead, and a default of two
background prefetch windows. Each prefetch operation creates a new operating-
system thread. The implementation caps simultaneously retained handles, but it
does not use the decoder's elastic worker machinery and does not expose these
threads through `DecoderHandle` or equivalent telemetry.

That leaves several problems:

- a reader can consume process thread capacity without the application seeing
  or reducing it;
- the default guesses that sequential future windows will be read, even though
  `IndexedReader` exists primarily for non-linear access;
- a far seek can make already-running work useless after it has consumed CPU
  and I/O;
- cache occupancy is bounded, but queued work and useful-hit rate are not part
  of admission feedback;
- spawning and joining short-lived threads can cost more than the inflate they
  perform; and
- changing the owned source to `Arc<R>` complicates `into_inner` unless worker
  shutdown and cloned control handles are designed together.

The existing `reader_from_index` API already handles sequential full-stream
indexed decode in parallel, with strict verification, adaptive workers, and
telemetry. Seek prefetch should not duplicate that path or become its implicit
replacement.

## Workloads and expected utility

The intended wins are:

1. repeated seeks into the same decompressed region;
2. overlapping byte or line ranges;
3. a batch of disjoint ranges known before extraction; and
4. remote or high-latency `ReadAt` implementations where several positional
   reads can make progress concurrently.

There is little expected benefit for:

- one linear pass, which should use `reader_from_index` or an ordinary reader;
- one isolated seek followed by a short read;
- a source already bottlenecked on one underlying device queue; or
- unpredictable access where prefetched entries are evicted before a hit.

The implementation must measure these cases separately. A sequential-only
microbenchmark is not evidence for enabling automatic prefetch.

## Public API

The existing constructor and its behavior remain unchanged:

```rust,no_run
use rapidgzip_core::{DeflateIndex, IndexedReader};
use std::fs::File;

# fn example(index: DeflateIndex) -> Result<(), Box<dyn std::error::Error>> {
let reader = IndexedReader::new(File::open("reads.fastq.gz")?, index)?;
// No decoded-output cache and no background worker are created.
# let _ = reader;
# Ok(())
# }
```

Caching and prefetch use two explicit option types rather than adding seek
policy to `DecoderBuilder`. Decoder settings describe a complete decode;
random-access cache retention and advisory work have different semantics.

```rust,ignore
let reader = IndexedReader::new(source, index)?
    .with_decoded_cache(DecodedCacheOptions::new(
        NonZeroUsize::new(64 * MIB).unwrap(),
        NonZeroUsize::new(256 * KIB).unwrap(),
    ));

let mut reader = reader.with_prefetch(PrefetchOptions::new(
    NonZeroUsize::new(4).unwrap(),
    NonZeroUsize::new(8).unwrap(),
))?;
let handle = reader.prefetch_handle().expect("prefetch was enabled");
let admission = handle.prefetch_ranges(&ranges)?;
```

`DecodedCacheOptions` contains:

- a total retained-byte budget; and
- a canonical decompressed chunk size.

`PrefetchOptions` contains:

- a maximum worker budget; and
- a maximum number of queued canonical chunks.

`IndexedReader::with_prefetch` is available only for
`R: ReadAt + 'static`, because operating-system threads can outlive the method
call. Plain caching keeps the existing `R: ReadAt` bound and remains usable
with borrowed sources. Prefetch requires a non-zero decoded-cache budget and
returns a configuration error otherwise.

There is deliberately no default sequential read-ahead option in the initial
surface. A later explicit policy can be added after the usefulness counters
show that callers need it.

### Control and telemetry

Prefetch uses a dedicated `IndexedReaderHandle`, not `DecoderHandle`.
`DecoderStats::decompressed_bytes` and `member_count` describe one complete
decode; random seeks may decode the same bytes repeatedly and may never finish
a member. Reusing those fields would make existing telemetry lie.

The handle is cloneable and contains control/statistics state, but does not own
the compressed source or index. Therefore an outstanding handle does not
prevent `IndexedReader::into_inner` from returning `R` after workers join.

The initial control surface is:

```rust,ignore
pub fn stats(&self) -> IndexedReaderStats;
pub fn set_worker_limit(&self, workers: usize) -> Result<(), PrefetchLimitError>;
pub fn prefetch_ranges(
    &self,
    ranges: &[Range<u64>],
) -> Result<PrefetchAdmission, IndexedPrefetchError>;
```

Unlike `DecoderHandle`, a prefetch worker limit of zero is valid and means
"admit no advisory work." Raising the limit recreates workers lazily when
queued work exists.

`IndexedReaderStats` is a non-exhaustive `Copy` snapshot containing:

- configured, current-limit, live, and busy prefetch workers;
- queued canonical chunks;
- cache entries, bytes, hits, misses, inserts, and evictions;
- requested, admitted, completed, cancelled, and failed prefetch chunks;
- prefetched chunks later hit by foreground reads; and
- prefetched chunks evicted without a foreground hit.

The last two counters are essential feedback. They distinguish successful
latency hiding from CPU and I/O spent on unused guesses. `PrefetchAdmission`
reports requested canonical chunks, already-cached chunks, newly admitted
chunks, and chunks refused because the queue or cache budget is full.

## Canonical decoded chunks

Cache keys are absolute decompressed offsets aligned down to the configured
chunk size. A final entry may be shorter at known decompressed EOF. Canonical
alignment prevents overlapping range requests from filling the cache with
different representations of the same bytes.

Entries contain `Arc<[u8]>` plus whether their origin was foreground or
prefetch work and whether a prefetched entry received a foreground hit.
Foreground reads clone the `Arc` while holding the cache lock, then copy from
it without holding the lock. The byte budget counts the payload once, not each
temporary `Arc` clone. Eviction is least-recently-used and accounts by actual
payload length.

When the decoded cache is disabled, the current inflater, buffers, allocation
pattern, and seek behavior remain compiled through the existing path. The
default decode must not pay cache locking or canonical-chunk assembly.

## Decode engine factoring

The current `IndexedReader` owns framing state, a raw inflater, positional
input, predecessor-window expansion, and footer verification. Prefetch workers
must not duplicate a second interpretation of those rules.

Factor the resumable portion into a private `IndexedSession` that can:

1. resume from one typed checkpoint;
2. install the exact predecessor dictionary and primed bits;
3. discard output to a canonical chunk start;
4. fill at most one canonical chunk; and
5. cross gzip member or zlib framing using the same parser and verification
   policy as the foreground reader.

The foreground reader retains one session across linear reads. Each worker
retains and resets one private session across jobs. No inflater is shared
between threads.

A cache entry is inserted atomically only after the complete requested chunk,
or its exact EOF-shortened form, has been produced successfully. Failed or
cancelled work publishes no partial bytes. Advisory failure is counted but not
returned asynchronously by a later unrelated read; a foreground miss retries
the decode and returns its own deterministic error.

This preserves the existing integrity boundary: a chunk resumed at an interior
DEFLATE checkpoint cannot authenticate skipped prefix bytes, while a chunk
resumed from a member/header checkpoint verifies everything it traverses.
Prefetch must not claim stronger verification than `IndexedReader` already
provides.

## Worker admission and lifetime

Workers form one lazy persistent pool per prefetch-enabled reader. The initial
spawn target is the minimum of:

```text
configured prefetch budget
current runtime limit
affinity-visible processors
uncached queued chunks
number of cache-sized slots available
```

No workers are created by construction alone. A queue transition from empty to
non-empty wakes existing workers and creates only the ranks immediately useful
for admitted chunks. Idle excess workers retire after a bounded grace period;
lowering the limit to zero wakes all workers so they retire after their current
chunk. Raising it later permits lazy recreation.

Each job carries a generation. A foreground far seek does not automatically
invalidate caller-requested disjoint ranges, but replacing the prefetch plan or
shrinking the cache can cancel older generations explicitly. Workers check
cancellation before source reads, inflate steps, and cache insertion.

Drop, `into_inner`, and every constructor failure set cancellation, close the
queue, and join all workers. The work context owns `Arc<R>` and
`Arc<DeflateIndex>` only while the pool is active; after shutdown those arcs are
dropped before `into_inner` unwraps the reader-owned values. The public handle
owns only statistics/control state.

## Memory bound

The aggregate retained memory is bounded by:

```text
decoded cache byte budget
+ one input page and one canonical output chunk per live worker
+ one input page and foreground output buffer
+ expanded predecessor-window cache
+ bounded queue metadata
```

Queue admission also refuses work when every cache-sized slot is already
occupied by entries newer than the request. This prevents a large range list
from turning a small cache into continuous decode-and-evict churn.

No crate dependency and no new unsafe block are required. The existing
zlib-rs pointer argument remains inside the factored private session with the
same initialization, bounds, and unique-borrow safety argument.

## Implementation plan

1. Add a reusable byte-bounded decoded LRU with pure unit tests for canonical
   keys, replacement, byte accounting, and unused-prefetch eviction counters.
2. Factor `IndexedSession` from the current foreground reader without changing
   default behavior. Differentially test foreground reads before enabling the
   cache.
3. Add explicit synchronous cache insertion/hits and a random/overlapping seek
   benchmark. Stop here if caching does not improve a representative repeated-
   range workload without regressing uncached reads.
4. Add the dedicated control/statistics state and bounded range admission.
5. Add the lazy persistent worker pool, per-worker sessions, cancellation,
   retirement, and `into_inner` ownership tests.
6. Connect the CLI range planner to hinted prefetch only when an imported index
   is already available. Index construction and complete verification passes
   remain separate.
7. Document the partial-verification boundary and memory formula in rustdocs,
   the README, architecture, and changelog.

## Validation and acceptance gates

Correctness tests cover gzip, concatenated and empty gzip members, BGZF, zlib,
raw DEFLATE, non-byte-aligned checkpoints, compressed predecessor windows,
line seeks, short reads, source errors, corruption, cancellation, queue
replacement, far seeks, and drop/`into_inner` with cloned handles.

Concurrency tests assert that:

- construction and cache-only use spawn zero threads;
- no more than the useful/budgeted rank count becomes live;
- a limit of zero admits no work and retires idle workers;
- lowering a limit lets active jobs finish but starts no new ones;
- failed and stale jobs never enter the cache; and
- every worker is joined before source ownership is returned.

Benchmarks use the public FASTQ input and generated multi-member/BGZF fixtures
with these traces:

- repeated same-range seeks;
- overlapping nearby ranges;
- sorted disjoint ranges;
- randomized disjoint ranges;
- a linear read control; and
- a deliberately slow `ReadAt` source.

Report latency distribution, total decoded work, cache useful-hit ratio,
unused-prefetch eviction ratio, source bytes read, live worker maximum, and
peak RSS. Prefetch is accepted only if it improves at least one representative
multi-range trace materially, leaves the default uncached path statistically
unchanged, obeys the thread and memory bounds, and does not make the linear
control slower when disabled.

