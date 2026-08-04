# Decoded-buffer recycling assessment

Status: rejected for the default `DecoderReader` path after implementation and
FASTQ benchmarking

Origin: the decode-local byte-buffer free lists proposed in PR #5

## Decision

Do not add automatic cross-thread decoded-buffer recycling to
`DecoderReader`. The strongest experimental implementation materially reduced
allocation volume, but every automatic design perturbed at least one primary
FASTQ reader shape. The standard 1 MiB `Read` path remained 3–5% slower even
after bulk consumers stopped constructing the recycler. That violates the
no-regression gate for a default-path optimization.

A separate opt-in reader/output implementation could keep the default path
structurally identical, but it would add public API and duplicate coordinator
machinery for a measured benefit limited to small reads. Paraseq's roughly
256 KiB batches would deliberately stay on the ordinary path. That utility is
not sufficient to justify the surface before the first release.

The implementation is preserved on the local
`decoded-buffer-recycling-experiment` branch for reference. The release branch
keeps only the reproducible benchmark infrastructure and this assessment.

## Existing allocation ownership

Several allocations already survive without a general pool:

- `DirectOutput::emit_reusable` writes, clears, and returns the same vector;
- sequential decoding threads that vector through `next_chunk`;
- strict indexed-parallel workers retain completed output in a decode-local
  worker queue;
- `IndexedReader` retains its foreground input and output buffers; and
- persistent zlib-rs workers reset inflater state instead of rebuilding it.

The positional reader is different. Ownership crosses a bounded channel, the
producer continues with `Vec::new()`, and the reader drops each allocation
after copying its bytes. Returning that allocation also transfers its pages
from the consumer core back to the producer core. Avoiding allocator work is
therefore not automatically a throughput win.

Marker-resolution parts, dense-member/BGZF results, stored-stream results,
analysis buffers, and random-access cache entries have different lifetimes and
contention patterns. They must not be routed through a shared pool based on the
reader result.

## Designs evaluated

The assessment implemented progressively narrower alternatives:

1. A private three-class pool with actual-capacity and entry bounds, backed by
   decode-local lock-free queues. Reader messages used an RAII payload owner.
2. A two-slot mutex pool and then a bounded return channel, removing the
   general size-class machinery.
3. The original `Message::Data(Vec<u8>)` representation plus a lazy return
   channel and opaque allocation tags. Tags were compared only and never
   dereferenced; the complete prototype used safe Rust.
4. One-worker-only recycling retired at the first completed member boundary,
   which removed the dense-member and BGZF page-transfer problem.
5. Consumer-shape admission: probe one full output chunk without recycling,
   enable only for small requests, and never construct the recycler for bulk
   reads or paraseq-sized batches.
6. Separate compact plain and recycling `Read` loops plus a no-inline slow
   producer path, minimizing code-layout effects after admission.

All queues were non-blocking, decode-local, and bounded by both entry count and
accepted capacity. Normal EOF, partial reads, `finish`, cancellation, errors,
and dropped messages retained ordinary single-owner `Vec` semantics. None of
the rejected implementations required unsafe code or a new dependency.

The general pool was also rejected on design grounds independent of timing. A
count-only free list does not bound retained bytes, a shared unsized list can
return poor capacity matches, and routing unrelated worker/result buffers
through one allocator abstraction makes wins and regressions difficult to
attribute.

## FASTQ benchmark matrix

The retained `reader_decode` target and `run-reader-ab.sh` runner use valid,
deterministic 128 MiB FASTQ in four archive shapes:

| fixture | gzip members |
|---|---:|
| single member | 1 |
| sparse members | 4 |
| dense members | 512 |
| BGZF | 2,186 |

Every generated fixture is parsed through paraseq as a correctness preflight.
The runner alternates fresh exact-`main` and candidate processes on the same CPU
affinity, performs multiple archives per process to amortize startup, and
reports the median of within-repetition candidate/`main` deltas. It covers:

- ordinary `Read` at 8 KiB and 1 MiB;
- a 8 KiB, 64 KiB, 256 KiB, 512 KiB, and 1 MiB consumer-buffer sweep;
- actual paraseq record parsing;
- 1, 4, and 16 requested decoder workers;
- ordinary, indexed-reader, early-`finish`, slow-consumer, and tiny-read modes;
  and
- wall, user, system, decoded-throughput, and peak-RSS measurements.

Independent medians were insufficient on the dual-socket benchmark host:
frequency and NUMA changes produced implausible isolated wins. Pairing each
candidate observation with the adjacent `main` observation before aggregation
made the default-path regressions repeatable and visible.

## Results

Valgrind DHAT demonstrated that the idea can reduce churn. The strongest lazy
one-worker prototype allocated 53,533,488 bytes in 67 blocks for a 128 MiB
single-member decode, versus 145,807,326 bytes in 83 blocks on exact `main`.
Sampled global heap peak fell from 23,119,112 to 18,925,675 bytes.

Throughput did not pass:

- the initial broad pool perturbed hot paths despite lower allocation volume;
- unrestricted recycling made one-worker dense-member paraseq about 6% slower;
- member-boundary retirement restored dense-member and BGZF parity, but the
  single-member 1 MiB `Read` cell repeatedly remained roughly 3–5% slower;
- a buffer sweep found the only repeatable positive cell at 8 KiB, while 64
  KiB through 1 MiB were slower; and
- fresh, isolated target directories reproduced the result: the final
  automatic prototype was 2.4% faster at 8 KiB and 4.3% slower at 1 MiB over
  nine paired, 30-archive observations.

Hard-disabling recycler creation inside the refactored path still left the
1 MiB cell about 3.5% behind. This establishes that an automatic implementation
cannot merely gate allocation return; it must keep the ordinary concrete
reader and output code structurally unchanged.

## Revisit criteria

Reconsider only with a design that satisfies all of these conditions:

- the existing default `DecoderReader`/`ChannelOutput` hot path is unchanged;
- any opt-in API has a concrete downstream use case that warrants its surface;
- retained bytes and entries have explicit bounds;
- single, sparse multi-member, dense multi-member, and BGZF FASTQ all pass at
  1, 4, and 16 workers;
- ordinary bulk reads and actual paraseq parsing are non-regressing; and
- allocation evidence is reported alongside paired throughput and RSS.

Until then, worker-local reuse and the existing direct-output return contract
remain the cleanest allocation strategy.
