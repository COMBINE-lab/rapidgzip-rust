# Bounded decoded-buffer recycling

Status: implemented for the evidence-backed single-member positional-reader
path; other paths remain separately benchmark-gated

Origin: the decode-local byte-buffer free lists proposed in PR #5

## Decision

The direct `Write` path already returns a cleared output vector to sequential
decode, and strict indexed-parallel decode already has a worker-local recycled
vector queue. A positional `DecoderReader` historically could not do this:
ownership crossed a bounded channel, the producer continued with
`Vec::new()`, and the reader dropped each allocation after copying its bytes.

The reader now makes a bounded ownership round trip only when all of the
following hold:

- the source is positional rather than a non-seekable stream;
- the configured decoder budget is exactly one worker;
- the producer explicitly calls `Output::emit_reusable`; and
- the handoff contains at least one configured decoded chunk and its capacity
  is between one and two configured chunks; and
- every consumer request used for the first eligible chunk is smaller than the
  lesser of 64 KiB and one quarter of a configured decoded chunk; and
- no completed gzip member has yet established that more output belongs to a
  later member.

This preserves the allocation win on the ordinary single-member FASTQ path.
The consumer-shape gate keeps conventional 8 KiB buffered reads eligible while
leaving paraseq's roughly 256 KiB batches and the 1 MiB bulk-copy control on
their original allocation path. A buffer-size sweep found positive results at
8 KiB but regressions from 64 KiB through 1 MiB when allocations crossed the
producer/consumer boundary.
Once the first member footer completes, any later output uses the original
plain `Message::Data(Vec<u8>)` path. That rule matters for dense multi-member
and BGZF FASTQ: repeatedly transferring the same pages between the producer
and consumer saved allocator work but created enough cache-ownership traffic
to regress actual paraseq parsing by about 6% on the benchmark host. Retiring
recycling at the first member boundary restored that workload to parity.

There is no public setting, dependency, global pool, unsafe code, or change to
the output and verification contract.

## Existing ownership map

Allocations that already survive remain unchanged:

- `DirectOutput::emit_reusable` writes, clears, and returns the same vector;
- sequential decoding threads that vector through `next_chunk`;
- strict indexed-parallel workers retain completed output in a decode-local
  worker queue;
- `IndexedReader` retains its foreground input and output buffers; and
- persistent zlib-rs workers reset inflater state instead of rebuilding it.

Marker-resolution parts, specialized dense-member/BGZF results, stored-stream
results, analysis buffers, and random-access cache entries are not routed
through the reader recycler. Each has a different lifetime and contention
profile and requires its own allocation profile and paired throughput result.

## Why the original general pool was not retained

PR #5 proposed a count-capped `Injector<Vec<u8>>` shared by several decode
paths. A count alone does not bound retained capacity, and a shared unsized
free list can retain an exceptional allocation or return a poor capacity match.
The first implementation on this branch tightened that idea into a private
three-class pool with byte and entry accounting, then wrapped reader messages
in an RAII owner.

FASTQ benchmarking rejected that design. Although allocation volume fell, the
extra message representation and pool machinery perturbed paths that did not
benefit, and the cache cost appeared most clearly in one-worker dense-member
paraseq. A later two-slot pool and a boxed return channel had the same problem.
The shipped design therefore retains the exact ordinary `Message::Data(Vec<u8>)`
representation and initializes its return mechanism only on the first eligible
handoff.

## Ownership protocol

The coordinator and reader first share a small `RecyclingControl` whose
consumer shape is unknown and whose `OnceLock` is empty. While the shape is
unknown, the coordinator uses the original plain output path. The reader
samples every request used to consume the first full-sized chunk. Any request
at or above the smaller of 64 KiB and one quarter of the decoded chunk marks
the shape as bulk; consuming the whole chunk with smaller requests marks it as
small. This makes a one-byte format probe followed by a bulk request classify
as bulk.

A bulk classification never constructs the recycler. After a small-read
classification, the next eligible `emit_reusable` creates a two-entry
synchronous return channel and publishes a private `RecyclingState`. Before
sending the ordinary data message, it records the vector allocation's address
as an opaque integer. The address is used only as an ownership tag; it is never
dereferenced. Short dense-member and BGZF handoffs fail the full-chunk size
gate before this state is initialized.

When the caller next asks the reader to receive output after consuming a
registered vector, the reader:

1. removes the matching allocation tag under a small mutex;
2. takes and clears the vector;
3. accepts only capacity in `[decoded_chunk_size, 2 * decoded_chunk_size]`;
4. attempts a non-blocking return through the two-entry channel; and
5. drops the vector if the channel is full or disconnected.

Delaying the return until the next `Read` boundary keeps the producer from
claiming pages while the caller is still processing its just-filled buffer.
On its next reusable emission, the coordinator polls the return channel. A
hit becomes the next decode buffer; a miss returns `Vec::new()` and preserves
the pre-optimization behavior. Neither side ever waits for recycling.

The address tag is sound in safe Rust because the registered allocation stays
owned by exactly one in-flight message or by the reader until the tag is
removed. Its storage cannot be freed or reallocated to another live vector in
that interval. A failed data send is terminal for that decode, so any remaining
tag is dropped with the decode-local state and cannot match future storage.

Plain `Output::emit` messages are never registered. Non-reusable and reusable
messages therefore keep the same enum representation, while only the proven
allocation round trip opts into the return mechanism.

## Bounds and lifecycle

At most two empty vectors can wait in the return channel. Only capacities from
one through two configured decoded chunks are accepted, so recycler-only
retention is bounded by four configured chunks in the unreachable worst case
where both slots hold maximum-capacity vectors. In steady state the returned
vector replaces the producer's next allocation rather than adding another
payload. The allocation-tag list is bounded by the existing in-flight output
channel and is decode-local.

The recycler is lazy and one-worker-only. A consumer request at or above the
smaller of 64 KiB and one quarter of the configured decoded chunk while
sampling the first full chunk
prevents it from being constructed; sampling the complete chunk prevents a
one-byte format probe from hiding the caller's later bulk-read shape.
Multi-worker readers have no return channel or registry. A multi-member reader
retires the mechanism before output from its second member and disables
reader-side registry work with one relaxed atomic flag. Non-seekable readers
continue to reuse their one local vector synchronously and need no cross-thread
mechanism.

Normal EOF, short reads, `finish`, early drop, cancellation, a disconnected
consumer, and terminal failure all retain ordinary `Vec` ownership. Recycling
is opportunistic: failure to return or obtain a vector cannot change decode
success, ordering, checksums, reports, or errors.

## Benchmark and correctness gates

The reproducible reader harness decodes deterministic 128 MiB valid FASTQ
fixtures in four shapes:

| fixture | gzip members |
|---|---:|
| single member | 1 |
| sparse members | 4 |
| dense members | 512 |
| BGZF | 2,186 |

Each fixture is independently parsed through paraseq before benchmarking. The
paired runner alternates exact `main` and candidate binaries, pins both to the
same CPU set, and covers ordinary `Read` with 8 KiB and 1 MiB consumer buffers
plus actual paraseq parsing at 1, 4, and 16 requested workers. It also records
wall/user/system time and peak RSS. Indexed-reader handoff, early `finish`,
slow-consumer, and tiny-read controls are separate cells.

The critical dense-member/paraseq one-worker cell initially measured about 6%
behind `main`. With the member-boundary retirement rule it measured 1,581.8
MiB/s versus 1,570.5 MiB/s in a seven-pair, 20-archive-per-process run, a 0.7%
difference in favor of the candidate. The complete ordinary and paraseq tables
are recorded in the performance audit rather than selecting only favorable
cells.

Valgrind DHAT on the single-member, one-worker, 8 KiB-read fixture measured:

| revision | allocated bytes | allocation blocks | bytes at global heap peak |
|---|---:|---:|---:|
| exact pre-change `main` | 145,807,326 | 83 | 23,119,112 |
| final recycler | 53,533,488 | 67 | 18,925,675 |

That is 63.3% fewer allocated bytes, 19.3% fewer allocations, and a 4.0 MiB
lower sampled heap peak. The feature is accepted only with the complete Rust
test suite, clippy, rustdoc, valid-FASTQ parser checks, and paired shape matrix.

## Deferred work

Marker-resolution scratch recycling remains deferred. Dense-member, BGZF,
stored, analysis, and random-access paths must not adopt this mechanism merely
because it exists. A later change needs its own allocation evidence, complete
FASTQ-shape matrix, bounded memory argument, and a no-regression result on the
standard programmatic-reader and paraseq hot paths.
