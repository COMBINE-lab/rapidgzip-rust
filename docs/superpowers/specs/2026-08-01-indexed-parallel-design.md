# Parallel decoding driven by an index

Date: 2026-08-01
Status: approved design
Scope: follow-up to the five pull requests replacing PR #5

## Background

The speculative marker/window grid decodes a file nobody has seen before, so a
worker landing on a grid point knows neither its exact DEFLATE block boundary
nor the 32 KiB of history preceding it. Markers exist to decode without that
history and have the coordinator resolve it afterwards.

That costs. Measured on a 46 MB text corpus at gzip -6, the native
unknown-history decoder runs at roughly 291 MiB/s per worker against
662 MiB/s for zlib-rs inflate on the sequential path. Two speculative workers
therefore lose to one sequential one, which is why PR #13 raised the entry
threshold to three.

An index removes the unknown. Every checkpoint carries a compressed bit offset
and the window needed to resume there, which is exactly what
`IndexedReader::resume` already feeds to `inflatePrime` and
`inflateSetDictionary` before running plain zlib. The information that makes
speculation necessary is already recorded; it is simply not used in parallel.

## Goals

- Decode with every worker on plain zlib when an index is available, with no
  speculation, no marker symbols, and no resolution stage.
- Make two workers faster than one, which the speculative path cannot be.
- Reuse the existing index, in any of the five supported formats.

## Non-goals

- Replacing the speculative grid. A file being read for the first time has no
  index, and building one requires decoding it. The grid remains the only way
  to decode an unseen file in parallel.
- Building an index in order to then use it. One decode to build plus one to
  use is slower than one speculative decode. The path applies when an index
  already exists.
- Changing `IndexedReader`, which serves scattered reads and stays
  single-threaded on purpose.

## When the path applies

All of:

- an index was supplied by the caller;
- it validates against the source, which `GzipIndex::validate` and the
  recorded compressed size already check;
- it holds at least two checkpoints beyond the first, so there is work to
  split;
- the worker budget is at least two.

Otherwise the existing dispatch is unchanged.

## How it decodes

The index partitions the file: consecutive checkpoints delimit spans whose
decompressed sizes are known from their offsets. Each span becomes one task.

A worker takes a span, reads its compressed bytes, and decodes it exactly as
`IndexedReader` resumes: `inflatePrime` for the bits below a byte, then
`inflateSetDictionary` with the checkpoint's window, then plain zlib until the
next checkpoint's decompressed offset is reached. Output is a plain `Vec<u8>`
of known length, so it can be reserved exactly once.

The coordinator emits spans in order, as it already does for every other
parallel path. There is no resolution stage, because there is nothing to
resolve.

Verification is what it always was. Each gzip member's CRC32 and ISIZE are
checked, which the coordinator can do because it sees the decompressed stream
in order and the index records where members begin.

## Interface

`DecoderBuilder::index(Option<GzipIndex>)` supplies the index. The CLI reaches
it through `--import-index`, which currently errors unless `--ranges` was also
given; that restriction goes away, since an imported index now accelerates an
ordinary decompression.

`DecoderStats::path` gains `DecoderPath::Indexed`, so telemetry reports what
actually ran rather than implying speculation.

## Expected result

Per-worker throughput should approach the sequential figure, since each worker
runs the same zlib inflate over a different span. On the measured corpus that
predicts roughly 1300 MiB/s at two workers against 582 today, scaling with
worker count until the spans run out or the source I/O saturates.

The prediction is worth stating because it is falsifiable: if per-worker
throughput lands materially below the sequential 662 MiB/s, the cause is the
per-span resume cost, and the fix is coarser spans rather than more workers.

## Risks

A span's decompressed size is known, so a corrupt or mismatched index would be
caught by the size check before its output is emitted, and by the member CRC32
after. An index that validates but describes a different file is the dangerous
case; the recorded compressed size and the first member header make that
detectable, and the existing `validate` already refuses an index whose
checkpoints are not ordered or whose sizes disagree.

Memory is bounded by the same in-flight chunk count as the other parallel
paths, with one difference worth watching: span sizes come from the index
rather than from the configured chunk size, so a sparse index produces large
spans. The task planner therefore splits a span larger than the configured
decoded chunk size across several sequential reads within one task rather than
buffering it whole.

## Testing

- Decoding through an index must produce bytes identical to decoding without
  one, on single-member, multi-member, and BGZF corpora, at one through eight
  workers.
- An index built by this crate and one imported from indexed_gzip, gztool, and
  `.gzi` must all drive the path.
- A truncated or mismatched index must fail rather than emit wrong bytes.
- Telemetry must report `DecoderPath::Indexed`, and the existing path
  selection test gains that case.
- A benchmark comparing the indexed path against the speculative one at equal
  worker counts, with the numbers reported in the pull request.

## Delivery

One branch, `indexed-parallel`, and one pull request, stacked on #13.
