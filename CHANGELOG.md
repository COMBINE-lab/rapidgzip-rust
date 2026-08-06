# Changelog

All notable changes to this project are documented in this file. The project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Opt-in `DecoderPool` process-wide decode budgets with a validated `bon`
  builder, runtime resizing, stable max-min cross-decoder allocation, fair
  contended admission, and aggregate queue/thread/activity telemetry. Existing
  decoders retain private elastic scheduling unless a pool is attached.
- Persistent runtime growth through `DecoderHandle::request_workers` and
  reader forwarding methods. `DecoderStats` now exposes the explicit request,
  desired concurrency before pool contention, and whether the shared pool is
  limiting progress.
- A concurrent programmatic-reader benchmark driver for shared versus private
  scheduling through both ordinary `Read` and paraseq FASTQ parsing.

### Changed

- Marker/window admission now uses immutable configured decoder and pool
  capacity rather than the transient application ceiling. A low early throttle
  bounds screen execution but no longer permanently latches a suitable stream
  to the sequential path, allowing reliable later growth.
- Pool execution accounting covers specialized workers, sequential chunks,
  bounded format scans, coordinator bridges/fallbacks, and ordered checksums.
  Permits are released before bounded result and final reader handoffs so a
  backpressured decoder cannot strand capacity needed by another file.
- Shared BGZF and dense-member workers amortize global permit traffic with a
  worker-local lease that is retained only across nonblocking handoffs and
  immediately available work. Shared positional readers sample an
  allowance-sized logical output backlog only when the pool is divided, so
  broad per-file headroom does not multiply persistent parser buffering or
  penalize a sole fast reader.

## [0.2.1] - 2026-08-05

### Fixed

- Marker/window resolution waits now drain the bounded speculative-result
  channel. This prevents all shared workers from blocking while publishing
  decode results when the coordinator needs queued resolution work to finish.
- Parallel-encoder sync flushes can no longer force an otherwise suitable
  gzip or zlib stream off the marker/window path. Small discontinuities between
  speculative chunks are exact-decoded with authenticated history and retain
  the existing safe fallback for invalid or larger gaps.

## [0.2.0] - 2026-08-04

### Upgrade notes

- `DecodeReport` remains `Copy`, but now includes the mandatory concrete
  `format: Format` field and optional `line_count: Option<u64>` field. Code
  constructing reports or destructuring them without `..` must account for
  both additions. Existing gzip decoding defaults and verification semantics
  are unchanged.
- The new random-access API uses the format-neutral `DeflateIndex` and returns
  an owning `IndexedDecodeReport` only from operations that explicitly request
  index construction. Ordinary decode operations continue to return the small
  `Copy` `DecodeReport`.

### Fixed

- Parallel task queues now publish their availability counters before making
  work visible, preventing a worker from consuming a task before the matching
  counter increment.
- CLI index import now streams through the core allocation limits, requires an
  exact format length, and rejects trailing data instead of buffering an
  untrusted file or treating arbitrary bytes as an empty GZI.
- Index export now serializes transactionally through a same-directory
  temporary file, preserving an existing destination after incompatibility or
  write failures. Normal decode output treats an early closed pipe as a
  successful consumer exit through wrapped decoder errors as well as direct
  I/O errors.
- Output collision checks now compare the already-open input file with the
  destination by file identity on Unix and Windows, preventing hard-link and
  symlink aliases from truncating a forced input.

### Changed

- Runtime telemetry now documents the distinction between the effective
  `active_workers` admission target, approximate `busy_workers` decode
  activity, and live `spawned_workers`. A worker that owns a completed bounded
  result may remain live during consumer backpressure until output advances or
  the decode is cancelled.
- Programmatic-reader performance work now has an alternating A/B runner over
  validated single-member, sparse/dense multi-member, and BGZF FASTQ, including
  ordinary, indexed, and actual paraseq consumption. Reports compute paired
  candidate/`main` deltas before aggregation to reduce host-state bias.

- Structural analysis now uses an inlined bit-buffer fast path and one bounded
  linear history/checksum buffer. The default no-output-limit path is
  monomorphized separately, avoiding configured-limit and redundant structural
  counter checks for each symbol while preserving checked total output.
- The `rapidgzip-rust` CLI now exposes rapidgzip-compatible decoding, counting,
  index import/export, range extraction, output, format, and reporting options.
  Imported indexes drive strict full-stream indexed decoding. Options whose
  semantics are not implemented, including disabled verification, sparse
  windows, and shared-cursor I/O strategies, are rejected instead of ignored.
- The CLI's format-neutral index default is now native. Payload output can be
  combined with byte and line counting in one pass; reports move to stderr when
  stdout carries decoded bytes. Imported range extraction documents its
  partial-verification boundary, while `--verify` performs a complete strict
  pass and otherwise-ignored decoder options are rejected.
- Line-aware index construction annotates the retained checkpoint vector in
  place instead of duplicating every point in ordered tree structures. Strict
  indexed decoding with line counting enabled now authenticates imported
  checkpoint and total line counters.

- Generic gzip, zlib, and raw-DEFLATE path selection now uses a bounded
  machine-, runtime-budget-, task-count-, and input-aware admission screen.
  It replaces a fixed low-thread marker/window policy, keeps short inputs and
  unfavorable workloads on zlib-rs, and preserves specialized BGZF, stored,
  and dense-member routing. `DecoderPath::MarkerAdmission` exposes the
  transient decision through existing telemetry.
- The random-access API uses the format-neutral `DeflateIndex` name instead of
  `GzipIndex`. This, the new mandatory `DecodeReport::format` field, and
  multi-format index provenance ship in 0.2.0 rather than a 0.1.x patch.

### Added

- Deterministic, self-verifying release benchmark corpora covering ordinary
  single/multi-member gzip, true BGZF, stored DEFLATE, low-compression data,
  zlib, and raw DEFLATE. The fair runner uses explicit competitor identities,
  decoded SHA-256 preflight, prebuilt indexes, affinity, rotated sample order,
  complete failure rows, provenance capture, and strict Rust-generated TSV and
  Markdown summaries. Hosted CI validates reproducibility and harness behavior
  without imposing a timing threshold.

- Bounded structural analysis through `Decoder::analyze`,
  `Decoder::analyze_with_options`, and streaming counterparts. The structured,
  deterministic result covers gzip/zlib/raw framing, every DEFLATE block,
  dynamic Huffman alphabets, symbol composition, and exact predecessor-window
  summaries while retaining only one 32 KiB output history. Concatenated and
  empty gzip members and BGZF remain distinct and fully checksum-verified.
- `AnalyzeOptions` and typed analysis errors for stream, block, aggregate
  optional-header, and detailed-reference budgets, allocation failures, and
  checked-counter overflow. Individual references are optional; exact counts,
  length histograms, reach, coverage, and deterministic interval unions remain
  available when detail is omitted.
- `rapidgzip-rust --analyze`, including non-seekable input, bounded verbose
  reference detail, rapidgzip 0.16.0-compatible presentation, pinned
  differential CI, and a generated FASTQ-like analysis benchmark.
- Opt-in newline counting through `DecoderBuilder::count_lines` and the new
  `DecodeReport::line_count` scalar, preserving `DecodeReport: Copy`. Combining
  counting with explicit index construction annotates every retained
  checkpoint and the index total on final ordered output. Concatenated and
  empty gzip members, BGZF, zlib, raw DEFLATE, streaming push/pull, marker
  decoding, and strict indexed decoding share the same semantics.
- `DeflateIndex::checkpoint_at_or_before_line` and
  `IndexedReader::seek_to_line` for zero-based line access. gztool version 1
  import/export translates its one-based checkpoint numbering and refuses
  incomplete line metadata.
- CLI byte and line ranges using rapidgzip's comma-separated `SIZE@OFFSET`
  syntax, including binary byte units, `L`, `inf`, overlaps, and ordered
  extraction. The CLI can read and write native, GZIDX, gztool, line-aware
  gztool, and BGZF `.gzi` indexes.

- Strict parallel full-stream reuse of caller-supplied `DeflateIndex` values
  through `Decoder::decode_from_index` and `Decoder::reader_from_index`. Every
  span must match its next compressed-bit and decompressed-byte checkpoint;
  invalid indexes never fall back. The path supports gzip, concatenated and
  empty gzip members, BGZF `.gzi` without a known final output size, zlib, and
  raw DEFLATE while preserving complete trailer verification, bounded ordered
  output, adaptive workers, runtime ceilings, `Read + Send`, and telemetry via
  `DecoderPath::IndexedParallel`.
- Deterministic interior block checkpoints from one-worker indexed decoding,
  making an index built by the authoritative sequential zlib-rs path reusable
  for later parallel decoding of single-stream gzip, zlib, and raw DEFLATE.
- Explicit zlib and raw-DEFLATE decoding through every push and pull API, plus
  opt-in gzip/zlib auto-detection that never guesses raw input. Large positional
  streams share the adaptive rapidgzip marker/window path; non-seekable streams
  share the resumable sequential engine. Zlib validates CMF/FLG, enforces CINFO
  history limits, and verifies Adler-32, while raw DEFLATE is structurally
  validated through its exact final byte.
- `Format`, `DecoderBuilder::format`, `DecoderBuilder::auto_detect_format`, and
  concrete `DecodeReport::format`, with `DecodeReport` remaining `Copy`.
  `DecoderBuilder::expected_uncompressed_size` adds an exact output contract
  for every format and rejects overruns before output handoff.
- Format-aware `DeflateIndex` provenance and checkpoint kinds for gzip, BGZF,
  zlib, and raw DEFLATE. The native version-1 format round-trips every kind;
  gzip-specific external exporters now reject incompatible indexes.
- Explicit random-access indexing operations for positional and non-seekable
  push/pull decoding, returning `IndexedDecodeReport` while preserving the
  existing `Copy` `DecodeReport` and zero-indexing default path. The new
  `IndexingDecoderReader` remains `Read + Send` and exposes the same runtime
  telemetry and worker controls as `DecoderReader`.
- A validated `DeflateIndex` model with explicit member/header/DEFLATE checkpoint
  provenance, bounded native and external-format parsing, optional compressed
  32 KiB predecessor windows, and import/export for native version 1,
  indexed_gzip GZIDX, htslib BGZF `.gzi`, and gztool formats.
- `IndexedReader`, a `Read + Seek` view that checks source-size provenance,
  strictly handles truncation and trailing data, fully verifies members entered
  at member checkpoints, and documents the checksum limitation when a foreign
  index resumes inside a member.
- Decoding of non-seekable compressed input through `Decoder::decode_stream`
  and `Decoder::stream_reader`, which accept any `std::io::Read` and mirror
  `Decoder::decode` and `Decoder::reader`. `Decoder::open` and the new push
  counterpart `Decoder::decode_path` now route a
  non-regular path accepted by `File::open`, such as a FIFO or character
  device, to the streaming decoder instead of failing, and the CLI accepts `-`
  for standard input. Such input runs the sequential zlib-rs path that the
  parallel paths already use as their authoritative fallback, sharing its
  framing, footer verification, trailing-garbage detection, and output limit,
  so it is verified identically but is not decoded in parallel.
  `DecoderStats` and `DecodeReport` preserve the configured thread budget while
  reporting an effective target of one and no spawned worker or auxiliary
  threads. Nothing is spooled; input memory is one configured input window.
  A streaming `DecoderReader` advances a resumable sequential decoder in the
  caller's `Read::read`, so dropping it immediately drops the source and cannot
  leave a coordinator blocked on a stalled producer.

## [0.1.0] - 2026-07-31

Initial release of the decoder-only `rapidgzip-rust` implementation.

### Added

- Parallel decoding of ordinary gzip streams using rapidgzip's speculative
  marker/window algorithm and zlib-rs as the inflate backend.
- Correct verified decoding of concatenated multi-member gzip and BGZF,
  including empty members, optional gzip header fields, and conventional BGZF
  EOF members.
- Specialized parallel paths for BGZF, stored DEFLATE streams, and archives
  containing dense runs of independently decodable gzip members.
- A push API over `ReadAt` and `Write`, plus an owned streaming
  `DecoderReader` implementing `Read + Send` for consumers such as paraseq.
- Elastic worker scheduling with affinity- and requested-budget-aware
  bootstrapping, empirical upward/downward calibration, lazy OS-thread
  creation, and persistent worker retirement.
- Cloneable `DecoderHandle` runtime control and lock-free `DecoderStats`
  telemetry covering active, busy, and spawned workers; decoder path;
  backpressure; verified members; byte progress; and throughput.
- Automatic decoder throttling when the final reader handoff is
  consumer-bound, with excess workers retiring after their in-flight work.
- Runtime-dispatched AVX2 and SSE4.1 acceleration on x86-64, baseline NEON on
  AArch64, and scalar fallbacks.
- The `rapidgzip-rust` decode/verify CLI, distributed in the
  `rapidgzip-rust-cli` package.
- Multi-platform CI, an explicit Rust 1.87 MSRV check, safety documentation,
  reproducible performance reports, and a guarded release script.

### Correctness and resource guarantees

- Every accepted member must reach a real final DEFLATE block and match its
  CRC32 and ISIZE footer.
- Candidate member headers inside compressed data are accepted only after
  independent inflation, footer authentication, and exact adjacency to the
  preceding verified member.
- Output limits, speculative work, queues, reader handoff, and dense-member
  scanning are bounded.
- Dropping a reader cancels and joins its pipeline; reaching EOF or calling
  `finish` verifies the complete input.

### Current scope

- Decoding only; compression is not implemented.
- No persisted indexes, decoded-output seeking, or non-positional compressed
  input.
- ISA-L-enabled C++ rapidgzip remains faster in the measured one-worker FASTQ
  cell; multi-worker parity and the zlib-ng-backed C++ performance gate are
  met on the published workloads.

[Unreleased]: https://github.com/COMBINE-lab/rapidgzip-rust/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/COMBINE-lab/rapidgzip-rust/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/COMBINE-lab/rapidgzip-rust/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/COMBINE-lab/rapidgzip-rust/releases/tag/v0.1.0
