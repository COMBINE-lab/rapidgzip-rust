# Changelog

All notable changes to this project are documented in this file. The project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- The unreleased random-access API now uses the format-neutral `DeflateIndex`
  name instead of `GzipIndex`. This, the new mandatory `DecodeReport::format`
  field, and multi-format index provenance are planned for the next minor
  release rather than a patch release.

### Added

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

[Unreleased]: https://github.com/COMBINE-lab/rapidgzip-rust/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/COMBINE-lab/rapidgzip-rust/releases/tag/v0.1.0
