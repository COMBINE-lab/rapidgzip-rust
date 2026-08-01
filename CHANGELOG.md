# Changelog

All notable changes to this project are documented in this file. The project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Two gzip index interoperability bugs, found by a test in which real gztool
  extracts by line from an index this crate wrote. gztool numbers lines from
  one while the index counts newlines before a point, so export and import now
  convert. More seriously, a member checkpoint recorded the member start, so
  gztool resumed by inflating raw DEFLATE at the gzip header and reported a
  compressed data error. Member checkpoints now record the DEFLATE start,
  matching indexed_gzip and gztool. BGZF block checkpoints still record the
  block start, which is what `.gzi` means by an offset.
- Writing gztool's line-aware format from an index that never counted lines
  wrote zero for every counter, producing a file gztool accepts and then
  trusts. It is now refused.

### Added

- Newline counting, through `DecoderBuilder::count_lines`. It fills
  `DecodeReport::line_count` and, when an index is also collected, a line
  offset for every checkpoint plus the index total.
  `IndexedReader::seek_to_line` seeks by zero-based line number, refusing an
  index that carries no counters rather than scanning from the start.
- Command-line parity with rapidgzip 0.16.0. `rapidgzip-rust` now accepts
  every option that tool accepts, under the same names: `--ranges` with byte
  and line addressing, `--import-index` and `--export-index` with
  `--index-format`, `--count`, `--count-lines`, `--format`, `-f`, `-k`, `-d`,
  `--chunk-size`, `-q`, `-v`, and `--oss-attributions`. Output goes where
  rapidgzip sends it, including the derived name with the compressed suffix
  stripped. `--io-read-method` and `--sparse-windows` are accepted no-ops;
  `--no-verify` is refused, since verification here is structural.
- An optional ISA-L raw-inflate backend, behind the off-by-default `isal`
  feature of `rapidgzip-core`. It replaces zlib-rs on the paths that decode a
  whole stream from its start: sequential gzip members, single-stream zlib and
  raw DEFLATE, and BGZF blocks. The parallel marker/window path and
  `IndexedReader` stay on zlib-rs either way, since both resume at arbitrary
  bit offsets and the parallel path needs zlib's `Z_BLOCK` contract. The
  feature links a system `libisal` rather than building one. Default builds are
  unchanged in dependencies and behaviour. Whether ISA-L is faster is a
  measurement; `README.md` reports it.
- zlib (RFC 1950) and raw DEFLATE (RFC 1951) decoding, selected through
  `DecoderBuilder::format`. `Format::Auto`, the default, detects gzip against
  zlib; raw DEFLATE must be requested. Both containers decode sequentially, in
  parallel, from non-seekable input, and with a random-access index.
  `DecoderBuilder::expected_uncompressed_size` verifies raw DEFLATE output,
  which carries no checksum of its own. `DecodeReport` gains `format`.
- Random-access indexing. `DecoderBuilder::build_index` collects a `GzipIndex`
  during an ordinary decode and returns it in `DecodeReport::index`.
  `DecoderBuilder::index_spacing` and `DecoderBuilder::compress_index_windows`
  tune checkpoint density and resident memory.
- `IndexedReader`, a `Read + Seek` view over compressed input that resumes at
  the nearest checkpoint instead of decoding from the start.
- Index persistence in four formats, importing and exporting: the crate's own
  versioned format, indexed_gzip `GZIDX`, htslib BGZF `.gzi`, and gztool.
  Interoperability with all three tools is covered by tests.

- Decoding of non-seekable compressed input through `Decoder::decode_stream`
  and `Decoder::stream_reader`, which accept any `std::io::Read` and mirror
  `Decoder::decode` and `Decoder::reader`. `Decoder::open` now routes a path
  that cannot be read positionally, such as a FIFO, character device, or
  socket, to the streaming decoder instead of failing, and the CLI accepts `-`
  for standard input. Such input runs the sequential zlib-rs path that the
  parallel paths already use as their authoritative fallback, sharing its
  framing, footer verification, trailing-garbage detection, and output limit,
  so it is verified identically but is not decoded in parallel.
  `DecoderStats` and `DecodeReport` report a single worker for it rather than
  the configured thread budget. Nothing is spooled; input memory is one
  configured input window. Dropping a streaming `DecoderReader` before end of
  output cancels without waiting for its background thread, so a stalled
  producer cannot block the drop.

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
