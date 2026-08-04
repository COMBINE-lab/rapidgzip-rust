# rapidgzip-rust

`rapidgzip-rust` is a Rust 2024, decoder-only implementation of the
[rapidgzip] approach to parallel DEFLATE decompression. It uses the same
marker/window strategy for gzip, zlib, and raw-DEFLATE streams, with zlib-rs as
the inflate backend, and adds direct parallel paths for BGZF, stored gzip, and
dense multi-member gzip archives.

The project provides:

- the `rapidgzip-core` library crate;
- the `rapidgzip-rust` binary, distributed by the `rapidgzip-rust-cli` package;
- verified decoding of single-member gzip, concatenated/multi-member gzip,
  BGZF, and zlib, plus structurally validated raw DEFLATE;
- both a push API over `std::io::Write` and an owned `std::io::Read + Send`
  stream suitable for parsers such as [paraseq];
- opt-in random-access index construction, interoperable index formats, and a
  decoded-output `Read + Seek` adapter;
- opt-in newline counting, line-annotated indexes, and indexed seeking by
  zero-based line number;
- bounded structural analysis of container framing, every DEFLATE block,
  dynamic Huffman alphabets, symbol composition, and predecessor-window use;
- decoding of non-seekable compressed input such as standard input, a FIFO, a
  process substitution, or a socket.

Non-seekable input is decoded sequentially by the same zlib-rs path the parallel
decoders fall back to, so it receives the same format-specific validation as a
regular file but is not decoded in parallel. See
[Non-seekable input](#non-seekable-input).

The project intentionally does not provide compression. Its random-access API
is decoder-only and leaves ordinary decode operations unchanged.

## Library installation

Add the decoder to a Rust project with:

```console
cargo add rapidgzip-core
```

The package name contains a hyphen and its Rust crate name is
`rapidgzip_core`. Rust 1.87 or newer is required.

### Container formats

Strict gzip is the compatibility-preserving default. Select zlib or raw
DEFLATE explicitly, or opt into detection between gzip and zlib:

```rust
use rapidgzip_core::{Decoder, Format};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let zlib = Decoder::builder().format(Format::Zlib).build()?;
let raw = Decoder::builder()
    .format(Format::RawDeflate)
    .expected_uncompressed_size(Some(1_000_000))
    .build()?;
let detected = Decoder::builder().auto_detect_format().build()?;
# let _ = (zlib, raw, detected);
# Ok(())
# }
```

Auto-detection performs an exact, non-consuming two-byte check and never
guesses raw DEFLATE, which has no identifying header. Zlib CMF/FLG, its declared
history window, and its Adler-32 trailer are checked. Raw DEFLATE has no
container checksum or stored size: successful decoding establishes structural
validity and exact input consumption, while
[`DecoderBuilder::expected_uncompressed_size`] can add an exact size contract.
`DecodeReport::format` always contains the concrete detected or selected
format and the report remains `Copy`.

### Streaming reader

[`Decoder::open`] owns the compressed file and returns a movable
[`DecoderReader`] implementing `Read + Send`:

```rust,no_run
use rapidgzip_core::Decoder;
use std::io::{self, Read};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decoder = Decoder::builder().decoder_threads(8).build()?;
    let mut reader = decoder.open("reads.fastq.gz")?;

    // A parser can own this as Box<dyn Read + Send> instead.
    io::copy(&mut reader, &mut io::sink())?;
    let report = reader.finish()?;
    assert!(report.member_count >= 1);
    Ok(())
}
```

Reaching EOF verifies every member footer. Calling `finish` before EOF discards
unread decoded bytes, verifies the remainder, and returns the final report.
Dropping the reader early cancels its background work and does not claim that
the unread remainder was verified.

### Runtime telemetry and worker control

[`DecoderReader::handle`] returns a cloneable [`DecoderHandle`]. Retain it
before moving the reader into paraseq or a `Box<dyn Read + Send>`:

```rust,no_run
use rapidgzip_core::{Decoder, DecoderPressure};
use std::io::Read;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decoder = Decoder::builder().decoder_threads(32).build()?;
    let reader = decoder.open("reads.fastq.gz")?;
    let control = reader.handle();

    // The parser may now take ownership of the reader.
    let mut parser_input: Box<dyn Read + Send> = Box::new(reader);

    // A process-wide scheduler can reduce or restore the decoder's ceiling.
    control.set_worker_limit(8)?;
    let stats = control.stats();
    if matches!(stats.pressure, DecoderPressure::ConsumerBound { .. }) {
        control.set_worker_limit(2)?;
    }

    std::io::copy(&mut parser_input, &mut std::io::sink())?;
    Ok(())
}
```

The configured worker count is an immutable maximum, not an eager allocation.
Workers are created lazily as the empirical controller finds useful parallel
work. Lowering the runtime ceiling is nonblocking: in-flight tasks finish,
excess workers stop accepting tasks, and persistently excess OS threads retire.
They can be recreated if the ceiling and measured demand later increase.
Sustained backpressure at the final reader handoff automatically reduces task
admission to one worker and retires the rest.

`DecoderStats` distinguishes the configured maximum, current application
ceiling, adaptive active target, workers executing decode tasks, and live OS
threads. It also reports the selected decode path, verified members, produced
and consumed bytes, average rates, and a high-level pressure classification.
Snapshots use relaxed atomic loads, are deliberately approximate, and describe
rapidgzip task activity rather than operating-system CPU utilization.

For ordinary gzip, zlib, and raw-DEFLATE files, telemetry may briefly report
`DecoderPath::MarkerAdmission`. This bounded input-aware screen compares exact
zlib-rs work with a useful-width speculative marker wave after applying the
configured budget, runtime ceiling, visible processors, and available task
count. Its terminal path is `Sequential` or `MarkerWindow`; BGZF, stored, and
dense-member inputs retain their specialized routes.

### Push decoding

[`Decoder::decode`] avoids the final reader copy and calls a `Write` value only
from the calling thread. [`Decoder::decode_path`] is its filesystem-path
counterpart and automatically applies the same regular/non-regular routing as
[`Decoder::open`]. The writer therefore does not need to implement `Send`:

```rust,no_run
use rapidgzip_core::Decoder;
use std::io;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decoder = Decoder::default();
    let report = decoder.decode_path("reads.fastq.gz", &mut io::sink())?;
    println!("verified {} gzip members", report.member_count);
    Ok(())
}
```

Parallel input is represented by the [`ReadAt`] trait. Implementations are
provided for files on Unix and Windows, byte slices, `Vec<u8>`, `Arc<T>`, and
`Box<T>`. A custom source must support concurrent positional reads and keep its
length and contents stable for the complete decode. Input that cannot satisfy
that contract is handled by the entry points below instead.

### Non-seekable input

[`Decoder::stream_reader`] and [`Decoder::decode_stream`] accept any
`std::io::Read`, so compressed input arriving on standard input, a FIFO, a process
substitution, or a socket can be decoded without a second decompressor. They
mirror [`Decoder::reader`] and [`Decoder::decode`], and `stream_reader` returns
the same [`DecoderReader`]:

```rust,no_run
use rapidgzip_core::Decoder;
use std::io::{self, Read};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decoder = Decoder::default();
    let reader = decoder.stream_reader(io::stdin())?;

    // Still Read + Send, so a parser can own it.
    let mut parser_input: Box<dyn Read + Send> = Box::new(reader);
    io::copy(&mut parser_input, &mut io::sink())?;
    Ok(())
}
```

[`Decoder::open`] does this routing itself, so a program whose input is a path
that may or may not be a regular file needs no special case:

```rust,no_run
use rapidgzip_core::Decoder;
use std::io;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decoder = Decoder::builder().decoder_threads(8).build()?;
    // A regular file decodes in parallel; a FIFO decodes sequentially.
    let mut reader = decoder.open("reads.fastq.gz")?;
    io::copy(&mut reader, &mut io::sink())?;
    Ok(())
}
```

What works exactly as it does for a regular file: every supported format,
including concatenated and empty gzip members, BGZF's 28-byte EOF member, and
fully stored streams. Gzip members require matching CRC32 and ISIZE; zlib
requires a matching Adler-32; raw DEFLATE requires a complete final block and
exact source end. Truncation, invalid DEFLATE, framing/checksum mismatches, and
trailing bytes are errors, and
[`DecoderBuilder::output_limit`] still fails before emitting bytes past the
limit. Reaching EOF, or an `Ok` report, still means the complete input was
verified.

What differs: the four parallel decode paths all need positional reads, so a
non-seekable source always uses the sequential path. Telemetry preserves the
builder contract: [`DecoderStats::configured_workers`] and
`DecodeReport::decoder_threads` remain the requested maximum budget, while
[`DecoderStats::active_workers`] is one and both `spawned_workers` and
`auxiliary_threads` are zero. Nothing is spooled to memory or disk: input memory
is one [`DecoderBuilder::input_page_size`] window, raised to two bytes only when
configured smaller so detection can retain its prefix. `stream_reader` advances its
resumable inflater only inside the consumer's `Read::read` call, so a slow
consumer naturally stops reading the producer. Dropping it immediately drops
the source; there is no streaming coordinator thread to block or detach.

### Structural analysis

[`Decoder::analyze`] verifies the complete input while returning structured
container, block, alphabet, symbol, and predecessor-window facts. Analysis is
an explicit operation, so ordinary decoding and its small `Copy`
`DecodeReport` are unchanged:

```rust,no_run
use rapidgzip_core::{AnalyzeOptions, Decoder};
use std::fs::File;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let source = File::open("reads.fastq.gz")?;
let options = AnalyzeOptions::default()
    .maximum_blocks(250_000)
    .maximum_retained_backreferences(10_000);
let analysis = Decoder::default().analyze_with_options(&source, options)?;

println!("{} members", analysis.streams.len());
for (kind, count) in analysis.block_type_counts() {
    println!("{kind:?}: {count}");
}
assert_eq!(analysis.compressed_size_in_bytes, source.metadata()?.len());
# Ok(())
# }
```

The default result retains up to 100,000 streams, 100,000 blocks, and 1 MiB of
optional gzip-header metadata across the input. Individual predecessor-window
references are omitted by default. Their counts, length histogram, farthest
reach, deterministic interval-union count, and window coverage remain exact;
each block says how many details were omitted. [`AnalyzeOptions`] makes every
retention limit explicit, and exceeding a structural limit returns a typed
[`AnalysisErrorKind`] through `DecodeError::Analysis` rather than allocating
without bound.

The walk is intentionally single-threaded and causal. It keeps one 32 KiB
history ring and verifies gzip CRC32/ISIZE or zlib Adler-32 without retaining
decoded output. Concatenated and empty gzip members, BGZF (including its EOF
member), zlib, raw DEFLATE, format detection, output limits, exact-size
contracts, trailing-data rules, positional [`ReadAt`] sources, and streaming
`Read` sources are supported. [`Decoder::analyze_stream`] is the forward-only
counterpart. Timings and rapidgzip-specific text formatting remain CLI
presentation data and are not part of the deterministic [`Analysis`] value.

### Random-access indexes and seeking

Index construction is explicit per decode operation. This keeps the existing
`DecodeReport` small and `Copy`, and keeps checkpoint-window work out of calls
that do not request it:

```rust,no_run
use rapidgzip_core::{Decoder, DeflateIndex, IndexOptions, IndexedReader};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decoder = Decoder::builder().decoder_threads(8).build()?;
    let source = File::open("reads.fastq.gz")?;
    let indexed = decoder.decode_with_index(
        &source,
        &mut io::sink(),
        IndexOptions::default(),
    )?;

    let mut serialized = File::create("reads.fastq.gz.rgzidx")?;
    indexed.index.write_native(&mut serialized)?;

    let mut serialized = File::open("reads.fastq.gz.rgzidx")?;
    let index = DeflateIndex::read_native(&mut serialized)?;
    let mut reader = IndexedReader::new(File::open("reads.fastq.gz")?, index)?;
    reader.seek(SeekFrom::Start(4_000_000))?;
    let mut buffer = [0; 4096];
    reader.read_exact(&mut buffer)?;
    Ok(())
}
```

The pull API is `Decoder::reader_with_index`; its
`IndexingDecoderReader` remains `Read + Send`, exposes the same telemetry and
worker controls as `DecoderReader`, and returns an `IndexedDecodeReport` from
`finish`. `decode_stream_with_index` and `stream_reader_with_index` can also
collect a coarse member-boundary index while consuming forward-only input. The
result can be used later only with a stable positional copy of the same
compressed bytes.

An existing index can also drive a complete parallel decode. The push API
borrows it; the `Read + Send` API takes an `Arc` so its stored windows are not
cloned into the background coordinator:

```rust,no_run
use rapidgzip_core::{Decoder, DeflateIndex};
use std::fs::File;
use std::io::{self, Read};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut serialized = File::open("reads.fastq.gz.rgzidx")?;
    let index = Arc::new(DeflateIndex::read_native(&mut serialized)?);
    let decoder = Decoder::builder().decoder_threads(16).build()?;
    let mut reader = decoder.reader_from_index(
        File::open("reads.fastq.gz")?,
        Arc::clone(&index),
    )?;

    let control = reader.handle();
    control.set_worker_limit(8)?;
    io::copy(&mut reader, &mut io::sink())?;
    let report = reader.finish()?;
    assert!(report.member_count >= 1);
    Ok(())
}
```

`decode_from_index` is the corresponding lower-overhead `Write` operation.
Both APIs are strict: the index must match the source, selected container,
compressed bit boundaries, and decompressed offsets. They never hide a bad
index by falling back to ordinary decoding. Gzip CRC32/ISIZE and zlib Adler-32
remain fully verified because the operation decodes every indexed span from
source origin. Concatenated and empty gzip members are preserved, and an
imported BGZF `.gzi` works even though that format omits the final decompressed
size. Telemetry identifies `DecoderPath::IndexedParallel` and exposes the same
dynamic worker ceiling as other parallel readers.

`DeflateIndex` records gzip, BGZF, zlib, or raw-DEFLATE provenance and reads and
writes:

- the native versioned format, which preserves all rapidgzip-rust metadata;
- indexed_gzip `GZIDX` versions 0/1 (writing version 1);
- htslib BGZF `.gzi` indexes; and
- gztool version 0/1 indexes.

Format parsers apply explicit checkpoint and window-allocation limits through
`IndexReadOptions`. The native format represents every container. `.gzi`
export requires an index proven to come from BGZF; GZIDX and gztool export
require gzip-family provenance; gztool line-aware export requires real line
counters and never invents them. The CLI detects formats from a bounded prefix,
streams index parsing, rejects trailing bytes, and writes exports through a
same-directory temporary file so failed conversions cannot truncate an
existing index.

Line metadata is collected only when requested. Enabling
[`DecoderBuilder::count_lines`] adds `DecodeReport::line_count`; combining it
with an explicit indexing operation also annotates every retained checkpoint:

```rust,no_run
use rapidgzip_core::{Decoder, IndexOptions, IndexedReader};
use std::fs::File;
use std::io;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decoder = Decoder::builder()
        .decoder_threads(8)
        .count_lines(true)
        .build()?;
    let source = File::open("reads.fastq.gz")?;
    let indexed = decoder.decode_with_index(
        &source,
        &mut io::sink(),
        IndexOptions::default(),
    )?;
    assert_eq!(indexed.decode.line_count, indexed.index.total_line_count());

    let mut reader = IndexedReader::new(source, indexed.index)?;
    let byte_offset = reader.seek_to_line(1_000_000)?;
    println!("line 1000000 begins at decoded byte {byte_offset}");
    Ok(())
}
```

A line offset is the number of `b'\n'` bytes preceding a position. Line zero
begins at decoded byte zero. A final unterminated line does not increase
`line_count`, while `seek_to_line` can still reach it after scanning from the
nearest checkpoint. Counting happens once on final ordered bytes, after marker
resolution, and is disabled by default. `DecodeReport` remains `Copy` because
the result is an optional scalar. A line-aware index is published only when
every retained checkpoint received an exact count; partial metadata is never
presented as complete. Strict full-stream decoding with line counting enabled
also recomputes imported per-checkpoint and total line counts, rejecting
structurally valid but incorrect navigation metadata. Without line counting,
imported line offsets remain explicitly trusted navigation data.

`IndexedReader` validates the index and any recorded source size before use.
Resuming at a gzip member or zlib-header checkpoint verifies that complete
framing unit, including bytes discarded during a seek. A raw stream has no
checksum to verify. An interior checkpoint cannot authenticate bytes skipped
before it because persisted indexes do not carry prefix checksum state.

## Command-line installation and use

Install the CLI package with:

```console
cargo install rapidgzip-rust-cli
```

The installed executable is named `rapidgzip-rust`:

```console
# Decompress to stdout.
rapidgzip-rust -P 16 reads.fastq.gz > reads.fastq

# Verify every member and discard decoded output.
rapidgzip-rust -P 16 --test reads.fastq.gz

# Count decoded bytes or newline bytes.
rapidgzip-rust --count reads.fastq.gz
rapidgzip-rust --count-lines reads.fastq.gz

# Decode and count in one pass. Counts use stderr when payload uses stdout.
rapidgzip-rust -c --count --count-lines reads.fastq.gz > reads.fastq

# Build a native index, then use it for strict full-stream parallel decoding.
rapidgzip-rust --test --export-index reads.rgzidx \
  --index-format native reads.fastq.gz
rapidgzip-rust --import-index reads.rgzidx -c reads.fastq.gz > reads.fastq

# Extract byte and zero-based line ranges in the requested order.
rapidgzip-rust --ranges '1KiB@4MiB,10L@1000L' -c reads.fastq.gz

# Authenticate the complete source before an imported random-access read.
rapidgzip-rust --import-index reads.rgzidx --verify \
  --ranges '10L@1000L' -c reads.fastq.gz

# Export gztool version 1 with real line counters.
rapidgzip-rust --count-lines --export-index reads.gzi \
  --index-format gztool-with-lines reads.fastq.gz

# Refuse to overwrite an existing output file.
rapidgzip-rust -P 16 --output reads.fastq reads.fastq.gz

# Read standard input, decoded sequentially and verified the same way.
cat reads.fastq.gz | rapidgzip-rust - > reads.fastq

# Print rapidgzip-compatible framing and DEFLATE block analysis.
rapidgzip-rust --analyze reads.fastq.gz

# Retain bounded per-reference detail; aggregate summaries are always exact.
rapidgzip-rust --analyze --verbose \
  --analysis-reference-limit 10000 reads.fastq.gz

# Streaming analysis uses the same bounded forward walk.
cat reads.fastq.gz | rapidgzip-rust --analyze -

# A FIFO or process substitution given as a path is routed the same way.
rapidgzip-rust <(some_producer) > reads.fastq
```

The CLI auto-detects gzip or zlib by default; raw DEFLATE requires
`--format raw-deflate`. `--chunk-size` controls the decoded handoff size in
KiB. Output defaults to stdout when redirected; at a terminal, a regular input
derives a safe output filename using case-insensitive compression suffixes.
Existing files require `--force`. Index export defaults to the native,
format-neutral representation; gzip-specific interoperable formats must be
selected explicitly.

`-P`/`--decoder-parallelism` (`--threads` is an alias) is a maximum
decoder-worker budget. Parallel paths bootstrap
from the smaller of the affinity-visible processors and this requested budget,
then create more workers only while measurements justify them. They may retain
fewer active workers when the input exposes less parallel work, the consumer is
backpressured, or additional concurrency reduces throughput.

Imported indexes are never advisory. Full-stream operations use
`decode_from_index`, and malformed, incomplete, or source-mismatched indexes
fail without falling back. Range extraction uses `IndexedReader`; a line range
requires complete line metadata in an imported index or builds a line-aware
index first. A seek from an interior DEFLATE checkpoint cannot authenticate
the skipped prefix. For that reason, `--verify` on an imported range performs
a complete strict indexed decode before extraction. Decoder options that would
otherwise be ignored by an unverified imported range are rejected and explain
that `--verify` is required. `--no-verify`, `--sparse-windows`, and the `sequential` and
`locked-read` I/O methods are rejected because the current implementation
cannot honor their semantics. Outside imported ranges, complete decode paths
already verify their selected framing. `--no-sparse-windows`, `-d`, and `-k`
remain compatibility aliases. This is a deliberately compatible subset, not a
claim that every rapidgzip CLI option is implemented.

## Correctness and resource behavior

- Every accepted gzip member is terminated by an actual final DEFLATE block
  and checked against its CRC32 and modulo-2^32 uncompressed size.
- Every accepted zlib stream has a valid CMF/FLG header, respects its declared
  window, ends exactly once, and matches its Adler-32 trailer.
- Raw DEFLATE must end exactly after its final block; it is structurally
  validated but not described as checksum-authenticated.
- Concatenated members, empty members, optional gzip headers, BGZF data, and
  the conventional BGZF EOF member are supported.
- Bytes resembling a gzip header inside DEFLATE data are never trusted as a
  boundary without independent inflation, footer authentication, and exact
  adjacency to the preceding verified member.
- Trailing non-gzip data, truncated input, invalid DEFLATE, and footer
  mismatches are errors. Output written before an error is not rolled back.
- [`DecoderBuilder::output_limit`] bounds total decoded output. The decoder
  fails before emitting bytes beyond the configured limit.
- [`DecoderBuilder::expected_uncompressed_size`] optionally requires one exact
  total for any format, rejecting overruns before handoff and underruns at end.
- Work queues and reader handoff are bounded. Memory still scales with active
  workers and configured chunk sizes; the defaults are intended for throughput
  on general-purpose machines rather than minimum memory use. Reusable
  positional-reader allocations are recycled only after consumption through a
  decode-local pool capped at two decoded chunks and four entries.
- Structural analysis retains one output-history window plus explicitly
  bounded stream, block, optional-header, alphabet, and detailed-reference
  results. Checked counter or allocation failure is reported as a typed
  analysis error; exact summaries do not require detailed references.
- All of the above hold identically for non-seekable input, because it runs the
  same sequential decoder that the parallel paths already use as their
  authoritative fallback. It is not decoded in parallel, and the telemetry says
  so rather than reporting an unused thread budget.

There is no unsafe public API. Private unsafe code is limited to the zlib-rs
ABI, checked SIMD operations, proven initialized-buffer operations, and the
audited `Send` implementation for exclusively owned resumable inflate state;
each site has a local safety argument.
See [SAFETY.md] for the complete audit.

## Performance status

The current implementation clears its zlib-ng-backed C++ rapidgzip parity gate
on the public FASTQ workload and on synthetic single-member, concatenated, and
BGZF corpora. On the public FASTQ workload it also exceeds the ISA-L-enabled
C++ build at multi-worker budgets; the remaining measured ISA-L gap is the
one-worker case.

Performance is workload- and machine-dependent. Reproduce the published
measurements rather than treating these results as a universal speed claim:

- [ARCHITECTURE.md] describes the algorithm and scheduling paths.
- [BENCHMARKING.md] records corpora, commands, versions, thread counts, hashes,
  throughput, and memory measurements.
- [PERFORMANCE_AUDIT.md] records the ISA-L comparison and optimization audit.
- [CHANGELOG.md] summarizes each published release.

The release runner can generate self-verified single-member, multi-member,
true-BGZF, stored, zlib, and raw-DEFLATE controls without downloading data:

```console
benchmarks/run-fair.sh --generate --cpus 0-43
```

ISA-L, zlib-ng, and gzippy competitors are configured through explicit paths;
the runner records their identities and versions, requires decoded SHA-256
parity before timing, rotates measured order, and writes raw observations plus
deterministic summaries under `target/bench-results`. See
[BENCHMARKING.md] for the complete interface and release-host requirements.

## Platform and compatibility policy

- Rust edition: 2024
- Minimum supported Rust version (MSRV): 1.87
- First-class positional file sources: Unix and Windows
- SIMD: runtime-dispatched x86-64 AVX2/SSE4.1 where applicable, baseline NEON
  on AArch64, and scalar fallbacks
- Inflate backend: zlib-rs through `libz-rs-sys`

Pre-1.0 releases should be treated as an evolving initial API. Correct
format-specific decoding, multi-member gzip verification, and the `Read + Send`
contract are foundational; configuration details may be refined as additional
workloads are measured.

## Development

```console
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

The integration suite covers gzip, zlib, raw DEFLATE, multi-member streams,
BGZF, corruption, format detection across short/interrupted reads, false header
candidates, output limits and exact sizes, index construction, indexed
full-stream parallel decode, seeking, cancellation, one-byte consumer buffers,
line counting and seeking, CLI index/range workflows, and direct paraseq
consumption. Generated benchmark corpora and large
sequencing files are deliberately not stored in the repository.

## Releasing

Maintainers can validate a prospective release without retaining version-file
changes:

```console
scripts/bump_and_publish.sh --dry-run 0.1.0
```

Omitting `--dry-run` updates the workspace version, repeats formatting, lint,
test, documentation, and package checks, creates and pushes a release commit
and annotated tag, publishes the crates to crates.io, and creates a GitHub
release from that version's changelog section. The script requires a clean
`main` branch plus Cargo and GitHub authentication, and asks for confirmation
before external changes; `--yes` is available for an intentional unattended
release.

## License

The repository is distributed under the combined terms of BSD-3-Clause and
MIT. See [LICENSE-BSD-3-CLAUSE] and [LICENSE-MIT].

[`Decoder::decode`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.decode
[`Decoder::decode_path`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.decode_path
[`Decoder::decode_from_index`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.decode_from_index
[`Decoder::decode_with_index`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.decode_with_index
[`Decoder::decode_stream`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.decode_stream
[`Decoder::analyze`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.analyze
[`Decoder::analyze_stream`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.analyze_stream
[`Decoder::open`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.open
[`Decoder::reader`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.reader
[`Decoder::reader_from_index`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.reader_from_index
[`Decoder::stream_reader`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.stream_reader
[`DeflateIndex`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DeflateIndex.html
[`DecoderHandle`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderHandle.html
[`DecoderPath::IndexedParallel`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/enum.DecoderPath.html#variant.IndexedParallel
[`DecoderPath::Sequential`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/enum.DecoderPath.html#variant.Sequential
[`DecoderReader::handle`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderReader.html#method.handle
[`DecoderBuilder::input_page_size`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderBuilder.html#method.input_page_size
[`DecoderBuilder::output_limit`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderBuilder.html#method.output_limit
[`DecoderBuilder::expected_uncompressed_size`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderBuilder.html#method.expected_uncompressed_size
[`DecoderBuilder::count_lines`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderBuilder.html#method.count_lines
[`DecoderReader`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderReader.html
[`DecoderStats::path`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderStats.html#structfield.path
[`DecoderStats::configured_workers`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderStats.html#structfield.configured_workers
[`DecoderStats::active_workers`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderStats.html#structfield.active_workers
[`ReadAt`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/trait.ReadAt.html
[`Analysis`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Analysis.html
[`AnalysisErrorKind`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/enum.AnalysisErrorKind.html
[`AnalyzeOptions`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.AnalyzeOptions.html
[ARCHITECTURE.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/ARCHITECTURE.md
[BENCHMARKING.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/BENCHMARKING.md
[CHANGELOG.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/CHANGELOG.md
[LICENSE-BSD-3-CLAUSE]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/LICENSE-BSD-3-CLAUSE
[LICENSE-MIT]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/LICENSE-MIT
[PERFORMANCE_AUDIT.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/PERFORMANCE_AUDIT.md
[SAFETY.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/SAFETY.md
[paraseq]: https://docs.rs/paraseq/
[rapidgzip]: https://github.com/mxmlnkn/rapidgzip
