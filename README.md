# rapidgzip-rust

`rapidgzip-rust` is a Rust 2024, decoder-only implementation of the
[rapidgzip] approach to parallel gzip decompression. It uses the same
marker/window strategy for ordinary gzip streams, with zlib-rs as the inflate
backend, and adds direct parallel paths for BGZF, stored streams, and dense
multi-member archives.

The project provides:

- the `rapidgzip-core` library crate;
- the `rapidgzip-rust` binary, distributed by the `rapidgzip-rust-cli` package;
- verified decoding of single-member gzip, concatenated/multi-member gzip, and
  BGZF;
- both a push API over `std::io::Write` and an owned `std::io::Read + Send`
  stream suitable for parsers such as [paraseq];
- decoding of non-seekable compressed input such as standard input, a FIFO, a
  process substitution, or a socket.

Non-seekable input is decoded sequentially by the same zlib-rs path the parallel
decoders fall back to, so it is verified exactly as strictly as a regular file
but is not decoded in parallel. See [Non-seekable input](#non-seekable-input).

The project intentionally does not provide compression.

## Library installation

Add the decoder to a Rust project with:

```console
cargo add rapidgzip-core
```

The package name contains a hyphen and its Rust crate name is
`rapidgzip_core`. Rust 1.87 or newer is required.

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

### Push decoding

[`Decoder::decode`] avoids the final reader copy and calls a `Write` value only
from the calling thread. The writer therefore does not need to implement
`Send`:

```rust,no_run
use rapidgzip_core::Decoder;
use std::fs::File;
use std::io;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = File::open("reads.fastq.gz")?;
    let decoder = Decoder::default();
    let report = decoder.decode(&input, &mut io::sink())?;
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
`std::io::Read`, so gzip arriving on standard input, a FIFO, a process
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

What works exactly as it does for a regular file: single-member, concatenated
and empty members, BGZF including its 28-byte EOF member, and fully stored
streams. A member is accepted only after a real final DEFLATE block whose CRC32
and ISIZE both match. Truncation, invalid DEFLATE, footer mismatches, and
trailing non-gzip bytes are all errors, and
[`DecoderBuilder::output_limit`] still fails before emitting bytes past the
limit. Reaching EOF, or an `Ok` report, still means the complete input was
verified.

What differs: the four parallel decode paths all need positional reads, so a
non-seekable source always uses the sequential path. Telemetry reports this
honestly rather than reporting an unused thread budget, so
[`DecoderStats::path`] is [`DecoderPath::Sequential`], `configured_workers` is
`1`, and `DecodeReport::decoder_threads` is `1`, whatever the builder was given.
Nothing is spooled to memory or to disk: input memory is one
[`DecoderBuilder::input_page_size`] window, and a slow consumer is throttled by
the same bounded handoff as a positional reader, which in turn stops reading the
pipe. Dropping a streaming [`DecoderReader`] before end of output cancels but
does not wait for the background thread, so a producer that stalls without
closing cannot block the drop.

## Containers

The decoder accepts gzip, concatenated gzip, BGZF, zlib streams, and raw
DEFLATE:

```rust
use rapidgzip_core::{Decoder, Format};

// Auto-detects gzip against zlib.
let decoder = Decoder::default();

// Raw DEFLATE has no header, so it is requested explicitly. It also carries
// no checksum, so an expected size is the only end-to-end check available.
let decoder = Decoder::builder()
    .format(Format::RawDeflate)
    .expected_uncompressed_size(Some(4_000_000))
    .build()?;
```

zlib streams verify their Adler-32 trailer, gzip members verify CRC32 and
ISIZE, and every container refuses trailing bytes. All of them decode in
parallel and support the random-access index below.

## Random access

Building an index during a decode costs one predecessor window per checkpoint
and makes later reads seekable:

```rust
use rapidgzip_core::{Decoder, IndexedReader};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

let decoder = Decoder::builder().build_index(true).build()?;
let mut reader = decoder.open("reads.fastq.gz")?;
io::copy(&mut reader, &mut io::sink())?;
let index = reader.finish()?.index.expect("index requested");
index.write_native(&mut File::create("reads.fastq.gz.idx")?)?;

let mut random = IndexedReader::new(File::open("reads.fastq.gz")?, index)?;
random.seek(SeekFrom::Start(4_000_000_000))?;
let mut buffer = [0u8; 4096];
random.read_exact(&mut buffer)?;
```

Indexes read and write four formats: the crate's own versioned format, the
indexed_gzip `GZIDX` format, the htslib BGZF `.gzi` format, and gztool's. An
index written here is usable by `bgzip -b`, `gztool -b`, and
`indexed_gzip.IndexedGzipFile.import_index`, and indexes written by those tools
are usable here. A BGZF index records every block, so it exports as a complete
`.gzi`; an index built from a non-seekable source records member starts only,
because the zlib backend does not expose DEFLATE block boundaries.

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

# Refuse to overwrite an existing output file.
rapidgzip-rust -P 16 --output reads.fastq reads.fastq.gz

# Read standard input, decoded sequentially and verified the same way.
cat reads.fastq.gz | rapidgzip-rust - > reads.fastq

# A FIFO or process substitution given as a path is routed the same way.
rapidgzip-rust <(some_producer) > reads.fastq
```

Every option rapidgzip 0.16.0 accepts is accepted here under the same names:

```console
# Build a random-access index while decoding, then extract from it.
rapidgzip-rust --export-index reads.gzi --index-format indexed_gzip -t reads.fastq.gz
rapidgzip-rust --import-index reads.gzi --ranges 1KiB@4GiB -c reads.fastq.gz

# Byte and line addressing, mixed, in the order given.
rapidgzip-rust --ranges 10@0,1KiB@15KiB,5L@20L,inf@40L -c reads.fastq.gz

# Sizes and line counts without keeping the output.
rapidgzip-rust --count reads.fastq.gz
rapidgzip-rust --count-lines reads.fastq.gz

# zlib and raw DEFLATE, the latter with the only check it allows.
rapidgzip-rust --format zlib -c payload.zlib
rapidgzip-rust --format raw-deflate --expected-size 4000000 -c payload.deflate
```

`--index-format` accepts `indexed_gzip` (the default), `gztool`,
`gztool-with-lines`, this crate's own `native`, and htslib's `gzi`. Line
addressing and `gztool-with-lines` need `--count-lines`, which is what fills
the per-checkpoint line offsets those features read.

### Inspecting a file's structure

`--analyze` walks every DEFLATE block and prints rapidgzip's report: stream
headers, one section per block with its offsets to the bit, Huffman alphabet
shapes and symbol ratios, then the file-wide distributions.

```console
rapidgzip-rust --analyze reads.fastq.gz | head -40
```

The output reproduces rapidgzip 0.16.0's, and `tests/analyze_interop.rs` diffs
against the real tool in CI to keep it that way. Two things are excluded from
that comparison:

- the benchmark profile, which prints wall-clock durations, so ours carries its
  own measurements;
- `Number of merged back-references`. The reference merges pairwise after an
  unstable sort, in a way that can shorten the current run, so its value
  depends on how equal distances happened to be ordered. Ours is a plain
  interval union, which is deterministic and, where they disagree, correct.

The library exposes the same data as `Decoder::analyze`, which returns a
structured `Analysis` rather than text.

### Deliberate differences from rapidgzip

Three options are accepted and do nothing, because they name behaviour this
crate does not have: `--io-read-method`, which has a single strategy here, and
`--sparse-windows` with its negation, since index windows are always dense. A
dense index is valid and interoperable, just larger.

`--no-verify` is refused rather than ignored. Verification is structural here:
a member is accepted only after its CRC32 and size check out, so there is
nothing to switch off and no speed to gain by pretending otherwise.

rapidgzip skips the decode entirely when output is piped to `/dev/null` without
`-l` or `--force`. This tool always decodes, so `rapidgzip-rust file.gz >
/dev/null` costs a full decode and verifies the file.

`-P`/`--threads` is a maximum decoder-worker budget. Parallel paths bootstrap
from the smaller of the affinity-visible processors and this requested budget,
then create more workers only while measurements justify them. They may retain
fewer active workers when the input exposes less parallel work, the consumer is
backpressured, or additional concurrency reduces throughput.

## Correctness and resource behavior

- Every accepted gzip member is terminated by an actual final DEFLATE block
  and checked against its CRC32 and modulo-2^32 uncompressed size.
- Concatenated members, empty members, optional gzip headers, BGZF data, and
  the conventional BGZF EOF member are supported.
- Bytes resembling a gzip header inside DEFLATE data are never trusted as a
  boundary without independent inflation, footer authentication, and exact
  adjacency to the preceding verified member.
- Trailing non-gzip data, truncated input, invalid DEFLATE, and footer
  mismatches are errors. Output written before an error is not rolled back.
- [`DecoderBuilder::output_limit`] bounds total decoded output. The decoder
  fails before emitting bytes beyond the configured limit.
- Work queues and reader handoff are bounded. Memory still scales with active
  workers and configured chunk sizes; the defaults are intended for throughput
  on general-purpose machines rather than minimum memory use.
- All of the above hold identically for non-seekable input, because it runs the
  same sequential decoder that the parallel paths already use as their
  authoritative fallback. It is not decoded in parallel, and the telemetry says
  so rather than reporting an unused thread budget.

There is no unsafe public API and no manual `Send` or `Sync` implementation.
Private unsafe code is limited to the zlib-rs ABI, checked SIMD operations, and
proven initialized-buffer operations; each site has a local safety argument.
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

## Optional ISA-L inflate backend

Raw inflate runs on zlib-rs. The `isal` feature of `rapidgzip-core` replaces it
with Intel's ISA-L on the paths that decode a whole stream from its start:
sequential gzip members, single-stream zlib and raw DEFLATE, and BGZF blocks.
The parallel marker/window path and `IndexedReader` stay on zlib-rs either way,
because both resume at arbitrary bit offsets and the parallel path needs zlib's
`Z_BLOCK` contract, neither of which ISA-L exposes.

The feature is off by default and links a system library rather than building
one: `libisal-dev` on Debian and Ubuntu, `isa-l` on Homebrew, or a prefix named
by `ISAL_INSTALL_PREFIX`.

```console
cargo add rapidgzip-core --features isal
```

Whether it is worth enabling is a measurement. On an Apple M-series machine
with isa-l 2.32.1, ISA-L was **20% slower** than zlib-rs on both benchmarked
paths, at 1.24 GiB/s against 1.49 GiB/s for sequential gzip and 1.29 GiB/s
against 1.56 GiB/s for raw DEFLATE, over a 16 MiB semi-structured log corpus.
The x86-64 comparison, where ISA-L's assembly decoder is strongest, is run by
the `isal` CI job on every push; read its logs for the current number. The
feature stays off by default regardless.

Reproduce it locally with two runs, since the backend is a compile-time choice:

```console
cargo bench -p rapidgzip-bench --bench inflate_backend -- --save-baseline zlib-rs
cargo bench -p rapidgzip-bench --bench inflate_backend \
  --features rapidgzip-core/isal -- --baseline zlib-rs
```

## Platform and compatibility policy

- Rust edition: 2024
- Minimum supported Rust version (MSRV): 1.87
- First-class positional file sources: Unix and Windows
- SIMD: runtime-dispatched x86-64 AVX2/SSE4.1 where applicable, baseline NEON
  on AArch64, and scalar fallbacks
- Inflate backend: zlib-rs through `libz-rs-sys`, with ISA-L available on the
  whole-stream paths behind the off-by-default `isal` feature

The `0.1.x` series should be treated as an evolving initial API. Correct gzip
decoding, member verification, and the `Read + Send` contract are foundational;
configuration details may be refined as additional workloads are measured.

## Development

```console
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

The integration suite covers ordinary gzip, multi-member streams, BGZF,
corruption, false header candidates, output limits, cancellation, one-byte
consumer buffers, and direct paraseq consumption. Generated benchmark corpora
and large sequencing files are deliberately not stored in the repository.

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
[`Decoder::decode_stream`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.decode_stream
[`Decoder::open`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.open
[`Decoder::reader`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.reader
[`Decoder::stream_reader`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.stream_reader
[`DecoderHandle`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderHandle.html
[`DecoderPath::Sequential`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/enum.DecoderPath.html#variant.Sequential
[`DecoderReader::handle`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderReader.html#method.handle
[`DecoderBuilder::input_page_size`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderBuilder.html#method.input_page_size
[`DecoderBuilder::output_limit`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderBuilder.html#method.output_limit
[`DecoderReader`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderReader.html
[`DecoderStats::path`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderStats.html#structfield.path
[`ReadAt`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/trait.ReadAt.html
[ARCHITECTURE.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/ARCHITECTURE.md
[BENCHMARKING.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/BENCHMARKING.md
[CHANGELOG.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/CHANGELOG.md
[LICENSE-BSD-3-CLAUSE]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/LICENSE-BSD-3-CLAUSE
[LICENSE-MIT]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/LICENSE-MIT
[PERFORMANCE_AUDIT.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/PERFORMANCE_AUDIT.md
[SAFETY.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/SAFETY.md
[paraseq]: https://docs.rs/paraseq/
[rapidgzip]: https://github.com/mxmlnkn/rapidgzip
