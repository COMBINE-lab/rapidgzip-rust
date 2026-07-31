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
  stream suitable for parsers such as [paraseq].

This is currently a release candidate for the initial `0.1.0` crates.io
publication. The project intentionally does not yet provide compression,
random-access indexes, seeking in decoded output, or non-seekable compressed
input.

## Library installation

Once published, add the decoder to a Rust project with:

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
length and contents stable for the complete decode.

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
```

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

## Platform and compatibility policy

- Rust edition: 2024
- Minimum supported Rust version (MSRV): 1.87
- First-class positional file sources: Unix and Windows
- SIMD: runtime-dispatched x86-64 AVX2/SSE4.1 where applicable, baseline NEON
  on AArch64, and scalar fallbacks
- Inflate backend: zlib-rs through `libz-rs-sys`

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
and annotated tag, and publishes the crates to crates.io. The script requires a
clean `main` branch and asks for confirmation before external changes;
`--yes` is available for an intentional unattended release.

## License

The repository is distributed under the combined terms of BSD-3-Clause and
MIT. See [LICENSE-BSD-3-CLAUSE] and [LICENSE-MIT].

[`Decoder::decode`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.decode
[`Decoder::open`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.open
[`DecoderHandle`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderHandle.html
[`DecoderReader::handle`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderReader.html#method.handle
[`DecoderBuilder::output_limit`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderBuilder.html#method.output_limit
[`DecoderReader`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.DecoderReader.html
[`ReadAt`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/trait.ReadAt.html
[ARCHITECTURE.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/ARCHITECTURE.md
[BENCHMARKING.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/BENCHMARKING.md
[LICENSE-BSD-3-CLAUSE]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/LICENSE-BSD-3-CLAUSE
[LICENSE-MIT]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/LICENSE-MIT
[PERFORMANCE_AUDIT.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/PERFORMANCE_AUDIT.md
[SAFETY.md]: https://github.com/COMBINE-lab/rapidgzip-rust/blob/main/SAFETY.md
[paraseq]: https://docs.rs/paraseq/
[rapidgzip]: https://github.com/mxmlnkn/rapidgzip
