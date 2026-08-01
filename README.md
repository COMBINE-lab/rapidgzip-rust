# rapidgzip-rust

`rapidgzip-rust` is a Rust 2024, decoder-only implementation of the
[rapidgzip] approach to parallel gzip decompression. It uses the same
marker/window strategy for ordinary gzip streams, with zlib-rs as the inflate
backend, and adds direct parallel paths for BGZF, stored streams, and dense
multi-member archives.

The project provides:

- the `rapidgzip-core` library crate;
- the `rapidgzip-rust` binary, distributed by the `rapidgzip-rust-cli` package;
- verified decoding of single-member gzip, concatenated/multi-member gzip,
  BGZF, zlib-wrapped DEFLATE (RFC 1950; auto-detected; parallel multi-stream
  and long single-stream estimated paths), and raw DEFLATE (RFC 1951;
  explicit `--format raw`; long streams use the same estimated marker path
  when `decoder_threads >= 4`);
- both a push API over `std::io::Write` and an owned `std::io::Read + Send`
  stream suitable for parsers such as [paraseq].

This is currently a release candidate for the feature-complete `0.2.0`
crates.io publication (crates.io `rapidgzip-core` 0.1.0 was an incomplete early
stub with a smaller API). The project intentionally does not yet provide
compression.
Non-seekable compressed input (stdin/pipes) uses [`Decoder::decode_read`]:
single-thread gzip, zlib, and raw DEFLATE stream page-at-a-time (no full-archive
RAM buffer); multi-thread paths spill to a private temporary file then run the
positional backend (parallel gzip / multi-stream or marker zlib / marker raw
when each format’s thread and size gates allow).

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

### Streaming / non-seekable input

[`Decoder::decode_read`] accepts any [`std::io::Read`] (stdin, sockets, pipes).

| Condition | Behavior | Memory (compressed side) |
|-----------|----------|---------------------------|
| `decoder_threads == 1` | Sequential stream (gzip / zlib / raw) | O(input page) + inflate working set |
| `decoder_threads > 1` | Spill stream to a private tempfile, then positional backend (parallel gzip; multi-stream or marker zlib; marker raw DEFLATE when `decoder_threads >= 4` and size amortizes ~2× the compressed grid) | Compressed size **on disk** + decoder working set |

Prefer a file or other [`ReadAt`] source with [`Decoder::decode`] /
[`Decoder::open`] when you already have positional input and want to avoid the
spill.

```rust,no_run
use rapidgzip_core::Decoder;
use std::io;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decoder = Decoder::builder().decoder_threads(8).build()?;
    let report = decoder.decode_read(io::stdin().lock(), &mut io::sink())?;
    println!("verified {} gzip members", report.member_count);
    Ok(())
}
```

## Command-line installation and use

Install the CLI package with:

```console
cargo install rapidgzip-rust-cli
```

The installed executable is named `rapidgzip-rust`:

```console
# Decompress to stdout (default when stdout is a pipe, or with -c).
rapidgzip-rust -P 16 reads.fastq.gz > reads.fastq
rapidgzip-rust -c reads.fastq.gz | wc -c

# Interactive terminal + file input: write to the path with one trailing
# compression suffix stripped (.gz / .gzip / .bgz / .bgzf / .tgz / .taz;
# ASCII case-insensitive). .tgz/.taz become <stem>.tar. Refuses overwrite
# unless -f/--force. Use -c to force stdout, or -o PATH.
rapidgzip-rust reads.fastq.gz          # -> reads.fastq when stdout is a TTY
rapidgzip-rust reads.FASTQ.GZ          # -> reads.FASTQ (case-insensitive strip)
rapidgzip-rust archive.tgz             # -> archive.tar
rapidgzip-rust -f reads.fastq.gz       # overwrite reads.fastq if it exists

# Force container format (default: auto-detect gzip vs zlib).
rapidgzip-rust --format gzip -c reads.fastq.gz > /dev/null
rapidgzip-rust --format zlib -c stream.zz > /dev/null
rapidgzip-rust --format raw -c payload.deflate > /dev/null

# -d/--decompress is accepted for gzip compatibility (always decompress-only).
rapidgzip-rust -d -c reads.fastq.gz > /dev/null

# Verify every member and discard decoded output.
rapidgzip-rust -P 16 --test reads.fastq.gz

# Explicit output path (refuses overwrite without -f/--force).
rapidgzip-rust -P 16 --output reads.fastq reads.fastq.gz
rapidgzip-rust -f -o reads.fastq reads.fastq.gz


# Tee: write to a file and to stdout together (`-o` + `-c`).
rapidgzip-rust -c -o reads.fastq reads.fastq.gz | wc -c

# CRC/Adler verification is on by default; --verify is explicit enable.
rapidgzip-rust --verify -t reads.fastq.gz
rapidgzip-rust --no-verify -c reads.fastq.gz > /dev/null

# Open-source attributions.
rapidgzip-rust --oss-attributions

# Quiet verification (no "ok" line on stderr). Quiet wins over verbose.
rapidgzip-rust -q --test reads.fastq.gz

# Verbose stats on stderr after a successful decode/test/count.
rapidgzip-rust -v -t reads.fastq.gz
rapidgzip-rust -v -c reads.fastq.gz > /dev/null

# Smaller decoded worker chunks (value is KiB; default is 4096 = 4 MiB).
rapidgzip-rust --chunk-size 1024 -c reads.fastq.gz > /dev/null

# Count Unix newlines (`\n` bytes) in the decompressed stream.
rapidgzip-rust --count-lines reads.fastq.gz

# Print gzip/BGZF/zlib/raw DEFLATE structure (no payload).
rapidgzip-rust --analyze reads.fastq.gz
rapidgzip-rust --analyze --format raw file.deflate

# Extract byte or line ranges (line numbers are 1-based, like gztool `-L`).
rapidgzip-rust --ranges '100@0' reads.fastq.gz
rapidgzip-rust --ranges '5L@20L' reads.fastq.gz

# Build / reuse a random-access index (import auto-detects GZIDX / gztool / BGZI).
rapidgzip-rust --export-index reads.gzidx -t reads.fastq.gz
rapidgzip-rust --export-index reads.gzi --index-format gztool -t reads.fastq.gz
rapidgzip-rust --export-index reads.gz.gzi --index-format bgzi -t reads.fastq.gz
rapidgzip-rust --import-index reads.gzidx --count reads.fastq.gz
rapidgzip-rust --import-index reads.gzidx -c reads.fastq.gz

# IndexedReader background prefetch window count (library default is 2; 0 disables).
rapidgzip-rust --seek-prefetch 0 --ranges '100@0' --import-index reads.gzidx reads.fastq.gz

# Read compressed data from stdin (`-`, or omit INPUT when stdin is a pipe).
# Ordinary decompress: sequential page stream, or multi-thread spill-to-temp + positional backend.
printf 'hello\n' | gzip | rapidgzip-rust -P 8 -c -

# --ranges / --analyze / --import-index on stdin spill to a private temp file (not full RAM).
printf 'hello world\n' | gzip | rapidgzip-rust --ranges '5@0' -c
```

`-P`/`--threads` is a maximum decoder-worker budget. Some formats or the
empirical controller may use fewer active workers when the input exposes less
parallel work or when additional concurrency reduces throughput.

Default output naming (file input, no `-c`/`-o`, not test/count/export/ranges):
when stdout is a TTY, write to the input path with one trailing `.gz`, `.gzip`,
`.bgz`, `.bgzf`, `.tgz`, or `.taz` suffix removed (ASCII case-insensitive).
`.tgz` / `.taz` rewrite to `<stem>.tar`. If stripping leaves an empty name, or
no known suffix is present, use `<input>.out`. When stdout is a pipe, stream to
stdout (gzip-compatible). Stdin always streams to stdout. Use `--format
auto|gzip|zlib|raw` (default `auto`) to require a container type; raw DEFLATE
is never auto-detected.

Library callers can enable [`DecoderBuilder::gather_line_offsets`] to obtain
[`DecodeReport::line_count`] and, with [`DecoderBuilder::keep_index`], stamp
per-checkpoint line offsets for [`IndexedReader::seek_to_line`]. Index
import/export supports indexed_gzip (`GZIDX`), gztool (`gzipindx` /
`gzipindX`), and htslib/bgzip BGZF block indexes (`.gzi` / BGZI via
`--index-format bgzi`); use [`read_gzip_index`] for auto-detect.
[`IndexedReader`] keeps an LRU of decoded windows with sequential read-ahead
and optional background prefetch of further windows (see
[`DecoderBuilder::seek_cache_chunks`] and
[`DecoderBuilder::seek_prefetch_windows`]).

## Correctness and resource behavior

- Every accepted gzip member is terminated by an actual final DEFLATE block
  and checked against its CRC32 and modulo-2^32 uncompressed size.
- zlib streams (RFC 1950 CMF/FLG + DEFLATE + Adler-32) are auto-detected. On
  positional `ReadAt` input:
  - **≥2 concatenated streams** with `decoder_threads > 1` use stream-granularity
    parallel zlib-rs (two-pass discard-index of boundaries, then ordered workers),
    unless a large solitary stream is preferred for the marker path below.
  - A **single long stream** uses the estimated marker/window path when
    `decoder_threads >= 4` and compressed size is at least about two grid cells
    (`2 × compressed_chunk_size` plus CMF/FLG + Adler trailer; default grid is
    1 MiB). Multi-stream tails after that first stream use stream-granularity
    parallel when remaining frames exist.
  - Budgets of 1 thread and small single streams stay on sequential zlib-rs.
    Multi-thread non-seekable `decode_read` spills to a temp file so the same
    positional parallel gates apply.
  - Adler-32 is gated by the same `crc32_enabled` / verify flag as gzip CRC.
- raw DEFLATE (RFC 1951) requires explicit format selection; no container
  integrity trailer and no random-access index. On positional `ReadAt` (and
  after multi-thread `decode_read` spill):
  - **Long streams** use the estimated marker/window path when
    `decoder_threads >= 4` and compressed size is at least about two grid cells
    (`2 × compressed_chunk_size`; default 1 MiB cells → ~2 MiB). Optional
    whole-stream check via `raw_crc32_list`.
  - **P=1–3** and **small streams** stay on sequential zlib-rs raw inflate.
- Residual: ISA-L / a faster single-thread inflater is not shipped (P=1 still
  zlib-rs); small streams and marker budgets of 1–3 threads stay sequential for
  ordinary single-member gzip, single-stream zlib, and raw DEFLATE; random-access
  indexes remain gzip/BGZF-oriented. Multi-thread `decode_read` / CLI stdin
  spills to a **temp file** (secure temp-dir defaults, deleted on drop) rather
  than keeping the full archive only in RAM; single-thread paths stay pure
  streaming.
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

Full checklist, script behavior, and post-publish steps:
[docs/RELEASE.md](docs/RELEASE.md). Shipped surface for this series:
[CHANGELOG.md](CHANGELOG.md).

```console
scripts/bump_and_publish.sh --dry-run 0.2.0
```

### Packaging

CI verifies the `rapidgzip-core` crates.io tarball and that the CLI still
checks against the in-tree path dependency:

```console
cargo package -p rapidgzip-core --locked
cargo check -p rapidgzip-rust-cli --locked
```

Full CLI package verification (`cargo package -p rapidgzip-rust-cli`) needs
`rapidgzip-core` published on crates.io at the same version (or the release
script’s multi-package dry-run:
`cargo publish -p rapidgzip-core -p rapidgzip-rust-cli --dry-run`).
Publish order is **core first**, then CLI. See [docs/RELEASE.md](docs/RELEASE.md)
for packaging notes and the pre-release checklist.

## License

The repository is distributed under the combined terms of BSD-3-Clause and
MIT. See [LICENSE-BSD-3-CLAUSE] and [LICENSE-MIT].

[`Decoder::decode`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.decode
[`Decoder::decode_read`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.decode_read
[`Decoder::open`]: https://docs.rs/rapidgzip-core/latest/rapidgzip_core/struct.Decoder.html#method.open
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
