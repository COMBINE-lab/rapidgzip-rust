# rapidgzip-rust

`rapidgzip-rust` is a Rust 2024, decoder-only implementation of the
[rapidgzip](https://github.com/mxmlnkn/rapidgzip) approach to parallel gzip
decompression.

The library exposes both a push API writing decompressed bytes to
`std::io::Write` and an owned `std::io::Read + Send` adapter suitable for
streaming parsers such as
[paraseq](https://docs.rs/paraseq/latest/paraseq/).

The implementation treats a gzip file as a sequence of independently verified
members. Concatenated gzip and valid BGZF are part of the base correctness
contract.

```rust
use rapidgzip_core::Decoder;
use std::io::{self, Read};

let decoder = Decoder::builder().decoder_threads(8).build()?;
let reader = decoder.open("reads.fastq.gz")?;
let mut input: Box<dyn Read + Send> = Box::new(reader);
io::copy(&mut input, &mut io::sink())?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`Decoder::decode` is the lower-overhead push interface. `Decoder::reader` and
`Decoder::open` provide the `Read + Send` surface intended for parsers such as
paraseq. Parallel input is expressed through the documented `ReadAt` trait;
files, byte vectors, slices, `Arc<T>`, and `Box<T>` are supported.

The implementation meets the provisional zlib-ng C++ gate on the synthetic
single-member, concatenated-member, and BGZF validation corpus. A public FASTQ
benchmark also exposed a substantial generic-stream scaling gap against
ISA-L-enabled C++ rapidgzip; that is the next performance milestone. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the implementation,
[BENCHMARKING.md](BENCHMARKING.md) for reproducible measurements, and
[PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md) for the evidence and proposed
optimization order.

## Releasing

Maintainers can validate a prospective release without changing the working
tree:

```console
scripts/bump_and_publish.sh --dry-run 0.2.0
```

Omitting `--dry-run` updates the workspace version, repeats the formatting,
lint, test, documentation, and package checks, creates and pushes a release
commit and annotated tag, and publishes the crates to crates.io. The script
requires a clean `main` branch and asks for confirmation before making those
external changes; `--yes` is available for an intentional unattended release.
