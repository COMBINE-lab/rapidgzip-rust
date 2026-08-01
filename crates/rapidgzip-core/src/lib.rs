//! Parallel, verified gzip decompression.
//!
//! `rapidgzip-core` decodes single-member gzip, concatenated gzip, BGZF, zlib
//! streams, and raw DEFLATE.
//! It follows rapidgzip's marker/window algorithm for parallel decoding of
//! ordinary DEFLATE streams and uses zlib-rs as its inflate backend. It also
//! builds random-access indexes while decoding, reads and writes them in four
//! formats, and seeks decompressed output through them. Encoding is outside
//! this crate's scope.
//!
//! # Output interfaces
//!
//! - [`Decoder::decode`] is the lower-overhead push interface. It writes on the
//!   calling thread, so the supplied [`std::io::Write`] need not be [`Send`].
//! - [`Decoder::reader`] and [`Decoder::open`] return an owned [`DecoderReader`]
//!   implementing [`std::io::Read`] + [`Send`]. This is suitable for parsers
//!   that take `Box<dyn Read + Send>`, including `paraseq`.
//! - [`Decoder::decode_stream`] and [`Decoder::stream_reader`] are the same two
//!   interfaces for non-seekable input; see below.
//!
//! # Example
//!
//! ```no_run
//! use rapidgzip_core::Decoder;
//! use std::io;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let decoder = Decoder::builder().decoder_threads(8).build()?;
//! let mut reader = decoder.open("reads.fastq.gz")?;
//! let control = reader.handle();
//! control.set_worker_limit(4)?;
//! io::copy(&mut reader, &mut io::sink())?;
//! let report = reader.finish()?;
//! assert!(report.member_count >= 1);
//! # Ok(())
//! # }
//! ```
//!
//! # Containers
//!
//! [`DecoderBuilder::format`] selects the framing. The default,
//! [`Format::Auto`], reads the first two bytes: gzip magic selects gzip, and
//! otherwise a valid zlib `CMF`/`FLG` pair selects zlib. Raw DEFLATE has no
//! header to recognize, so [`Format::RawDeflate`] must be requested; letting
//! detection guess it would turn every corrupt input into a raw stream.
//!
//! Verification follows the container. gzip checks a CRC32 and ISIZE per
//! member, zlib checks its Adler-32 trailer, and both refuse trailing bytes.
//! Raw DEFLATE carries no checksum at all, so
//! [`DecoderBuilder::expected_uncompressed_size`] is the only end-to-end check
//! available for it and is verified when supplied.
//!
//! zlib and raw DEFLATE are each one DEFLATE stream, which is what the
//! parallel path already splits, so both decode in parallel and support the
//! random-access index below.
//!
//! # Random access
//!
//! [`DecoderBuilder::build_index`] collects a [`GzipIndex`] during an ordinary
//! decode. The index pairs compressed bit offsets with decompressed byte
//! offsets and the 32 KiB of history needed to resume there, so a later
//! [`IndexedReader`] can seek without decoding everything before the target.
//!
//! ```no_run
//! use rapidgzip_core::{Decoder, IndexedReader};
//! use std::fs::File;
//! use std::io::{self, Read, Seek, SeekFrom};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let decoder = Decoder::builder().build_index(true).build()?;
//! let mut reader = decoder.open("reads.fastq.gz")?;
//! io::copy(&mut reader, &mut io::sink())?;
//! let index = reader.finish()?.index.expect("index requested");
//!
//! index.write_native(&mut File::create("reads.fastq.gz.idx")?)?;
//!
//! let mut random = IndexedReader::new(File::open("reads.fastq.gz")?, index)?;
//! random.seek(SeekFrom::Start(4_000_000_000))?;
//! let mut buffer = [0u8; 4096];
//! random.read_exact(&mut buffer)?;
//! # Ok(())
//! # }
//! ```
//!
//! Indexes persist in the crate's own versioned format
//! ([`GzipIndex::write_native`]), in indexed_gzip `GZIDX`
//! ([`GzipIndex::write_gzidx`]), in htslib BGZF `.gzi`
//! ([`GzipIndex::write_gzi`]), and in gztool's format
//! ([`GzipIndex::write_gztool`]). All four import as well, so indexes written
//! by those tools work here and the reverse holds.
//!
//! Which checkpoints an index holds depends on the path that decoded the
//! input. The parallel path records interior points at its chunk boundaries,
//! spaced by [`DecoderBuilder::index_spacing`]. BGZF records every block. The
//! sequential and streaming paths record member starts only, because the zlib
//! backend does not expose DEFLATE block boundaries; an index built from
//! standard input is therefore coarse but valid.
//!
//! # Inflate backends
//!
//! Raw inflate runs on zlib-rs. The optional `isal` feature replaces it with
//! Intel's ISA-L on the paths that decode a whole stream from its start: the
//! sequential gzip loop, the single-stream zlib and raw DEFLATE loop, and BGZF
//! blocks.
//!
//! The parallel marker/window path and [`IndexedReader`] stay on zlib-rs
//! whether or not the feature is on. Both resume at arbitrary bit offsets,
//! which needs `inflatePrime`, and the parallel path finds DEFLATE block
//! boundaries through zlib's `Z_BLOCK` contract. ISA-L exposes neither.
//!
//! The feature is off by default and links a system `libisal`: `libisal-dev`
//! on Debian and Ubuntu, `isa-l` on Homebrew, or a prefix named by
//! `ISAL_INSTALL_PREFIX`. Whether it is worth enabling is a measurement, not a
//! given; see `README.md`.
//!
//! # Verification and errors
//!
//! Every member is accepted only after an actual final DEFLATE block and a
//! matching CRC32 and ISIZE footer. Reaching reader EOF or receiving a
//! successful [`DecodeReport`] means the complete compressed input was
//! verified. Dropping a [`DecoderReader`] before EOF cancels the unread work;
//! call [`DecoderReader::finish`] when decoded bytes are no longer needed but
//! complete verification is.
//!
//! Decoding can emit a verified prefix before discovering later corruption or
//! an I/O failure. Previously written or read bytes are not rolled back.
//!
//! # Input and concurrency
//!
//! Compressed input implements [`ReadAt`], allowing bounded worker tasks to use
//! positional reads without a shared cursor. Implementations are supplied for
//! files on Unix and Windows, in-memory byte storage, [`std::sync::Arc`], and
//! [`Box`]. The source length and contents must remain stable during decoding.
//!
//! # Non-seekable input
//!
//! [`Decoder::decode_stream`] and [`Decoder::stream_reader`] accept any
//! [`std::io::Read`], so standard input, a FIFO, a process substitution, or a
//! socket can be decoded. [`Decoder::open`] detects a path that cannot be read
//! positionally and routes it there itself.
//!
//! Verification is identical: such a source runs the same sequential zlib-rs
//! path that the parallel paths use as their authoritative fallback, sharing its
//! member framing, footer checks, trailing-garbage detection, and output limit.
//! It is not decoded in parallel, because every parallel path needs positional
//! reads, and the telemetry reports one worker rather than the configured
//! budget. Nothing is spooled: input memory is one
//! [`DecoderBuilder::input_page_size`] window. Dropping a streaming
//! [`DecoderReader`] before EOF cancels without waiting for its background
//! thread, so a producer that stalls without closing cannot block the drop.
//!
//! [`DecoderBuilder::decoder_threads`] sets a maximum worker budget rather than
//! eagerly creating that many threads. Parallel paths grow an elastic worker
//! population from an affinity- and budget-aware bootstrap. A cloned
//! [`DecoderHandle`] provides lock-free telemetry and can change the runtime
//! ceiling after a [`DecoderReader`] moves into another component. Excess
//! workers finish their current task and retire; sustained reader backpressure
//! also reduces admission automatically.
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(missing_docs)]

mod backend;
mod config;
mod crc32;
mod error;
mod format;
mod gzip;
mod indexed;
mod inflate;
mod inflate_backend;
#[cfg(feature = "isal")]
mod isal_backend;
mod read_at;
mod reader;
mod runtime;
mod single_stream;
mod zlib;

pub mod index;
pub mod parallel;

pub use config::{ConfigError, Decoder, DecoderBuilder};
pub use error::{DecodeError, DecodeReport, DeflateErrorKind, GzipErrorKind, ZlibErrorKind};
pub use format::Format;
pub use index::{Checkpoint, GzipIndex, IndexError, StoredWindow, WindowMap};
pub use indexed::IndexedReader;
pub use read_at::ReadAt;
pub use reader::DecoderReader;
pub use runtime::{DecoderHandle, DecoderPath, DecoderPressure, DecoderStats, WorkerLimitError};
