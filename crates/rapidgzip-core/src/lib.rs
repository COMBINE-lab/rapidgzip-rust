//! Parallel, verified gzip decompression.
//!
//! `rapidgzip-core` decodes single-member gzip, concatenated gzip, and BGZF.
//! It follows rapidgzip's marker/window algorithm for parallel decoding of
//! ordinary DEFLATE streams and uses zlib-rs as its inflate backend. Encoding,
//! index persistence, and decoded-output seeking are outside this crate's
//! current scope.
//!
//! # Output interfaces
//!
//! - [`Decoder::decode`] is the lower-overhead push interface, and
//!   [`Decoder::decode_path`] adds automatic regular/non-regular path routing.
//!   Both write on the calling thread, so [`std::io::Write`] need not be [`Send`].
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
//! socket can be decoded. [`Decoder::open`] routes non-regular paths accepted by
//! [`std::fs::File::open`], such as FIFOs and character devices, to the same
//! sequential engine.
//!
//! Verification is identical: such a source runs the same sequential zlib-rs
//! path that the parallel paths use as their authoritative fallback, sharing its
//! member framing, footer checks, trailing-garbage detection, and output limit.
//! It is not decoded in parallel, because every parallel path needs positional
//! reads. Telemetry retains the builder's configured worker budget while
//! reporting an effective target of one and zero spawned decoder/auxiliary
//! threads. Nothing is spooled: input memory is one
//! [`DecoderBuilder::input_page_size`] window. [`DecoderReader`] advances the
//! streaming inflater synchronously from [`std::io::Read::read`], so dropping it
//! immediately drops the source and cannot strand a thread blocked on input.
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
mod gzip;
mod indexed;
mod inflate;
mod read_at;
mod reader;
mod runtime;

pub mod index;
pub mod parallel;

pub use config::{ConfigError, Decoder, DecoderBuilder};
pub use error::{DecodeError, DecodeReport, DeflateErrorKind, GzipErrorKind};
pub use index::{Checkpoint, GzipIndex, IndexError, StoredWindow, WindowMap};
pub use indexed::IndexedReader;
pub use read_at::ReadAt;
pub use reader::DecoderReader;
pub use runtime::{DecoderHandle, DecoderPath, DecoderPressure, DecoderStats, WorkerLimitError};
