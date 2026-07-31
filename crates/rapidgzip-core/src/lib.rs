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
//! - [`Decoder::decode`] is the lower-overhead push interface. It writes on the
//!   calling thread, so the supplied [`std::io::Write`] need not be [`Send`].
//! - [`Decoder::reader`] and [`Decoder::open`] return an owned [`DecoderReader`]
//!   implementing [`std::io::Read`] + [`Send`]. This is suitable for parsers
//!   that take `Box<dyn Read + Send>`, including `paraseq`.
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
//! [`DecoderBuilder::decoder_threads`] sets a maximum worker budget. A format
//! path or the empirical controller may activate fewer workers when the input
//! exposes less parallelism or additional concurrency is counterproductive.
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(missing_docs)]

mod backend;
mod config;
mod crc32;
mod error;
mod gzip;
mod read_at;
mod reader;

pub mod parallel;

pub use config::{ConfigError, Decoder, DecoderBuilder};
pub use error::{DecodeError, DecodeReport, DeflateErrorKind, GzipErrorKind};
pub use read_at::ReadAt;
pub use reader::DecoderReader;
