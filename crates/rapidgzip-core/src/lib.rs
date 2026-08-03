//! Parallel decoding of gzip, zlib, and raw DEFLATE.
//!
//! `rapidgzip-core` decodes single-member and concatenated gzip, BGZF, zlib,
//! and raw DEFLATE. It follows rapidgzip's marker/window algorithm for parallel
//! decoding and uses zlib-rs as its inflate backend. Encoding is outside this
//! crate's current scope. Index construction, persistence, and decoded-output
//! seeking are available through explicit opt-in APIs.
//!
//! # Formats
//!
//! Strict [`Format::Gzip`] is the default. [`DecoderBuilder::format`] selects
//! zlib or raw DEFLATE explicitly; [`DecoderBuilder::auto_detect_format`]
//! recognizes gzip or zlib without consuming their prefix. Raw DEFLATE has no
//! identifying header and is never guessed.
//!
//! Gzip checks every member's CRC32 and ISIZE. Zlib validates CMF/FLG, enforces
//! its declared history window, and checks Adler-32. Raw DEFLATE has no
//! checksum, so success establishes structural validity and exact source
//! consumption. [`DecoderBuilder::expected_uncompressed_size`] can require an
//! exact decoded size for any format.
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
//! Reaching reader EOF or receiving a successful [`DecodeReport`] means the
//! complete compressed input passed every check carried by its selected
//! container. Dropping a [`DecoderReader`] before EOF cancels the unread work;
//! call [`DecoderReader::finish`] when decoded bytes are no longer needed but
//! complete validation is.
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
//! Validation is identical: such a source runs the same sequential zlib-rs
//! path that the parallel paths use as their authoritative fallback, sharing
//! framing, trailer checks, trailing-data detection, and output bounds. It is
//! not decoded in parallel, because every parallel path needs positional reads.
//! Telemetry retains the builder's configured worker budget while
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
//!
//! # Random access
//!
//! [`Decoder::decode_with_index`] and [`Decoder::reader_with_index`] collect a
//! [`DeflateIndex`] only when requested, leaving [`DecodeReport`] small and
//! [`Copy`]. The streaming counterparts collect a coarser member-boundary index
//! while reading a forward-only source. The native format represents every
//! supported container; GZIDX, htslib BGZF `.gzi`, and gztool are gzip-family
//! formats and reject incompatible export.
//!
//! [`Decoder::decode_from_index`] and [`Decoder::reader_from_index`] reuse an
//! existing index for strict parallel full-stream decoding. Every worker must
//! reach the next checkpoint's exact compressed bit and decompressed byte
//! offsets; invalid or source-mismatched indexes never trigger an ordinary
//! fallback. The reader remains [`std::io::Read`] + [`Send`] and exposes the
//! usual runtime worker controls. Concatenated and empty gzip members and BGZF
//! `.gzi` indexes are supported without weakening whole-stream verification.
//!
//! [`IndexedReader`] implements [`std::io::Read`] + [`std::io::Seek`] over a
//! stable [`ReadAt`] source. Framing-start checkpoints permit complete gzip or
//! zlib verification; an interior checkpoint cannot authenticate bytes skipped
//! earlier because indexes do not store prefix checksum state. Raw DEFLATE has
//! no checksum to authenticate.
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(missing_docs)]

mod backend;
mod config;
mod crc32;
mod error;
mod format;
mod gzip;
mod indexed;
mod indexed_parallel;
mod inflate;
mod read_at;
mod reader;
mod runtime;
mod zlib;

pub mod index;
pub mod parallel;

pub use config::{ConfigError, Decoder, DecoderBuilder};
pub use error::{
    DecodeError, DecodeReport, DeflateErrorKind, GzipErrorKind, IndexDecodeError,
    IndexedDecodeReport, IndexingError, ZlibErrorKind,
};
pub use format::Format;
pub use index::{
    Checkpoint, CheckpointKind, DeflateIndex, IndexError, IndexKind, IndexOptions,
    IndexReadOptions, StoredWindow, WindowMap, WindowStorage,
};
pub use indexed::{IndexedReader, IndexedReaderError};
pub use read_at::ReadAt;
pub use reader::{DecoderReader, IndexingDecoderReader};
pub use runtime::{DecoderHandle, DecoderPath, DecoderPressure, DecoderStats, WorkerLimitError};
