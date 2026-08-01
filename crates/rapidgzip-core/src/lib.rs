//! Parallel, verified gzip, zlib, and raw DEFLATE decompression (small or
//! low-thread single-stream paths stay sequential).
//!
//! `rapidgzip-core` decodes single-member gzip, concatenated gzip, BGZF,
//! zlib-wrapped DEFLATE (RFC 1950; stream-granularity parallel for concatenated
//! multi-stream inputs, estimated marker path for long single streams when
//! `decoder_threads >= 4`), and raw DEFLATE (RFC 1951, explicit
//! [`Format::RawDeflate`]; same estimated marker path when threads and size
//! allow). It follows rapidgzip's marker/window algorithm for parallel decoding
//! of ordinary DEFLATE streams and uses zlib-rs as its sequential inflate
//! backend (via a crate-private `InflateBackend` trait so a future optional
//! ISA-L-style backend can plug in without public API churn). Encoding is
//! outside this crate's scope.
//!
//! Random-access **index** support: the in-memory [`GzipIndex`] model and
//! indexed_gzip (`GZIDX`) / [gztool](https://github.com/circulosmeos/gztool) /
//! htslib BGZF `.gzi` (BGZI) import/export live in this crate
//! ([`read_gzip_index`] auto-detects the format). Enable
//! [`DecoderBuilder::keep_index`] to collect an index while decoding; the
//! result is returned on [`DecodeReport::index`]. Combine with
//! [`DecoderBuilder::gather_line_offsets`] to count Unix newlines (`\n`) and
//! stamp per-checkpoint line offsets for [`IndexedReader::seek_to_line`]
//! (1-based line numbers, gztool/rapidgzip style). Seek into decoded output
//! with [`Decoder::reader_with_index`] / [`Decoder::open_with_index`] and
//! [`IndexedReader`] ([`std::io::Read`] + [`std::io::Seek`]). Full-stream
//! decompress with an imported index can use the parallel
//! [`Decoder::decode_with_index`] path (checkpoint segments + zlib-rs, no
//! marker speculation).
//!
//! # Output interfaces
//!
//! - [`Decoder::decode`] is the lower-overhead push interface. It writes on the
//!   calling thread, so the supplied [`std::io::Write`] need not be [`Send`].
//! - [`Decoder::decode_read`] accepts a non-positional [`std::io::Read`]
//!   (stdin, sockets, pipes). Single-thread paths stream page-at-a-time without
//!   buffering the full archive; multi-thread paths spill to a private temp file
//!   then run the positional backend (parallel gzip / multi-stream or marker
//!   zlib / raw DEFLATE when eligible). Prefer file/`ReadAt` when you already
//!   have positional input.
//! - [`Decoder::reader`] and [`Decoder::open`] return an owned [`DecoderReader`]
//!   implementing [`std::io::Read`] + [`Send`]. This is suitable for parsers
//!   that take `Box<dyn Read + Send>`, including `paraseq`.
//! - [`Decoder::decode_with_index`] fully decompresses using a prebuilt or
//!   imported [`GzipIndex`], optionally in parallel across checkpoint spans.
//!   Member CRC is not verified on this path (same policy as seek reads).
//! - [`Decoder::reader_with_index`] and [`Decoder::open_with_index`] return an
//!   [`IndexedReader`] for random access using a prebuilt or imported
//!   [`GzipIndex`]. Seeking restarts inflate from the nearest preceding
//!   checkpoint when the target is not already covered by the decoded-window
//!   LRU or the active lookahead; sequential read-ahead warms the next window
//!   into the cache, and optional background workers prefetch further windows
//!   via independent checkpoint resumes ([`DecoderBuilder::seek_prefetch_windows`]).
//!   [`IndexedReader::seek_to_line`] supports 1-based line seeks when the index
//!   has line offsets. Member CRC is not verified on seek reads.
//! - [`Decoder::analyze`] walks members and DEFLATE blocks sequentially (no
//!   index required) and returns a structured [`ArchiveAnalysis`] for tooling
//!   such as the CLI `--analyze` flag.
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
//! Every gzip member is accepted only after an actual final DEFLATE block and a
//! matching ISIZE footer; CRC32 is verified by default. zlib streams use the
//! Adler-32 trailer instead of CRC32/ISIZE. Both integrity checks can be
//! disabled via [`DecoderBuilder::crc32_enabled`]. Raw DEFLATE has no on-stream
//! trailer; optional whole-stream CRC32 verification is available via
//! [`DecoderBuilder::raw_crc32_list`]. Format selection defaults to auto-detect
//! ([`Format::Auto`], gzip vs zlib only); use [`DecoderBuilder::format`] to
//! require gzip, zlib, or raw DEFLATE. Reaching reader EOF or receiving a
//! successful [`DecodeReport`] means the complete compressed input was verified
//! under the configured checks. Dropping a [`DecoderReader`] before EOF cancels
//! the unread work; call [`DecoderReader::finish`] when decoded bytes are no
//! longer needed but complete verification is.
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
//! Non-seekable streams are supported via [`Decoder::decode_read`]: single-thread
//! paths stream without a full-archive buffer; multi-thread paths spill to a
//! private temporary file (disk) rather than holding the archive only in RAM.
//! Prefer a file or other [`ReadAt`] source when possible to avoid the spill.
//!
//! [`DecoderBuilder::decoder_threads`] sets a maximum worker budget. A format
//! path or the empirical controller may activate fewer workers when the input
//! exposes less parallelism or additional concurrency is counterproductive.
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(missing_docs)]

mod analyze;
mod backend;
mod buffer_pool;
mod config;
mod crc32;
mod error;
mod gzip;
mod index;
mod indexed_decode;
mod inflate_backend;
mod read_at;
mod reader;
mod seek;
mod stream_decode;
mod zlib;

pub mod parallel;

pub use analyze::{
    ArchiveAnalysis, ArchiveKind, DeflateBlockInfo, DeflateBlockType, MemberAnalysis,
    analyze_source, analyze_source_with_format,
};
pub use config::{ConfigError, Decoder, DecoderBuilder, Format};
pub use error::{DecodeError, DecodeReport, DeflateErrorKind, GzipErrorKind, ZlibErrorKind};
pub use index::{
    Checkpoint, GZTOOL_MAGIC_V0, GZTOOL_MAGIC_V1, GzipIndex, INDEXED_GZIP_MAGIC,
    INDEXED_GZIP_WINDOW_SIZE, IndexError, StoredWindow, WindowCompression, WindowMap,
    decode_bit_offset, encode_bit_offset, read_bgzi_index, read_gzip_index, read_gztool_index,
    read_indexed_gzip_index, write_bgzi_index, write_gztool_index, write_indexed_gzip_index,
};
pub use read_at::ReadAt;
pub use reader::DecoderReader;
pub use seek::{IndexedReader, SeekCacheStats};
