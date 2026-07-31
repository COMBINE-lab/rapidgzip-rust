//! Parallel, multi-member gzip decompression.
//!
//! The crate has two output interfaces:
//!
//! - [`Decoder::decode`] writes into an arbitrary [`std::io::Write`].
//! - [`Decoder::reader`] returns a movable [`std::io::Read`] implementation.
//!
//! Both interfaces verify every gzip member independently.  The pull reader is
//! particularly useful with parsers that accept `Box<dyn Read + Send>`, such as
//! `paraseq`.
#![deny(unsafe_op_in_unsafe_fn)]

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
