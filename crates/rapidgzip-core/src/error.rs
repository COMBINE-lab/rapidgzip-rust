use crate::index::{GzipIndex, IndexError};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::sync::Arc;

/// The reason a gzip container was rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GzipErrorKind {
    /// The gzip identification bytes were absent.
    BadMagic,
    /// The member did not use the DEFLATE compression method.
    UnsupportedCompressionMethod(u8),
    /// One or more reserved flag bits were set.
    ReservedFlags(u8),
    /// The optional header checksum was incorrect.
    HeaderChecksumMismatch {
        /// Checksum stored in the header.
        expected: u16,
        /// Checksum computed over preceding header bytes.
        actual: u16,
    },
    /// A zero-terminated header field reached the end of input.
    UnterminatedHeaderField,
    /// The member header or footer was truncated.
    Truncated,
    /// Bytes after a valid member were not another gzip member.
    TrailingGarbage,
}

/// The reason a zlib (RFC 1950) container was rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ZlibErrorKind {
    /// The CMF/FLG bytes were not a valid zlib header (or were gzip magic).
    BadHeader,
    /// The stream did not use the DEFLATE compression method (CM ≠ 8).
    UnsupportedCompressionMethod(u8),
    /// CINFO requested a window larger than 32 KiB (CINFO > 7).
    UnsupportedWindow(u8),
    /// The CMF/FLG FCHECK residue was not zero modulo 31.
    BadHeaderChecksum,
    /// FDICT was set; preset dictionaries are not supported.
    DictionaryNotSupported,
    /// The header or Adler-32 trailer was truncated.
    Truncated,
    /// Bytes after a valid zlib stream were not another zlib stream.
    TrailingGarbage,
}

impl Display for GzipErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic => formatter.write_str("missing gzip magic bytes"),
            Self::UnsupportedCompressionMethod(method) => {
                write!(formatter, "unsupported gzip compression method {method}")
            }
            Self::ReservedFlags(flags) => {
                write!(formatter, "reserved gzip flag bits are set: {flags:#04x}")
            }
            Self::HeaderChecksumMismatch { expected, actual } => write!(
                formatter,
                "gzip header checksum mismatch: expected {expected:#06x}, got {actual:#06x}"
            ),
            Self::UnterminatedHeaderField => formatter.write_str("unterminated gzip header field"),
            Self::Truncated => formatter.write_str("truncated gzip header or footer"),
            Self::TrailingGarbage => formatter.write_str("trailing non-gzip data"),
        }
    }
}

impl Display for ZlibErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadHeader => formatter.write_str("invalid zlib CMF/FLG header"),
            Self::UnsupportedCompressionMethod(method) => {
                write!(formatter, "unsupported zlib compression method {method}")
            }
            Self::UnsupportedWindow(cinfo) => {
                write!(
                    formatter,
                    "unsupported zlib window size CINFO={cinfo} (max 7)"
                )
            }
            Self::BadHeaderChecksum => {
                formatter.write_str("zlib header FCHECK failed (CMF/FLG not multiple of 31)")
            }
            Self::DictionaryNotSupported => {
                formatter.write_str("zlib preset dictionary (FDICT) is not supported")
            }
            Self::Truncated => formatter.write_str("truncated zlib header or Adler-32 trailer"),
            Self::TrailingGarbage => formatter.write_str("trailing non-zlib data"),
        }
    }
}

/// The reason a DEFLATE stream was rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeflateErrorKind {
    /// The backend rejected the compressed stream.
    InvalidData,
    /// The stream unexpectedly requested a preset dictionary.
    UnexpectedDictionary,
    /// The backend returned an unexpected status code.
    BackendStatus(i32),
    /// No progress was possible before the end of the compressed input.
    Truncated,
    /// The decoder made no progress despite having input and output space.
    Stalled,
}

impl Display for DeflateErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidData => formatter.write_str("invalid DEFLATE data"),
            Self::UnexpectedDictionary => {
                formatter.write_str("gzip DEFLATE stream requested a preset dictionary")
            }
            Self::BackendStatus(status) => {
                write!(formatter, "unexpected DEFLATE backend status {status}")
            }
            Self::Truncated => formatter.write_str("truncated DEFLATE stream"),
            Self::Stalled => formatter.write_str("DEFLATE decoder made no progress"),
        }
    }
}

/// A terminal decoding error.
///
/// This type is cloneable so a [`std::io::Read`] adapter can return the same
/// logical failure on every read after the pipeline has failed.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DecodeError {
    /// A positional input read or output write failed.
    Io {
        /// Compressed offset, when the operation was tied to an input offset.
        offset: Option<u64>,
        /// Shared original I/O error.
        source: Arc<io::Error>,
    },
    /// The gzip framing was invalid.
    InvalidGzip {
        /// Compressed byte offset.
        offset: u64,
        /// Detailed reason.
        reason: GzipErrorKind,
    },
    /// The zlib (RFC 1950) framing was invalid.
    InvalidZlib {
        /// Compressed byte offset.
        offset: u64,
        /// Detailed reason.
        reason: ZlibErrorKind,
    },
    /// The raw DEFLATE payload was invalid.
    InvalidDeflate {
        /// Best-known compressed bit offset.
        bit_offset: u64,
        /// Detailed reason.
        reason: DeflateErrorKind,
    },
    /// A member's integrity checksum did not match its trailer (or external
    /// expected value).
    ///
    /// For gzip this is the CRC32 footer; for zlib it is the Adler-32 trailer
    /// (both controlled by [`crate::DecoderBuilder::crc32_enabled`]). For raw
    /// DEFLATE this is an optional external whole-stream CRC32 from
    /// [`crate::DecoderBuilder::raw_crc32_list`] (`member` is always 0).
    ChecksumMismatch {
        /// Zero-based member number (always 0 for raw DEFLATE external CRC).
        member: u64,
        /// Footer or externally supplied expected value.
        expected: u32,
        /// Computed value.
        actual: u32,
    },
    /// A member's modulo-2^32 output size did not match its footer.
    ///
    /// Gzip only (ISIZE). zlib streams have no size trailer.
    SizeMismatch {
        /// Zero-based member number.
        member: u64,
        /// Footer value.
        expected: u32,
        /// Computed value.
        actual_mod32: u32,
    },
    /// Decoded output would exceed the configured limit.
    OutputLimitExceeded {
        /// Configured maximum decoded byte count.
        limit: u64,
    },
    /// A decoder worker panicked.
    WorkerPanicked,
    /// Decoding was cancelled because the consumer stopped.
    Cancelled,
    /// A provided random-access index is invalid or does not match the archive.
    InvalidIndex(IndexError),
}

impl DecodeError {
    pub(crate) fn input_io(offset: u64, source: io::Error) -> Self {
        Self::Io {
            offset: Some(offset),
            source: Arc::new(source),
        }
    }

    pub(crate) fn output_io(source: io::Error) -> Self {
        Self::Io {
            offset: None,
            source: Arc::new(source),
        }
    }

    pub(crate) fn io_kind(&self) -> io::ErrorKind {
        match self {
            Self::Io { source, .. } => source.kind(),
            Self::InvalidGzip {
                reason: GzipErrorKind::Truncated,
                ..
            }
            | Self::InvalidZlib {
                reason: ZlibErrorKind::Truncated,
                ..
            }
            | Self::InvalidDeflate {
                reason: DeflateErrorKind::Truncated,
                ..
            } => io::ErrorKind::UnexpectedEof,
            Self::InvalidGzip { .. }
            | Self::InvalidZlib { .. }
            | Self::InvalidDeflate { .. }
            | Self::ChecksumMismatch { .. }
            | Self::SizeMismatch { .. } => io::ErrorKind::InvalidData,
            Self::OutputLimitExceeded { .. } => io::ErrorKind::FileTooLarge,
            Self::WorkerPanicked => io::ErrorKind::Other,
            Self::Cancelled => io::ErrorKind::Interrupted,
            Self::InvalidIndex(_) => io::ErrorKind::InvalidInput,
        }
    }

    pub(crate) fn to_io_error(&self) -> io::Error {
        io::Error::new(self.io_kind(), self.clone())
    }
}

impl Display for DecodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                offset: Some(offset),
                source,
            } => write!(
                formatter,
                "I/O error at compressed offset {offset}: {source}"
            ),
            Self::Io {
                offset: None,
                source,
            } => write!(formatter, "output I/O error: {source}"),
            Self::InvalidGzip { offset, reason } => {
                write!(formatter, "invalid gzip data at byte {offset}: {reason}")
            }
            Self::InvalidZlib { offset, reason } => {
                write!(formatter, "invalid zlib data at byte {offset}: {reason}")
            }
            Self::InvalidDeflate { bit_offset, reason } => {
                write!(
                    formatter,
                    "invalid DEFLATE data at bit {bit_offset}: {reason}"
                )
            }
            Self::ChecksumMismatch {
                member,
                expected,
                actual,
            } => write!(
                formatter,
                "member {member} checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
            Self::SizeMismatch {
                member,
                expected,
                actual_mod32,
            } => write!(
                formatter,
                "gzip member {member} ISIZE mismatch: expected {expected}, got {actual_mod32}"
            ),
            Self::OutputLimitExceeded { limit } => {
                write!(formatter, "decoded output exceeded the {limit}-byte limit")
            }
            Self::WorkerPanicked => formatter.write_str("a decoder worker panicked"),
            Self::Cancelled => formatter.write_str("decoding was cancelled"),
            Self::InvalidIndex(error) => write!(formatter, "invalid gzip index: {error}"),
        }
    }
}

impl Error for DecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source.as_ref()),
            Self::InvalidIndex(error) => Some(error),
            _ => None,
        }
    }
}

/// Statistics produced after the complete stream has been verified.
///
/// This type is intentionally not [`Copy`] so it can carry an optional
/// [`GzipIndex`] when [`crate::DecoderBuilder::keep_index`] was enabled.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DecodeReport {
    /// Compressed bytes consumed.
    pub compressed_bytes: u64,
    /// Decompressed bytes emitted.
    pub decompressed_bytes: u64,
    /// Number of verified members/streams (gzip members, zlib streams, or `1`
    /// for a successful raw DEFLATE stream).
    pub member_count: u64,
    /// Configured decoder-worker budget.
    pub decoder_threads: usize,
    /// Built random-access index when [`crate::DecoderBuilder::keep_index`] was enabled.
    pub index: Option<GzipIndex>,
    /// Total Unix newline (`\n`) count in the decompressed output when
    /// [`crate::DecoderBuilder::gather_line_offsets`] was enabled; `None`
    /// otherwise. Empty files yield `Some(0)`.
    pub line_count: Option<u64>,
}
