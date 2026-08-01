use crate::format::Format;
use crate::index::GzipIndex;
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

/// The reason a zlib container was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ZlibErrorKind {
    /// The header bytes are not a legal zlib header.
    BadHeader,
    /// The stream did not use the DEFLATE compression method.
    UnsupportedCompressionMethod(u8),
    /// The declared window exponent exceeds DEFLATE's 32 KiB history.
    UnsupportedWindowSize(u8),
    /// The stream requires a preset dictionary, which this crate cannot supply.
    PresetDictionary,
    /// The header or the Adler-32 trailer was truncated.
    Truncated,
    /// The Adler-32 trailer did not match the decompressed output.
    ChecksumMismatch {
        /// Checksum stored in the trailer.
        expected: u32,
        /// Checksum computed over the output.
        actual: u32,
    },
    /// Bytes followed a complete zlib stream.
    TrailingGarbage,
}

impl Display for ZlibErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadHeader => formatter.write_str("invalid zlib header bytes"),
            Self::UnsupportedCompressionMethod(method) => {
                write!(formatter, "unsupported zlib compression method {method}")
            }
            Self::UnsupportedWindowSize(exponent) => write!(
                formatter,
                "unsupported zlib window exponent {exponent}, expected at most 7"
            ),
            Self::PresetDictionary => {
                formatter.write_str("zlib stream requires a preset dictionary")
            }
            Self::Truncated => formatter.write_str("truncated zlib header or trailer"),
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "zlib Adler-32 mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
            Self::TrailingGarbage => formatter.write_str("trailing data after a zlib stream"),
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
    /// Bytes followed a complete DEFLATE stream.
    TrailingGarbage,
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
            Self::TrailingGarbage => {
                formatter.write_str("trailing data after a complete DEFLATE stream")
            }
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
    /// The zlib framing was invalid.
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
    /// A member's CRC32 did not match its footer.
    ChecksumMismatch {
        /// Zero-based member number.
        member: u64,
        /// Footer value.
        expected: u32,
        /// Computed value.
        actual: u32,
    },
    /// A member's modulo-2^32 output size did not match its footer.
    SizeMismatch {
        /// Zero-based member number.
        member: u64,
        /// Footer value.
        expected: u32,
        /// Computed value.
        actual_mod32: u32,
    },
    /// The decoded size disagreed with the size the caller expected.
    UnexpectedOutputSize {
        /// Size supplied through `DecoderBuilder::expected_uncompressed_size`.
        expected: u64,
        /// Size actually decoded.
        actual: u64,
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
            | Self::InvalidDeflate {
                reason: DeflateErrorKind::Truncated,
                ..
            }
            | Self::InvalidZlib {
                reason: ZlibErrorKind::Truncated,
                ..
            } => io::ErrorKind::UnexpectedEof,
            Self::InvalidGzip { .. }
            | Self::InvalidZlib { .. }
            | Self::InvalidDeflate { .. }
            | Self::ChecksumMismatch { .. }
            | Self::SizeMismatch { .. }
            | Self::UnexpectedOutputSize { .. } => io::ErrorKind::InvalidData,
            Self::OutputLimitExceeded { .. } => io::ErrorKind::FileTooLarge,
            Self::WorkerPanicked => io::ErrorKind::Other,
            Self::Cancelled => io::ErrorKind::Interrupted,
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
                "gzip member {member} CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
            Self::SizeMismatch {
                member,
                expected,
                actual_mod32,
            } => write!(
                formatter,
                "gzip member {member} ISIZE mismatch: expected {expected}, got {actual_mod32}"
            ),
            Self::UnexpectedOutputSize { expected, actual } => write!(
                formatter,
                "decoded {actual} bytes, but {expected} were expected"
            ),
            Self::OutputLimitExceeded { limit } => {
                write!(formatter, "decoded output exceeded the {limit}-byte limit")
            }
            Self::WorkerPanicked => formatter.write_str("a decoder worker panicked"),
            Self::Cancelled => formatter.write_str("decoding was cancelled"),
        }
    }
}

impl Error for DecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

/// Statistics produced after the complete stream has been verified.
///
/// This type is [`Clone`] rather than [`Copy`] because it can carry a
/// collected [`GzipIndex`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DecodeReport {
    /// Compressed bytes consumed.
    pub compressed_bytes: u64,
    /// Decompressed bytes emitted.
    pub decompressed_bytes: u64,
    /// Number of verified gzip members.
    pub member_count: u64,
    /// Configured decoder-worker budget.
    pub decoder_threads: usize,
    /// Container that was decoded, always a concrete variant.
    pub format: Format,
    /// Random-access index, present when
    /// [`DecoderBuilder::build_index`](crate::DecoderBuilder::build_index) was
    /// enabled.
    pub index: Option<GzipIndex>,
}
