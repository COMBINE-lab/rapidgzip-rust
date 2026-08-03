use crate::{DeflateIndex, Format, IndexError, IndexKind};
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

/// The reason an RFC 1950 zlib container was rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ZlibErrorKind {
    /// The header selected a compression method other than DEFLATE.
    UnsupportedCompressionMethod(u8),
    /// CINFO requested a window larger than DEFLATE's 32 KiB maximum.
    UnsupportedWindowSize(u8),
    /// The CMF/FLG pair did not satisfy the FCHECK residue.
    BadHeaderCheck,
    /// Preset dictionaries are not supported by this decoder.
    PresetDictionary,
    /// The zlib header or Adler-32 trailer was truncated.
    Truncated,
    /// The Adler-32 stored in the trailer did not match the output.
    ChecksumMismatch {
        /// Trailer value.
        expected: u32,
        /// Computed value.
        actual: u32,
    },
    /// Bytes remained after the one complete zlib stream.
    TrailingGarbage,
}

impl Display for ZlibErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedCompressionMethod(method) => {
                write!(formatter, "unsupported zlib compression method {method}")
            }
            Self::UnsupportedWindowSize(cinfo) => {
                write!(formatter, "unsupported zlib CINFO window value {cinfo}")
            }
            Self::BadHeaderCheck => formatter.write_str("invalid zlib FCHECK header residue"),
            Self::PresetDictionary => {
                formatter.write_str("zlib preset dictionaries are not supported")
            }
            Self::Truncated => formatter.write_str("truncated zlib header or trailer"),
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "zlib Adler-32 mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
            Self::TrailingGarbage => formatter.write_str("trailing data after zlib stream"),
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
    /// Bytes remained after the final block of a raw DEFLATE stream.
    TrailingGarbage,
}

impl Display for DeflateErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidData => formatter.write_str("invalid DEFLATE data"),
            Self::UnexpectedDictionary => {
                formatter.write_str("DEFLATE stream requested a preset dictionary")
            }
            Self::BackendStatus(status) => {
                write!(formatter, "unexpected DEFLATE backend status {status}")
            }
            Self::Truncated => formatter.write_str("truncated DEFLATE stream"),
            Self::Stalled => formatter.write_str("DEFLATE decoder made no progress"),
            Self::TrailingGarbage => formatter.write_str("trailing data after raw DEFLATE stream"),
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
    /// Automatic detection found neither gzip nor zlib framing.
    UnrecognizedFormat,
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
    /// Decoded output would exceed the configured limit.
    OutputLimitExceeded {
        /// Configured maximum decoded byte count.
        limit: u64,
    },
    /// Decoded output did not equal the caller's exact expectation.
    UnexpectedOutputSize {
        /// Required total output size.
        expected: u64,
        /// Observed total, or the total that the next handoff would produce.
        actual: u64,
    },
    /// Inflation did not reach an index checkpoint at its declared bit offset.
    IndexBoundaryMismatch {
        /// Compressed bit offset declared by the index.
        expected_bit_offset: u64,
        /// Compressed bit offset reached by the inflater.
        actual_bit_offset: u64,
    },
    /// A checkpoint's decompressed offset disagreed with decoded output.
    IndexOutputMismatch {
        /// Compressed checkpoint whose output offset was checked.
        checkpoint_bit_offset: u64,
        /// Decompressed bytes declared between the surrounding checkpoints.
        expected_bytes: u64,
        /// Decompressed bytes actually produced for that span.
        actual_bytes: u64,
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
            | Self::InvalidZlib {
                reason: ZlibErrorKind::Truncated,
                ..
            }
            | Self::InvalidDeflate {
                reason: DeflateErrorKind::Truncated,
                ..
            } => io::ErrorKind::UnexpectedEof,
            Self::InvalidGzip { .. }
            | Self::UnrecognizedFormat
            | Self::InvalidZlib { .. }
            | Self::InvalidDeflate { .. }
            | Self::ChecksumMismatch { .. }
            | Self::SizeMismatch { .. }
            | Self::UnexpectedOutputSize { .. }
            | Self::IndexBoundaryMismatch { .. }
            | Self::IndexOutputMismatch { .. } => io::ErrorKind::InvalidData,
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
            Self::UnrecognizedFormat => {
                formatter.write_str("input is neither recognizable gzip nor zlib data")
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
            Self::OutputLimitExceeded { limit } => {
                write!(formatter, "decoded output exceeded the {limit}-byte limit")
            }
            Self::UnexpectedOutputSize { expected, actual } => write!(
                formatter,
                "decoded output size mismatch: expected {expected} bytes, got {actual}"
            ),
            Self::IndexBoundaryMismatch {
                expected_bit_offset,
                actual_bit_offset,
            } => write!(
                formatter,
                "index checkpoint at bit {expected_bit_offset} did not match the inflater boundary at bit {actual_bit_offset}"
            ),
            Self::IndexOutputMismatch {
                checkpoint_bit_offset,
                expected_bytes,
                actual_bytes,
            } => write!(
                formatter,
                "index span ending at bit {checkpoint_bit_offset} declared {expected_bytes} output bytes but produced {actual_bytes}"
            ),
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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DecodeReport {
    /// Compressed bytes consumed.
    pub compressed_bytes: u64,
    /// Decompressed bytes emitted.
    pub decompressed_bytes: u64,
    /// Number of completed framing units: gzip members, or one zlib/raw stream.
    ///
    /// A raw-DEFLATE unit is structurally complete but has no container
    /// checksum to authenticate it.
    pub member_count: u64,
    /// Configured decoder-worker budget.
    pub decoder_threads: usize,
    /// Concrete container framing that was decoded.
    pub format: Format,
    /// Newline bytes in the decoded output when line counting was enabled.
    ///
    /// Enable this with
    /// [`DecoderBuilder::count_lines`](crate::DecoderBuilder::count_lines).
    /// The value counts `b'\n'` bytes; a final unterminated line does not add
    /// one to the count.
    pub line_count: Option<u64>,
}

impl AsRef<DecodeReport> for DecodeReport {
    fn as_ref(&self) -> &DecodeReport {
        self
    }
}

/// Result of a verified decode that also collected a random-access index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedDecodeReport {
    /// Scalar statistics for the verified decode.
    pub decode: DecodeReport,
    /// Random-access index built from authoritative decode boundaries.
    pub index: DeflateIndex,
}

impl IndexedDecodeReport {
    /// Returns the scalar decode report.
    #[must_use]
    pub const fn report(&self) -> &DecodeReport {
        &self.decode
    }

    /// Returns the collected random-access index.
    #[must_use]
    pub const fn index(&self) -> &DeflateIndex {
        &self.index
    }

    /// Separates the scalar report from the owning index.
    #[must_use]
    pub fn into_parts(self) -> (DecodeReport, DeflateIndex) {
        (self.decode, self.index)
    }
}

impl AsRef<DecodeReport> for IndexedDecodeReport {
    fn as_ref(&self) -> &DecodeReport {
        &self.decode
    }
}

/// Failure of an operation that decodes and builds an index.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum IndexingError {
    /// The compressed input could not be decoded and verified.
    Decode(DecodeError),
    /// The index could not be constructed or finalized.
    Index(IndexError),
}

/// Failure while decoding through a caller-supplied index.
///
/// Index-driven decoding is strict: an invalid, incomplete, or source-mismatched
/// index is reported rather than silently falling back to an unindexed path.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum IndexDecodeError {
    /// The compressed input could not be decoded and verified.
    Decode(DecodeError),
    /// The supplied index was invalid or did not describe the source.
    Index(IndexError),
    /// The decoder's selected container disagreed with the index provenance.
    FormatMismatch {
        /// Container selected on the decoder builder.
        selected: Format,
        /// Container provenance recorded by the index.
        indexed: IndexKind,
    },
}

impl From<DecodeError> for IndexDecodeError {
    fn from(error: DecodeError) -> Self {
        Self::Decode(error)
    }
}

impl From<IndexError> for IndexDecodeError {
    fn from(error: IndexError) -> Self {
        Self::Index(error)
    }
}

impl Display for IndexDecodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(error) => Display::fmt(error, formatter),
            Self::Index(error) => {
                write!(formatter, "index-driven decode rejected the index: {error}")
            }
            Self::FormatMismatch { selected, indexed } => write!(
                formatter,
                "decoder selected {selected}, but the index describes {indexed:?} data"
            ),
        }
    }
}

impl Error for IndexDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode(error) => Some(error),
            Self::Index(error) => Some(error),
            Self::FormatMismatch { .. } => None,
        }
    }
}

impl IndexingError {
    pub(crate) fn to_io_error(&self) -> io::Error {
        match self {
            Self::Decode(error) => error.to_io_error(),
            Self::Index(_) => io::Error::other(self.clone()),
        }
    }
}

impl From<DecodeError> for IndexingError {
    fn from(error: DecodeError) -> Self {
        Self::Decode(error)
    }
}

impl From<IndexError> for IndexingError {
    fn from(error: IndexError) -> Self {
        Self::Index(error)
    }
}

impl Display for IndexingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(error) => Display::fmt(error, formatter),
            Self::Index(error) => write!(formatter, "index construction failed: {error}"),
        }
    }
}

impl Error for IndexingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode(error) => Some(error),
            Self::Index(error) => Some(error),
        }
    }
}
