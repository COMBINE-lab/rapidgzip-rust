//! Pluggable raw-inflate backend for the whole-stream decode paths.
//!
//! The sequential gzip loop, the single-stream zlib and raw DEFLATE loop, and
//! the BGZF block decoder all inflate a complete stream from its start. They go
//! through [`InflateBackend`] so an alternative implementation can be
//! monomorphized in without touching a call site. [`ActiveInflater`] selects
//! which one those paths use.
//!
//! The marker/window path and [`crate::IndexedReader`] deliberately do not use
//! this trait. They resume at arbitrary bit offsets, which needs
//! `inflatePrime`, and they locate DEFLATE block boundaries through zlib's
//! `Z_BLOCK` contract. A backend that cannot express either must not be
//! reachable from them, so those two keep a concrete
//! [`crate::inflate::RawInflater`].

use crate::DecodeError;

/// What one [`InflateBackend::inflate`] call achieved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InflateOutcome {
    /// The backend made progress and the stream continues.
    Progress,
    /// The backend reached the end of the DEFLATE stream.
    StreamEnd,
    /// The backend needs more input or more output room to continue.
    Blocked,
}

/// Result of one inflate call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InflateStep {
    /// What the call achieved.
    pub(crate) outcome: InflateOutcome,
    /// Compressed bytes consumed from the supplied input.
    pub(crate) consumed: usize,
    /// Decompressed bytes appended to the supplied output.
    pub(crate) produced: usize,
}

/// A raw-inflate implementation usable by the whole-stream paths.
///
/// Implementations hold raw pointers into a C-ABI stream state, so they are
/// not `Send`. Every path creates its backend on the thread that uses it.
pub(crate) trait InflateBackend: Sized {
    /// Creates an initialized backend for raw DEFLATE.
    fn new() -> Result<Self, DecodeError>;

    /// Restarts the backend for a new stream.
    ///
    /// `bit_offset` names the stream start for diagnostics only.
    fn reset(&mut self, bit_offset: u64) -> Result<(), DecodeError>;

    /// Installs `window` as the DEFLATE history, at most 32768 bytes.
    fn set_dictionary(&mut self, window: &[u8], bit_offset: u64) -> Result<(), DecodeError>;

    /// Inflates from `input`, appending into the spare capacity of `output`.
    ///
    /// The caller reserves the capacity it wants filled; the backend never
    /// grows `output`. With `finish`, the backend is told the whole stream is
    /// present and the output has room for all of it.
    fn inflate(
        &mut self,
        input: &[u8],
        output: &mut Vec<u8>,
        finish: bool,
    ) -> Result<InflateStep, DecodeError>;

    /// Returns the backend's last diagnostic message, when it has one.
    fn message(&self) -> Option<String>;
}

/// The backend the whole-stream paths use.
#[cfg(not(feature = "isal"))]
pub(crate) type ActiveInflater = crate::inflate::RawInflater;

/// The backend the whole-stream paths use.
#[cfg(feature = "isal")]
pub(crate) type ActiveInflater = crate::isal_backend::IsalInflater;
