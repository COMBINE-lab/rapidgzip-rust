//! RAII wrapper around zlib-rs's raw-inflate ABI.
//!
//! This wrapper owns exactly one initialized `z_stream`, ends it exactly once,
//! and confines the crate's zlib-side `unsafe` to one place. Construction,
//! reset, inflate, and diagnostics arrive through
//! [`crate::inflate_backend::InflateBackend`], so the whole-stream paths reach
//! them without naming this type.
//!
//! What stays inherent is what the trait deliberately omits: resuming
//! mid-stream through [`RawInflater::prime`] for a bit offset that is not byte
//! aligned, and [`RawInflater::set_dictionary`] or
//! [`RawInflater::set_dictionary_bytes`] for the predecessor window.

use crate::inflate_backend::{InflateBackend, InflateOutcome, InflateStep};
use crate::parallel::Window;
use crate::{DecodeError, DeflateErrorKind};
use libz_rs_sys as z;
use std::ffi::CStr;
use std::mem::size_of;

/// RAII wrapper around zlib-rs's zlib-compatible raw-inflate ABI.
pub(crate) struct RawInflater {
    pub(crate) stream: z::z_stream,
    initialized: bool,
}

impl RawInflater {
    pub(crate) fn prime(
        &mut self,
        bits: u8,
        value: u8,
        bit_offset: u64,
    ) -> Result<(), DecodeError> {
        // SAFETY: the stream is initialized and uniquely borrowed. zlib accepts
        // at most 16 low-order bits before the first inflate call; this wrapper
        // supplies at most the seven unread bits from one source byte.
        let status =
            unsafe { z::inflatePrime(&mut self.stream, i32::from(bits), i32::from(value)) };
        if status == z::Z_OK {
            Ok(())
        } else {
            Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::BackendStatus(status),
            })
        }
    }

    pub(crate) fn set_dictionary(
        &mut self,
        window: &Window,
        bit_offset: u64,
    ) -> Result<(), DecodeError> {
        if window.as_slice().is_empty() {
            return Ok(());
        }
        // SAFETY: `window` remains immutably borrowed for the call, and its
        // slice is no larger than DEFLATE's 32 KiB history limit.
        let status = unsafe {
            z::inflateSetDictionary(
                &mut self.stream,
                window.as_slice().as_ptr(),
                window.as_slice().len() as u32,
            )
        };
        if status == z::Z_OK {
            Ok(())
        } else {
            Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::BackendStatus(status),
            })
        }
    }

    /// Installs `bytes` as the raw-inflate history.
    ///
    /// This is the byte-slice form of [`Self::set_dictionary`], used by the
    /// indexed reader, whose windows come from an index rather than from a
    /// marker-resolution [`Window`].
    #[allow(dead_code)] // Used by the indexed reader, which lands next.
    pub(crate) fn set_dictionary_bytes(
        &mut self,
        bytes: &[u8],
        bit_offset: u64,
    ) -> Result<(), DecodeError> {
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.len() > 32768 {
            return Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::InvalidData,
            });
        }
        // SAFETY: `bytes` remains immutably borrowed for the call, and its
        // length is checked against DEFLATE's 32 KiB history limit above.
        let status = unsafe {
            z::inflateSetDictionary(&mut self.stream, bytes.as_ptr(), bytes.len() as u32)
        };
        if status == z::Z_OK {
            Ok(())
        } else {
            Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::BackendStatus(status),
            })
        }
    }
}

impl Drop for RawInflater {
    fn drop(&mut self) {
        if self.initialized {
            // SAFETY: this wrapper calls `inflateEnd` exactly once for the
            // successfully initialized, uniquely owned stream.
            let _ = unsafe { z::inflateEnd(&mut self.stream) };
        }
    }
}

impl InflateBackend for RawInflater {
    fn new() -> Result<Self, DecodeError> {
        let mut result = Self {
            stream: z::z_stream::default(),
            initialized: false,
        };

        // SAFETY:
        // - `result.stream` is a live, uniquely borrowed `z_stream`.
        // - `zlibVersion` returns a static NUL-terminated version string.
        // - the structure size matches the exact Rust ABI type passed.
        // - `-15` requests raw DEFLATE with a 32 KiB window.
        let status = unsafe {
            z::inflateInit2_(
                &mut result.stream,
                -15,
                z::zlibVersion(),
                size_of::<z::z_stream>() as i32,
            )
        };
        if status != z::Z_OK {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: 0,
                reason: DeflateErrorKind::BackendStatus(status),
            });
        }
        result.initialized = true;
        Ok(result)
    }

    fn reset(&mut self, bit_offset: u64) -> Result<(), DecodeError> {
        // SAFETY: this wrapper owns a successfully initialized stream and
        // holds its unique mutable borrow. `inflateReset` retains the raw
        // window mode selected by `inflateInit2_`.
        let status = unsafe { z::inflateReset(&mut self.stream) };
        if status == z::Z_OK {
            Ok(())
        } else {
            Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::BackendStatus(status),
            })
        }
    }

    fn inflate(
        &mut self,
        input: &[u8],
        output: &mut Vec<u8>,
        finish: bool,
    ) -> Result<InflateStep, DecodeError> {
        let start = output.len();
        let spare = output.spare_capacity_mut();
        let output_length = spare.len().min(u32::MAX as usize);
        let input_length = input.len().min(u32::MAX as usize);

        self.stream.next_in = input.as_ptr();
        self.stream.avail_in = input_length as u32;
        self.stream.next_out = spare.as_mut_ptr().cast::<u8>();
        self.stream.avail_out = output_length as u32;
        let input_before = self.stream.avail_in;
        let output_before = self.stream.avail_out;

        let flush = if finish { z::Z_FINISH } else { z::Z_NO_FLUSH };
        // SAFETY:
        // - the stream is initialized and uniquely borrowed for this call;
        // - `next_in` covers `input`, which stays live and unmoved;
        // - `next_out` covers only the uniquely owned spare capacity of
        //   `output`, whose length is extended below by exactly the count
        //   zlib reports as initialized.
        let status = unsafe { z::inflate(&mut self.stream, flush) };

        let consumed = (input_before - self.stream.avail_in) as usize;
        let produced = (output_before - self.stream.avail_out) as usize;
        // SAFETY: zlib initialized exactly `produced` bytes of the spare
        // capacity supplied above and cannot report more than that capacity.
        unsafe { output.set_len(start + produced) };

        let outcome = match status {
            z::Z_STREAM_END => InflateOutcome::StreamEnd,
            z::Z_OK => InflateOutcome::Progress,
            z::Z_BUF_ERROR => {
                if consumed > 0 || produced > 0 {
                    InflateOutcome::Progress
                } else {
                    InflateOutcome::Blocked
                }
            }
            z::Z_NEED_DICT => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: 0,
                    reason: DeflateErrorKind::UnexpectedDictionary,
                });
            }
            z::Z_DATA_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: 0,
                    reason: DeflateErrorKind::InvalidData,
                });
            }
            other => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: 0,
                    reason: DeflateErrorKind::BackendStatus(other),
                });
            }
        };

        Ok(InflateStep {
            outcome,
            consumed,
            produced,
        })
    }

    fn message(&self) -> Option<String> {
        if self.stream.msg.is_null() {
            return None;
        }
        // SAFETY: while the initialized zlib stream is live, zlib owns `msg`
        // as a valid NUL-terminated diagnostic string or leaves it null.
        Some(
            unsafe { CStr::from_ptr(self.stream.msg) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}
