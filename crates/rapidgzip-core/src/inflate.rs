//! RAII wrapper around zlib-rs's raw-inflate ABI.
//!
//! Every decode path in this crate drives inflate through this wrapper, which
//! owns exactly one initialized `z_stream`, ends it exactly once, and confines
//! the crate's inflate-side `unsafe` to one place. Resuming mid-stream uses
//! [`RawInflater::prime`] for a bit offset that is not byte aligned and
//! [`RawInflater::set_dictionary_bytes`] for the predecessor window.

use crate::{DecodeError, DeflateErrorKind};
use libz_rs_sys as z;
use std::ffi::CStr;
use std::mem::size_of;

/// RAII wrapper around zlib-rs's zlib-compatible raw-inflate ABI.
pub(crate) struct RawInflater {
    pub(crate) stream: z::z_stream,
    initialized: bool,
    window_bits: u8,
}

impl RawInflater {
    pub(crate) fn new() -> Result<Self, DecodeError> {
        Self::new_with_window_bits(15)
    }

    /// Initializes raw inflation with an RFC 1950 CINFO-derived window.
    pub(crate) fn new_with_window_bits(window_bits: u8) -> Result<Self, DecodeError> {
        if !(8..=15).contains(&window_bits) {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: 0,
                reason: DeflateErrorKind::InvalidData,
            });
        }
        let mut result = Self {
            stream: z::z_stream::default(),
            initialized: false,
            window_bits,
        };

        // SAFETY:
        // - `result.stream` is a live, uniquely borrowed `z_stream`.
        // - `zlibVersion` returns a static NUL-terminated version string.
        // - the structure size matches the exact Rust ABI type passed.
        // - a negative `window_bits` requests raw DEFLATE with the container's
        //   declared history limit.
        let status = unsafe {
            z::inflateInit2_(
                &mut result.stream,
                -i32::from(window_bits),
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

    pub(crate) fn message(&self) -> Option<String> {
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

    pub(crate) fn reset(&mut self, bit_offset: u64) -> Result<(), DecodeError> {
        self.reset_with_window_bits(self.window_bits, bit_offset)
    }

    /// Resets raw inflation and changes the maximum history window.
    pub(crate) fn reset_with_window_bits(
        &mut self,
        window_bits: u8,
        bit_offset: u64,
    ) -> Result<(), DecodeError> {
        if !(8..=15).contains(&window_bits) {
            return Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::InvalidData,
            });
        }
        // SAFETY: this wrapper owns a successfully initialized stream and
        // holds its unique mutable borrow. A negative value keeps raw mode;
        // the accepted range is checked above.
        let status = unsafe { z::inflateReset2(&mut self.stream, -i32::from(window_bits)) };
        if status == z::Z_OK {
            self.window_bits = window_bits;
            Ok(())
        } else {
            Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::BackendStatus(status),
            })
        }
    }

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

    /// Installs `bytes` as the raw-inflate history.
    ///
    /// The indexed reader supplies stored windows, while the parallel decoder
    /// supplies only the suffix permitted by the selected container.
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
