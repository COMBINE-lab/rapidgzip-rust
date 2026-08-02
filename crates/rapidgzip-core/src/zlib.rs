//! RFC 1950 zlib framing and Adler-32 verification.

use crate::{DecodeError, ZlibErrorKind};
use std::ffi::c_ulong;

const DEFLATE_METHOD: u8 = 8;
const PRESET_DICTIONARY: u8 = 0x20;
const MAX_CINFO: u8 = 7;

/// Returns whether two bytes are recognizable as a zlib header.
///
/// A preset-dictionary header is recognizable and is rejected with the more
/// specific error when the selected decoder parses it.
pub(crate) const fn is_header(cmf: u8, flg: u8) -> bool {
    cmf & 0x0f == DEFLATE_METHOD
        && cmf >> 4 <= MAX_CINFO
        && (((cmf as u16) << 8) | flg as u16).is_multiple_of(31)
}

/// Validates a zlib header and returns its DEFLATE window size in bits.
pub(crate) const fn parse_header(bytes: [u8; 2], offset: u64) -> Result<u8, DecodeError> {
    let [cmf, flg] = bytes;
    let method = cmf & 0x0f;
    if method != DEFLATE_METHOD {
        return Err(DecodeError::InvalidZlib {
            offset,
            reason: ZlibErrorKind::UnsupportedCompressionMethod(method),
        });
    }
    let cinfo = cmf >> 4;
    if cinfo > MAX_CINFO {
        return Err(DecodeError::InvalidZlib {
            offset,
            reason: ZlibErrorKind::UnsupportedWindowSize(cinfo),
        });
    }
    if !(((cmf as u16) << 8) | flg as u16).is_multiple_of(31) {
        return Err(DecodeError::InvalidZlib {
            offset,
            reason: ZlibErrorKind::BadHeaderCheck,
        });
    }
    if flg & PRESET_DICTIONARY != 0 {
        return Err(DecodeError::InvalidZlib {
            offset,
            reason: ZlibErrorKind::PresetDictionary,
        });
    }
    Ok(cinfo + 8)
}

/// Incremental Adler-32 as stored by a zlib trailer.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Adler32(u32);

impl Adler32 {
    pub(crate) const fn new() -> Self {
        Self(1)
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        // SAFETY: `bytes` is a live immutable allocation for the duration of
        // the call. `adler32_z` reads exactly `bytes.len()` bytes and retains no
        // pointer. The checksum value itself always fits in 32 bits even where
        // the C ABI's `c_ulong` is wider.
        self.0 = unsafe { libz_rs_sys::adler32_z(self.0 as c_ulong, bytes.as_ptr(), bytes.len()) }
            as u32;
    }

    pub(crate) const fn finish(self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_headers_and_window_sizes() {
        assert_eq!(parse_header([0x08, 0x1d], 0).unwrap(), 8);
        assert_eq!(parse_header([0x78, 0x9c], 0).unwrap(), 15);
        assert!(matches!(
            parse_header([0x78, 0x20], 4),
            Err(DecodeError::InvalidZlib {
                offset: 4,
                reason: ZlibErrorKind::PresetDictionary
            })
        ));
    }

    #[test]
    fn accepts_every_legal_cinfo_value() {
        for cinfo in 0_u8..=7 {
            let cmf = (cinfo << 4) | DEFLATE_METHOD;
            let flg = (0_u8..=255)
                .find(|flg| {
                    flg & PRESET_DICTIONARY == 0
                        && (((cmf as u16) << 8) | u16::from(*flg)).is_multiple_of(31)
                })
                .unwrap();
            assert_eq!(parse_header([cmf, flg], 0).unwrap(), cinfo + 8);
        }
    }

    #[test]
    fn adler_matches_reference_vector() {
        let mut checksum = Adler32::new();
        checksum.update(b"Wiki");
        checksum.update(b"pedia");
        assert_eq!(checksum.finish(), 0x11e6_0398);
    }
}
