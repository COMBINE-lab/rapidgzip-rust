//! zlib container framing (RFC 1950).
//!
//! A zlib stream is a two-byte `CMF`/`FLG` header, one DEFLATE stream, and a
//! four-byte big-endian Adler-32 of the decompressed output. Preset
//! dictionaries are rejected, so the header is always exactly two bytes.

use crate::{DecodeError, ZlibErrorKind};

/// Preset-dictionary flag in the `FLG` byte.
const FLAG_DICT: u8 = 0x20;

/// Largest window exponent this crate accepts, giving DEFLATE's 32 KiB window.
const MAX_WINDOW_EXPONENT: u8 = 7;

/// Byte length of a zlib header without a preset dictionary.
pub(crate) const HEADER_LENGTH: u64 = 2;

/// Byte length of the Adler-32 trailer.
pub(crate) const TRAILER_LENGTH: u64 = 4;

/// Returns whether `cmf` and `flg` form a legal zlib header.
///
/// The compression method must be DEFLATE, the window exponent at most seven,
/// and the two bytes together a multiple of 31, which is the format's `FCHECK`
/// rule. A preset dictionary is legal here and rejected by
/// [`validate_header`], so detection does not depend on that flag.
#[must_use]
pub(crate) const fn is_zlib_header(cmf: u8, flg: u8) -> bool {
    if cmf & 0x0f != 8 || cmf >> 4 > MAX_WINDOW_EXPONENT {
        return false;
    }
    (((cmf as u16) << 8) | flg as u16).is_multiple_of(31)
}

/// Checks a zlib header, reporting why it is unusable.
///
/// `offset` is the compressed byte offset the header starts at, used for
/// diagnostics.
pub(crate) const fn validate_header(cmf: u8, flg: u8, offset: u64) -> Result<(), DecodeError> {
    let method = cmf & 0x0f;
    if method != 8 {
        return Err(DecodeError::InvalidZlib {
            offset,
            reason: ZlibErrorKind::UnsupportedCompressionMethod(method),
        });
    }
    let exponent = cmf >> 4;
    if exponent > MAX_WINDOW_EXPONENT {
        return Err(DecodeError::InvalidZlib {
            offset,
            reason: ZlibErrorKind::UnsupportedWindowSize(exponent),
        });
    }
    if !(((cmf as u16) << 8) | flg as u16).is_multiple_of(31) {
        return Err(DecodeError::InvalidZlib {
            offset,
            reason: ZlibErrorKind::BadHeader,
        });
    }
    if flg & FLAG_DICT != 0 {
        return Err(DecodeError::InvalidZlib {
            offset,
            reason: ZlibErrorKind::PresetDictionary,
        });
    }
    Ok(())
}

/// Incremental Adler-32, as the zlib trailer defines it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Adler32(u32);

impl Adler32 {
    /// Returns the checksum of the empty input, which is one.
    pub(crate) const fn new() -> Self {
        Self(1)
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        // SAFETY: the pointer and length describe one live, immutable
        // allocation for the duration of the call, and `adler32_z` only reads
        // through them.
        self.0 = unsafe {
            libz_rs_sys::adler32_z(u64::from(self.0), bytes.as_ptr(), bytes.len()) as u32
        };
    }

    pub(crate) const fn finish(self) -> u32 {
        self.0
    }
}

/// Checks a four-byte big-endian Adler-32 trailer against `actual`.
pub(crate) fn verify_trailer(trailer: [u8; 4], actual: u32) -> Result<(), DecodeError> {
    let expected = u32::from_be_bytes(trailer);
    if expected == actual {
        Ok(())
    } else {
        Err(DecodeError::InvalidZlib {
            offset: 0,
            reason: ZlibErrorKind::ChecksumMismatch { expected, actual },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_headers_zlib_emits() {
        for (cmf, flg) in [(0x78, 0x01), (0x78, 0x5e), (0x78, 0x9c), (0x78, 0xda)] {
            assert!(is_zlib_header(cmf, flg), "{cmf:#04x} {flg:#04x}");
            assert!(validate_header(cmf, flg, 0).is_ok());
        }
    }

    #[test]
    fn rejects_a_foreign_compression_method() {
        // Method 7 with a residue that still satisfies FCHECK.
        let (cmf, flg) = (0x77, 0x09);
        assert!((((cmf as u16) << 8) | flg as u16).is_multiple_of(31));
        assert!(!is_zlib_header(cmf, flg));
        assert!(matches!(
            validate_header(cmf, flg, 10),
            Err(DecodeError::InvalidZlib {
                offset: 10,
                reason: ZlibErrorKind::UnsupportedCompressionMethod(7),
            })
        ));
    }

    #[test]
    fn rejects_an_oversized_window() {
        // CINFO 8 asks for a 64 KiB window, which DEFLATE does not have.
        let (cmf, flg) = (0x88, 0x1c);
        assert!((((cmf as u16) << 8) | flg as u16).is_multiple_of(31));
        assert!(!is_zlib_header(cmf, flg));
        assert!(matches!(
            validate_header(cmf, flg, 0),
            Err(DecodeError::InvalidZlib {
                offset: 0,
                reason: ZlibErrorKind::UnsupportedWindowSize(8),
            })
        ));
    }

    #[test]
    fn rejects_a_bad_check_residue() {
        assert!(!is_zlib_header(0x78, 0x9d));
        assert!(matches!(
            validate_header(0x78, 0x9d, 0),
            Err(DecodeError::InvalidZlib {
                offset: 0,
                reason: ZlibErrorKind::BadHeader,
            })
        ));
    }

    #[test]
    fn rejects_a_preset_dictionary() {
        // 0x78 0x20 sets FDICT and still satisfies FCHECK.
        assert!(is_zlib_header(0x78, 0x20));
        assert!(matches!(
            validate_header(0x78, 0x20, 4),
            Err(DecodeError::InvalidZlib {
                offset: 4,
                reason: ZlibErrorKind::PresetDictionary,
            })
        ));
    }

    #[test]
    fn adler32_matches_the_reference_vectors() {
        let mut checksum = Adler32::new();
        assert_eq!(checksum.finish(), 1);
        checksum.update(b"Wikipedia");
        assert_eq!(checksum.finish(), 0x11E6_0398);

        // Updating in pieces matches updating at once.
        let mut split = Adler32::new();
        split.update(b"Wiki");
        split.update(b"pedia");
        assert_eq!(split.finish(), 0x11E6_0398);
    }

    #[test]
    fn verifies_a_trailer() {
        assert!(verify_trailer(0x11E6_0398u32.to_be_bytes(), 0x11E6_0398).is_ok());
        assert!(matches!(
            verify_trailer(1u32.to_be_bytes(), 2),
            Err(DecodeError::InvalidZlib {
                offset: 0,
                reason: ZlibErrorKind::ChecksumMismatch {
                    expected: 1,
                    actual: 2,
                },
            })
        ));
    }
}
