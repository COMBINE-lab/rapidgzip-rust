//! zlib container (RFC 1950) framing: CMF/FLG header and Adler-32 trailer.
//!
//! Raw DEFLATE without a zlib wrapper is handled on the `Format::RawDeflate`
//! path in `backend` (not here). gzip (`1f 8b`) is rejected by the header
//! checks below so auto-detection can try gzip first.

use crate::gzip::SourceCursor;
use crate::{DecodeError, ReadAt, ZlibErrorKind};

/// FDICT flag in the zlib FLG byte.
const FLAG_DICT: u8 = 0x20;

/// Incremental Adler-32 used by the zlib trailer (RFC 1950).
pub(crate) struct Adler32(u32);

impl Adler32 {
    /// Initial Adler-32 state (checksum of the empty string is 1).
    pub(crate) const fn new() -> Self {
        Self(1)
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        // SAFETY: `bytes.as_ptr()` and `bytes.len()` describe one live,
        // immutable allocation for the duration of the call. `adler32_z`
        // performs runtime-dispatched Adler-32 when supported.
        self.0 =
            unsafe { libz_rs_sys::adler32_z(self.0.into(), bytes.as_ptr(), bytes.len()) as u32 };
    }

    pub(crate) const fn finish(&self) -> u32 {
        self.0
    }
}

/// Parsed zlib stream header.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ZlibHeader {
    pub(crate) start: u64,
    pub(crate) deflate_start: u64,
    /// Compression method and flags byte (`CMF`).
    pub(crate) cmf: u8,
    /// Flags byte (`FLG`), including FCHECK and optional FDICT.
    pub(crate) flg: u8,
}

/// Returns true when `cmf`/`flg` form a legal zlib header with CM=DEFLATE.
///
/// Checks compression method 8, window size CINFO ≤ 7, and FCHECK
/// (`(CMF << 8 | FLG) % 31 == 0`). Does not consume FDICT; callers must reject
/// or load a dictionary separately.
pub(crate) const fn is_zlib_cmf_flg(cmf: u8, flg: u8) -> bool {
    let method = cmf & 0x0f;
    let cinfo = cmf >> 4;
    if method != 8 || cinfo > 7 {
        return false;
    }
    let check = (cmf as u16) * 256 + (flg as u16);
    check.is_multiple_of(31)
}

/// Peeks the first two bytes of `source` and reports whether they look like a
/// zlib CMF/FLG header (not gzip magic).
pub(crate) fn looks_like_zlib<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
) -> Result<bool, DecodeError> {
    let mut cursor = SourceCursor::new(source, page_size)?;
    if cursor.at_end() {
        return Ok(false);
    }
    let bytes = match cursor.read_exact::<2>(0) {
        Ok(bytes) => bytes,
        Err(DecodeError::InvalidGzip {
            reason: crate::GzipErrorKind::Truncated,
            ..
        }) => return Ok(false),
        Err(error) => return Err(error),
    };
    if bytes == [0x1f, 0x8b] {
        return Ok(false);
    }
    Ok(is_zlib_cmf_flg(bytes[0], bytes[1]))
}

fn map_truncated(start: u64, first_member: bool, error: DecodeError) -> DecodeError {
    match error {
        DecodeError::InvalidGzip {
            reason: crate::GzipErrorKind::Truncated,
            ..
        } => DecodeError::InvalidZlib {
            offset: start,
            reason: if first_member {
                ZlibErrorKind::Truncated
            } else {
                ZlibErrorKind::TrailingGarbage
            },
        },
        other => other,
    }
}

/// Parses a zlib member header at the cursor's exact current position.
pub(crate) fn parse_zlib_header<R: ReadAt + ?Sized>(
    cursor: &mut SourceCursor<'_, R>,
    first_member: bool,
) -> Result<ZlibHeader, DecodeError> {
    let start = cursor.position();
    let [cmf, flg] = cursor
        .read_exact::<2>(start)
        .map_err(|error| map_truncated(start, first_member, error))?;

    if (cmf, flg) == (0x1f, 0x8b) {
        return Err(DecodeError::InvalidZlib {
            offset: start,
            reason: if first_member {
                ZlibErrorKind::BadHeader
            } else {
                ZlibErrorKind::TrailingGarbage
            },
        });
    }

    let method = cmf & 0x0f;
    if method != 8 {
        return Err(DecodeError::InvalidZlib {
            offset: start,
            reason: ZlibErrorKind::UnsupportedCompressionMethod(method),
        });
    }
    let cinfo = cmf >> 4;
    if cinfo > 7 {
        return Err(DecodeError::InvalidZlib {
            offset: start,
            reason: ZlibErrorKind::UnsupportedWindow(cinfo),
        });
    }
    let check = u16::from(cmf) * 256 + u16::from(flg);
    if !check.is_multiple_of(31) {
        return Err(DecodeError::InvalidZlib {
            offset: start,
            reason: ZlibErrorKind::BadHeaderChecksum,
        });
    }

    if flg & FLAG_DICT != 0 {
        // DICTID is four big-endian bytes; we never accept preset dictionaries.
        let _dict_id = cursor
            .read_exact::<4>(start)
            .map_err(|error| map_truncated(start, true, error))?;
        return Err(DecodeError::InvalidZlib {
            offset: start,
            reason: ZlibErrorKind::DictionaryNotSupported,
        });
    }

    Ok(ZlibHeader {
        start,
        deflate_start: cursor.position(),
        cmf,
        flg,
    })
}

/// Validates that `source` begins with a zlib CMF/FLG header.
pub(crate) fn validate_initial_zlib_header<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
) -> Result<(), DecodeError> {
    let mut cursor = SourceCursor::new(source, page_size)?;
    parse_zlib_header(&mut cursor, true).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::{Adler32, is_zlib_cmf_flg, parse_zlib_header};
    use crate::gzip::SourceCursor;
    use crate::{DecodeError, ZlibErrorKind};

    #[test]
    fn accepts_common_zlib_header() {
        // Default zlib header used by many compressors (CM=8, CINFO=7, FCHECK).
        assert!(is_zlib_cmf_flg(0x78, 0x9c));
        assert!(is_zlib_cmf_flg(0x78, 0x01));
    }

    #[test]
    fn rejects_gzip_magic_as_zlib() {
        assert!(!is_zlib_cmf_flg(0x1f, 0x8b));
    }

    #[test]
    fn adler32_empty_and_hello() {
        let empty = Adler32::new();
        assert_eq!(empty.finish(), 1);

        let mut adler = Adler32::new();
        adler.update(b"hello");
        // Matches Python zlib.adler32(b"hello") & 0xffffffff == 0x062c0215.
        assert_eq!(adler.finish(), 0x062c_0215);
    }

    #[test]
    fn parses_minimal_zlib_header() {
        let bytes = [0x78_u8, 0x01];
        let mut cursor = SourceCursor::new(bytes.as_slice(), 4).unwrap();
        let header = parse_zlib_header(&mut cursor, true).unwrap();
        assert_eq!(header.start, 0);
        assert_eq!(header.deflate_start, 2);
        assert_eq!(header.cmf, 0x78);
        assert_eq!(header.flg, 0x01);
    }

    #[test]
    fn rejects_dictionary_flag() {
        // Find an FLG with FDICT set and valid FCHECK for CMF=0x78.
        let flg = (0_u8..=255)
            .find(|&candidate| candidate & 0x20 != 0 && is_zlib_cmf_flg(0x78, candidate))
            .expect("there exists an FDICT FLG with valid FCHECK");
        let bytes = [0x78, flg, 0, 0, 0, 1];
        let mut cursor = SourceCursor::new(bytes.as_slice(), 8).unwrap();
        let error = parse_zlib_header(&mut cursor, true).unwrap_err();
        assert!(matches!(
            error,
            DecodeError::InvalidZlib {
                reason: ZlibErrorKind::DictionaryNotSupported,
                ..
            }
        ));
    }
}
