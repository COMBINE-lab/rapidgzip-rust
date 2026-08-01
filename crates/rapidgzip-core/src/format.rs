//! Container framing of the compressed input.
//!
//! The crate decodes three containers around the same DEFLATE payload: gzip
//! (RFC 1952, including BGZF), zlib (RFC 1950), and no container at all
//! (RFC 1951). Detection distinguishes the first two from the stream prefix;
//! raw DEFLATE has no magic bytes and must be requested.

use std::fmt::{self, Display, Formatter};

/// Container framing of the compressed input.
///
/// [`Format::Auto`] inspects the first two bytes: `1f 8b` selects
/// [`Format::Gzip`], and otherwise a valid zlib `CMF`/`FLG` pair selects
/// [`Format::Zlib`]. Anything else is reported as missing gzip magic.
///
/// Auto-detection never selects [`Format::RawDeflate`], because raw DEFLATE
/// has no header to recognize, so every corrupt input would otherwise be
/// accepted as raw data.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum Format {
    /// Detect gzip against zlib from the stream prefix.
    #[default]
    Auto,
    /// Require gzip framing, which includes concatenated members and BGZF.
    Gzip,
    /// Require zlib framing: a `CMF`/`FLG` header and an Adler-32 trailer.
    Zlib,
    /// Require raw DEFLATE with no container.
    RawDeflate,
}

impl Format {
    /// Returns whether this is a concrete container rather than [`Self::Auto`].
    #[must_use]
    pub const fn is_concrete(self) -> bool {
        !matches!(self, Self::Auto)
    }
}

impl Display for Format {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Auto => "auto",
            Self::Gzip => "gzip",
            Self::Zlib => "zlib",
            Self::RawDeflate => "raw deflate",
        })
    }
}

/// Returns the container `prefix` starts with, when it is recognizable.
///
/// `None` means neither gzip nor zlib, which callers report as missing gzip
/// magic. A prefix shorter than two bytes is never recognized: gzip needs both
/// magic bytes and zlib needs both header bytes to check.
pub(crate) fn detect(prefix: &[u8]) -> Option<Format> {
    if prefix.len() < 2 {
        return None;
    }
    if prefix[0] == 0x1f && prefix[1] == 0x8b {
        return Some(Format::Gzip);
    }
    if crate::zlib::is_zlib_header(prefix[0], prefix[1]) {
        return Some(Format::Zlib);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_gzip_magic() {
        assert_eq!(detect(&[0x1f, 0x8b, 0x08]), Some(Format::Gzip));
    }

    #[test]
    fn detects_a_zlib_header() {
        // 0x78 0x9c is the common default-compression zlib header.
        assert_eq!(detect(&[0x78, 0x9c]), Some(Format::Zlib));
        // 0x78 0x01 and 0x78 0xda are the other levels zlib emits.
        assert_eq!(detect(&[0x78, 0x01]), Some(Format::Zlib));
        assert_eq!(detect(&[0x78, 0xda]), Some(Format::Zlib));
    }

    #[test]
    fn refuses_prefixes_that_are_neither() {
        assert_eq!(detect(&[]), None);
        assert_eq!(detect(&[0x1f]), None);
        assert_eq!(detect(&[0x78]), None);
        // Valid method, but the FCHECK residue is wrong.
        assert_eq!(detect(&[0x78, 0x9d]), None);
        assert_eq!(detect(b"not compressed"), None);
    }

    #[test]
    fn gzip_magic_is_not_also_a_legal_zlib_header() {
        // The two containers cannot be confused: gzip's magic fails FCHECK.
        assert!(!((0x1f_u16 << 8) | 0x8b).is_multiple_of(31));
        assert_eq!(detect(&[0x1f, 0x8b]), Some(Format::Gzip));
    }

    #[test]
    fn auto_is_the_only_non_concrete_format() {
        assert!(!Format::Auto.is_concrete());
        assert!(Format::Gzip.is_concrete());
        assert!(Format::Zlib.is_concrete());
        assert!(Format::RawDeflate.is_concrete());
    }
}
