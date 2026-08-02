//! Container selection and prefix detection.

use crate::zlib;
use std::fmt::{self, Display, Formatter};

/// Container framing around a DEFLATE stream.
///
/// Gzip includes ordinary single-member files, concatenated multi-member
/// archives, and BGZF. Raw DEFLATE has no recognizable header and therefore
/// must always be selected explicitly.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum Format {
    /// RFC 1952 gzip framing.
    #[default]
    Gzip,
    /// RFC 1950 zlib framing.
    Zlib,
    /// An unwrapped RFC 1951 DEFLATE stream.
    RawDeflate,
}

impl Display for Format {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Gzip => "gzip",
            Self::Zlib => "zlib",
            Self::RawDeflate => "raw DEFLATE",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FormatSelection {
    Explicit(Format),
    Auto,
}

impl Default for FormatSelection {
    fn default() -> Self {
        Self::Explicit(Format::Gzip)
    }
}

pub(crate) fn detect(prefix: [u8; 2]) -> Option<Format> {
    if prefix == [0x1f, 0x8b] {
        Some(Format::Gzip)
    } else if zlib::is_header(prefix[0], prefix[1]) {
        Some(Format::Zlib)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_only_framed_formats() {
        assert_eq!(detect([0x1f, 0x8b]), Some(Format::Gzip));
        assert_eq!(detect([0x78, 0x01]), Some(Format::Zlib));
        assert_eq!(detect([0x78, 0x9c]), Some(Format::Zlib));
        assert_eq!(detect([0x78, 0xda]), Some(Format::Zlib));
        assert_eq!(detect([0x78, 0x9d]), None);
        assert_eq!(detect([0x03, 0x00]), None);
    }
}
