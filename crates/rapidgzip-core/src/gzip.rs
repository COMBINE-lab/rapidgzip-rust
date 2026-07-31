use crate::crc32::Crc32;
use crate::{DecodeError, GzipErrorKind, ReadAt};
use std::io;

const FLAG_HEADER_CRC: u8 = 0x02;
const FLAG_EXTRA: u8 = 0x04;
const FLAG_NAME: u8 = 0x08;
const FLAG_COMMENT: u8 = 0x10;
const RESERVED_FLAGS: u8 = 0xE0;

/// Buffered cursor over an immutable positional source.
pub(crate) struct SourceCursor<'a, R: ReadAt + ?Sized> {
    source: &'a R,
    length: u64,
    position: u64,
    page: Vec<u8>,
    page_start: u64,
    page_length: usize,
}

impl<'a, R: ReadAt + ?Sized> SourceCursor<'a, R> {
    pub(crate) fn new(source: &'a R, page_size: usize) -> Result<Self, DecodeError> {
        let length = source
            .len()
            .map_err(|error| DecodeError::input_io(0, error))?;
        Ok(Self {
            source,
            length,
            position: 0,
            page: vec![0; page_size],
            page_start: u64::MAX,
            page_length: 0,
        })
    }

    pub(crate) const fn position(&self) -> u64 {
        self.position
    }

    pub(crate) const fn length(&self) -> u64 {
        self.length
    }

    pub(crate) const fn at_end(&self) -> bool {
        self.position >= self.length
    }

    pub(crate) fn seek(&mut self, position: u64) -> Result<(), DecodeError> {
        if position > self.length {
            return Err(DecodeError::InvalidGzip {
                offset: self.position,
                reason: GzipErrorKind::Truncated,
            });
        }
        self.position = position;
        Ok(())
    }

    fn refill(&mut self) -> Result<(), DecodeError> {
        if self.at_end() {
            self.page_start = self.position;
            self.page_length = 0;
            return Ok(());
        }

        self.page_start = self.position;
        let remaining = usize::try_from(self.length - self.position).unwrap_or(usize::MAX);
        let wanted = remaining.min(self.page.len());
        let read = self
            .source
            .read_at(self.position, &mut self.page[..wanted])
            .map_err(|error| DecodeError::input_io(self.position, error))?;
        if read == 0 {
            return Err(DecodeError::input_io(
                self.position,
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "positional source ended before its snapshotted length",
                ),
            ));
        }
        self.page_length = read;
        Ok(())
    }

    pub(crate) fn available(&mut self) -> Result<&[u8], DecodeError> {
        let page_end = self.page_start.saturating_add(self.page_length as u64);
        if self.position < self.page_start || self.position >= page_end {
            self.refill()?;
        }
        let relative = usize::try_from(self.position - self.page_start)
            .expect("a page-relative offset always fits usize");
        Ok(&self.page[relative..self.page_length])
    }

    pub(crate) fn advance(&mut self, count: usize) {
        self.position = self.position.saturating_add(count as u64);
    }

    fn byte(&mut self, truncated_at: u64) -> Result<u8, DecodeError> {
        let Some(&byte) = self.available()?.first() else {
            return Err(DecodeError::InvalidGzip {
                offset: truncated_at,
                reason: GzipErrorKind::Truncated,
            });
        };
        self.advance(1);
        Ok(byte)
    }

    pub(crate) fn read_exact<const N: usize>(
        &mut self,
        truncated_at: u64,
    ) -> Result<[u8; N], DecodeError> {
        let mut result = [0_u8; N];
        for byte in &mut result {
            *byte = self.byte(truncated_at)?;
        }
        Ok(result)
    }
}

/// Parsed member header.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MemberHeader {
    pub(crate) start: u64,
    pub(crate) deflate_start: u64,
    pub(crate) bgzf_block_size: Option<u16>,
}

fn checked_header_byte<R: ReadAt + ?Sized>(
    cursor: &mut SourceCursor<'_, R>,
    crc: &mut Option<Crc32>,
    start: u64,
) -> Result<u8, DecodeError> {
    let byte = cursor.byte(start)?;
    if let Some(crc) = crc {
        crc.update(&[byte]);
    }
    Ok(byte)
}

/// Parses a gzip member header at the cursor's exact current position.
pub(crate) fn parse_member_header<R: ReadAt + ?Sized>(
    cursor: &mut SourceCursor<'_, R>,
    first_member: bool,
) -> Result<MemberHeader, DecodeError> {
    let start = cursor.position();

    let id1 = cursor.byte(start)?;
    let id2 = cursor.byte(start)?;
    if (id1, id2) != (0x1F, 0x8B) {
        return Err(DecodeError::InvalidGzip {
            offset: start,
            reason: if first_member {
                GzipErrorKind::BadMagic
            } else {
                GzipErrorKind::TrailingGarbage
            },
        });
    }

    let compression_method = cursor.byte(start)?;
    if compression_method != 8 {
        return Err(DecodeError::InvalidGzip {
            offset: start + 2,
            reason: GzipErrorKind::UnsupportedCompressionMethod(compression_method),
        });
    }
    let flags = cursor.byte(start)?;
    if flags & RESERVED_FLAGS != 0 {
        return Err(DecodeError::InvalidGzip {
            offset: start + 3,
            reason: GzipErrorKind::ReservedFlags(flags),
        });
    }
    let mut header_crc = if flags & FLAG_HEADER_CRC != 0 {
        let mut crc = Crc32::new();
        crc.update(&[id1, id2, compression_method, flags]);
        Some(crc)
    } else {
        None
    };

    // MTIME, XFL, and OS are metadata only.
    for _ in 0..6 {
        checked_header_byte(cursor, &mut header_crc, start)?;
    }

    let mut bgzf_block_size = None;
    if flags & FLAG_EXTRA != 0 {
        let low = checked_header_byte(cursor, &mut header_crc, start)?;
        let high = checked_header_byte(cursor, &mut header_crc, start)?;
        let extra_length = usize::from(u16::from_le_bytes([low, high]));
        let mut extra = Vec::with_capacity(extra_length);
        for _ in 0..extra_length {
            extra.push(checked_header_byte(cursor, &mut header_crc, start)?);
        }

        // Merely observe a well-formed BGZF BC field. Generic gzip decoding does
        // not depend on it and therefore does not reject unrelated/malformed
        // subfields in v0.1.
        let mut offset: usize = 0;
        while offset.saturating_add(4) <= extra.len() {
            let subfield_length =
                usize::from(u16::from_le_bytes([extra[offset + 2], extra[offset + 3]]));
            let data_start = offset + 4;
            let data_end = data_start.saturating_add(subfield_length);
            if data_end > extra.len() {
                break;
            }
            if &extra[offset..offset + 2] == b"BC" && subfield_length == 2 {
                bgzf_block_size = Some(u16::from_le_bytes([
                    extra[data_start],
                    extra[data_start + 1],
                ]));
            }
            offset = data_end;
        }
    }

    for flag in [FLAG_NAME, FLAG_COMMENT] {
        if flags & flag != 0 {
            loop {
                if cursor.at_end() {
                    return Err(DecodeError::InvalidGzip {
                        offset: start,
                        reason: GzipErrorKind::UnterminatedHeaderField,
                    });
                }
                if checked_header_byte(cursor, &mut header_crc, start)? == 0 {
                    break;
                }
            }
        }
    }

    if flags & FLAG_HEADER_CRC != 0 {
        let expected = u16::from_le_bytes(cursor.read_exact::<2>(start)?);
        let actual = header_crc
            .expect("FHCRC initialized a header checksum")
            .finish() as u16;
        if actual != expected {
            return Err(DecodeError::InvalidGzip {
                offset: cursor.position() - 2,
                reason: GzipErrorKind::HeaderChecksumMismatch { expected, actual },
            });
        }
    }

    Ok(MemberHeader {
        start,
        deflate_start: cursor.position(),
        bgzf_block_size,
    })
}

pub(crate) fn validate_initial_header<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
) -> Result<(), DecodeError> {
    let mut cursor = SourceCursor::new(source, page_size)?;
    parse_member_header(&mut cursor, true).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::{SourceCursor, parse_member_header};
    use crate::GzipErrorKind;

    #[test]
    fn parses_minimal_header() {
        let bytes = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff";
        let mut cursor = SourceCursor::new(bytes.as_slice(), 4).unwrap();
        let header = parse_member_header(&mut cursor, true).unwrap();
        assert_eq!(header.start, 0);
        assert_eq!(header.deflate_start, 10);
    }

    #[test]
    fn reports_trailing_garbage_after_a_member() {
        let bytes = b"not gzip";
        let mut cursor = SourceCursor::new(bytes.as_slice(), 4).unwrap();
        let error = parse_member_header(&mut cursor, false).unwrap_err();
        assert!(matches!(
            error,
            crate::DecodeError::InvalidGzip {
                reason: GzipErrorKind::TrailingGarbage,
                ..
            }
        ));
    }
}
