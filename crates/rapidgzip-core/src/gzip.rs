use crate::crc32::Crc32;
use crate::{DecodeError, GzipErrorKind, ReadAt};
use std::io::{self, Read};

const FLAG_HEADER_CRC: u8 = 0x02;
const FLAG_EXTRA: u8 = 0x04;
const FLAG_NAME: u8 = 0x08;
const FLAG_COMMENT: u8 = 0x10;
const RESERVED_FLAGS: u8 = 0xE0;

/// Forward-reading view of compressed input shared by every gzip framing step.
///
/// Member framing, footer verification, and the sequential inflate loop only
/// ever move forward through the compressed bytes. Expressing exactly that much
/// lets the positional [`SourceCursor`] and the non-seekable [`StreamCursor`]
/// run the identical framing and verification code, so a streaming decode
/// cannot drift from a positional one.
///
/// The trait is deliberately not object safe: [`InputCursor::read_exact`] is
/// const generic and every use site is monomorphized.
pub(crate) trait InputCursor {
    /// Returns the absolute offset of the next unconsumed byte.
    fn position(&self) -> u64;

    /// Returns whether every byte of the input has been consumed.
    ///
    /// This is fallible because a non-seekable source only learns that it is
    /// exhausted by attempting a read.
    fn is_at_end(&mut self) -> Result<bool, DecodeError>;

    /// Returns the currently readable bytes at the cursor, refilling if needed.
    ///
    /// The returned slice is empty only at end of input. Implementations must
    /// not move or mutate the returned bytes until the next call, because the
    /// sequential inflate loop hands the slice to zlib-rs as `next_in`.
    fn available(&mut self) -> Result<&[u8], DecodeError>;

    /// Consumes `count` bytes previously reported by [`InputCursor::available`].
    fn advance(&mut self, count: usize);

    /// Confirms that the input did not change underneath a completed decode.
    ///
    /// A positional source re-reads its length and compares it against the
    /// snapshot taken when the cursor was created. A non-seekable source has no
    /// length to snapshot: end of input is established by the reader itself, so
    /// there is nothing to re-check.
    fn verify_source_unchanged(&self) -> Result<(), DecodeError>;

    /// Consumes one byte, reporting truncation at `truncated_at` on exhaustion.
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

    /// Consumes exactly `N` bytes, reporting truncation at `truncated_at`.
    fn read_exact<const N: usize>(&mut self, truncated_at: u64) -> Result<[u8; N], DecodeError> {
        let mut result = [0_u8; N];
        for byte in &mut result {
            *byte = self.byte(truncated_at)?;
        }
        Ok(result)
    }
}

impl<T: InputCursor + ?Sized> InputCursor for &mut T {
    fn position(&self) -> u64 {
        (**self).position()
    }

    fn is_at_end(&mut self) -> Result<bool, DecodeError> {
        (**self).is_at_end()
    }

    fn available(&mut self) -> Result<&[u8], DecodeError> {
        (**self).available()
    }

    fn advance(&mut self, count: usize) {
        (**self).advance(count);
    }

    fn verify_source_unchanged(&self) -> Result<(), DecodeError> {
        (**self).verify_source_unchanged()
    }
}

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
}

impl<R: ReadAt + ?Sized> InputCursor for SourceCursor<'_, R> {
    fn position(&self) -> u64 {
        self.position
    }

    fn is_at_end(&mut self) -> Result<bool, DecodeError> {
        Ok(self.at_end())
    }

    fn available(&mut self) -> Result<&[u8], DecodeError> {
        let page_end = self.page_start.saturating_add(self.page_length as u64);
        if self.position < self.page_start || self.position >= page_end {
            self.refill()?;
        }
        let relative = usize::try_from(self.position - self.page_start)
            .expect("a page-relative offset always fits usize");
        Ok(&self.page[relative..self.page_length])
    }

    fn advance(&mut self, count: usize) {
        self.position = self.position.saturating_add(count as u64);
    }

    fn verify_source_unchanged(&self) -> Result<(), DecodeError> {
        let final_length = self
            .source
            .len()
            .map_err(|error| DecodeError::input_io(self.position, error))?;
        if final_length != self.length {
            return Err(DecodeError::input_io(
                self.position,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "compressed source length changed during decoding",
                ),
            ));
        }
        Ok(())
    }
}

/// Buffered forward-only cursor over a non-seekable byte stream.
///
/// The window holds at most one `page_size` buffer. Consumed bytes are dropped
/// by compacting the window on the next refill, so peak input memory does not
/// depend on the stream length and the cursor never needs to seek backwards.
pub(crate) struct StreamCursor<R> {
    source: R,
    buffer: Vec<u8>,
    consumed: usize,
    filled: usize,
    position: u64,
    at_end: bool,
}

impl<R: Read> StreamCursor<R> {
    pub(crate) fn new(source: R, page_size: usize) -> Self {
        Self {
            source,
            buffer: vec![0; page_size.max(1)],
            consumed: 0,
            filled: 0,
            position: 0,
            at_end: false,
        }
    }

    /// Reads once into the free tail of the window, compacting first.
    ///
    /// Compaction happens only here, never in
    /// [`InputCursor::advance`], so a slice handed out by
    /// [`InputCursor::available`] stays valid until the next `available` call.
    fn refill(&mut self) -> Result<(), DecodeError> {
        if self.consumed != 0 {
            self.buffer.copy_within(self.consumed..self.filled, 0);
            self.filled -= self.consumed;
            self.consumed = 0;
        }
        // Reading into an empty tail would return `Ok(0)` and be mistaken for
        // end of input. Callers only refill a drained window, so this cannot
        // happen, but the consequence of being wrong is silent truncation.
        if self.filled == self.buffer.len() {
            return Ok(());
        }
        loop {
            match self.source.read(&mut self.buffer[self.filled..]) {
                Ok(0) => {
                    self.at_end = true;
                    return Ok(());
                }
                Ok(count) => {
                    self.filled += count;
                    return Ok(());
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(DecodeError::input_io(self.position, error)),
            }
        }
    }

    /// Returns the buffered prefix without consuming it.
    ///
    /// Only meaningful before anything has been consumed, which is exactly when
    /// [`validate_initial_stream_header`] runs.
    fn buffered(&self) -> &[u8] {
        &self.buffer[self.consumed..self.filled]
    }
}

impl<R: Read> InputCursor for StreamCursor<R> {
    fn position(&self) -> u64 {
        self.position
    }

    fn is_at_end(&mut self) -> Result<bool, DecodeError> {
        Ok(self.available()?.is_empty())
    }

    fn available(&mut self) -> Result<&[u8], DecodeError> {
        if self.consumed == self.filled && !self.at_end {
            self.refill()?;
        }
        Ok(&self.buffer[self.consumed..self.filled])
    }

    fn advance(&mut self, count: usize) {
        debug_assert!(count <= self.filled - self.consumed);
        let count = count.min(self.filled - self.consumed);
        self.consumed += count;
        self.position = self.position.saturating_add(count as u64);
    }

    fn verify_source_unchanged(&self) -> Result<(), DecodeError> {
        // A non-seekable source has no snapshotted length to re-check. End of
        // input was established by the reader returning zero, and the framing
        // loop has already refused to stop anywhere except a verified member
        // boundary.
        Ok(())
    }
}

/// Cursor over an already materialized byte slice.
///
/// Used to parse a header out of a stream's buffered prefix without consuming
/// it, so a streaming decoder can reject bad input before it spawns anything.
pub(crate) struct SliceCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> SliceCursor<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
}

impl InputCursor for SliceCursor<'_> {
    fn position(&self) -> u64 {
        self.position as u64
    }

    fn is_at_end(&mut self) -> Result<bool, DecodeError> {
        Ok(self.position >= self.bytes.len())
    }

    fn available(&mut self) -> Result<&[u8], DecodeError> {
        Ok(&self.bytes[self.position.min(self.bytes.len())..])
    }

    fn advance(&mut self, count: usize) {
        self.position = self.position.saturating_add(count).min(self.bytes.len());
    }

    fn verify_source_unchanged(&self) -> Result<(), DecodeError> {
        Ok(())
    }
}

/// Parsed member header.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MemberHeader {
    pub(crate) start: u64,
    pub(crate) deflate_start: u64,
    pub(crate) bgzf_block_size: Option<u16>,
}

fn checked_header_byte<C: InputCursor>(
    cursor: &mut C,
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
pub(crate) fn parse_member_header<C: InputCursor>(
    cursor: &mut C,
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
                if cursor.is_at_end()? {
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

/// Validates the first member header against a stream's buffered prefix.
///
/// The prefix is inspected in place, so nothing is consumed and the cursor is
/// still positioned at offset zero afterwards. This provides best-effort
/// fail-fast validation for `Decoder::stream_reader`: unlike a positional
/// source, an arbitrary `Read` may return a short prefix without reaching EOF.
///
/// A short read or a header longer than the buffered prefix cannot be validated
/// here. That case is reported as `Ok(())` and left to the decode itself, which
/// sees the whole header, rather than being mistaken for truncation. Genuine
/// truncation is distinguished by the stream having already reached its end.
pub(crate) fn validate_initial_stream_header<R: Read>(
    cursor: &mut StreamCursor<R>,
) -> Result<(), DecodeError> {
    // Populate the window without consuming from it.
    cursor.available()?;
    let stream_ended = cursor.at_end;
    let mut prefix = SliceCursor::new(cursor.buffered());
    match parse_member_header(&mut prefix, true) {
        Ok(_) => Ok(()),
        Err(DecodeError::InvalidGzip {
            reason: GzipErrorKind::Truncated | GzipErrorKind::UnterminatedHeaderField,
            ..
        }) if !stream_ended => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        InputCursor, SourceCursor, StreamCursor, parse_member_header,
        validate_initial_stream_header,
    };
    use crate::GzipErrorKind;

    /// Yields at most `step` bytes per read, the way a pipe does.
    struct Trickle<'a> {
        bytes: &'a [u8],
        step: usize,
    }

    impl super::Read for Trickle<'_> {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            let count = self.bytes.len().min(output.len()).min(self.step);
            output[..count].copy_from_slice(&self.bytes[..count]);
            self.bytes = &self.bytes[count..];
            Ok(count)
        }
    }

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

    #[test]
    fn stream_cursor_reads_across_window_refills() {
        let bytes: Vec<u8> = (0..=255_u8).collect();
        let mut cursor = StreamCursor::new(
            Trickle {
                bytes: &bytes,
                step: 3,
            },
            7,
        );

        let mut seen = Vec::new();
        while !cursor.is_at_end().unwrap() {
            // Consume less than is offered so the window keeps a live remainder
            // across the compaction that the next refill performs.
            let available = cursor.available().unwrap();
            let take = available.len().min(2);
            seen.extend_from_slice(&available[..take]);
            cursor.advance(take);
        }
        assert_eq!(seen, bytes);
        assert_eq!(cursor.position(), bytes.len() as u64);
    }

    #[test]
    fn stream_cursor_parses_a_header_from_a_trickling_source() {
        let bytes = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xffrest";
        let mut cursor = StreamCursor::new(
            Trickle {
                bytes: bytes.as_slice(),
                step: 1,
            },
            4,
        );
        let header = parse_member_header(&mut cursor, true).unwrap();
        assert_eq!(header.deflate_start, 10);
        assert_eq!(cursor.position(), 10);
    }

    #[test]
    fn initial_stream_validation_rejects_bad_magic_without_consuming() {
        let bytes = b"not gzip at all";
        let mut cursor = StreamCursor::new(
            Trickle {
                bytes: bytes.as_slice(),
                step: 4,
            },
            16,
        );
        let error = validate_initial_stream_header(&mut cursor).unwrap_err();
        assert!(matches!(
            error,
            crate::DecodeError::InvalidGzip {
                reason: GzipErrorKind::BadMagic,
                ..
            }
        ));
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn initial_stream_validation_defers_a_header_longer_than_the_window() {
        // FNAME set, with the name running past the first window.
        let mut bytes = b"\x1f\x8b\x08\x08\0\0\0\0\x00\xff".to_vec();
        bytes.extend_from_slice(&[b'n'; 64]);
        bytes.push(0);
        let mut cursor = StreamCursor::new(
            Trickle {
                bytes: &bytes,
                step: 8,
            },
            8,
        );
        validate_initial_stream_header(&mut cursor).unwrap();
        assert_eq!(cursor.position(), 0);
        // The full header is still readable afterwards.
        let header = parse_member_header(&mut cursor, true).unwrap();
        assert_eq!(header.deflate_start, bytes.len() as u64);
    }
}
