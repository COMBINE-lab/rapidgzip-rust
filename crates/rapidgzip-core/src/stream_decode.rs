//! Streaming decode for non-seekable [`std::io::Read`] inputs (stdin/pipes).
//!
//! Pulls compressed bytes on demand via a [`std::io::BufReader`]. Single-thread
//! paths (gzip, zlib, raw DEFLATE) never buffer the full archive. When
//! `decoder_threads > 1`, the stream is spilled to a private temporary file and
//! the positional [`crate::backend::decode_source`] path runs on that file
//! (parallel gzip / multi-stream or marker zlib / marker raw DEFLATE when each
//! format’s thread and size gates allow).

use crate::backend::{DirectOutput, Output, decode_source};
use crate::config::{Config, Format};
use crate::crc32::Crc32;
use crate::gzip::MemberHeader;
use crate::index::IndexBuilder;
use crate::inflate_backend::{
    ActiveInflater, InflateBackend, InflateFlush, status as inflate_status,
};
use crate::zlib::{Adler32, ZlibHeader, is_zlib_cmf_flg};
use crate::{DecodeError, DecodeReport, DeflateErrorKind, GzipErrorKind, ZlibErrorKind};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::sync::atomic::AtomicBool;

const FLAG_HEADER_CRC: u8 = 0x02;
const FLAG_EXTRA: u8 = 0x04;
const FLAG_NAME: u8 = 0x08;
const FLAG_COMMENT: u8 = 0x10;
const RESERVED_FLAGS: u8 = 0xE0;
const FLAG_DICT: u8 = 0x20;

/// Buffered cursor over a non-seekable [`Read`] source with a logical position.
struct StreamCursor<R> {
    reader: BufReader<R>,
    position: u64,
}

impl<R: Read> StreamCursor<R> {
    fn new(reader: R, capacity: usize) -> Self {
        Self {
            reader: BufReader::with_capacity(capacity.max(64), reader),
            position: 0,
        }
    }

    const fn position(&self) -> u64 {
        self.position
    }

    fn map_io(position: u64, error: io::Error) -> DecodeError {
        DecodeError::input_io(position, error)
    }

    fn truncated(truncated_at: u64) -> DecodeError {
        DecodeError::InvalidGzip {
            offset: truncated_at,
            reason: GzipErrorKind::Truncated,
        }
    }

    /// Returns true when the underlying reader is at EOF with an empty buffer.
    fn at_end(&mut self) -> Result<bool, DecodeError> {
        let pos = self.position;
        let buf = self.reader.fill_buf().map_err(|e| Self::map_io(pos, e))?;
        Ok(buf.is_empty())
    }

    fn byte(&mut self, truncated_at: u64) -> Result<u8, DecodeError> {
        // Prefer buffer when non-empty so we don't fight inflate's residual input.
        let pos = self.position;
        let buf = self.reader.fill_buf().map_err(|e| Self::map_io(pos, e))?;
        if let Some(&b) = buf.first() {
            self.reader.consume(1);
            self.position = self.position.saturating_add(1);
            return Ok(b);
        }
        // fill_buf returned empty → true EOF.
        Err(Self::truncated(truncated_at))
    }

    fn read_exact<const N: usize>(&mut self, truncated_at: u64) -> Result<[u8; N], DecodeError> {
        let mut result = [0_u8; N];
        // Drain any residual buffer first so multi-byte footers work after
        // inflate left partial trailer bytes in the BufReader.
        let pos = self.position;
        let take = {
            let buf = self.reader.fill_buf().map_err(|e| Self::map_io(pos, e))?;
            let take = buf.len().min(N);
            result[..take].copy_from_slice(&buf[..take]);
            take
        };
        let mut filled = take;
        if filled > 0 {
            self.reader.consume(filled);
            self.position = self.position.saturating_add(filled as u64);
        }
        while filled < N {
            match self.reader.read(&mut result[filled..]) {
                Ok(0) => return Err(Self::truncated(truncated_at)),
                Ok(n) => {
                    filled += n;
                    self.position = self.position.saturating_add(n as u64);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(Self::map_io(self.position, error)),
            }
        }
        Ok(result)
    }

    /// Peeks up to `n` bytes without consuming. May return fewer at EOF.
    fn peek(&mut self, n: usize) -> Result<&[u8], DecodeError> {
        let pos = self.position;
        let buf = self.reader.fill_buf().map_err(|e| Self::map_io(pos, e))?;
        Ok(&buf[..buf.len().min(n)])
    }

    /// Provides a live input page for inflate. Caller must [`Self::consume`] after.
    fn input_page(&mut self) -> Result<&[u8], DecodeError> {
        let pos = self.position;
        self.reader.fill_buf().map_err(|e| Self::map_io(pos, e))
    }

    fn consume(&mut self, count: usize) {
        self.reader.consume(count);
        self.position = self.position.saturating_add(count as u64);
    }
}

fn new_index_builder(config: &Config) -> IndexBuilder {
    IndexBuilder::new(
        config.keep_index,
        config.gather_line_offsets,
        config.checkpoint_spacing,
        config.compress_index_windows,
    )
}

fn finish_report(
    config: &Config,
    compressed_bytes: u64,
    decompressed_bytes: u64,
    member_count: u64,
    index_builder: IndexBuilder,
) -> DecodeReport {
    let line_count = index_builder.report_line_count();
    DecodeReport {
        compressed_bytes,
        decompressed_bytes,
        member_count,
        decoder_threads: config.decoder_threads,
        index: index_builder.finish(compressed_bytes, decompressed_bytes),
        line_count,
    }
}

fn checked_header_byte<R: Read>(
    cursor: &mut StreamCursor<R>,
    crc: &mut Option<Crc32>,
    start: u64,
) -> Result<u8, DecodeError> {
    let byte = cursor.byte(start)?;
    if let Some(crc) = crc {
        crc.update(&[byte]);
    }
    Ok(byte)
}

/// Parses a gzip member header at the cursor's current position.
fn parse_member_header<R: Read>(
    cursor: &mut StreamCursor<R>,
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
                if cursor.at_end()? {
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

fn map_zlib_truncate(start: u64, first_member: bool, error: DecodeError) -> DecodeError {
    match error {
        DecodeError::InvalidGzip {
            reason: GzipErrorKind::Truncated,
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

/// Parses a zlib stream header at the cursor's current position.
fn parse_zlib_header<R: Read>(
    cursor: &mut StreamCursor<R>,
    first_member: bool,
) -> Result<ZlibHeader, DecodeError> {
    let start = cursor.position();
    let [cmf, flg] = cursor
        .read_exact::<2>(start)
        .map_err(|error| map_zlib_truncate(start, first_member, error))?;

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
        let _dict_id = cursor
            .read_exact::<4>(start)
            .map_err(|error| map_zlib_truncate(start, true, error))?;
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

/// Peeks the first two buffered bytes and reports whether they look like zlib.
fn looks_like_zlib_stream<R: Read>(cursor: &mut StreamCursor<R>) -> Result<bool, DecodeError> {
    let peek = cursor.peek(2)?;
    if peek.len() < 2 {
        return Ok(false);
    }
    if peek == [0x1f, 0x8b] {
        return Ok(false);
    }
    Ok(is_zlib_cmf_flg(peek[0], peek[1]))
}

/// Resolves format for a streaming source (Auto → gzip or zlib; never raw).
fn resolve_stream_format<R: Read>(
    cursor: &mut StreamCursor<R>,
    config: &Config,
) -> Result<Format, DecodeError> {
    match config.format {
        Format::Gzip => Ok(Format::Gzip),
        Format::Zlib => Ok(Format::Zlib),
        Format::RawDeflate => Ok(Format::RawDeflate),
        Format::Auto => {
            if looks_like_zlib_stream(cursor)? {
                Ok(Format::Zlib)
            } else {
                Ok(Format::Gzip)
            }
        }
    }
}

/// Shared inflate-loop helper: pulls from the stream cursor, writes to output,
/// updates integrity state and the index builder. Returns on stream end.
///
/// Inflate goes through [`InflateBackend`] (monomorphized to [`RawInflater`]
/// at the gzip/zlib/raw call sites).
#[allow(clippy::too_many_arguments)]
fn inflate_until_stream_end<R, O, I>(
    cursor: &mut StreamCursor<R>,
    config: &Config,
    inflater: &mut I,
    flush: InflateFlush,
    index_builder: &mut IndexBuilder,
    total_output: &mut u64,
    member_output: &mut u32,
    mut on_output: impl FnMut(&[u8]),
    output: &mut O,
    deflate_start_bits: u64,
) -> Result<(), DecodeError>
where
    R: Read,
    O: Output,
    I: InflateBackend,
{
    let mut decoded = Vec::with_capacity(config.decoded_chunk_size);

    loop {
        if cursor.at_end()? {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: cursor.position().saturating_mul(8),
                reason: DeflateErrorKind::Truncated,
            });
        }

        if decoded.capacity() < config.decoded_chunk_size {
            decoded.reserve_exact(config.decoded_chunk_size - decoded.capacity());
        }
        // Sequential path always emits then clears; force empty so
        // InflateBackend appends into a fresh buffer for this step.
        decoded.clear();

        let input = cursor.input_page()?;
        let step = inflater.inflate(input, &mut decoded, flush)?;
        cursor.consume(step.consumed);

        if !decoded.is_empty() {
            let new_total = total_output.checked_add(decoded.len() as u64).ok_or(
                DecodeError::OutputLimitExceeded {
                    limit: config.output_limit.unwrap_or(u64::MAX),
                },
            )?;
            if config.output_limit.is_some_and(|limit| new_total > limit) {
                return Err(DecodeError::OutputLimitExceeded {
                    limit: config.output_limit.expect("checked as some"),
                });
            }
            *total_output = new_total;
            *member_output = member_output.wrapping_add(decoded.len() as u32);
            on_output(&decoded);
            index_builder.push_output(&decoded);
            decoded = output.emit_reusable(decoded)?;
        }

        // Bit-accurate offset: bytes consumed so far, less bits still in the
        // inflater bit buffer. Only meaningful with Block flush; with NoFlush
        // keep_index is false and this is unused for raw DEFLATE.
        if config.keep_index {
            let bit_pos = cursor
                .position()
                .saturating_mul(8)
                .saturating_sub(u64::from(step.unused_bits));
            if step.status == inflate_status::STREAM_END || step.at_block_end {
                index_builder.maybe_checkpoint(bit_pos);
            }
        }

        match step.status {
            inflate_status::STREAM_END => return Ok(()),
            inflate_status::OK => {
                if step.consumed == 0 && step.produced == 0 {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: cursor.position().saturating_mul(8),
                        reason: DeflateErrorKind::Stalled,
                    });
                }
            }
            inflate_status::BUF_ERROR if step.consumed > 0 || step.produced > 0 => {}
            inflate_status::BUF_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
                });
            }
            inflate_status::NEED_DICT => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: deflate_start_bits,
                    reason: DeflateErrorKind::UnexpectedDictionary,
                });
            }
            inflate_status::DATA_ERROR => {
                let _diagnostic = inflater.message();
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::InvalidData,
                });
            }
            other => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::BackendStatus(other),
                });
            }
        }
    }
}

fn decode_gzip_stream<R, O>(
    cursor: &mut StreamCursor<R>,
    config: &Config,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: Read,
    O: Output,
{
    let mut index_builder = new_index_builder(config);
    let mut total_output = 0_u64;
    let mut member_count = 0_u64;
    let flush = if config.keep_index {
        InflateFlush::Block
    } else {
        InflateFlush::NoFlush
    };
    // Reuse one raw-inflate stream across concatenated gzip members. Each member
    // starts with an empty window; `reset` clears history.
    let mut inflater = <ActiveInflater as InflateBackend>::create()?;

    while !cursor.at_end()? {
        let header = parse_member_header(cursor, member_count == 0)?;
        debug_assert!(header.start <= header.deflate_start);
        debug_assert_eq!(header.deflate_start, cursor.position());
        let _observed_bgzf_size = header.bgzf_block_size;
        index_builder.force_checkpoint(header.deflate_start.saturating_mul(8), true);

        inflater.reset(header.deflate_start.saturating_mul(8))?;
        let mut crc = Crc32::new();
        let mut member_output = 0_u32;

        inflate_until_stream_end(
            cursor,
            config,
            &mut inflater,
            flush,
            &mut index_builder,
            &mut total_output,
            &mut member_output,
            |bytes| {
                if config.crc32_enabled {
                    crc.update(bytes);
                }
            },
            output,
            header.deflate_start.saturating_mul(8),
        )?;

        let footer_offset = cursor.position();
        let footer = cursor.read_exact::<8>(footer_offset)?;
        let expected_crc = u32::from_le_bytes(footer[0..4].try_into().expect("four bytes"));
        let expected_size = u32::from_le_bytes(footer[4..8].try_into().expect("four bytes"));
        if config.crc32_enabled {
            let actual_crc = crc.finish();
            if expected_crc != actual_crc {
                return Err(DecodeError::ChecksumMismatch {
                    member: member_count,
                    expected: expected_crc,
                    actual: actual_crc,
                });
            }
        }
        if expected_size != member_output {
            return Err(DecodeError::SizeMismatch {
                member: member_count,
                expected: expected_size,
                actual_mod32: member_output,
            });
        }

        member_count += 1;
    }

    if member_count == 0 {
        return Err(DecodeError::InvalidGzip {
            offset: 0,
            reason: GzipErrorKind::BadMagic,
        });
    }

    Ok(finish_report(
        config,
        cursor.position(),
        total_output,
        member_count,
        index_builder,
    ))
}

fn decode_zlib_stream<R, O>(
    cursor: &mut StreamCursor<R>,
    config: &Config,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: Read,
    O: Output,
{
    let mut index_builder = new_index_builder(config);
    let mut total_output = 0_u64;
    let mut member_count = 0_u64;
    let flush = if config.keep_index {
        InflateFlush::Block
    } else {
        InflateFlush::NoFlush
    };
    // Reuse one raw-inflate stream across concatenated zlib members. Each member
    // starts with an empty window; `reset` clears history.
    let mut inflater = <ActiveInflater as InflateBackend>::create()?;

    while !cursor.at_end()? {
        let header = parse_zlib_header(cursor, member_count == 0)?;
        debug_assert!(header.start <= header.deflate_start);
        debug_assert_eq!(header.deflate_start, cursor.position());
        index_builder.force_checkpoint(header.deflate_start.saturating_mul(8), true);

        inflater.reset(header.deflate_start.saturating_mul(8))?;
        let mut adler = Adler32::new();
        let mut member_output = 0_u32;

        inflate_until_stream_end(
            cursor,
            config,
            &mut inflater,
            flush,
            &mut index_builder,
            &mut total_output,
            &mut member_output,
            |bytes| {
                if config.crc32_enabled {
                    adler.update(bytes);
                }
            },
            output,
            header.deflate_start.saturating_mul(8),
        )?;

        let footer_offset = cursor.position();
        let footer = match cursor.read_exact::<4>(footer_offset) {
            Ok(bytes) => bytes,
            Err(DecodeError::InvalidGzip {
                reason: GzipErrorKind::Truncated,
                ..
            }) => {
                return Err(DecodeError::InvalidZlib {
                    offset: footer_offset,
                    reason: ZlibErrorKind::Truncated,
                });
            }
            Err(error) => return Err(error),
        };
        let expected_adler = u32::from_be_bytes(footer);
        if config.crc32_enabled {
            let actual_adler = adler.finish();
            if expected_adler != actual_adler {
                return Err(DecodeError::ChecksumMismatch {
                    member: member_count,
                    expected: expected_adler,
                    actual: actual_adler,
                });
            }
        }

        member_count += 1;
    }

    if member_count == 0 {
        return Err(DecodeError::InvalidZlib {
            offset: 0,
            reason: ZlibErrorKind::BadHeader,
        });
    }

    Ok(finish_report(
        config,
        cursor.position(),
        total_output,
        member_count,
        index_builder,
    ))
}

fn decode_raw_deflate_stream<R, O>(
    cursor: &mut StreamCursor<R>,
    config: &Config,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: Read,
    O: Output,
{
    debug_assert!(
        !config.keep_index,
        "keep_index for raw DEFLATE is rejected by DecoderBuilder::build"
    );

    if cursor.at_end()? {
        return Err(DecodeError::InvalidDeflate {
            bit_offset: 0,
            reason: DeflateErrorKind::Truncated,
        });
    }

    let mut index_builder = new_index_builder(config);
    let mut total_output = 0_u64;
    let mut inflater = <ActiveInflater as InflateBackend>::create()?;
    let mut member_output = 0_u32;
    let verify_external_crc = !config.raw_crc32_list.is_empty();
    let mut external_crc = Crc32::new();

    inflate_until_stream_end(
        cursor,
        config,
        &mut inflater,
        InflateFlush::NoFlush,
        &mut index_builder,
        &mut total_output,
        &mut member_output,
        |bytes| {
            if verify_external_crc {
                external_crc.update(bytes);
            }
        },
        output,
        0,
    )?;

    // Single stream must consume the entire source (no trailer, no concat).
    if !cursor.at_end()? {
        return Err(DecodeError::InvalidDeflate {
            bit_offset: cursor.position().saturating_mul(8),
            reason: DeflateErrorKind::InvalidData,
        });
    }

    crate::crc32::verify_raw_crc32_list(&config.raw_crc32_list, &external_crc)?;

    Ok(finish_report(
        config,
        cursor.position(),
        total_output,
        1,
        index_builder,
    ))
}

/// Decodes gzip, zlib, or raw DEFLATE by pulling from `reader` as needed.
///
/// **Sequential** when `decoder_threads == 1`: compressed-side memory is a small
/// input page plus inflate state (not the full archive). Pure streaming; no
/// temporary file.
///
/// **Spill + positional backend** when `decoder_threads > 1`: the full compressed
/// stream is copied to a private temporary file (secure temp-dir defaults;
/// deleted on drop), then [`decode_source`] runs with the resolved format forced
/// (so [`Format::Auto`] cannot re-route after a successful peek). That path uses
/// the same parallel gates as file/`ReadAt` input: parallel gzip, multi-stream
/// or marker zlib, and marker raw DEFLATE when threads/size allow.
/// Peak cost is roughly the compressed size **on disk** plus the decoder working
/// set (not a second full RAM copy of the archive).
pub(crate) fn decode_read_stream<R, W>(
    reader: R,
    config: &Config,
    output: &mut W,
) -> Result<DecodeReport, DecodeError>
where
    R: Read,
    W: Write,
{
    let page = config.input_page_size.max(64);
    let mut cursor = StreamCursor::new(reader, page);
    let format = resolve_stream_format(&mut cursor, config)?;
    if config.decoder_threads > 1 {
        let temp = spill_cursor_to_tempfile(cursor)?;
        let cancelled = AtomicBool::new(false);
        let mut sink = DirectOutput::new(output);
        // Force the peeked/forced concrete format so Auto cannot re-route.
        let mut parallel_config = config.clone();
        parallel_config.format = format;
        return decode_source(temp.as_file(), &parallel_config, &cancelled, &mut sink);
    }
    let mut sink = DirectOutput::new(output);
    match format {
        Format::Gzip => decode_gzip_stream(&mut cursor, config, &mut sink),
        Format::Zlib => decode_zlib_stream(&mut cursor, config, &mut sink),
        Format::RawDeflate => decode_raw_deflate_stream(&mut cursor, config, &mut sink),
        Format::Auto => unreachable!("resolve_stream_format returns concrete formats only"),
    }
}

/// Copy the remaining stream (including any BufReader residual) to a private
/// tempfile suitable for concurrent [`crate::ReadAt`] access.
///
/// Callers must not have consumed bytes from `cursor` yet (format resolution
/// only peeks), so the tempfile starts at compressed offset 0.
fn spill_cursor_to_tempfile<R: Read>(
    cursor: StreamCursor<R>,
) -> Result<tempfile::NamedTempFile, DecodeError> {
    debug_assert_eq!(
        cursor.position, 0,
        "spill_cursor_to_tempfile expects an unconsumed stream"
    );
    let mut temp = tempfile::Builder::new()
        .prefix("rapidgzip-")
        .suffix(".bin")
        .tempfile()
        .map_err(|error| DecodeError::input_io(0, error))?;
    // BufReader::read drains its internal buffer first, then the underlying Read.
    let mut reader = cursor.reader;
    io::copy(&mut reader, temp.as_file_mut()).map_err(|error| DecodeError::input_io(0, error))?;
    // Ensure subsequent ReadAt sees a stable length on all platforms.
    temp.as_file_mut()
        .flush()
        .map_err(|error| DecodeError::input_io(0, error))?;
    Ok(temp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn stored_deflate(bytes: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        if bytes.is_empty() {
            encoded.extend_from_slice(&[1, 0, 0, 0xFF, 0xFF]);
            return encoded;
        }
        let chunks = bytes.chunks(u16::MAX as usize);
        let chunk_count = chunks.len();
        for (index, chunk) in chunks.enumerate() {
            encoded.push(u8::from(index + 1 == chunk_count));
            let length = chunk.len() as u16;
            encoded.extend_from_slice(&length.to_le_bytes());
            encoded.extend_from_slice(&(!length).to_le_bytes());
            encoded.extend_from_slice(chunk);
        }
        encoded
    }

    fn member_crc(bytes: &[u8]) -> u32 {
        let mut crc = Crc32::new();
        crc.update(bytes);
        crc.finish()
    }

    fn member(bytes: &[u8]) -> Vec<u8> {
        let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
        encoded.extend_from_slice(&stored_deflate(bytes));
        encoded.extend_from_slice(&member_crc(bytes).to_le_bytes());
        encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        encoded
    }

    #[test]
    fn streams_multi_member_gzip() {
        let mut compressed = member(b"first\n");
        compressed.extend(member(b""));
        compressed.extend(member(b"second\n"));
        let config = Config {
            decoder_threads: 4,
            decoded_chunk_size: 64 * 1024,
            input_page_size: 256,
            compressed_chunk_size: 1024 * 1024,
            in_flight_chunks: 4,
            output_limit: None,
            format: Format::Gzip,
            crc32_enabled: true,
            keep_index: false,
            gather_line_offsets: false,
            checkpoint_spacing: 4 * 1024 * 1024,
            seek_cache_max_chunks: 0,
            seek_cache_max_bytes: 0,
            seek_readahead: false,
            seek_prefetch_windows: 0,
            compress_index_windows: true,
            raw_crc32_list: Vec::new(),
        };
        let mut decoded = Vec::new();
        let report =
            decode_read_stream(Cursor::new(compressed.as_slice()), &config, &mut decoded).unwrap();
        assert_eq!(decoded, b"first\nsecond\n");
        assert_eq!(report.member_count, 3);
        assert_eq!(report.compressed_bytes, compressed.len() as u64);
    }

    /// One-byte-at-a-time reader to force true streaming refill behaviour.
    struct ByteAtATime<'a> {
        data: &'a [u8],
        pos: usize,
    }

    impl Read for ByteAtATime<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.pos >= self.data.len() || buf.is_empty() {
                return Ok(0);
            }
            buf[0] = self.data[self.pos];
            self.pos += 1;
            Ok(1)
        }
    }

    #[test]
    fn streams_from_one_byte_reader() {
        let compressed = member(b"byte-at-a-time");
        let config = Config {
            decoder_threads: 1,
            decoded_chunk_size: 4096,
            input_page_size: 64,
            compressed_chunk_size: 1024 * 1024,
            in_flight_chunks: 2,
            output_limit: None,
            format: Format::Auto,
            crc32_enabled: true,
            keep_index: false,
            gather_line_offsets: false,
            checkpoint_spacing: 4 * 1024 * 1024,
            seek_cache_max_chunks: 0,
            seek_cache_max_bytes: 0,
            seek_readahead: false,
            seek_prefetch_windows: 0,
            compress_index_windows: true,
            raw_crc32_list: Vec::new(),
        };
        let mut decoded = Vec::new();
        let report = decode_read_stream(
            ByteAtATime {
                data: &compressed,
                pos: 0,
            },
            &config,
            &mut decoded,
        )
        .unwrap();
        assert_eq!(decoded, b"byte-at-a-time");
        assert_eq!(report.member_count, 1);
    }

    fn adler32(bytes: &[u8]) -> u32 {
        const BASE: u32 = 65521;
        let mut s1 = 1_u32;
        let mut s2 = 0_u32;
        for &byte in bytes {
            s1 = (s1 + u32::from(byte)) % BASE;
            s2 = (s2 + s1) % BASE;
        }
        (s2 << 16) | s1
    }

    fn zlib_member(bytes: &[u8]) -> Vec<u8> {
        let mut encoded = vec![0x78, 0x01];
        encoded.extend_from_slice(&stored_deflate(bytes));
        encoded.extend_from_slice(&adler32(bytes).to_be_bytes());
        encoded
    }

    fn test_config(threads: usize, format: Format) -> Config {
        Config {
            decoder_threads: threads,
            decoded_chunk_size: 64 * 1024,
            input_page_size: 256,
            compressed_chunk_size: 1024 * 1024,
            in_flight_chunks: 4,
            output_limit: None,
            format,
            crc32_enabled: true,
            keep_index: false,
            gather_line_offsets: false,
            checkpoint_spacing: 4 * 1024 * 1024,
            seek_cache_max_chunks: 0,
            seek_cache_max_bytes: 0,
            seek_readahead: false,
            seek_prefetch_windows: 0,
            compress_index_windows: true,
            raw_crc32_list: Vec::new(),
        }
    }

    #[test]
    fn multi_thread_zlib_spill_matches_sequential() {
        // Multi-member zlib so stream-granularity parallel can activate after spill.
        let mut compressed = zlib_member(b"first zlib stream\n");
        compressed.extend(zlib_member(b""));
        compressed.extend(zlib_member(b"second zlib stream\n"));
        compressed.extend(zlib_member(b"third\n"));
        let expected = b"first zlib stream\nsecond zlib stream\nthird\n";

        let sequential = test_config(1, Format::Zlib);
        let mut seq_out = Vec::new();
        let seq_report = decode_read_stream(
            Cursor::new(compressed.as_slice()),
            &sequential,
            &mut seq_out,
        )
        .unwrap();

        let parallel = test_config(4, Format::Zlib);
        let mut par_out = Vec::new();
        let par_report =
            decode_read_stream(Cursor::new(compressed.as_slice()), &parallel, &mut par_out)
                .unwrap();

        assert_eq!(par_out, seq_out);
        assert_eq!(par_out, expected);
        assert_eq!(par_report.member_count, seq_report.member_count);
        assert_eq!(par_report.member_count, 4);
        assert_eq!(par_report.compressed_bytes, seq_report.compressed_bytes);
        assert_eq!(par_report.decompressed_bytes, seq_report.decompressed_bytes);
    }

    #[test]
    fn multi_thread_raw_deflate_spill_matches_sequential() {
        let payload = b"raw deflate spill payload for multi-thread decode_read";
        let compressed = stored_deflate(payload);

        let sequential = test_config(1, Format::RawDeflate);
        let mut seq_out = Vec::new();
        let seq_report = decode_read_stream(
            Cursor::new(compressed.as_slice()),
            &sequential,
            &mut seq_out,
        )
        .unwrap();

        let parallel = test_config(4, Format::RawDeflate);
        let mut par_out = Vec::new();
        let par_report =
            decode_read_stream(Cursor::new(compressed.as_slice()), &parallel, &mut par_out)
                .unwrap();

        assert_eq!(par_out, seq_out);
        assert_eq!(par_out, payload);
        assert_eq!(par_report.member_count, 1);
        assert_eq!(par_report.member_count, seq_report.member_count);
        assert_eq!(par_report.compressed_bytes, seq_report.compressed_bytes);
    }

    #[test]
    fn single_thread_zlib_and_raw_stream_without_spill_semantics() {
        // Threads == 1 stays on pure streaming paths (Limited-style small pages).
        let zlib = zlib_member(b"single-thread zlib");
        let raw = stored_deflate(b"single-thread raw");

        let mut zlib_out = Vec::new();
        let zlib_report = decode_read_stream(
            Cursor::new(zlib.as_slice()),
            &test_config(1, Format::Zlib),
            &mut zlib_out,
        )
        .unwrap();
        assert_eq!(zlib_out, b"single-thread zlib");
        assert_eq!(zlib_report.member_count, 1);

        let mut raw_out = Vec::new();
        let raw_report = decode_read_stream(
            Cursor::new(raw.as_slice()),
            &test_config(1, Format::RawDeflate),
            &mut raw_out,
        )
        .unwrap();
        assert_eq!(raw_out, b"single-thread raw");
        assert_eq!(raw_report.member_count, 1);
    }
}
