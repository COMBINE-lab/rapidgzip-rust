//! Sequential gzip / BGZF / zlib / raw DEFLATE structure analysis.
//!
//! Walks members and DEFLATE blocks with raw inflate via
//! [`InflateBackend`] and [`InflateFlush::Block`] so callers can inspect
//! framing without writing payload. Used by the CLI `--analyze` path and
//! available as [`crate::Decoder::analyze`]. Monomorphized to zlib-rs
//! [`RawInflater`] today.
//!
//! Supports gzip (including BGZF), zlib (RFC 1950), and raw DEFLATE (RFC 1951
//! via explicit [`Format::RawDeflate`]; never auto-selected).

use crate::backend::RawInflater;
use crate::config::Format;
use crate::crc32::Crc32;
use crate::gzip::{SourceCursor, parse_member_header};
use crate::inflate_backend::{InflateBackend, InflateFlush, status as inflate_status};
use crate::zlib::{Adler32, looks_like_zlib, parse_zlib_header};
use crate::{DecodeError, DeflateErrorKind, GzipErrorKind, ReadAt, ZlibErrorKind};
use std::fmt::{self, Display, Formatter};

/// Container kind reported by structure analysis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ArchiveKind {
    /// Gzip members (including BGZF blocks as separate members).
    Gzip,
    /// zlib (RFC 1950) streams.
    Zlib,
    /// Raw DEFLATE (RFC 1951) with no gzip/zlib wrapper.
    ///
    /// Only selected via explicit [`Format::RawDeflate`]; never auto-detected.
    RawDeflate,
}

impl Display for ArchiveKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Gzip => formatter.write_str("gzip"),
            Self::Zlib => formatter.write_str("zlib"),
            Self::RawDeflate => formatter.write_str("raw DEFLATE"),
        }
    }
}

/// DEFLATE block coding type (`BTYPE` in RFC 1951).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeflateBlockType {
    /// Non-compressed stored block (`BTYPE = 00`).
    Stored,
    /// Fixed Huffman codes (`BTYPE = 01`).
    Fixed,
    /// Dynamic Huffman codes (`BTYPE = 10`).
    Dynamic,
    /// Reserved / invalid type (`BTYPE = 11`); value is the raw two-bit field.
    Reserved(u8),
}

impl DeflateBlockType {
    fn from_btype(bits: u8) -> Self {
        match bits & 0b11 {
            0b00 => Self::Stored,
            0b01 => Self::Fixed,
            0b10 => Self::Dynamic,
            other => Self::Reserved(other),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Stored => "stored",
            Self::Fixed => "fixed",
            Self::Dynamic => "dynamic",
            Self::Reserved(_) => "reserved",
        }
    }
}

impl Display for DeflateBlockType {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reserved(raw) => write!(formatter, "reserved({raw})"),
            other => formatter.write_str(other.as_str()),
        }
    }
}

/// One DEFLATE block within a member / zlib stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeflateBlockInfo {
    /// Zero-based index of this block within its member.
    pub index: u64,
    /// Coding type of the block.
    pub block_type: DeflateBlockType,
    /// Whether the DEFLATE `BFINAL` bit was set.
    pub is_final: bool,
    /// Absolute compressed bit offset of the block header (`BFINAL`/`BTYPE`).
    pub compressed_bit_start: u64,
    /// Absolute compressed bit offset of the end of this block (start of the
    /// next block, or the byte-aligned DEFLATE stream end after a final block).
    pub compressed_bit_end: u64,
    /// Uncompressed size produced by this block, in bytes.
    pub uncompressed_size: u64,
}

/// Analysis of a single gzip / BGZF member or zlib stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberAnalysis {
    /// Zero-based member index in the archive.
    pub index: u64,
    /// Absolute compressed byte offset of the member / stream header.
    pub compressed_start: u64,
    /// Absolute compressed byte offset of the first byte after the footer.
    pub compressed_end: u64,
    /// Absolute compressed byte offset where the raw DEFLATE payload begins.
    pub deflate_start: u64,
    /// Total uncompressed size of this member, in bytes.
    pub uncompressed_size: u64,
    /// BGZF `BC` subfield block size (BSIZE), when present (gzip only).
    pub bgzf_block_size: Option<u16>,
    /// zlib CMF/FLG header bytes when this is a zlib stream; `None` for gzip
    /// and raw DEFLATE.
    pub zlib_header: Option<(u8, u8)>,
    /// Integrity footer check: gzip CRC32 or zlib Adler-32.
    ///
    /// `Some(true/false)` when verification ran, `None` when checking was
    /// disabled (`crc32_enabled = false`) or when the format has no integrity
    /// trailer (raw DEFLATE). Successful analysis only returns `Some(true)` or
    /// `None`; a mismatch fails the call.
    pub crc32_ok: Option<bool>,
    /// Whether the gzip ISIZE footer matched the uncompressed length mod 2³².
    ///
    /// Always `true` for zlib and raw DEFLATE (no ISIZE field).
    pub isize_ok: bool,
    /// DEFLATE blocks in decode order.
    pub blocks: Vec<DeflateBlockInfo>,
}

/// Structured description of a gzip, BGZF, zlib, or raw DEFLATE archive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveAnalysis {
    /// Container kind (gzip/BGZF, zlib, or raw DEFLATE).
    pub kind: ArchiveKind,
    /// Members / streams in file order (each BGZF block is one member; raw
    /// DEFLATE is always a single stream).
    pub members: Vec<MemberAnalysis>,
    /// Total compressed size covered (end of last member footer, or end of the
    /// raw DEFLATE stream).
    pub compressed_bytes: u64,
    /// Sum of per-member uncompressed sizes.
    pub uncompressed_bytes: u64,
}

impl ArchiveAnalysis {
    /// Number of gzip / BGZF members, zlib streams, or raw DEFLATE streams.
    pub fn member_count(&self) -> u64 {
        self.members.len() as u64
    }
}

impl Display for ArchiveAnalysis {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.kind {
            ArchiveKind::Gzip => writeln!(formatter, "gzip archive analysis")?,
            ArchiveKind::Zlib => writeln!(formatter, "zlib archive analysis")?,
            ArchiveKind::RawDeflate => writeln!(formatter, "raw DEFLATE analysis")?,
        }
        let unit = match self.kind {
            ArchiveKind::Gzip => "members",
            ArchiveKind::Zlib | ArchiveKind::RawDeflate => "streams",
        };
        writeln!(formatter, "  {unit}: {}", self.member_count())?;
        writeln!(formatter, "  compressed: {} bytes", self.compressed_bytes)?;
        writeln!(
            formatter,
            "  uncompressed: {} bytes",
            self.uncompressed_bytes
        )?;
        for member in &self.members {
            writeln!(formatter)?;
            write!(
                formatter,
                "{}",
                MemberDisplay {
                    member,
                    kind: self.kind,
                }
            )?;
        }
        Ok(())
    }
}

/// Display adapter so member formatting can depend on archive kind.
struct MemberDisplay<'a> {
    member: &'a MemberAnalysis,
    kind: ArchiveKind,
}

impl Display for MemberDisplay<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let member = self.member;
        let compressed_len = member
            .compressed_end
            .saturating_sub(member.compressed_start);
        let label = match self.kind {
            ArchiveKind::Gzip => "member",
            ArchiveKind::Zlib | ArchiveKind::RawDeflate => "stream",
        };
        writeln!(formatter, "{label} {}", member.index)?;
        writeln!(
            formatter,
            "  compressed: [{}, {}) ({} bytes)",
            member.compressed_start, member.compressed_end, compressed_len
        )?;
        writeln!(formatter, "  deflate_start: {}", member.deflate_start)?;
        if let Some((cmf, flg)) = member.zlib_header {
            writeln!(formatter, "  header: CMF={cmf:#04x} FLG={flg:#04x}")?;
        }
        writeln!(
            formatter,
            "  uncompressed: {} bytes",
            member.uncompressed_size
        )?;
        match self.kind {
            ArchiveKind::Gzip => {
                match member.bgzf_block_size {
                    Some(size) => writeln!(formatter, "  bgzf_block_size: {size}")?,
                    None => writeln!(formatter, "  bgzf_block_size: none")?,
                }
                let crc = match member.crc32_ok {
                    Some(true) => "ok",
                    Some(false) => "mismatch",
                    None => "skipped",
                };
                let isize = if member.isize_ok { "ok" } else { "mismatch" };
                writeln!(formatter, "  footer: crc={crc} isize={isize}")?;
            }
            ArchiveKind::Zlib => {
                let adler = match member.crc32_ok {
                    Some(true) => "ok",
                    Some(false) => "mismatch",
                    None => "skipped",
                };
                writeln!(formatter, "  footer: adler32={adler}")?;
            }
            ArchiveKind::RawDeflate => {
                // No container footer or integrity trailer (RFC 1951).
                writeln!(formatter, "  footer: none")?;
            }
        }
        writeln!(formatter, "  blocks: {}", member.blocks.len())?;
        for block in &member.blocks {
            writeln!(
                formatter,
                "    block {}: type={} final={} compressed_bits=[{}, {}) uncompressed={}",
                block.index,
                block.block_type,
                block.is_final,
                block.compressed_bit_start,
                block.compressed_bit_end,
                block.uncompressed_size
            )?;
        }
        Ok(())
    }
}

impl Display for MemberAnalysis {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        // Infer container kind when displayed alone (prefer explicit fields).
        let kind = if self.zlib_header.is_some() {
            ArchiveKind::Zlib
        } else if self.deflate_start == self.compressed_start {
            // Raw DEFLATE has no wrapper bytes before the first block bit.
            ArchiveKind::RawDeflate
        } else {
            ArchiveKind::Gzip
        };
        write!(formatter, "{}", MemberDisplay { member: self, kind })
    }
}

/// Walks `source` sequentially and reports members / streams and DEFLATE blocks.
///
/// Auto-detects gzip (`1f 8b`) vs zlib (RFC 1950 CMF/FLG). Does not require an
/// index. Integrity checks (gzip CRC32 / zlib Adler-32) run when
/// `crc32_enabled` is true; gzip ISIZE is always checked. On corruption the
/// call fails with a [`DecodeError`] (no partial [`ArchiveAnalysis`] is
/// returned).
///
/// Raw DEFLATE is never auto-selected (no magic bytes); use
/// [`analyze_source_with_format`] with [`Format::RawDeflate`].
///
/// # Errors
///
/// Returns the same class of framing, DEFLATE, checksum, and I/O errors as
/// full decode paths.
pub fn analyze_source<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
    crc32_enabled: bool,
) -> Result<ArchiveAnalysis, DecodeError> {
    analyze_source_with_format(source, page_size, crc32_enabled, Format::Auto)
}

/// Like [`analyze_source`], but respects an explicit [`Format`] selection.
///
/// [`Format::Auto`] inspects the prefix (zlib CMF/FLG vs gzip magic) and never
/// selects raw DEFLATE. Forced [`Format::Gzip`] / [`Format::Zlib`] require that
/// wrapper. [`Format::RawDeflate`] walks a single DEFLATE stream from bit 0
/// (no integrity trailer; trailing bytes after EOS are an error).
///
/// # Errors
///
/// Returns framing, DEFLATE, checksum, or I/O errors when the archive is
/// truncated, the wrong format, or corrupt.
pub fn analyze_source_with_format<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
    crc32_enabled: bool,
    format: Format,
) -> Result<ArchiveAnalysis, DecodeError> {
    let page_size = page_size.max(1);
    let kind = resolve_analyze_format(source, page_size, format)?;
    match kind {
        ArchiveKind::Gzip => analyze_gzip(source, page_size, crc32_enabled),
        ArchiveKind::Zlib => analyze_zlib(source, page_size, crc32_enabled),
        ArchiveKind::RawDeflate => analyze_raw_deflate(source, page_size),
    }
}

fn resolve_analyze_format<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
    format: Format,
) -> Result<ArchiveKind, DecodeError> {
    match format {
        Format::Gzip => Ok(ArchiveKind::Gzip),
        Format::Zlib => Ok(ArchiveKind::Zlib),
        Format::RawDeflate => Ok(ArchiveKind::RawDeflate),
        Format::Auto => {
            if looks_like_zlib(source, page_size)? {
                Ok(ArchiveKind::Zlib)
            } else if looks_like_gzip_magic(source, page_size)? {
                Ok(ArchiveKind::Gzip)
            } else {
                // Neither gzip magic nor a legal zlib header: Auto never
                // selects raw DEFLATE (match decode policy).
                Err(DecodeError::InvalidGzip {
                    offset: 0,
                    reason: GzipErrorKind::BadMagic,
                })
            }
        }
    }
}

fn looks_like_gzip_magic<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
) -> Result<bool, DecodeError> {
    let mut cursor = SourceCursor::new(source, page_size)?;
    if cursor.at_end() {
        return Ok(false);
    }
    match cursor.read_exact::<2>(0) {
        Ok(bytes) => Ok(bytes == [0x1f, 0x8b]),
        Err(DecodeError::InvalidGzip {
            reason: GzipErrorKind::Truncated,
            ..
        }) => Ok(false),
        Err(error) => Err(error),
    }
}

fn analyze_gzip<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
    crc32_enabled: bool,
) -> Result<ArchiveAnalysis, DecodeError> {
    let mut cursor = SourceCursor::new(source, page_size)?;
    let mut members = Vec::new();
    let mut total_uncompressed = 0_u64;

    while !cursor.at_end() {
        let member = analyze_gzip_member(source, &mut cursor, members.len() as u64, crc32_enabled)?;
        total_uncompressed = total_uncompressed
            .checked_add(member.uncompressed_size)
            .ok_or(DecodeError::OutputLimitExceeded { limit: u64::MAX })?;
        members.push(member);
    }

    if members.is_empty() {
        return Err(DecodeError::InvalidGzip {
            offset: 0,
            reason: GzipErrorKind::BadMagic,
        });
    }

    let compressed_bytes = members
        .last()
        .map(|member| member.compressed_end)
        .unwrap_or(0);
    Ok(ArchiveAnalysis {
        kind: ArchiveKind::Gzip,
        members,
        compressed_bytes,
        uncompressed_bytes: total_uncompressed,
    })
}

fn analyze_zlib<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
    crc32_enabled: bool,
) -> Result<ArchiveAnalysis, DecodeError> {
    let mut cursor = SourceCursor::new(source, page_size)?;
    let mut members = Vec::new();
    let mut total_uncompressed = 0_u64;

    while !cursor.at_end() {
        let member = analyze_zlib_stream(source, &mut cursor, members.len() as u64, crc32_enabled)?;
        total_uncompressed = total_uncompressed
            .checked_add(member.uncompressed_size)
            .ok_or(DecodeError::OutputLimitExceeded { limit: u64::MAX })?;
        members.push(member);
    }

    if members.is_empty() {
        return Err(DecodeError::InvalidZlib {
            offset: 0,
            reason: ZlibErrorKind::BadHeader,
        });
    }

    let compressed_bytes = members
        .last()
        .map(|member| member.compressed_end)
        .unwrap_or(0);
    Ok(ArchiveAnalysis {
        kind: ArchiveKind::Zlib,
        members,
        compressed_bytes,
        uncompressed_bytes: total_uncompressed,
    })
}

/// Single raw DEFLATE stream from bit 0 to `Z_STREAM_END`.
///
/// No integrity trailer. Trailing bytes after EOS match the decode path and
/// fail with [`DeflateErrorKind::InvalidData`]. Empty input is truncated.
fn analyze_raw_deflate<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
) -> Result<ArchiveAnalysis, DecodeError> {
    let mut cursor = SourceCursor::new(source, page_size)?;
    if cursor.at_end() {
        return Err(DecodeError::InvalidDeflate {
            bit_offset: 0,
            reason: DeflateErrorKind::Truncated,
        });
    }

    let deflate_start = cursor.position();
    debug_assert_eq!(deflate_start, 0);

    let walk =
        walk_deflate_blocks::<_, _, RawInflater>(source, &mut cursor, deflate_start, |_| Ok(()))?;

    // Single stream must consume the entire source (no trailer, no concat).
    if !cursor.at_end() {
        return Err(DecodeError::InvalidDeflate {
            bit_offset: cursor.position().saturating_mul(8),
            reason: DeflateErrorKind::InvalidData,
        });
    }

    let member = MemberAnalysis {
        index: 0,
        compressed_start: deflate_start,
        compressed_end: cursor.position(),
        deflate_start,
        uncompressed_size: walk.uncompressed_size,
        bgzf_block_size: None,
        zlib_header: None,
        // No gzip CRC / zlib Adler trailer for raw DEFLATE.
        crc32_ok: None,
        isize_ok: true,
        blocks: walk.blocks,
    };

    Ok(ArchiveAnalysis {
        kind: ArchiveKind::RawDeflate,
        compressed_bytes: member.compressed_end,
        uncompressed_bytes: member.uncompressed_size,
        members: vec![member],
    })
}

fn analyze_gzip_member<R: ReadAt + ?Sized>(
    source: &R,
    cursor: &mut SourceCursor<'_, R>,
    member_index: u64,
    crc32_enabled: bool,
) -> Result<MemberAnalysis, DecodeError> {
    let header = parse_member_header(cursor, member_index == 0)?;
    debug_assert_eq!(header.deflate_start, cursor.position());

    let mut crc = Crc32::new();
    let walk =
        walk_deflate_blocks::<_, _, RawInflater>(source, cursor, header.deflate_start, |chunk| {
            if crc32_enabled {
                crc.update(chunk);
            }
            Ok(())
        })?;

    let footer_offset = cursor.position();
    let footer = cursor.read_exact::<8>(footer_offset)?;
    let expected_crc = u32::from_le_bytes(footer[0..4].try_into().expect("four bytes"));
    let expected_size = u32::from_le_bytes(footer[4..8].try_into().expect("four bytes"));

    let crc32_ok = if crc32_enabled {
        let actual = crc.finish();
        if expected_crc != actual {
            return Err(DecodeError::ChecksumMismatch {
                member: member_index,
                expected: expected_crc,
                actual,
            });
        }
        Some(true)
    } else {
        None
    };

    let isize_ok = expected_size == walk.output_mod32;
    if !isize_ok {
        return Err(DecodeError::SizeMismatch {
            member: member_index,
            expected: expected_size,
            actual_mod32: walk.output_mod32,
        });
    }

    Ok(MemberAnalysis {
        index: member_index,
        compressed_start: header.start,
        compressed_end: cursor.position(),
        deflate_start: header.deflate_start,
        uncompressed_size: walk.uncompressed_size,
        bgzf_block_size: header.bgzf_block_size,
        zlib_header: None,
        crc32_ok,
        isize_ok,
        blocks: walk.blocks,
    })
}

fn analyze_zlib_stream<R: ReadAt + ?Sized>(
    source: &R,
    cursor: &mut SourceCursor<'_, R>,
    member_index: u64,
    crc32_enabled: bool,
) -> Result<MemberAnalysis, DecodeError> {
    let header = parse_zlib_header(cursor, member_index == 0)?;
    debug_assert_eq!(header.deflate_start, cursor.position());

    let mut adler = Adler32::new();
    let walk =
        walk_deflate_blocks::<_, _, RawInflater>(source, cursor, header.deflate_start, |chunk| {
            if crc32_enabled {
                adler.update(chunk);
            }
            Ok(())
        })?;

    // Adler-32 trailer is four big-endian bytes (RFC 1950).
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

    let crc32_ok = if crc32_enabled {
        let actual = adler.finish();
        if expected_adler != actual {
            return Err(DecodeError::ChecksumMismatch {
                member: member_index,
                expected: expected_adler,
                actual,
            });
        }
        Some(true)
    } else {
        None
    };

    Ok(MemberAnalysis {
        index: member_index,
        compressed_start: header.start,
        compressed_end: cursor.position(),
        deflate_start: header.deflate_start,
        uncompressed_size: walk.uncompressed_size,
        bgzf_block_size: None,
        zlib_header: Some((header.cmf, header.flg)),
        crc32_ok,
        isize_ok: true,
        blocks: walk.blocks,
    })
}

struct DeflateWalk {
    blocks: Vec<DeflateBlockInfo>,
    uncompressed_size: u64,
    output_mod32: u32,
}

/// Inflates raw DEFLATE from the cursor with [`InflateFlush::Block`], recording
/// block spans. Generic over [`InflateBackend`]; monomorphized to [`RawInflater`].
fn walk_deflate_blocks<R, F, I>(
    source: &R,
    cursor: &mut SourceCursor<'_, R>,
    deflate_start: u64,
    mut on_output: F,
) -> Result<DeflateWalk, DecodeError>
where
    R: ReadAt + ?Sized,
    F: FnMut(&[u8]) -> Result<(), DecodeError>,
    I: InflateBackend,
{
    let mut inflater = I::create()?;
    let mut output_mod32 = 0_u32;
    let mut uncompressed_size = 0_u64;
    let mut blocks = Vec::new();

    // First block starts at the first DEFLATE bit (byte-aligned after header).
    let mut block_bit_start = deflate_start.saturating_mul(8);
    let mut block_uncomp_start = 0_u64;
    let (mut block_type, mut block_is_final) = peek_block_header(source, block_bit_start)?;
    // After a final block's Block-flush boundary, the next inflate yields
    // stream-end without opening another block. Skip recording a phantom.
    let mut expect_stream_end_only = false;
    let mut decoded = Vec::with_capacity(64 * 1024);
    let mut finished = false;

    while !finished {
        let input = cursor.available()?;
        // After a final block boundary, the backend may still need one more
        // inflate (often with empty input) to report stream-end. Gzip/zlib leave
        // footer bytes so the cursor is rarely empty here; raw DEFLATE has no
        // trailer and hits the empty-input case at EOS.
        if input.is_empty() && !expect_stream_end_only {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: cursor.position().saturating_mul(8),
                reason: DeflateErrorKind::Truncated,
            });
        }

        if decoded.capacity() < 64 * 1024 {
            decoded.reserve_exact(64 * 1024 - decoded.capacity());
        }
        // Analyze discards payload after the integrity/size callback; force empty
        // so InflateBackend appends into a fresh buffer for this step.
        decoded.clear();

        let step = inflater.inflate(input, &mut decoded, InflateFlush::Block)?;
        cursor.advance(step.consumed);

        if !decoded.is_empty() {
            output_mod32 = output_mod32.wrapping_add(decoded.len() as u32);
            uncompressed_size = uncompressed_size
                .checked_add(decoded.len() as u64)
                .ok_or(DecodeError::OutputLimitExceeded { limit: u64::MAX })?;
            on_output(&decoded)?;
            decoded.clear();
        }

        let bit_pos = cursor
            .position()
            .saturating_mul(8)
            .saturating_sub(u64::from(step.unused_bits));
        let at_block_end = step.at_block_end;
        let last_block_flag = step.last_block;

        match step.status {
            inflate_status::STREAM_END => {
                let end_bit = bit_pos.div_ceil(8).saturating_mul(8);
                let end_byte = end_bit / 8;
                if cursor.position() != end_byte {
                    cursor.seek(end_byte)?;
                }
                if !expect_stream_end_only {
                    blocks.push(DeflateBlockInfo {
                        index: blocks.len() as u64,
                        block_type,
                        is_final: block_is_final || last_block_flag,
                        compressed_bit_start: block_bit_start,
                        compressed_bit_end: end_bit,
                        uncompressed_size: uncompressed_size.saturating_sub(block_uncomp_start),
                    });
                } else if let Some(last) = blocks.last_mut() {
                    // Prefer the definitive stream-end bit position.
                    last.compressed_bit_end = end_bit;
                    last.is_final = true;
                }
                finished = true;
            }
            inflate_status::OK | inflate_status::BUF_ERROR => {
                if at_block_end && !expect_stream_end_only {
                    let end_bit = if last_block_flag {
                        bit_pos.div_ceil(8).saturating_mul(8)
                    } else {
                        bit_pos
                    };
                    blocks.push(DeflateBlockInfo {
                        index: blocks.len() as u64,
                        block_type,
                        is_final: block_is_final || last_block_flag,
                        compressed_bit_start: block_bit_start,
                        compressed_bit_end: end_bit,
                        uncompressed_size: uncompressed_size.saturating_sub(block_uncomp_start),
                    });

                    // Prefer zlib's last-block data_type flag; also honor the
                    // peeked BFINAL so we never treat footer/padding as a new
                    // block when bit 6 is unset on a final block.
                    if last_block_flag || block_is_final {
                        expect_stream_end_only = true;
                        block_bit_start = end_bit;
                        block_uncomp_start = uncompressed_size;
                    } else {
                        block_bit_start = bit_pos;
                        block_uncomp_start = uncompressed_size;
                        let next = peek_block_header(source, block_bit_start)?;
                        block_type = next.0;
                        block_is_final = next.1;
                    }
                } else if step.consumed == 0 && step.produced == 0 {
                    // Includes the post-final-block wait for stream-end: if
                    // the backend makes no progress, treat as truncation/stall.
                    let reason = if step.status == inflate_status::BUF_ERROR {
                        DeflateErrorKind::Truncated
                    } else {
                        DeflateErrorKind::Stalled
                    };
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: cursor.position().saturating_mul(8),
                        reason,
                    });
                }
            }
            inflate_status::NEED_DICT => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: deflate_start.saturating_mul(8),
                    reason: DeflateErrorKind::UnexpectedDictionary,
                });
            }
            inflate_status::DATA_ERROR => {
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

    Ok(DeflateWalk {
        blocks,
        uncompressed_size,
        output_mod32,
    })
}

/// Peeks `BFINAL` and `BTYPE` at an absolute compressed bit offset (LSB-first).
fn peek_block_header<R: ReadAt + ?Sized>(
    source: &R,
    bit_offset: u64,
) -> Result<(DeflateBlockType, bool), DecodeError> {
    let bits = peek_bits(source, bit_offset, 3)?;
    let is_final = bits & 1 != 0;
    let btype = ((bits >> 1) & 0b11) as u8;
    let block_type = DeflateBlockType::from_btype(btype);
    if matches!(block_type, DeflateBlockType::Reserved(_)) {
        return Err(DecodeError::InvalidDeflate {
            bit_offset,
            reason: DeflateErrorKind::InvalidData,
        });
    }
    Ok((block_type, is_final))
}

/// Reads up to 16 little-endian-packed DEFLATE bits starting at `bit_offset`.
fn peek_bits<R: ReadAt + ?Sized>(
    source: &R,
    bit_offset: u64,
    bit_count: u8,
) -> Result<u32, DecodeError> {
    debug_assert!(bit_count <= 16);
    let byte_start = bit_offset / 8;
    let bit_in_byte = (bit_offset % 8) as u32;
    let total_bits = u32::from(bit_count) + bit_in_byte;
    let bytes_needed = total_bits.div_ceil(8) as usize;
    let length = source
        .len()
        .map_err(|error| DecodeError::input_io(byte_start, error))?;
    if byte_start >= length {
        return Err(DecodeError::InvalidDeflate {
            bit_offset,
            reason: DeflateErrorKind::Truncated,
        });
    }

    let mut buf = [0_u8; 4];
    let available = usize::try_from(length - byte_start).unwrap_or(usize::MAX);
    let to_read = bytes_needed.min(available).min(buf.len());
    let read = source
        .read_at(byte_start, &mut buf[..to_read])
        .map_err(|error| DecodeError::input_io(byte_start, error))?;
    if read < bytes_needed {
        return Err(DecodeError::InvalidDeflate {
            bit_offset,
            reason: DeflateErrorKind::Truncated,
        });
    }

    let mut value = 0_u32;
    for (index, &byte) in buf[..bytes_needed].iter().enumerate() {
        value |= u32::from(byte) << (8 * index);
    }
    let mask = (1_u32 << bit_count) - 1;
    Ok((value >> bit_in_byte) & mask)
}

#[cfg(test)]
mod tests {
    use super::{ArchiveKind, DeflateBlockType, analyze_source, analyze_source_with_format};
    use crate::config::Format;
    use crate::{DecodeError, DeflateErrorKind, GzipErrorKind};

    fn crc32(bytes: &[u8]) -> u32 {
        let mut value = u32::MAX;
        for &byte in bytes {
            value ^= u32::from(byte);
            for _ in 0..8 {
                value = (value >> 1) ^ (0xEDB8_8320 & 0_u32.wrapping_sub(value & 1));
            }
        }
        !value
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

    fn stored_deflate(bytes: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        if bytes.is_empty() {
            encoded.extend_from_slice(&[1, 0, 0, 0xFF, 0xFF]);
            return encoded;
        }
        let chunks: Vec<_> = bytes.chunks(u16::MAX as usize).collect();
        let chunk_count = chunks.len();
        for (index, chunk) in chunks.into_iter().enumerate() {
            encoded.push(u8::from(index + 1 == chunk_count));
            let length = chunk.len() as u16;
            encoded.extend_from_slice(&length.to_le_bytes());
            encoded.extend_from_slice(&(!length).to_le_bytes());
            encoded.extend_from_slice(chunk);
        }
        encoded
    }

    fn member(bytes: &[u8]) -> Vec<u8> {
        let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
        encoded.extend_from_slice(&stored_deflate(bytes));
        encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
        encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        encoded
    }

    fn member_from_raw_deflate(deflate: &[u8], decoded: &[u8]) -> Vec<u8> {
        let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
        encoded.extend_from_slice(deflate);
        encoded.extend_from_slice(&crc32(decoded).to_le_bytes());
        encoded.extend_from_slice(&(decoded.len() as u32).to_le_bytes());
        encoded
    }

    fn bgzf_member(bytes: &[u8]) -> Vec<u8> {
        let deflate = stored_deflate(bytes);
        let total_size = 18 + deflate.len() + 8;
        let block_size = (total_size - 1) as u16;
        let mut encoded = b"\x1f\x8b\x08\x04\0\0\0\0\x00\xff\x06\x00BC\x02\x00".to_vec();
        encoded.extend_from_slice(&block_size.to_le_bytes());
        encoded.extend_from_slice(&deflate);
        encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
        encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        encoded
    }

    fn zlib_member(bytes: &[u8]) -> Vec<u8> {
        let mut encoded = vec![0x78, 0x01];
        encoded.extend_from_slice(&stored_deflate(bytes));
        encoded.extend_from_slice(&adler32(bytes).to_be_bytes());
        encoded
    }

    #[test]
    fn analyzes_single_stored_member() {
        let payload = b"the quick brown fox";
        let compressed = member(payload);
        let analysis = analyze_source(compressed.as_slice(), 64, true).unwrap();
        assert_eq!(analysis.kind, ArchiveKind::Gzip);
        assert_eq!(analysis.member_count(), 1);
        assert_eq!(analysis.uncompressed_bytes, payload.len() as u64);
        assert_eq!(analysis.compressed_bytes, compressed.len() as u64);
        let member = &analysis.members[0];
        assert_eq!(member.uncompressed_size, payload.len() as u64);
        assert_eq!(member.compressed_start, 0);
        assert_eq!(member.compressed_end, compressed.len() as u64);
        assert_eq!(member.deflate_start, 10);
        assert_eq!(member.crc32_ok, Some(true));
        assert!(member.isize_ok);
        assert!(member.zlib_header.is_none());
        assert_eq!(member.blocks.len(), 1);
        assert_eq!(member.blocks[0].block_type, DeflateBlockType::Stored);
        assert!(member.blocks[0].is_final);
        assert_eq!(member.blocks[0].uncompressed_size, payload.len() as u64);
    }

    #[test]
    fn analyzes_multi_member_gzip() {
        let mut compressed = member(b"first");
        compressed.extend_from_slice(&member(b"second-payload"));
        let analysis = analyze_source(compressed.as_slice(), 32, true).unwrap();
        assert_eq!(analysis.kind, ArchiveKind::Gzip);
        assert_eq!(analysis.member_count(), 2);
        assert_eq!(analysis.uncompressed_bytes, (5 + 14) as u64);
        assert_eq!(analysis.members[0].uncompressed_size, 5);
        assert_eq!(analysis.members[1].uncompressed_size, 14);
        assert_eq!(
            analysis.members[1].compressed_start,
            analysis.members[0].compressed_end
        );
        assert_eq!(
            analysis.members[0].blocks[0].block_type,
            DeflateBlockType::Stored
        );
        assert_eq!(
            analysis.members[1].blocks[0].block_type,
            DeflateBlockType::Stored
        );
    }

    #[test]
    fn analyzes_bgzf_multi_block() {
        let mut compressed = bgzf_member(b"block-a");
        compressed.extend_from_slice(&bgzf_member(b"block-bb"));
        // Conventional empty EOF member.
        compressed.extend_from_slice(&bgzf_member(b""));
        let analysis = analyze_source(compressed.as_slice(), 64, true).unwrap();
        assert_eq!(analysis.member_count(), 3);
        assert_eq!(analysis.uncompressed_bytes, 15);
        assert!(analysis.members[0].bgzf_block_size.is_some());
        assert!(analysis.members[1].bgzf_block_size.is_some());
        assert_eq!(analysis.members[0].uncompressed_size, 7);
        assert_eq!(analysis.members[1].uncompressed_size, 8);
        assert_eq!(analysis.members[2].uncompressed_size, 0);
    }

    #[test]
    fn analyzes_stored_then_fixed_blocks() {
        let bytes = b"payload";
        let length = bytes.len() as u16;
        let mut deflate = vec![0, length as u8, (length >> 8) as u8];
        deflate.extend_from_slice(&(!length).to_le_bytes());
        deflate.extend_from_slice(bytes);
        // Empty final fixed-Huffman block.
        deflate.extend_from_slice(&[0x03, 0x00]);
        let compressed = member_from_raw_deflate(&deflate, bytes);
        let analysis = analyze_source(compressed.as_slice(), 64, true).unwrap();
        assert_eq!(analysis.member_count(), 1);
        assert_eq!(analysis.members[0].blocks.len(), 2);
        assert_eq!(
            analysis.members[0].blocks[0].block_type,
            DeflateBlockType::Stored
        );
        assert!(!analysis.members[0].blocks[0].is_final);
        assert_eq!(
            analysis.members[0].blocks[0].uncompressed_size,
            bytes.len() as u64
        );
        assert_eq!(
            analysis.members[0].blocks[1].block_type,
            DeflateBlockType::Fixed
        );
        assert!(analysis.members[0].blocks[1].is_final);
        assert_eq!(analysis.members[0].blocks[1].uncompressed_size, 0);
    }

    #[test]
    fn skips_crc_when_disabled() {
        let compressed = member(b"x");
        let analysis = analyze_source(compressed.as_slice(), 64, false).unwrap();
        assert_eq!(analysis.members[0].crc32_ok, None);
        assert!(analysis.members[0].isize_ok);
    }

    #[test]
    fn analyzes_single_zlib_stream() {
        let payload = b"the quick brown fox";
        let compressed = zlib_member(payload);
        let analysis = analyze_source(compressed.as_slice(), 64, true).unwrap();
        assert_eq!(analysis.kind, ArchiveKind::Zlib);
        assert_eq!(analysis.member_count(), 1);
        assert_eq!(analysis.uncompressed_bytes, payload.len() as u64);
        assert_eq!(analysis.compressed_bytes, compressed.len() as u64);
        let stream = &analysis.members[0];
        assert_eq!(stream.compressed_start, 0);
        assert_eq!(stream.compressed_end, compressed.len() as u64);
        assert_eq!(stream.deflate_start, 2);
        assert_eq!(stream.zlib_header, Some((0x78, 0x01)));
        assert_eq!(stream.crc32_ok, Some(true));
        assert!(stream.isize_ok);
        assert!(stream.bgzf_block_size.is_none());
        assert_eq!(stream.blocks.len(), 1);
        assert_eq!(stream.blocks[0].block_type, DeflateBlockType::Stored);
        assert!(stream.blocks[0].is_final);
        assert_eq!(stream.blocks[0].uncompressed_size, payload.len() as u64);
        let text = analysis.to_string();
        assert!(text.contains("zlib archive analysis"));
        assert!(text.contains("CMF=0x78"));
        assert!(text.contains("adler32=ok"));
    }

    #[test]
    fn analyzes_concatenated_zlib_streams() {
        let mut compressed = zlib_member(b"first");
        compressed.extend_from_slice(&zlib_member(b""));
        compressed.extend_from_slice(&zlib_member(b"second-payload"));
        let analysis = analyze_source(compressed.as_slice(), 32, true).unwrap();
        assert_eq!(analysis.kind, ArchiveKind::Zlib);
        assert_eq!(analysis.member_count(), 3);
        assert_eq!(analysis.uncompressed_bytes, 19);
        assert_eq!(analysis.members[0].uncompressed_size, 5);
        assert_eq!(analysis.members[1].uncompressed_size, 0);
        assert_eq!(analysis.members[2].uncompressed_size, 14);
        assert_eq!(
            analysis.members[1].compressed_start,
            analysis.members[0].compressed_end
        );
        assert_eq!(
            analysis.members[2].compressed_start,
            analysis.members[1].compressed_end
        );
        for member in &analysis.members {
            assert_eq!(member.zlib_header, Some((0x78, 0x01)));
            assert_eq!(member.crc32_ok, Some(true));
            assert_eq!(member.blocks[0].block_type, DeflateBlockType::Stored);
        }
    }

    #[test]
    fn skips_adler_when_disabled() {
        let compressed = zlib_member(b"payload");
        let analysis = analyze_source(compressed.as_slice(), 64, false).unwrap();
        assert_eq!(analysis.kind, ArchiveKind::Zlib);
        assert_eq!(analysis.members[0].crc32_ok, None);
        assert!(analysis.to_string().contains("adler32=skipped"));
    }

    #[test]
    fn forced_zlib_format_analyzes_zlib() {
        let compressed = zlib_member(b"forced");
        let analysis =
            analyze_source_with_format(compressed.as_slice(), 64, true, Format::Zlib).unwrap();
        assert_eq!(analysis.kind, ArchiveKind::Zlib);
        assert_eq!(analysis.members[0].uncompressed_size, 6);
    }

    #[test]
    fn forced_gzip_format_rejects_zlib() {
        let compressed = zlib_member(b"not gzip");
        let error =
            analyze_source_with_format(compressed.as_slice(), 64, true, Format::Gzip).unwrap_err();
        assert!(matches!(
            error,
            DecodeError::InvalidGzip {
                reason: GzipErrorKind::BadMagic,
                ..
            }
        ));
    }

    #[test]
    fn auto_rejects_raw_deflate() {
        // Empty final stored block: valid raw DEFLATE, not a gzip/zlib wrapper.
        // Auto never selects raw (match decode policy).
        let raw = [0x01_u8, 0x00, 0x00, 0xFF, 0xFF];
        let error = analyze_source(raw.as_slice(), 64, true).unwrap_err();
        assert!(matches!(
            error,
            DecodeError::InvalidGzip {
                reason: GzipErrorKind::BadMagic,
                ..
            }
        ));
        let forced_err =
            analyze_source_with_format(raw.as_slice(), 64, true, Format::Auto).unwrap_err();
        assert!(matches!(
            forced_err,
            DecodeError::InvalidGzip {
                reason: GzipErrorKind::BadMagic,
                ..
            }
        ));
    }

    #[test]
    fn analyzes_raw_deflate_empty_stored_block() {
        let raw = stored_deflate(b"");
        let analysis =
            analyze_source_with_format(raw.as_slice(), 64, true, Format::RawDeflate).unwrap();
        assert_eq!(analysis.kind, ArchiveKind::RawDeflate);
        assert_eq!(analysis.member_count(), 1);
        assert_eq!(analysis.compressed_bytes, raw.len() as u64);
        assert_eq!(analysis.uncompressed_bytes, 0);
        let stream = &analysis.members[0];
        assert_eq!(stream.compressed_start, 0);
        assert_eq!(stream.compressed_end, raw.len() as u64);
        assert_eq!(stream.deflate_start, 0);
        assert!(stream.zlib_header.is_none());
        assert_eq!(stream.crc32_ok, None);
        assert!(stream.isize_ok);
        assert!(stream.bgzf_block_size.is_none());
        assert_eq!(stream.blocks.len(), 1);
        assert_eq!(stream.blocks[0].block_type, DeflateBlockType::Stored);
        assert!(stream.blocks[0].is_final);
        assert_eq!(stream.blocks[0].uncompressed_size, 0);
        let text = analysis.to_string();
        assert!(text.contains("raw DEFLATE analysis"));
        assert!(text.contains("footer: none"));
    }

    #[test]
    fn analyzes_raw_deflate_multi_block_stored_then_fixed() {
        let bytes = b"payload";
        let length = bytes.len() as u16;
        let mut deflate = vec![0, length as u8, (length >> 8) as u8];
        deflate.extend_from_slice(&(!length).to_le_bytes());
        deflate.extend_from_slice(bytes);
        // Empty final fixed-Huffman block.
        deflate.extend_from_slice(&[0x03, 0x00]);
        let analysis =
            analyze_source_with_format(deflate.as_slice(), 64, true, Format::RawDeflate).unwrap();
        assert_eq!(analysis.kind, ArchiveKind::RawDeflate);
        assert_eq!(analysis.member_count(), 1);
        assert_eq!(analysis.uncompressed_bytes, bytes.len() as u64);
        assert_eq!(analysis.compressed_bytes, deflate.len() as u64);
        let stream = &analysis.members[0];
        assert_eq!(stream.crc32_ok, None);
        assert!(stream.zlib_header.is_none());
        assert_eq!(stream.blocks.len(), 2);
        assert_eq!(stream.blocks[0].block_type, DeflateBlockType::Stored);
        assert!(!stream.blocks[0].is_final);
        assert_eq!(stream.blocks[0].uncompressed_size, bytes.len() as u64);
        assert_eq!(stream.blocks[1].block_type, DeflateBlockType::Fixed);
        assert!(stream.blocks[1].is_final);
        assert_eq!(stream.blocks[1].uncompressed_size, 0);
    }

    #[test]
    fn raw_deflate_analyze_trailing_garbage_errors() {
        let mut raw = stored_deflate(b"ok");
        raw.extend_from_slice(b"garbage");
        let error =
            analyze_source_with_format(raw.as_slice(), 64, true, Format::RawDeflate).unwrap_err();
        assert!(matches!(
            error,
            DecodeError::InvalidDeflate {
                reason: DeflateErrorKind::InvalidData,
                ..
            }
        ));
    }

    #[test]
    fn raw_deflate_analyze_empty_source_errors() {
        let empty: &[u8] = &[];
        let error = analyze_source_with_format(empty, 64, true, Format::RawDeflate).unwrap_err();
        assert!(matches!(
            error,
            DecodeError::InvalidDeflate {
                reason: DeflateErrorKind::Truncated,
                ..
            }
        ));
    }

    #[test]
    fn gzip_path_unaffected_by_raw_analyze_support() {
        let compressed = member(b"still gzip");
        let analysis = analyze_source(compressed.as_slice(), 64, true).unwrap();
        assert_eq!(analysis.kind, ArchiveKind::Gzip);
        assert_eq!(analysis.members[0].uncompressed_size, 10);
        // Forced raw on gzip bytes should fail DEFLATE framing (gzip magic is
        // not a valid DEFLATE block stream starting at bit 0).
        let raw_err =
            analyze_source_with_format(compressed.as_slice(), 64, true, Format::RawDeflate)
                .unwrap_err();
        assert!(
            matches!(raw_err, DecodeError::InvalidDeflate { .. }),
            "unexpected error forcing raw on gzip: {raw_err:?}"
        );
    }
}
