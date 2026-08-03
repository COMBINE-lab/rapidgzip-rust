//! Bounded parallel full-stream decoding through a [`DeflateIndex`].
//!
//! Unlike the marker/window path, every task starts at an authoritative
//! checkpoint with its exact predecessor history. Workers therefore run plain
//! zlib-rs raw inflation and must finish at the next checkpoint's exact bit and
//! output offsets. Output travels through one-slot per-span channels, bounding
//! decoded reordering without allocating an entire sparse span.

use crate::backend::{LineCounter, Output};
use crate::config::Config;
use crate::crc32::Crc32;
use crate::format::FormatSelection;
use crate::gzip::{MemberHeader, SourceCursor, parse_member_header};
use crate::index::{Checkpoint, CheckpointKind, DeflateIndex, IndexError, IndexKind};
use crate::inflate::RawInflater;
use crate::line::count_newlines;
use crate::parallel::adaptive::AdaptiveWorkers;
use crate::runtime::{DecoderPath, RuntimeState};
use crate::zlib::{self, Adler32};
use crate::{
    DecodeError, DecodeReport, DeflateErrorKind, Format, GzipErrorKind, IndexDecodeError, ReadAt,
    ZlibErrorKind,
};
use crossbeam_deque::{Injector, Steal};
use libz_rs_sys as z;
use std::collections::BTreeMap;
use std::ffi::c_ulong;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
struct Span {
    start: Checkpoint,
    end: Option<Checkpoint>,
    expected_output: Option<u64>,
}

/// Checks caller-supplied line annotations while output is in final order.
///
/// This remains inactive unless line counting was requested and the imported
/// index actually carries line metadata. It deliberately lives outside worker
/// tasks so speculative or out-of-order bytes can never affect the result.
struct ImportedLineVerifier<'a> {
    checkpoints: &'a [Checkpoint],
    total: Option<u64>,
    next: usize,
    active: bool,
}

impl<'a> ImportedLineVerifier<'a> {
    fn new(index: &'a DeflateIndex, enabled: bool) -> Self {
        let checkpoints = index.checkpoints();
        Self {
            checkpoints,
            total: index.total_line_count(),
            next: 0,
            active: enabled
                && (index.total_line_count().is_some()
                    || checkpoints
                        .iter()
                        .any(|checkpoint| checkpoint.line_offset.is_some())),
        }
    }

    fn note_output(
        &mut self,
        start: u64,
        lines_before: u64,
        bytes: &[u8],
    ) -> Result<(), DecodeError> {
        if !self.active {
            return Ok(());
        }
        let end = start.saturating_add(bytes.len() as u64);
        let mut scanned = 0_usize;
        let mut lines = lines_before;
        while let Some(checkpoint) = self.checkpoints.get(self.next) {
            let offset = checkpoint.uncompressed_offset_in_bytes;
            if offset >= end {
                break;
            }
            if offset < start {
                return Err(DecodeError::IndexLineMismatch {
                    checkpoint_byte_offset: offset,
                    expected_lines: checkpoint.line_offset.unwrap_or(lines),
                    actual_lines: lines,
                });
            }
            if let Some(expected_lines) = checkpoint.line_offset {
                let target = usize::try_from(offset - start)
                    .expect("checkpoint is inside the current output slice");
                lines = lines.saturating_add(count_newlines(&bytes[scanned..target]));
                scanned = target;
                if expected_lines != lines {
                    return Err(DecodeError::IndexLineMismatch {
                        checkpoint_byte_offset: offset,
                        expected_lines,
                        actual_lines: lines,
                    });
                }
            }
            self.next += 1;
        }
        Ok(())
    }

    fn finish(&mut self, bytes: u64, lines: u64) -> Result<(), DecodeError> {
        if !self.active {
            return Ok(());
        }
        while let Some(checkpoint) = self.checkpoints.get(self.next) {
            let expected_lines = checkpoint.line_offset.unwrap_or(lines);
            if checkpoint.uncompressed_offset_in_bytes != bytes || expected_lines != lines {
                return Err(DecodeError::IndexLineMismatch {
                    checkpoint_byte_offset: checkpoint.uncompressed_offset_in_bytes,
                    expected_lines,
                    actual_lines: lines,
                });
            }
            self.next += 1;
        }
        if let Some(expected_lines) = self.total {
            if expected_lines != lines {
                return Err(DecodeError::IndexTotalLineMismatch {
                    expected_lines,
                    actual_lines: lines,
                });
            }
        }
        Ok(())
    }
}

/// Source-validated work description built before any worker is created.
pub(crate) struct IndexedPlan {
    format: Format,
    kind: IndexKind,
    source_length: u64,
    window_bits: u8,
    spans: Vec<Span>,
}

impl IndexedPlan {
    pub(crate) fn build<R: ReadAt + ?Sized>(
        source: &R,
        config: &Config,
        index: &DeflateIndex,
    ) -> Result<Self, IndexDecodeError> {
        index.validate()?;
        let source_length = source
            .len()
            .map_err(|error| DecodeError::input_io(0, error))?;
        if let Some(index_length) = index.compressed_size() {
            if index_length != source_length {
                return Err(IndexError::ArchiveSizeMismatch {
                    index_size: index_length,
                    archive_size: source_length,
                }
                .into());
            }
        }

        let format = format_for_index(index.kind());
        if let FormatSelection::Explicit(selected) = config.format {
            if selected != format {
                return Err(IndexDecodeError::FormatMismatch {
                    selected,
                    indexed: index.kind(),
                });
            }
        }

        let checkpoints = index.checkpoints();
        let Some(first) = checkpoints.first().copied() else {
            return Err(IndexError::MissingMetadata("at least one checkpoint").into());
        };
        if first.uncompressed_offset_in_bytes != 0 {
            return Err(IndexError::InvalidCheckpoint(
                "the first checkpoint does not begin at decompressed offset zero",
            )
            .into());
        }

        let window_bits =
            validate_initial_checkpoint(source, config.input_page_size, index, first, format)?;

        let total_output = index.uncompressed_size();
        if total_output.is_none() && index.kind() != IndexKind::Bgzf {
            return Err(IndexError::MissingMetadata(
                "uncompressed size for the final indexed span",
            )
            .into());
        }
        if let Some(total) = total_output {
            if total
                < checkpoints
                    .last()
                    .expect("nonempty")
                    .uncompressed_offset_in_bytes
            {
                return Err(IndexError::InvalidCheckpoint(
                    "the final checkpoint is after the recorded decompressed size",
                )
                .into());
            }
        }

        let mut spans = Vec::new();
        spans
            .try_reserve_exact(checkpoints.len())
            .map_err(|_| IndexError::AllocationFailed {
                what: "indexed decode spans",
            })?;
        for (position, start) in checkpoints.iter().copied().enumerate() {
            if index.kind() == IndexKind::Bgzf && matches!(start.kind, CheckpointKind::DeflateBlock)
            {
                return Err(IndexError::InvalidCheckpoint(
                    "a BGZF index must resume at complete member boundaries",
                )
                .into());
            }
            let end = checkpoints.get(position + 1).copied();
            let expected_output = match end {
                Some(end) => Some(
                    end.uncompressed_offset_in_bytes
                        .checked_sub(start.uncompressed_offset_in_bytes)
                        .ok_or(IndexError::InvalidCheckpoint(
                            "decompressed checkpoint offsets are decreasing",
                        ))?,
                ),
                None => total_output.map(|total| total - start.uncompressed_offset_in_bytes),
            };
            spans.push(Span {
                start,
                end,
                expected_output,
            });
        }

        Ok(Self {
            format,
            kind: index.kind(),
            source_length,
            window_bits,
            spans,
        })
    }
}

const fn format_for_index(kind: IndexKind) -> Format {
    match kind {
        IndexKind::Gzip | IndexKind::Bgzf => Format::Gzip,
        IndexKind::Zlib => Format::Zlib,
        IndexKind::RawDeflate => Format::RawDeflate,
    }
}

fn validate_initial_checkpoint<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
    index: &DeflateIndex,
    first: Checkpoint,
    format: Format,
) -> Result<u8, IndexDecodeError> {
    match format {
        Format::Gzip => {
            let mut cursor = SourceCursor::new(source, page_size.min(64 * 1024))?;
            let header = parse_member_header(&mut cursor, true)?;
            if index.kind() == IndexKind::Bgzf && header.bgzf_block_size.is_none() {
                return Err(IndexError::InvalidCheckpoint(
                    "a BGZF index was paired with a non-BGZF first member",
                )
                .into());
            }
            let matches = match first.kind {
                CheckpointKind::GzipMemberHeader => {
                    first.compressed_offset_in_bits == header.start.saturating_mul(8)
                }
                CheckpointKind::GzipMemberDeflate {
                    header_offset_in_bytes,
                } => {
                    header_offset_in_bytes == header.start
                        && first.compressed_offset_in_bits == header.deflate_start.saturating_mul(8)
                }
                CheckpointKind::DeflateBlock => {
                    index
                        .windows()
                        .get(first.compressed_offset_in_bits)
                        .is_none()
                        && first.compressed_offset_in_bits == header.deflate_start.saturating_mul(8)
                }
                CheckpointKind::ZlibHeader | CheckpointKind::RawDeflateStart => false,
            };
            if !matches {
                return Err(IndexError::InvalidCheckpoint(
                    "the first gzip checkpoint does not match the parsed first member",
                )
                .into());
            }
            Ok(15)
        }
        Format::Zlib => {
            if first.kind != CheckpointKind::ZlibHeader || first.compressed_offset_in_bits != 0 {
                return Err(IndexError::InvalidCheckpoint(
                    "a zlib index must begin at its container header",
                )
                .into());
            }
            let header = read_exact_at::<2, _>(source, 0)?;
            Ok(zlib::parse_header(header, 0)?)
        }
        Format::RawDeflate => {
            if first.kind != CheckpointKind::RawDeflateStart || first.compressed_offset_in_bits != 0
            {
                return Err(IndexError::InvalidCheckpoint(
                    "a raw-DEFLATE index must begin at bit zero",
                )
                .into());
            }
            Ok(15)
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ChunkChecksum {
    Crc32(u32),
    Adler32(u32),
    None,
}

enum SpanEvent {
    Data {
        bytes: Vec<u8>,
        checksum: ChunkChecksum,
    },
    GzipFooter {
        expected_crc: u32,
        expected_size: u32,
    },
    ZlibFooter {
        expected_adler: u32,
    },
    Finished,
    Failed(DecodeError),
}

struct Work {
    span_index: usize,
    sender: SyncSender<SpanEvent>,
}

struct ActiveSpan {
    receiver: Receiver<SpanEvent>,
}

/// Reusable positional input page that never reads beyond one indexed span.
struct SpanCursor<'a, R: ReadAt + ?Sized> {
    source: &'a R,
    limit: u64,
    position: u64,
    page: Vec<u8>,
    page_start: u64,
    page_length: usize,
}

impl<'a, R: ReadAt + ?Sized> SpanCursor<'a, R> {
    fn new(source: &'a R, page_size: usize) -> Self {
        Self {
            source,
            limit: 0,
            position: 0,
            page: vec![0; page_size],
            page_start: u64::MAX,
            page_length: 0,
        }
    }

    fn set_limit(&mut self, limit: u64) {
        self.limit = limit;
    }

    fn seek(&mut self, position: u64) -> Result<(), DecodeError> {
        if position > self.limit {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: position.saturating_mul(8),
                reason: DeflateErrorKind::Truncated,
            });
        }
        self.position = position;
        Ok(())
    }

    const fn position(&self) -> u64 {
        self.position
    }

    fn advance(&mut self, count: usize) {
        self.position = self.position.saturating_add(count as u64).min(self.limit);
    }

    fn available(&mut self) -> Result<&[u8], DecodeError> {
        if self.position >= self.limit {
            return Ok(&[]);
        }
        let page_end = self.page_start.saturating_add(self.page_length as u64);
        if self.position < self.page_start || self.position >= page_end {
            self.page_start = self.position;
            let remaining = usize::try_from(self.limit - self.position).unwrap_or(usize::MAX);
            let wanted = remaining.min(self.page.len());
            let read = self
                .source
                .read_at(self.position, &mut self.page[..wanted])
                .map_err(|error| DecodeError::input_io(self.position, error))?;
            if read == 0 {
                return Err(DecodeError::input_io(
                    self.position,
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "positional source ended inside an indexed span",
                    ),
                ));
            }
            self.page_length = read;
        }
        let relative = usize::try_from(self.position - self.page_start)
            .expect("a page-relative offset fits usize");
        Ok(&self.page[relative..self.page_length])
    }
}

fn read_exact_at<const N: usize, R: ReadAt + ?Sized>(
    source: &R,
    offset: u64,
) -> Result<[u8; N], DecodeError> {
    let mut bytes = [0_u8; N];
    let mut filled = 0;
    while filled < N {
        let absolute = offset.saturating_add(filled as u64);
        let read = source
            .read_at(absolute, &mut bytes[filled..])
            .map_err(|error| DecodeError::input_io(absolute, error))?;
        if read == 0 {
            return Err(DecodeError::input_io(
                absolute,
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "compressed source ended before indexed framing bytes",
                ),
            ));
        }
        filled += read;
    }
    Ok(bytes)
}

fn parse_header_at<R: ReadAt + ?Sized>(
    cursor: &mut SourceCursor<'_, R>,
    offset: u64,
) -> Result<MemberHeader, DecodeError> {
    cursor.seek(offset)?;
    parse_member_header(cursor, offset == 0)
}

fn start_span<R: ReadAt + ?Sized>(
    source: &R,
    frame_cursor: &mut SourceCursor<'_, R>,
    cursor: &mut SpanCursor<'_, R>,
    inflater: &mut RawInflater,
    index: &DeflateIndex,
    checkpoint: Checkpoint,
    window_bits: u8,
) -> Result<Option<MemberHeader>, DecodeError> {
    let bit_offset = checkpoint.compressed_offset_in_bits;
    inflater.reset_with_window_bits(window_bits, bit_offset)?;
    let byte_offset = bit_offset / 8;
    let mut member_header = None;
    let start = match checkpoint.kind {
        CheckpointKind::GzipMemberHeader => {
            let header = parse_header_at(frame_cursor, byte_offset)?;
            member_header = Some(header);
            header.deflate_start
        }
        CheckpointKind::GzipMemberDeflate {
            header_offset_in_bytes,
        } => {
            let header = parse_header_at(frame_cursor, header_offset_in_bytes)?;
            if header.deflate_start.saturating_mul(8) != bit_offset {
                return Err(DecodeError::IndexBoundaryMismatch {
                    expected_bit_offset: bit_offset,
                    actual_bit_offset: header.deflate_start.saturating_mul(8),
                });
            }
            member_header = Some(header);
            header.deflate_start
        }
        CheckpointKind::ZlibHeader => {
            let header = read_exact_at::<2, _>(source, byte_offset)?;
            let parsed = zlib::parse_header(header, byte_offset)?;
            if parsed != window_bits {
                return Err(DecodeError::InvalidZlib {
                    offset: byte_offset,
                    reason: ZlibErrorKind::UnsupportedWindowSize(parsed.saturating_sub(8)),
                });
            }
            byte_offset + 2
        }
        CheckpointKind::RawDeflateStart => byte_offset,
        CheckpointKind::DeflateBlock => {
            let remainder = (bit_offset % 8) as u8;
            if remainder == 0 {
                byte_offset
            } else {
                let first = read_exact_at::<1, _>(source, byte_offset)?[0];
                inflater.prime(8 - remainder, first >> remainder, bit_offset)?;
                byte_offset + 1
            }
        }
    };

    if let Some(stored) = index.windows().get(bit_offset) {
        let expanded = stored
            .decompressed()
            .map_err(|error| DecodeError::input_io(byte_offset, std::io::Error::other(error)))?;
        let allowed = 1_usize << window_bits;
        let dictionary = &expanded[expanded.len().saturating_sub(allowed)..];
        inflater.set_dictionary_bytes(dictionary, bit_offset)?;
    }
    cursor.seek(start)?;
    Ok(member_header)
}

fn checksum_chunk(kind: IndexKind, bytes: &[u8]) -> ChunkChecksum {
    match kind {
        IndexKind::Gzip | IndexKind::Bgzf => {
            let mut checksum = Crc32::new();
            checksum.update(bytes);
            ChunkChecksum::Crc32(checksum.finish())
        }
        IndexKind::Zlib => {
            let mut checksum = Adler32::new();
            checksum.update(bytes);
            ChunkChecksum::Adler32(checksum.finish())
        }
        IndexKind::RawDeflate => ChunkChecksum::None,
    }
}

struct SpanSink<'a> {
    sender: &'a SyncSender<SpanEvent>,
    cancelled: &'a AtomicBool,
    stopped: &'a AtomicBool,
}

impl SpanSink<'_> {
    fn send(&self, mut event: SpanEvent) -> Result<(), DecodeError> {
        loop {
            if self.cancelled.load(Ordering::Relaxed) || self.stopped.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            match self.sender.try_send(event) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(_)) => return Err(DecodeError::Cancelled),
                Err(TrySendError::Full(returned)) => {
                    event = returned;
                    // Each span owns its bounded channel. Sleeping briefly
                    // avoids a shared condition-variable wakeup convoy when
                    // many later spans are ready before ordered emission,
                    // while the retry keeps cancellation observable.
                    thread::park_timeout(Duration::from_micros(25));
                }
            }
        }
    }
}

enum MemberTransition {
    ContinueAt {
        deflate_start: u64,
        bgzf_end: Option<u64>,
    },
    SpanFinished,
}

#[allow(clippy::too_many_arguments)]
fn finish_gzip_member<R: ReadAt + ?Sized>(
    source: &R,
    frame_cursor: &mut SourceCursor<'_, R>,
    source_length: u64,
    end_bit: u64,
    target: Option<Checkpoint>,
    require_bgzf: bool,
    expected_bgzf_end: Option<u64>,
    sink: &SpanSink<'_>,
) -> Result<MemberTransition, DecodeError> {
    let footer_offset = end_bit.div_ceil(8);
    let footer = read_exact_at::<8, _>(source, footer_offset)?;
    sink.send(SpanEvent::GzipFooter {
        expected_crc: u32::from_le_bytes(footer[..4].try_into().expect("four bytes")),
        expected_size: u32::from_le_bytes(footer[4..].try_into().expect("four bytes")),
    })?;
    let next_header = footer_offset
        .checked_add(8)
        .ok_or(DecodeError::InvalidGzip {
            offset: footer_offset,
            reason: GzipErrorKind::Truncated,
        })?;
    if expected_bgzf_end.is_some_and(|expected| expected != next_header) {
        return Err(DecodeError::InvalidGzip {
            offset: next_header,
            reason: GzipErrorKind::TrailingGarbage,
        });
    }

    if let Some(target) = target {
        let target_bit = target.compressed_offset_in_bits;
        if matches!(target.kind, CheckpointKind::GzipMemberHeader)
            && next_header.saturating_mul(8) == target_bit
        {
            return Ok(MemberTransition::SpanFinished);
        }
        if next_header.saturating_mul(8) > target_bit {
            return Err(DecodeError::IndexBoundaryMismatch {
                expected_bit_offset: target_bit,
                actual_bit_offset: next_header.saturating_mul(8),
            });
        }
    } else if next_header == source_length {
        return Ok(MemberTransition::SpanFinished);
    }

    if next_header >= source_length {
        return Err(DecodeError::InvalidGzip {
            offset: next_header,
            reason: if next_header == source_length {
                GzipErrorKind::TrailingGarbage
            } else {
                GzipErrorKind::Truncated
            },
        });
    }
    let header = parse_header_at(frame_cursor, next_header)?;
    if require_bgzf && header.bgzf_block_size.is_none() {
        return Err(DecodeError::InvalidGzip {
            offset: next_header,
            reason: GzipErrorKind::TrailingGarbage,
        });
    }
    let deflate_bit = header.deflate_start.saturating_mul(8);
    if let Some(target) = target {
        let target_bit = target.compressed_offset_in_bits;
        let target_is_payload = matches!(
            target.kind,
            CheckpointKind::GzipMemberDeflate { .. } | CheckpointKind::DeflateBlock
        );
        if target_is_payload && deflate_bit == target_bit {
            if let CheckpointKind::GzipMemberDeflate {
                header_offset_in_bytes,
            } = target.kind
            {
                if header_offset_in_bytes != next_header {
                    return Err(DecodeError::IndexBoundaryMismatch {
                        expected_bit_offset: target_bit,
                        actual_bit_offset: deflate_bit,
                    });
                }
            }
            return Ok(MemberTransition::SpanFinished);
        }
        if deflate_bit > target_bit {
            return Err(DecodeError::IndexBoundaryMismatch {
                expected_bit_offset: target_bit,
                actual_bit_offset: deflate_bit,
            });
        }
    }
    Ok(MemberTransition::ContinueAt {
        deflate_start: header.deflate_start,
        bgzf_end: header.bgzf_block_size.map(|size| {
            header
                .start
                .saturating_add(u64::from(size))
                .saturating_add(1)
        }),
    })
}

fn finish_non_gzip<R: ReadAt + ?Sized>(
    source: &R,
    kind: IndexKind,
    source_length: u64,
    end_bit: u64,
    sink: &SpanSink<'_>,
) -> Result<(), DecodeError> {
    let trailer_offset = end_bit.div_ceil(8);
    match kind {
        IndexKind::Zlib => {
            let trailer = read_exact_at::<4, _>(source, trailer_offset)?;
            let end = trailer_offset
                .checked_add(4)
                .ok_or(DecodeError::InvalidZlib {
                    offset: trailer_offset,
                    reason: ZlibErrorKind::Truncated,
                })?;
            if end != source_length {
                return Err(DecodeError::InvalidZlib {
                    offset: end,
                    reason: ZlibErrorKind::TrailingGarbage,
                });
            }
            sink.send(SpanEvent::ZlibFooter {
                expected_adler: u32::from_be_bytes(trailer),
            })
        }
        IndexKind::RawDeflate => {
            if trailer_offset != source_length {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: end_bit,
                    reason: DeflateErrorKind::TrailingGarbage,
                });
            }
            Ok(())
        }
        IndexKind::Gzip | IndexKind::Bgzf => unreachable!("handled above"),
    }
}

fn verify_span_output(span: Span, actual: u64) -> Result<(), DecodeError> {
    if let Some(expected) = span.expected_output {
        if expected != actual {
            return Err(DecodeError::IndexOutputMismatch {
                checkpoint_bit_offset: span
                    .end
                    .map_or(span.start.compressed_offset_in_bits, |end| {
                        end.compressed_offset_in_bits
                    }),
                expected_bytes: expected,
                actual_bytes: actual,
            });
        }
    }
    Ok(())
}

struct WorkerResources<'a, R: ReadAt + ?Sized> {
    cursor: SpanCursor<'a, R>,
    frame_cursor: SourceCursor<'a, R>,
    inflater: RawInflater,
    decoded: Vec<u8>,
}

impl<'a, R: ReadAt + ?Sized> WorkerResources<'a, R> {
    fn new(source: &'a R, config: &Config, window_bits: u8) -> Result<Self, DecodeError> {
        Ok(Self {
            cursor: SpanCursor::new(source, config.input_page_size),
            frame_cursor: SourceCursor::new(source, config.input_page_size.min(64 * 1024))?,
            inflater: RawInflater::new_with_window_bits(window_bits)?,
            decoded: Vec::new(),
        })
    }
}

fn flush_decoded(
    kind: IndexKind,
    decoded: &mut Vec<u8>,
    sink: &SpanSink<'_>,
) -> Result<(), DecodeError> {
    if decoded.is_empty() {
        return Ok(());
    }
    let checksum = checksum_chunk(kind, decoded);
    sink.send(SpanEvent::Data {
        bytes: std::mem::take(decoded),
        checksum,
    })
}

#[allow(clippy::too_many_arguments)]
fn decode_span<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    index: &DeflateIndex,
    plan: &IndexedPlan,
    span: Span,
    resources: &mut WorkerResources<'_, R>,
    recycled: &Injector<Vec<u8>>,
    sink: &SpanSink<'_>,
) -> Result<u64, DecodeError> {
    resources
        .cursor
        .set_limit(span.end.map_or(plan.source_length, |end| {
            end.compressed_offset_in_bits.div_ceil(8)
        }));
    let header = start_span(
        source,
        &mut resources.frame_cursor,
        &mut resources.cursor,
        &mut resources.inflater,
        index,
        span.start,
        plan.window_bits,
    )?;
    let mut bgzf_end = header.and_then(|header| {
        header.bgzf_block_size.map(|size| {
            header
                .start
                .saturating_add(u64::from(size))
                .saturating_add(1)
        })
    });
    if plan.kind == IndexKind::Bgzf && bgzf_end.is_none() {
        return Err(DecodeError::InvalidGzip {
            offset: span.start.compressed_offset_in_bits / 8,
            reason: GzipErrorKind::TrailingGarbage,
        });
    }

    let mut span_output = 0_u64;
    resources.decoded.clear();
    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }
        let (input_pointer, input_length) = {
            let input = resources.cursor.available()?;
            (input.as_ptr(), input.len().min(u32::MAX as usize))
        };
        if resources.decoded.is_empty() && resources.decoded.capacity() < config.decoded_chunk_size
        {
            loop {
                match recycled.steal() {
                    Steal::Success(bytes) => {
                        resources.decoded = bytes;
                        resources.decoded.clear();
                        break;
                    }
                    Steal::Retry => std::hint::spin_loop(),
                    Steal::Empty => break,
                }
            }
        }
        if resources.decoded.capacity() < config.decoded_chunk_size {
            resources
                .decoded
                .reserve_exact(config.decoded_chunk_size - resources.decoded.capacity());
        }
        resources.inflater.stream.next_in = input_pointer;
        resources.inflater.stream.avail_in = input_length as u32;
        let decoded_before = resources.decoded.len();
        let output_space = config.decoded_chunk_size - decoded_before;
        resources.inflater.stream.next_out =
            resources.decoded.spare_capacity_mut().as_mut_ptr().cast();
        resources.inflater.stream.avail_out = output_space as u32;
        let input_before = resources.inflater.stream.avail_in;
        let output_before = resources.inflater.stream.avail_out;
        // SAFETY: the inflater is initialized and uniquely borrowed. The input
        // cursor does not move during the call, and the output pointer covers
        // exactly the vector's spare capacity advertised through `avail_out`.
        let status = unsafe { z::inflate(&mut resources.inflater.stream, z::Z_BLOCK) };
        let consumed = (input_before - resources.inflater.stream.avail_in) as usize;
        let produced = (output_before - resources.inflater.stream.avail_out) as usize;
        resources.inflater.stream.next_in = std::ptr::null();
        resources.inflater.stream.avail_in = 0;
        resources.inflater.stream.next_out = std::ptr::null_mut();
        resources.inflater.stream.avail_out = 0;
        resources.cursor.advance(consumed);
        // SAFETY: zlib initialized exactly `produced` bytes of the spare
        // capacity supplied above and cannot report more than `avail_out`.
        unsafe { resources.decoded.set_len(decoded_before + produced) };

        if produced != 0 {
            let next = span_output.checked_add(produced as u64).ok_or(
                DecodeError::IndexOutputMismatch {
                    checkpoint_bit_offset: span.start.compressed_offset_in_bits,
                    expected_bytes: span.expected_output.unwrap_or(u64::MAX),
                    actual_bytes: u64::MAX,
                },
            )?;
            if span.expected_output.is_some_and(|expected| next > expected) {
                return Err(DecodeError::IndexOutputMismatch {
                    checkpoint_bit_offset: span
                        .end
                        .map_or(span.start.compressed_offset_in_bits, |end| {
                            end.compressed_offset_in_bits
                        }),
                    expected_bytes: span.expected_output.expect("checked as some"),
                    actual_bytes: next,
                });
            }
            span_output = next;
        }
        if resources.decoded.len() == config.decoded_chunk_size {
            flush_decoded(plan.kind, &mut resources.decoded, sink)?;
        }

        let unused_bits = u64::try_from(resources.inflater.stream.data_type & 0x3f)
            .expect("the low six data_type bits are non-negative");
        let current_bit = resources
            .cursor
            .position()
            .saturating_mul(8)
            .saturating_sub(unused_bits);
        let at_boundary = resources.inflater.stream.data_type & 0x80 != 0;
        let last_block = resources.inflater.stream.data_type & 0x40 != 0;
        let stream_end = status == z::Z_STREAM_END || (at_boundary && last_block);

        if stream_end {
            // A footer is an ordered checksum boundary, so all bytes from the
            // completed member must precede its event even when a span crosses
            // into the next member.
            flush_decoded(plan.kind, &mut resources.decoded, sink)?;
            match plan.kind {
                IndexKind::Gzip | IndexKind::Bgzf => match finish_gzip_member(
                    source,
                    &mut resources.frame_cursor,
                    plan.source_length,
                    current_bit,
                    span.end,
                    plan.kind == IndexKind::Bgzf,
                    bgzf_end,
                    sink,
                )? {
                    MemberTransition::SpanFinished => {
                        verify_span_output(span, span_output)?;
                        sink.send(SpanEvent::Finished)?;
                        return Ok(span_output);
                    }
                    MemberTransition::ContinueAt {
                        deflate_start,
                        bgzf_end: next_bgzf_end,
                    } => {
                        resources.inflater.reset(deflate_start.saturating_mul(8))?;
                        resources.cursor.seek(deflate_start)?;
                        bgzf_end = next_bgzf_end;
                    }
                },
                IndexKind::Zlib | IndexKind::RawDeflate => {
                    if let Some(target) = span.end {
                        return Err(DecodeError::IndexBoundaryMismatch {
                            expected_bit_offset: target.compressed_offset_in_bits,
                            actual_bit_offset: current_bit,
                        });
                    }
                    finish_non_gzip(source, plan.kind, plan.source_length, current_bit, sink)?;
                    verify_span_output(span, span_output)?;
                    sink.send(SpanEvent::Finished)?;
                    return Ok(span_output);
                }
            }
            continue;
        }

        if at_boundary {
            if let Some(target) = span.end {
                let target_bit = target.compressed_offset_in_bits;
                if matches!(target.kind, CheckpointKind::DeflateBlock) {
                    if current_bit == target_bit {
                        flush_decoded(plan.kind, &mut resources.decoded, sink)?;
                        verify_span_output(span, span_output)?;
                        sink.send(SpanEvent::Finished)?;
                        return Ok(span_output);
                    }
                    if current_bit > target_bit {
                        return Err(DecodeError::IndexBoundaryMismatch {
                            expected_bit_offset: target_bit,
                            actual_bit_offset: current_bit,
                        });
                    }
                }
            }
        }

        match status {
            z::Z_OK | z::Z_BUF_ERROR if consumed != 0 || produced != 0 => {}
            z::Z_DATA_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: current_bit,
                    reason: DeflateErrorKind::InvalidData,
                });
            }
            z::Z_NEED_DICT => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: span.start.compressed_offset_in_bits,
                    reason: DeflateErrorKind::UnexpectedDictionary,
                });
            }
            z::Z_OK | z::Z_BUF_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: current_bit,
                    reason: if resources.cursor.position() >= plan.source_length {
                        DeflateErrorKind::Truncated
                    } else {
                        DeflateErrorKind::Stalled
                    },
                });
            }
            other => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: current_bit,
                    reason: DeflateErrorKind::BackendStatus(other),
                });
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn worker_loop<R: ReadAt + ?Sized>(
    worker_index: usize,
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    index: &DeflateIndex,
    plan: &IndexedPlan,
    queue: &Injector<Work>,
    recycled: &Injector<Vec<u8>>,
    available_tasks: &AtomicUsize,
    work_signal: &(Mutex<()>, Condvar),
    stopped: &AtomicBool,
    adaptive: &Arc<AdaptiveWorkers>,
) {
    let _registration = adaptive.runtime.register_worker();
    let mut resources = None;
    loop {
        if stopped.load(Ordering::Relaxed) || cancelled.load(Ordering::Relaxed) {
            return;
        }
        if !adaptive.worker_enabled(worker_index) {
            if adaptive.wait_until_enabled_or_retire(worker_index, stopped) {
                continue;
            }
            return;
        }
        let work = loop {
            match queue.steal() {
                Steal::Success(work) => break Some(work),
                Steal::Retry => std::hint::spin_loop(),
                Steal::Empty => {
                    if stopped.load(Ordering::Relaxed) || cancelled.load(Ordering::Relaxed) {
                        return;
                    }
                    if !adaptive.worker_enabled(worker_index) {
                        break None;
                    }
                    let guard = work_signal.0.lock().expect("indexed work mutex poisoned");
                    let _guard = work_signal
                        .1
                        .wait_timeout_while(guard, Duration::from_millis(10), |_| {
                            available_tasks.load(Ordering::Acquire) == 0
                                && adaptive.worker_enabled(worker_index)
                                && !stopped.load(Ordering::Relaxed)
                                && !cancelled.load(Ordering::Relaxed)
                        })
                        .expect("indexed work mutex poisoned");
                }
            }
        };
        let Some(work) = work else {
            continue;
        };
        let remaining = available_tasks
            .fetch_sub(1, Ordering::AcqRel)
            .saturating_sub(1);
        adaptive.runtime.set_queued_tasks(remaining);
        let generation = adaptive.start_work();
        let sink = SpanSink {
            sender: &work.sender,
            cancelled,
            stopped,
        };
        if resources.is_none() {
            match WorkerResources::new(source, config, plan.window_bits) {
                Ok(created) => resources = Some(created),
                Err(error) => {
                    let _ = sink.send(SpanEvent::Failed(error));
                    return;
                }
            }
        }
        let result = {
            let _busy = adaptive.runtime.begin_task();
            decode_span(
                source,
                config,
                cancelled,
                index,
                plan,
                plan.spans[work.span_index],
                resources.as_mut().expect("initialized above"),
                recycled,
                &sink,
            )
        };
        let bytes = result.as_ref().copied().unwrap_or(0);
        adaptive.observe_work(generation, usize::try_from(bytes).unwrap_or(usize::MAX));
        if let Err(error) = result {
            let _ = sink.send(SpanEvent::Failed(error));
        }
    }
}

enum OrderedVerifier {
    Gzip { crc: u32, size: u64, members: u64 },
    Zlib { adler: u32, footer_seen: bool },
    Raw,
}

impl OrderedVerifier {
    const fn new(kind: IndexKind) -> Self {
        match kind {
            IndexKind::Gzip | IndexKind::Bgzf => Self::Gzip {
                crc: 0,
                size: 0,
                members: 0,
            },
            IndexKind::Zlib => Self::Zlib {
                adler: 1,
                footer_seen: false,
            },
            IndexKind::RawDeflate => Self::Raw,
        }
    }

    fn accept_chunk(&mut self, checksum: ChunkChecksum, length: usize) -> Result<(), DecodeError> {
        match (self, checksum) {
            (Self::Gzip { crc, size, .. }, ChunkChecksum::Crc32(part)) => {
                *crc = z::crc32_combine64(*crc as c_ulong, part as c_ulong, length as z::z_off64_t)
                    as u32;
                *size = size.saturating_add(length as u64);
                Ok(())
            }
            (Self::Zlib { adler, .. }, ChunkChecksum::Adler32(part)) => {
                *adler = z::adler32_combine64(
                    *adler as c_ulong,
                    part as c_ulong,
                    length as z::z_off64_t,
                ) as u32;
                Ok(())
            }
            (Self::Raw, ChunkChecksum::None) => Ok(()),
            _ => Err(DecodeError::WorkerPanicked),
        }
    }

    fn accept_gzip_footer(
        &mut self,
        expected_crc: u32,
        expected_size: u32,
    ) -> Result<u64, DecodeError> {
        let Self::Gzip { crc, size, members } = self else {
            return Err(DecodeError::WorkerPanicked);
        };
        if *crc != expected_crc {
            return Err(DecodeError::ChecksumMismatch {
                member: *members,
                expected: expected_crc,
                actual: *crc,
            });
        }
        if *size as u32 != expected_size {
            return Err(DecodeError::SizeMismatch {
                member: *members,
                expected: expected_size,
                actual_mod32: *size as u32,
            });
        }
        *members += 1;
        *crc = 0;
        *size = 0;
        Ok(*members)
    }

    fn accept_zlib_footer(&mut self, expected: u32) -> Result<(), DecodeError> {
        let Self::Zlib { adler, footer_seen } = self else {
            return Err(DecodeError::WorkerPanicked);
        };
        if *adler != expected {
            return Err(DecodeError::InvalidZlib {
                offset: 0,
                reason: ZlibErrorKind::ChecksumMismatch {
                    expected,
                    actual: *adler,
                },
            });
        }
        *footer_seen = true;
        Ok(())
    }

    fn finish(self) -> Result<u64, DecodeError> {
        match self {
            Self::Gzip {
                size: 0, members, ..
            } if members != 0 => Ok(members),
            Self::Gzip { .. } => Err(DecodeError::InvalidGzip {
                offset: 0,
                reason: GzipErrorKind::Truncated,
            }),
            Self::Zlib {
                footer_seen: true, ..
            } => Ok(1),
            Self::Zlib { .. } => Err(DecodeError::InvalidZlib {
                offset: 0,
                reason: ZlibErrorKind::Truncated,
            }),
            Self::Raw => Ok(1),
        }
    }
}

struct StopGuard<'a> {
    stopped: &'a AtomicBool,
    signal: &'a Condvar,
    runtime: &'a RuntimeState,
}

impl Drop for StopGuard<'_> {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.signal.notify_all();
        self.runtime.notify_limit_waiters();
    }
}

/// Runs an already validated indexed plan and emits ordered decoded chunks.
pub(crate) fn decode<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    index: &DeflateIndex,
    plan: &IndexedPlan,
    runtime: &Arc<RuntimeState>,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    runtime.set_path(DecoderPath::IndexedParallel);
    let maximum_workers = config
        .decoder_threads
        .min(config.in_flight_chunks)
        .min(plan.spans.len())
        .max(1);
    let machine_parallelism = thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let adaptive = Arc::new(AdaptiveWorkers::new(
        maximum_workers,
        machine_parallelism,
        config.decoded_chunk_size.saturating_mul(4),
        plan.spans.len(),
        Arc::clone(runtime),
    ));
    let worker_pool = adaptive.worker_pool_limit().min(maximum_workers).max(1);
    let queue = Arc::new(Injector::new());
    let recycled = Arc::new(Injector::new());
    let available_tasks = Arc::new(AtomicUsize::new(0));
    let work_signal = Arc::new((Mutex::new(()), Condvar::new()));
    let stopped = Arc::new(AtomicBool::new(false));
    let mut active = BTreeMap::<usize, ActiveSpan>::new();
    let mut next_to_schedule = 0_usize;
    let mut next_to_emit = 0_usize;
    let mut verifier = OrderedVerifier::new(plan.kind);
    let mut total_output = 0_u64;
    let mut line_counter = LineCounter::new(config.count_lines);
    let mut line_verifier = ImportedLineVerifier::new(index, config.count_lines);

    let result = thread::scope(|scope| -> Result<(), DecodeError> {
        let _stop = StopGuard {
            stopped: &stopped,
            signal: &work_signal.1,
            runtime,
        };
        let (exited_sender, exited_receiver) = mpsc::channel::<usize>();
        let mut live_workers = vec![false; worker_pool];

        while next_to_emit < plan.spans.len() {
            while let Ok(worker) = exited_receiver.try_recv() {
                live_workers[worker] = false;
            }
            let target = adaptive.current_limit().min(worker_pool).max(1);
            for (worker_index, live) in live_workers.iter_mut().enumerate().take(target) {
                if *live {
                    continue;
                }
                let exited_sender = exited_sender.clone();
                let queue = Arc::clone(&queue);
                let recycled = Arc::clone(&recycled);
                let available_tasks = Arc::clone(&available_tasks);
                let work_signal = Arc::clone(&work_signal);
                let stopped = Arc::clone(&stopped);
                let adaptive = Arc::clone(&adaptive);
                scope.spawn(move || {
                    worker_loop(
                        worker_index,
                        source,
                        config,
                        cancelled,
                        index,
                        plan,
                        &queue,
                        &recycled,
                        &available_tasks,
                        &work_signal,
                        &stopped,
                        &adaptive,
                    );
                    let _ = exited_sender.send(worker_index);
                });
                *live = true;
            }

            while active.len() < target && next_to_schedule < plan.spans.len() {
                let (sender, receiver) = mpsc::sync_channel(1);
                active.insert(next_to_schedule, ActiveSpan { receiver });
                {
                    let _guard = work_signal.0.lock().expect("indexed work mutex poisoned");
                    // A worker decrements after a successful steal, so publish
                    // the count before the task becomes queue-visible.
                    let queued = available_tasks.fetch_add(1, Ordering::Release) + 1;
                    queue.push(Work {
                        span_index: next_to_schedule,
                        sender,
                    });
                    runtime.set_queued_tasks(queued);
                    work_signal.1.notify_one();
                }
                next_to_schedule += 1;
            }

            if cancelled.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            let receiver = &active
                .get(&next_to_emit)
                .expect("the next ordered span is always scheduled")
                .receiver;
            let event = match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(event) => event,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return Err(DecodeError::WorkerPanicked),
            };
            match event {
                SpanEvent::Data { bytes, checksum } => {
                    let next_total = config.checked_output_total(total_output, bytes.len())?;
                    verifier.accept_chunk(checksum, bytes.len())?;
                    let lines_before = line_counter.line_count();
                    line_counter.note_output(&bytes, None);
                    line_verifier.note_output(total_output, lines_before, &bytes)?;
                    total_output = next_total;
                    let reusable = output.emit_reusable(bytes)?;
                    if reusable.capacity() != 0 {
                        recycled.push(reusable);
                    }
                }
                SpanEvent::GzipFooter {
                    expected_crc,
                    expected_size,
                } => {
                    let members = verifier.accept_gzip_footer(expected_crc, expected_size)?;
                    runtime.set_member_count(members);
                }
                SpanEvent::ZlibFooter { expected_adler } => {
                    verifier.accept_zlib_footer(expected_adler)?;
                    runtime.set_member_count(1);
                }
                SpanEvent::Finished => {
                    active.remove(&next_to_emit);
                    next_to_emit += 1;
                }
                SpanEvent::Failed(error) => return Err(error),
            }
        }
        Ok(())
    });
    result?;

    let final_length = source
        .len()
        .map_err(|error| DecodeError::input_io(plan.source_length, error))?;
    if final_length != plan.source_length {
        return Err(DecodeError::input_io(
            plan.source_length,
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during indexed decoding",
            ),
        ));
    }
    let members = verifier.finish()?;
    config.verify_expected_output(total_output)?;
    let report = line_counter.finish_report(DecodeReport {
        compressed_bytes: plan.source_length,
        decompressed_bytes: total_output,
        member_count: members,
        decoder_threads: config.decoder_threads,
        format: plan.format,
        line_count: None,
    });
    line_verifier.finish(total_output, report.line_count.unwrap_or(0))?;
    Ok(report)
}
