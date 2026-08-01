//! Deciding whether an index can drive a parallel decode, and how to split it.
//!
//! The speculative grid exists because a worker landing on a guessed offset
//! knows neither its DEFLATE block boundary nor the history before it. An
//! index records both, so its checkpoints partition the file into spans that
//! plain zlib can decode independently.
//!
//! This module owns that decision and the resulting plan. Running the plan is
//! [`crate::backend`]'s business.

use crate::backend::Output;
use crate::config::Config;
use crate::crc32::Crc32;
use crate::index::{Checkpoint, GzipIndex, StoredWindow};
use crate::inflate::RawInflater;
use crate::inflate_backend::{InflateBackend, InflateOutcome};
use crate::runtime::{DecoderPath, RuntimeState};
use crate::{DecodeError, DecodeReport, DeflateErrorKind, Format, GzipErrorKind, ReadAt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

/// Fewest checkpoints worth splitting.
///
/// Two checkpoints describe one interior span plus a tail, which is one
/// worker's work with extra bookkeeping. Three is where a second worker has
/// something to do.
const MINIMUM_CHECKPOINTS: usize = 3;

/// Fewest workers worth planning for.
const MINIMUM_WORKERS: usize = 2;

/// Why an index cannot drive a parallel decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Unusable {
    /// No index was supplied.
    Absent,
    /// The index failed its own invariants.
    Invalid,
    /// The index describes a source of a different size.
    SizeMismatch,
    /// The index holds too few checkpoints to split.
    TooFewCheckpoints,
    /// The index does not record the decompressed size, so span lengths are
    /// unknown.
    UnknownOutputSize,
    /// A checkpoint that is not the first carries no window, so nothing can
    /// resume there.
    MissingWindow,
    /// The worker budget is too small to benefit.
    TooFewWorkers,
}

/// One independently decodable region of the file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Span {
    /// Compressed bit offset to resume at.
    pub(crate) start_bit: u64,
    /// First compressed byte to read, which holds `start_bit`.
    pub(crate) read_start: u64,
    /// One past the last compressed byte this span can need.
    pub(crate) read_end: u64,
    /// Decompressed byte offset of this span's first byte.
    pub(crate) output_start: u64,
    /// Decompressed length of this span.
    pub(crate) output_length: u64,
    /// Predecessor history, empty when the span starts a member.
    pub(crate) window: StoredWindow,
}

/// Decides whether `index` can drive a parallel decode of a source.
///
/// `compressed_size` is the source's actual length and `workers` the budget.
pub(crate) fn usable(
    index: Option<&GzipIndex>,
    compressed_size: u64,
    workers: usize,
) -> Result<(), Unusable> {
    let Some(index) = index else {
        return Err(Unusable::Absent);
    };
    if workers < MINIMUM_WORKERS {
        return Err(Unusable::TooFewWorkers);
    }
    if index.validate().is_err() {
        return Err(Unusable::Invalid);
    }
    // A size of zero means the format did not record one, which several of the
    // supported formats do not. Only a recorded mismatch is a refusal.
    if index.compressed_size_in_bytes != 0 && index.compressed_size_in_bytes != compressed_size {
        return Err(Unusable::SizeMismatch);
    }
    if index.uncompressed_size_in_bytes == u64::MAX {
        return Err(Unusable::UnknownOutputSize);
    }
    if index.checkpoint_count() < MINIMUM_CHECKPOINTS {
        return Err(Unusable::TooFewCheckpoints);
    }
    for checkpoint in index.checkpoints().iter().skip(1) {
        if window_for(index, checkpoint).is_none() {
            return Err(Unusable::MissingWindow);
        }
    }
    Ok(())
}

/// Returns the window a checkpoint resumes with, if the index has one.
///
/// The first checkpoint needs none: it starts a member, so its history is
/// empty by definition.
fn window_for<'a>(index: &'a GzipIndex, checkpoint: &Checkpoint) -> Option<&'a StoredWindow> {
    index.windows().get(checkpoint.compressed_offset_in_bits)
}

/// Splits `index` into spans covering the whole decompressed output.
///
/// Consecutive checkpoints delimit each span, and the last runs to the
/// recorded decompressed size. Spans tile the output exactly, so the caller
/// can emit them in order without gaps.
///
/// `maximum_output` bounds how much a worker holds at once. A span longer than
/// that is not split into separate tasks, because splitting it would need a
/// resume point the index does not have; it is decoded in several passes
/// inside one task instead, which is why it is recorded whole here.
pub(crate) fn plan(index: &GzipIndex, compressed_size: u64) -> Vec<Span> {
    let checkpoints = index.checkpoints();
    let mut spans = Vec::with_capacity(checkpoints.len());
    for (position, checkpoint) in checkpoints.iter().enumerate() {
        let output_end = checkpoints
            .get(position + 1)
            .map_or(index.uncompressed_size_in_bytes, |next| {
                next.uncompressed_offset_in_bytes
            });
        if output_end <= checkpoint.uncompressed_offset_in_bytes {
            continue;
        }
        // The next checkpoint's bit offset bounds what this span can read,
        // rounded up to a whole byte because a boundary can fall mid-byte.
        // The final span may read to the end of the source, since its footer
        // and any following member header live there.
        let read_end = checkpoints
            .get(position + 1)
            .map_or(compressed_size, |next| {
                next.compressed_offset_in_bits
                    .div_ceil(8)
                    .min(compressed_size)
            });
        spans.push(Span {
            start_bit: checkpoint.compressed_offset_in_bits,
            read_start: checkpoint.compressed_offset_in_bits / 8,
            read_end,
            output_start: checkpoint.uncompressed_offset_in_bytes,
            output_length: output_end - checkpoint.uncompressed_offset_in_bytes,
            window: window_for(index, checkpoint)
                .cloned()
                .unwrap_or_else(StoredWindow::empty),
        });
    }
    spans
}

/// One run of output inside a span, ending at a member boundary or at the
/// span's end.
///
/// The worker computes each run's CRC32 as it decodes, so the coordinator only
/// combines numbers and never walks the bytes again.
struct Segment {
    /// Decompressed bytes in this run.
    length: u64,
    /// CRC32 of exactly those bytes.
    crc: u32,
    /// The footer that closed the member, when this run ended one.
    footer: Option<(u32, u32)>,
}

/// What decoding one span produced.
struct SpanOutput {
    bytes: Vec<u8>,
    segments: Vec<Segment>,
}

/// Decodes one span with plain zlib, resuming from its checkpoint.
///
/// This is [`crate::IndexedReader`]'s resume, without the seeking: prime the
/// bits below a byte, install the window, then inflate. Nothing is
/// speculative, so nothing has to be validated afterwards beyond the member
/// checks every path performs.
fn decode_span<R: ReadAt + ?Sized>(
    source: &R,
    span: &Span,
    compressed: &mut Vec<u8>,
) -> Result<SpanOutput, DecodeError> {
    let length = usize::try_from(span.read_end.saturating_sub(span.read_start)).map_err(|_| {
        DecodeError::InvalidGzip {
            offset: span.read_start,
            reason: GzipErrorKind::Truncated,
        }
    })?;
    crate::backend::read_range_reuse(source, span.read_start, length, compressed)?;

    let mut inflater = RawInflater::new()?;
    let bit_in_byte = (span.start_bit % 8) as u8;
    let mut consumed = 0_usize;
    if bit_in_byte != 0 {
        let first = *compressed.first().ok_or(DecodeError::InvalidDeflate {
            bit_offset: span.start_bit,
            reason: DeflateErrorKind::Truncated,
        })?;
        inflater.prime(8 - bit_in_byte, first >> bit_in_byte, span.start_bit)?;
        consumed = 1;
    }
    let window = span
        .window
        .decompressed()
        .map_err(|error| DecodeError::input_io(span.read_start, std::io::Error::other(error)))?;
    inflater.set_dictionary_bytes(&window, span.start_bit)?;

    let expected = usize::try_from(span.output_length).map_err(|_| DecodeError::InvalidGzip {
        offset: span.read_start,
        reason: GzipErrorKind::Truncated,
    })?;
    let mut bytes = Vec::with_capacity(expected);
    let mut segments = Vec::new();
    let mut segment_start = 0_usize;
    let mut crc = Crc32::new();

    while bytes.len() < expected {
        if consumed >= compressed.len() {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: span.start_bit,
                reason: DeflateErrorKind::Truncated,
            });
        }
        let before = bytes.len();
        bytes.reserve(expected - bytes.len());
        let step = inflater.inflate(&compressed[consumed..], &mut bytes, false)?;
        consumed += step.consumed;
        crc.update(&bytes[before..]);

        match step.outcome {
            InflateOutcome::StreamEnd => {
                // A member ended inside this span. Its footer follows, byte
                // aligned, and the next member's DEFLATE stream after that.
                let footer_at = consumed;
                if footer_at + 8 > compressed.len() {
                    return Err(DecodeError::InvalidGzip {
                        offset: span.read_start + footer_at as u64,
                        reason: GzipErrorKind::Truncated,
                    });
                }
                let footer_crc = u32::from_le_bytes(
                    compressed[footer_at..footer_at + 4]
                        .try_into()
                        .expect("four bytes"),
                );
                let footer_size = u32::from_le_bytes(
                    compressed[footer_at + 4..footer_at + 8]
                        .try_into()
                        .expect("four bytes"),
                );
                segments.push(Segment {
                    length: (bytes.len() - segment_start) as u64,
                    crc: crc.finish(),
                    footer: Some((footer_crc, footer_size)),
                });
                segment_start = bytes.len();
                crc = Crc32::new();
                consumed = footer_at + 8;

                if bytes.len() >= expected {
                    break;
                }
                // The next member's header sits between the footer and its
                // DEFLATE stream, and the span continues into it.
                consumed += gzip_header_length(&compressed[consumed..], span.read_start)?;
                inflater.reset(span.start_bit)?;
            }
            InflateOutcome::Progress => {
                if step.consumed == 0 && step.produced == 0 {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: span.start_bit,
                        reason: DeflateErrorKind::Stalled,
                    });
                }
            }
            InflateOutcome::Blocked => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: span.start_bit,
                    reason: DeflateErrorKind::Truncated,
                });
            }
        }
    }

    if bytes.len() != expected {
        return Err(DecodeError::UnexpectedOutputSize {
            expected: span.output_length,
            actual: bytes.len() as u64,
        });
    }
    if bytes.len() > segment_start {
        segments.push(Segment {
            length: (bytes.len() - segment_start) as u64,
            crc: crc.finish(),
            footer: None,
        });
    }
    Ok(SpanOutput { bytes, segments })
}

/// Returns the length of the gzip member header at `bytes`.
fn gzip_header_length(bytes: &[u8], offset: u64) -> Result<usize, DecodeError> {
    let truncated = || DecodeError::InvalidGzip {
        offset,
        reason: GzipErrorKind::Truncated,
    };
    if bytes.len() < 10 {
        return Err(truncated());
    }
    if bytes[0] != 0x1f || bytes[1] != 0x8b {
        return Err(DecodeError::InvalidGzip {
            offset,
            reason: GzipErrorKind::BadMagic,
        });
    }
    let flags = bytes[3];
    let mut at = 10_usize;
    if flags & 0b0000_0100 != 0 {
        if at + 2 > bytes.len() {
            return Err(truncated());
        }
        let extra = u16::from_le_bytes(bytes[at..at + 2].try_into().expect("two bytes")) as usize;
        at += 2 + extra;
    }
    for bit in [0b0000_1000_u8, 0b0001_0000_u8] {
        if flags & bit == 0 {
            continue;
        }
        let end = bytes
            .get(at..)
            .and_then(|rest| rest.iter().position(|&byte| byte == 0))
            .ok_or_else(truncated)?;
        at += end + 1;
    }
    if flags & 0b0000_0010 != 0 {
        at += 2;
    }
    if at > bytes.len() {
        return Err(truncated());
    }
    Ok(at)
}

/// Folds span segments into per-member CRC32 and size checks.
struct MemberVerifier {
    crc: u32,
    length: u64,
    members: u64,
}

impl MemberVerifier {
    const fn new() -> Self {
        Self {
            crc: 0,
            length: 0,
            members: 0,
        }
    }

    /// Accepts one span's segments, verifying every member they complete.
    fn accept(&mut self, segments: &[Segment]) -> Result<(), DecodeError> {
        for segment in segments {
            // `crc32_combine64` gives the CRC of the concatenation, which is
            // what lets a member's checksum span several workers' output.
            self.crc = libz_rs_sys::crc32_combine64(
                self.crc as std::ffi::c_ulong,
                segment.crc as std::ffi::c_ulong,
                segment.length as libz_rs_sys::z_off64_t,
            ) as u32;
            self.length += segment.length;

            let Some((expected_crc, expected_size)) = segment.footer else {
                continue;
            };
            if self.crc != expected_crc {
                return Err(DecodeError::ChecksumMismatch {
                    member: self.members,
                    expected: expected_crc,
                    actual: self.crc,
                });
            }
            if (self.length as u32) != expected_size {
                return Err(DecodeError::SizeMismatch {
                    member: self.members,
                    expected: expected_size,
                    actual_mod32: self.length as u32,
                });
            }
            self.members += 1;
            self.crc = 0;
            self.length = 0;
        }
        Ok(())
    }
}

/// Decodes every span in parallel, emitting them in order.
///
/// Workers pull spans from a shared cursor and send their output back tagged
/// with its position; the coordinator emits in order and buffers whatever
/// arrives early. Nothing waits for a wave to finish, so a worker that draws a
/// large span does not idle the others.
///
/// Memory is bounded by an admission window rather than by a barrier: a span
/// is only handed out while it lies within `in_flight_chunks` of the one being
/// emitted. That caps both the reorder buffer and the number of spans resident
/// at once, without making fast workers wait for slow ones.
pub(crate) fn decode_indexed_parallel<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    index: &GzipIndex,
    runtime: &Arc<RuntimeState>,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized + Sync,
    O: Output,
{
    let compressed_size = source
        .len()
        .map_err(|error| DecodeError::input_io(0, error))?;
    let spans = plan(index, compressed_size);
    let workers = config.decoder_threads.min(spans.len()).max(1);
    runtime.set_path(DecoderPath::Indexed);
    runtime.set_adaptive_target(workers);

    let window = config.in_flight_chunks.max(workers).max(2);
    let next_span = AtomicUsize::new(0);
    let emit_cursor = AtomicUsize::new(0);
    let (sender, receiver) = mpsc::channel::<(usize, Result<SpanOutput, DecodeError>)>();

    let mut verifier = MemberVerifier::new();
    let mut total_output = 0_u64;

    let outcome = std::thread::scope(|scope| {
        for _ in 0..workers {
            let sender = sender.clone();
            let next_span = &next_span;
            let emit_cursor = &emit_cursor;
            let spans = &spans;
            scope.spawn(move || {
                let mut compressed = Vec::new();
                loop {
                    if cancelled.load(Ordering::Relaxed) {
                        return;
                    }
                    let position = next_span.load(Ordering::Acquire);
                    if position >= spans.len() {
                        return;
                    }
                    // Wait rather than run ahead of what the coordinator can
                    // emit, which is what bounds resident memory.
                    if position >= emit_cursor.load(Ordering::Acquire) + window {
                        std::thread::park_timeout(Duration::from_micros(50));
                        continue;
                    }
                    if next_span
                        .compare_exchange(
                            position,
                            position + 1,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    let decoded = decode_span(source, &spans[position], &mut compressed);
                    if sender.send((position, decoded)).is_err() {
                        return;
                    }
                }
            });
        }
        drop(sender);

        let mut pending: BTreeMap<usize, SpanOutput> = BTreeMap::new();
        let mut emitted = 0_usize;
        while emitted < spans.len() {
            if cancelled.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            while let Some(decoded) = pending.remove(&emitted) {
                // Verification precedes emission, so a span that fails its
                // member's checksum never reaches the caller.
                verifier.accept(&decoded.segments)?;
                total_output += decoded.bytes.len() as u64;
                output.emit(decoded.bytes)?;
                emitted += 1;
                emit_cursor.store(emitted, Ordering::Release);
            }
            if emitted >= spans.len() {
                break;
            }
            match receiver.recv() {
                Ok((position, result)) => {
                    pending.insert(position, result?);
                }
                Err(_) => return Err(DecodeError::WorkerPanicked),
            }
        }
        Ok(())
    });
    outcome?;

    if verifier.length != 0 {
        return Err(DecodeError::InvalidGzip {
            offset: compressed_size,
            reason: GzipErrorKind::Truncated,
        });
    }
    runtime.set_member_count(verifier.members);

    Ok(DecodeReport {
        compressed_bytes: compressed_size,
        decompressed_bytes: total_output,
        member_count: verifier.members,
        decoder_threads: workers,
        index: None,
        line_count: None,
        format: Format::Gzip,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::WINDOW_SIZE;

    fn indexed(points: &[(u64, u64)], uncompressed: u64, compressed: u64) -> GzipIndex {
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = compressed;
        index.uncompressed_size_in_bytes = uncompressed;
        for (position, &(bits, bytes)) in points.iter().enumerate() {
            let window = if position == 0 {
                StoredWindow::empty()
            } else {
                StoredWindow::from_raw(vec![0x5a; WINDOW_SIZE])
            };
            index.push(
                Checkpoint {
                    compressed_offset_in_bits: bits,
                    uncompressed_offset_in_bytes: bytes,
                    line_offset: 0,
                },
                window,
            );
        }
        index
    }

    fn sample() -> GzipIndex {
        indexed(
            &[(80, 0), (8 * 4_000, 100_000), (8 * 9_000, 250_000)],
            400_000,
            12_000,
        )
    }

    #[test]
    fn a_usable_index_is_accepted() {
        assert_eq!(usable(Some(&sample()), 12_000, 4), Ok(()));
    }

    #[test]
    fn every_reason_to_refuse_is_reported() {
        assert_eq!(usable(None, 12_000, 4), Err(Unusable::Absent));
        assert_eq!(
            usable(Some(&sample()), 12_000, 1),
            Err(Unusable::TooFewWorkers)
        );
        assert_eq!(
            usable(Some(&sample()), 99_999, 4),
            Err(Unusable::SizeMismatch)
        );

        let short = indexed(&[(80, 0), (8 * 4_000, 100_000)], 400_000, 12_000);
        assert_eq!(
            usable(Some(&short), 12_000, 4),
            Err(Unusable::TooFewCheckpoints)
        );

        let mut unknown_size = sample();
        unknown_size.uncompressed_size_in_bytes = u64::MAX;
        assert_eq!(
            usable(Some(&unknown_size), 12_000, 4),
            Err(Unusable::UnknownOutputSize)
        );
    }

    #[test]
    fn an_interior_checkpoint_without_a_window_is_refused() {
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 12_000;
        index.uncompressed_size_in_bytes = 400_000;
        for &(bits, bytes) in &[(80_u64, 0_u64), (8 * 4_000, 100_000), (8 * 9_000, 250_000)] {
            index.push(
                Checkpoint {
                    compressed_offset_in_bits: bits,
                    uncompressed_offset_in_bytes: bytes,
                    line_offset: 0,
                },
                StoredWindow::empty(),
            );
        }
        assert_eq!(
            usable(Some(&index), 12_000, 4),
            Err(Unusable::MissingWindow)
        );
    }

    #[test]
    fn an_index_without_a_recorded_compressed_size_is_accepted() {
        // Several of the supported formats do not store one, and an absent
        // size is not a disagreement.
        let mut index = sample();
        index.compressed_size_in_bytes = 0;
        assert_eq!(usable(Some(&index), 12_000, 4), Ok(()));
    }

    #[test]
    fn spans_tile_the_output_exactly() {
        let index = sample();
        let spans = plan(&index, 12_000);
        assert_eq!(spans.len(), 3);

        let mut expected_start = 0;
        for span in &spans {
            assert_eq!(span.output_start, expected_start);
            assert!(span.output_length > 0);
            expected_start += span.output_length;
        }
        assert_eq!(expected_start, index.uncompressed_size_in_bytes);
    }

    #[test]
    fn a_span_reads_from_its_own_byte_to_the_next_boundary() {
        let spans = plan(&sample(), 12_000);
        assert_eq!(spans[0].start_bit, 80);
        assert_eq!(spans[0].read_start, 10);
        assert_eq!(spans[0].read_end, 4_000);
        assert_eq!(spans[1].read_start, 4_000);
        assert_eq!(spans[1].read_end, 9_000);
        // The last span may need everything to the end of the source, since
        // its footer lives there.
        assert_eq!(spans[2].read_end, 12_000);
    }

    #[test]
    fn only_the_first_span_starts_without_history() {
        let spans = plan(&sample(), 12_000);
        assert!(spans[0].window.is_empty());
        assert!(!spans[1].window.is_empty());
        assert!(!spans[2].window.is_empty());
    }

    #[test]
    fn a_checkpoint_that_adds_no_output_is_dropped() {
        let index = indexed(
            &[(80, 0), (8 * 4_000, 100_000), (8 * 9_000, 100_000)],
            400_000,
            12_000,
        );
        let spans = plan(&index, 12_000);
        assert_eq!(spans.len(), 2, "an empty span is not worth a task");
        assert_eq!(spans[1].output_length, 300_000);
    }
}
