//! Parallel full-stream decode driven by an imported [`GzipIndex`].
//!
//! This is the original rapidgzip “with index” fast path: each worker resumes
//! raw DEFLATE at a known checkpoint (bit offset + predecessor window) and
//! produces a fixed uncompressed span. No marker/window speculation is used.
//!
//! # CRC / ISIZE policy (M2)
//!
//! Like [`crate::IndexedReader`], this path does **not** verify member payload
//! CRC32 or ISIZE. Segments may start mid-member, so a complete member CRC is
//! not always available. Correctness is guaranteed by producing the exact byte
//! ranges described by the index checkpoints.

use crate::backend::{Output, RawInflater};
use crate::config::Config;
use crate::gzip::{SourceCursor, parse_member_header};
use crate::index::{GzipIndex, INDEXED_GZIP_WINDOW_SIZE, IndexError, StoredWindow};
use crate::inflate_backend::{InflateBackend, InflateFlush, status as inflate_status};
use crate::parallel::Window;
use crate::{DecodeError, DecodeReport, DeflateErrorKind, ReadAt};
use crossbeam_deque::{Injector, Steal};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// Work unit: inflate from a checkpoint until `need` uncompressed bytes, or
/// (when `need` is `None`) until compressed input is exhausted after the final
/// member footer.
///
/// Segment order is the index in the `segments` slice (also the task id).
#[derive(Clone, Debug)]
struct Segment {
    start_bit: u64,
    /// Exact uncompressed length, or `None` for open-ended tail (decode until
    /// the compressed source ends — used when the index omits EOF size, e.g.
    /// third-party htslib BGZI indexes that only list block *starts*).
    need: Option<usize>,
    /// Predecessor history (empty = independent start / member boundary).
    window: Vec<u8>,
}

struct SegmentResult {
    id: usize,
    result: Result<SegmentOutput, DecodeError>,
}

struct SegmentOutput {
    bytes: Vec<u8>,
    /// Number of gzip members that reached `Z_STREAM_END` while decoding this
    /// segment (may be zero when the segment ends mid-member).
    members_ended: u64,
}

/// Validates `index` against `source` using the same rules as
/// [`crate::IndexedReader::new`].
fn validate_index<R: ReadAt + ?Sized>(source: &R, index: &GzipIndex) -> Result<(), DecodeError> {
    index.validate().map_err(DecodeError::InvalidIndex)?;
    if index.checkpoints.is_empty() {
        return Err(DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
            "index has no checkpoints",
        )));
    }

    if index.compressed_size_in_bytes != u64::MAX {
        let archive_size = source
            .len()
            .map_err(|error| DecodeError::input_io(0, error))?;
        if archive_size != index.compressed_size_in_bytes {
            return Err(DecodeError::InvalidIndex(IndexError::ArchiveSizeMismatch {
                index_size: index.compressed_size_in_bytes,
                archive_size,
            }));
        }
    }
    Ok(())
}

fn is_eof_checkpoint(index: &GzipIndex, checkpoint_uncompressed: u64, start_bit: u64) -> bool {
    let known_u = index.uncompressed_size_in_bytes;
    if known_u != u64::MAX && checkpoint_uncompressed >= known_u {
        return true;
    }
    let known_c = index.compressed_size_in_bytes;
    if known_c != u64::MAX {
        let max_bits = known_c.saturating_mul(8);
        if start_bit >= max_bits {
            return true;
        }
    }
    false
}

fn window_bytes_from_stored(stored: Option<&StoredWindow>) -> Result<Vec<u8>, DecodeError> {
    let owned = match stored {
        Some(window) => window.decompressed().map_err(DecodeError::InvalidIndex)?,
        None => return Ok(Vec::new()),
    };
    if owned.is_empty() {
        return Ok(Vec::new());
    }
    let window_size = INDEXED_GZIP_WINDOW_SIZE as usize;
    if owned.len() > window_size {
        Ok(owned[owned.len() - window_size..].to_vec())
    } else {
        Ok(owned.into_owned())
    }
}

/// Builds non-empty segments from consecutive checkpoint pairs.
///
/// The final EOF checkpoint (empty window, uncompressed offset equal to the
/// archive size) only appears as a segment *end* bound; a zero-length span
/// from a duplicate offset is skipped.
///
/// When the index declares a known uncompressed size but the last non-EOF
/// checkpoint sits strictly before that size (common for third-party gztool
/// indexes that omit an explicit EOF point), a final tail segment is appended
/// so `decode_with_index` covers the full payload.
fn build_segments(index: &GzipIndex) -> Result<Vec<Segment>, DecodeError> {
    let cps = &index.checkpoints;
    let mut segments = Vec::new();
    // Highest uncompressed offset covered by a scheduled segment so far.
    let mut covered_up_to = 0_u64;

    for i in 0..cps.len().saturating_sub(1) {
        let curr = &cps[i];
        let next = &cps[i + 1];
        if is_eof_checkpoint(
            index,
            curr.uncompressed_offset_in_bytes,
            curr.compressed_offset_in_bits,
        ) {
            continue;
        }
        let need_u64 = next
            .uncompressed_offset_in_bytes
            .saturating_sub(curr.uncompressed_offset_in_bytes);
        if need_u64 == 0 {
            continue;
        }
        let need = usize::try_from(need_u64).map_err(|_| {
            DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
                "segment uncompressed span exceeds addressable memory",
            ))
        })?;
        let window = window_bytes_from_stored(index.window_for(curr.compressed_offset_in_bits))?;
        segments.push(Segment {
            start_bit: curr.compressed_offset_in_bits,
            need: Some(need),
            window,
        });
        covered_up_to = covered_up_to.max(next.uncompressed_offset_in_bytes);
    }

    // Tail after the last scheduled pair.
    //
    // - Known uncompressed size larger than coverage: exact-length tail
    //   (gztool-style indexes without an EOF checkpoint).
    // - Unknown uncompressed size: open-ended tail from the last non-EOF
    //   checkpoint through compressed EOF (htslib BGZI lists only block
    //   *starts*, so the final block has no end bound in the index).
    let known_u = index.uncompressed_size_in_bytes;
    let need_exact_tail = known_u != u64::MAX && known_u > covered_up_to;
    let need_open_tail = known_u == u64::MAX
        && cps.iter().any(|cp| {
            !is_eof_checkpoint(
                index,
                cp.uncompressed_offset_in_bytes,
                cp.compressed_offset_in_bits,
            )
        });

    if need_exact_tail || need_open_tail {
        // Prefer the checkpoint at `covered_up_to` (end of last pair). Fall
        // back to the last non-EOF checkpoint at or before that point.
        let start = cps
            .iter()
            .rev()
            .find(|cp| {
                !is_eof_checkpoint(
                    index,
                    cp.uncompressed_offset_in_bytes,
                    cp.compressed_offset_in_bits,
                ) && cp.uncompressed_offset_in_bytes == covered_up_to
            })
            .or_else(|| {
                cps.iter().rev().find(|cp| {
                    !is_eof_checkpoint(
                        index,
                        cp.uncompressed_offset_in_bytes,
                        cp.compressed_offset_in_bits,
                    ) && cp.uncompressed_offset_in_bytes <= covered_up_to
                })
            });
        if let Some(curr) = start {
            if curr.uncompressed_offset_in_bytes > covered_up_to {
                return Err(DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
                    "index checkpoints leave a gap before the claimed uncompressed size",
                )));
            }
            if curr.uncompressed_offset_in_bytes < covered_up_to {
                return Err(DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
                    "index has no checkpoint at the end of the last segment for the remaining tail",
                )));
            }
            let window =
                window_bytes_from_stored(index.window_for(curr.compressed_offset_in_bits))?;
            if need_exact_tail {
                let need_u64 = known_u - covered_up_to;
                let need = usize::try_from(need_u64).map_err(|_| {
                    DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
                        "segment uncompressed span exceeds addressable memory",
                    ))
                })?;
                segments.push(Segment {
                    start_bit: curr.compressed_offset_in_bits,
                    need: Some(need),
                    window,
                });
            } else {
                // Avoid a zero-length open tail when pairs already exhausted
                // coverage and the last checkpoint is itself EOF-like under
                // compressed size alone (handled by is_eof above).
                segments.push(Segment {
                    start_bit: curr.compressed_offset_in_bits,
                    need: None,
                    window,
                });
            }
        }
    }

    Ok(segments)
}

/// Inflates from `start_bit`: exactly `Some(need)` uncompressed bytes, or until
/// compressed EOF when `need` is `None`.
///
/// Crosses gzip member boundaries when `Z_STREAM_END` is reached before the
/// span is complete (footer is consumed without CRC/ISIZE checks; the next
/// member header is parsed and raw inflate continues with an empty window).
/// Open-ended tails stop after a footer when no further compressed input
/// remains (including trailing BGZF empty EOF members that produce zero
/// payload before the next end).
fn inflate_segment<R: ReadAt + ?Sized>(
    source: &R,
    start_bit: u64,
    window_bytes: &[u8],
    need: Option<usize>,
    page_size: usize,
    cancelled: &AtomicBool,
) -> Result<SegmentOutput, DecodeError> {
    if need == Some(0) {
        return Ok(SegmentOutput {
            bytes: Vec::new(),
            members_ended: 0,
        });
    }

    let window = if window_bytes.is_empty() {
        Window::empty()
    } else {
        Window::new(window_bytes.to_vec()).map_err(|_| {
            DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
                "checkpoint window exceeds 32 KiB",
            ))
        })?
    };

    let (mut inflater, mut compressed_byte) =
        RawInflater::prepare_at_bit_offset(start_bit, &window, source, page_size, true)?;

    let exact = need;
    let mut out: Vec<u8> = match exact {
        Some(n) => vec![0_u8; n],
        None => Vec::with_capacity(page_size.min(64 * 1024)),
    };
    let mut filled = 0usize;
    let mut members_ended = 0_u64;

    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }
        if let Some(n) = exact
            && filled >= n
        {
            break;
        }

        let mut cursor = SourceCursor::new(source, page_size)?;
        cursor.seek(compressed_byte)?;

        if cursor.at_end() {
            if exact.is_none() {
                // Open-ended: natural completion at compressed EOF.
                break;
            }
            return Err(DecodeError::InvalidDeflate {
                bit_offset: compressed_byte.saturating_mul(8),
                reason: DeflateErrorKind::Truncated,
            });
        }

        let room = match exact {
            Some(n) => n - filled,
            None => {
                // Grow geometrically for open-ended tails.
                if filled == out.len() {
                    let grow = out.capacity().clamp(4096, 4 * 1024 * 1024);
                    out.resize(filled + grow, 0);
                }
                out.len() - filled
            }
        };
        if room == 0 {
            break;
        }

        let dest = &mut out[filled..filled + room];
        let input = cursor.available()?;
        let step =
            InflateBackend::inflate_into_slice(&mut inflater, input, dest, InflateFlush::NoFlush)?;
        cursor.advance(step.consumed);
        compressed_byte = cursor.position();
        filled += step.produced;

        match step.status {
            inflate_status::STREAM_END => {
                members_ended = members_ended.saturating_add(1);
                // Skip gzip footer (CRC32 + ISIZE); do not verify on this path.
                if cursor.position() + 8 > cursor.length() {
                    return Err(DecodeError::InvalidGzip {
                        offset: cursor.position(),
                        reason: crate::GzipErrorKind::Truncated,
                    });
                }
                let _footer = cursor.read_exact::<8>(cursor.position())?;
                compressed_byte = cursor.position();

                if let Some(n) = exact {
                    if filled >= n {
                        break;
                    }
                } else if cursor.at_end() {
                    // Open-ended complete after last member footer.
                    break;
                }

                if cursor.at_end() {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: compressed_byte.saturating_mul(8),
                        reason: DeflateErrorKind::Truncated,
                    });
                }

                // Next gzip member: parse header and start raw inflate with empty window.
                let header = parse_member_header(&mut cursor, false)?;
                debug_assert_eq!(header.deflate_start, cursor.position());
                inflater = <RawInflater as InflateBackend>::create()?;
                let empty = Window::empty();
                InflateBackend::set_dictionary(
                    &mut inflater,
                    &empty,
                    header.deflate_start.saturating_mul(8),
                )?;
                compressed_byte = cursor.position();
            }
            inflate_status::OK | inflate_status::BUF_ERROR
                if step.consumed != 0 || step.produced != 0 =>
            {
                // Progress made; continue until the target is satisfied.
            }
            inflate_status::DATA_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: compressed_byte.saturating_mul(8),
                    reason: DeflateErrorKind::InvalidData,
                });
            }
            inflate_status::NEED_DICT => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: compressed_byte.saturating_mul(8),
                    reason: DeflateErrorKind::UnexpectedDictionary,
                });
            }
            inflate_status::OK | inflate_status::BUF_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: compressed_byte.saturating_mul(8),
                    reason: DeflateErrorKind::Stalled,
                });
            }
            other => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: compressed_byte.saturating_mul(8),
                    reason: DeflateErrorKind::BackendStatus(other),
                });
            }
        }
    }

    if let Some(n) = exact {
        debug_assert_eq!(filled, n);
        out.truncate(n);
    } else {
        out.truncate(filled);
    }
    Ok(SegmentOutput {
        bytes: out,
        members_ended,
    })
}

fn report_from_totals(
    config: &Config,
    source_len: u64,
    index: &GzipIndex,
    decompressed_bytes: u64,
    member_count: u64,
) -> DecodeReport {
    let compressed_bytes = if index.compressed_size_in_bytes != u64::MAX {
        index.compressed_size_in_bytes
    } else {
        source_len
    };
    DecodeReport {
        compressed_bytes,
        decompressed_bytes,
        member_count,
        decoder_threads: config.decoder_threads,
        // Prefer not cloning huge window maps; the caller's index is unchanged.
        index: None,
        // Indexed full-stream decode does not re-count newlines; use the
        // caller's index or a keep_index decode when line totals are needed.
        line_count: None,
    }
}

fn check_output_limit(config: &Config, total: u64, add: u64) -> Result<u64, DecodeError> {
    let next = total
        .checked_add(add)
        .ok_or(DecodeError::OutputLimitExceeded {
            limit: config.output_limit.unwrap_or(u64::MAX),
        })?;
    if config.output_limit.is_some_and(|limit| next > limit) {
        return Err(DecodeError::OutputLimitExceeded {
            limit: config.output_limit.expect("checked as some"),
        });
    }
    Ok(next)
}

fn decode_segments_serial<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    segments: &[Segment],
) -> Result<(u64, u64), DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    let mut total_output = 0_u64;
    let mut member_count = 0_u64;
    for segment in segments {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }
        let decoded = inflate_segment(
            source,
            segment.start_bit,
            &segment.window,
            segment.need,
            config.input_page_size,
            cancelled,
        )?;
        total_output = check_output_limit(config, total_output, decoded.bytes.len() as u64)?;
        member_count = member_count.saturating_add(decoded.members_ended);
        if !decoded.bytes.is_empty() {
            output.emit(decoded.bytes)?;
        }
    }
    Ok((total_output, member_count))
}

struct StopGuard<'a>(&'a AtomicBool);

impl Drop for StopGuard<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

fn decode_segments_parallel<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    segments: &[Segment],
) -> Result<(u64, u64), DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    let segment_count = segments.len();
    let worker_count = config.decoder_threads.min(segment_count).max(1);
    let task_queue = Arc::new(Injector::new());
    let stopped = Arc::new(AtomicBool::new(false));
    let available_tasks = Arc::new(AtomicUsize::new(0));
    let work_signal = Arc::new((Mutex::new(()), Condvar::new()));
    let pending_limit = config.in_flight_chunks.max(worker_count);

    // Schedule an initial window of tasks.
    let initial = worker_count.min(segment_count);
    for id in 0..initial {
        task_queue.push(id);
    }
    available_tasks.store(initial, Ordering::Release);

    let (sender, receiver) = mpsc::sync_channel::<SegmentResult>(pending_limit);
    let mut next_to_schedule = initial;
    let mut next_to_emit = 0usize;
    let mut running = initial;
    let mut reordered = BTreeMap::new();
    let mut total_output = 0_u64;
    let mut member_count = 0_u64;

    let page_size = config.input_page_size;
    let scoped_result = thread::scope(|scope| -> Result<(), DecodeError> {
        let _stop_on_exit = StopGuard(&stopped);
        for _ in 0..worker_count {
            let queue = Arc::clone(&task_queue);
            let worker_stopped = Arc::clone(&stopped);
            let available_tasks = Arc::clone(&available_tasks);
            let work_signal = Arc::clone(&work_signal);
            let sender = sender.clone();
            scope.spawn(move || {
                while !worker_stopped.load(Ordering::Relaxed) && !cancelled.load(Ordering::Relaxed)
                {
                    let id = loop {
                        match queue.steal() {
                            Steal::Success(id) => {
                                available_tasks.fetch_sub(1, Ordering::AcqRel);
                                break id;
                            }
                            Steal::Retry => std::hint::spin_loop(),
                            Steal::Empty => {
                                if worker_stopped.load(Ordering::Relaxed)
                                    || cancelled.load(Ordering::Relaxed)
                                {
                                    return;
                                }
                                let (lock, signal) = &*work_signal;
                                let guard =
                                    lock.lock().expect("indexed decode work mutex poisoned");
                                let _ = signal
                                    .wait_timeout_while(guard, Duration::from_millis(1), |_| {
                                        available_tasks.load(Ordering::Acquire) == 0
                                            && !worker_stopped.load(Ordering::Relaxed)
                                            && !cancelled.load(Ordering::Relaxed)
                                    })
                                    .expect("indexed decode work mutex poisoned");
                            }
                        }
                    };

                    let segment = &segments[id];
                    let result = if cancelled.load(Ordering::Relaxed) {
                        Err(DecodeError::Cancelled)
                    } else {
                        inflate_segment(
                            source,
                            segment.start_bit,
                            &segment.window,
                            segment.need,
                            page_size,
                            cancelled,
                        )
                    };
                    // Best-effort send; coordinator stop will drain/disconnect.
                    let _ = sender.send(SegmentResult { id, result });
                }
            });
        }
        drop(sender);

        while next_to_emit < segment_count {
            if cancelled.load(Ordering::Relaxed) {
                stopped.store(true, Ordering::Relaxed);
                return Err(DecodeError::Cancelled);
            }
            let result = match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(result) => {
                    running = running.saturating_sub(1);
                    result
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    stopped.store(true, Ordering::Relaxed);
                    return Err(DecodeError::WorkerPanicked);
                }
            };
            reordered.insert(result.id, result.result);

            while let Some(result) = reordered.remove(&next_to_emit) {
                let decoded = match result {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        stopped.store(true, Ordering::Relaxed);
                        return Err(error);
                    }
                };
                total_output =
                    check_output_limit(config, total_output, decoded.bytes.len() as u64)?;
                member_count = member_count.saturating_add(decoded.members_ended);
                if !decoded.bytes.is_empty() {
                    output.emit(decoded.bytes)?;
                }
                next_to_emit += 1;
            }

            while running < worker_count
                && next_to_schedule < segment_count
                && reordered.len() < pending_limit
            {
                let (lock, signal) = &*work_signal;
                let _guard = lock.lock().expect("indexed decode work mutex poisoned");
                task_queue.push(next_to_schedule);
                available_tasks.fetch_add(1, Ordering::Release);
                next_to_schedule += 1;
                running += 1;
                signal.notify_one();
            }
        }
        stopped.store(true, Ordering::Relaxed);
        Ok(())
    });
    stopped.store(true, Ordering::Relaxed);
    scoped_result?;
    Ok((total_output, member_count))
}

/// Full-stream decode using a prebuilt or imported index.
///
/// When `config.decoder_threads > 1` and more than one non-empty segment exists,
/// segments are inflated in parallel and emitted in checkpoint order.
///
/// The caller's `index` is not modified. The returned [`DecodeReport::index`]
/// is always `None` so large window maps are not cloned.
pub(crate) fn decode_with_index<R, O>(
    source: &R,
    index: &GzipIndex,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    validate_index(source, index)?;
    let source_len = source
        .len()
        .map_err(|error| DecodeError::input_io(0, error))?;

    let segments = build_segments(index)?;
    if segments.is_empty() {
        // Empty payload (or only EOF / zero-length checkpoints).
        let decompressed = if index.uncompressed_size_in_bytes != u64::MAX {
            index.uncompressed_size_in_bytes
        } else {
            0
        };
        if decompressed != 0 {
            // Index claims data but no usable segment could be formed.
            return Err(DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
                "index has no usable decode segments for the claimed uncompressed size",
            )));
        }
        return Ok(report_from_totals(config, source_len, index, 0, 0));
    }

    let (total_output, member_count) = if config.decoder_threads == 1 || segments.len() <= 1 {
        decode_segments_serial(source, config, cancelled, output, &segments)?
    } else {
        decode_segments_parallel(source, config, cancelled, output, &segments)?
    };

    // Prefer the index size when known (matches CLI indexed path).
    let decompressed_bytes = if index.uncompressed_size_in_bytes != u64::MAX {
        if total_output != index.uncompressed_size_in_bytes {
            return Err(DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
                "decoded output size does not match index uncompressed size",
            )));
        }
        index.uncompressed_size_in_bytes
    } else {
        total_output
    };

    Ok(report_from_totals(
        config,
        source_len,
        index,
        decompressed_bytes,
        member_count,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{Checkpoint, WindowMap};

    fn sample_index() -> GzipIndex {
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 100;
        index.uncompressed_size_in_bytes = 1000;
        index.checkpoints = vec![
            Checkpoint {
                compressed_offset_in_bits: 80,
                uncompressed_offset_in_bytes: 0,
                line_offset: 0,
            },
            Checkpoint {
                compressed_offset_in_bits: 400,
                uncompressed_offset_in_bytes: 500,
                line_offset: 0,
            },
            Checkpoint {
                compressed_offset_in_bits: 800,
                uncompressed_offset_in_bytes: 1000,
                line_offset: 0,
            },
        ];
        index.windows = WindowMap::new();
        index.windows.insert(80, StoredWindow::empty());
        index
            .windows
            .insert(400, StoredWindow::from_raw(vec![1, 2, 3]));
        index.windows.insert(800, StoredWindow::empty());
        index
    }

    #[test]
    fn build_segments_skips_zero_and_eof() {
        let index = sample_index();
        let segments = build_segments(&index).unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].start_bit, 80);
        assert_eq!(segments[0].need, Some(500));
        assert!(segments[0].window.is_empty());
        assert_eq!(segments[1].start_bit, 400);
        assert_eq!(segments[1].need, Some(500));
        assert_eq!(segments[1].window, vec![1, 2, 3]);
    }

    #[test]
    fn build_segments_empty_when_only_eof_pair() {
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 20;
        index.uncompressed_size_in_bytes = 0;
        index.checkpoints = vec![
            Checkpoint {
                compressed_offset_in_bits: 80,
                uncompressed_offset_in_bytes: 0,
                line_offset: 0,
            },
            Checkpoint {
                compressed_offset_in_bits: 160,
                uncompressed_offset_in_bytes: 0,
                line_offset: 0,
            },
        ];
        index.windows.insert(80, StoredWindow::empty());
        index.windows.insert(160, StoredWindow::empty());
        let segments = build_segments(&index).unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn build_segments_appends_tail_when_last_checkpoint_before_eof() {
        // Third-party style: points at 0 and 500, size 1000, no EOF checkpoint.
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 100;
        index.uncompressed_size_in_bytes = 1000;
        index.checkpoints = vec![
            Checkpoint {
                compressed_offset_in_bits: 80,
                uncompressed_offset_in_bytes: 0,
                line_offset: 0,
            },
            Checkpoint {
                compressed_offset_in_bits: 400,
                uncompressed_offset_in_bytes: 500,
                line_offset: 0,
            },
        ];
        index.windows.insert(80, StoredWindow::empty());
        index
            .windows
            .insert(400, StoredWindow::from_raw(vec![9, 8, 7]));
        let segments = build_segments(&index).unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].start_bit, 80);
        assert_eq!(segments[0].need, Some(500));
        assert_eq!(segments[1].start_bit, 400);
        assert_eq!(segments[1].need, Some(500));
        assert_eq!(segments[1].window, vec![9, 8, 7]);
    }

    #[test]
    fn build_segments_single_checkpoint_covers_to_known_size() {
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 50;
        index.uncompressed_size_in_bytes = 250;
        index.checkpoints = vec![Checkpoint {
            compressed_offset_in_bits: 80,
            uncompressed_offset_in_bytes: 0,
            line_offset: 0,
        }];
        index.windows.insert(80, StoredWindow::empty());
        let segments = build_segments(&index).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].start_bit, 80);
        assert_eq!(segments[0].need, Some(250));
    }

    #[test]
    fn build_segments_open_tail_when_uncompressed_size_unknown() {
        // htslib BGZI shape: block starts only, no EOF, size unknown.
        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = 300;
        index.uncompressed_size_in_bytes = u64::MAX;
        index.checkpoints = vec![
            Checkpoint {
                compressed_offset_in_bits: 0,
                uncompressed_offset_in_bytes: 0,
                line_offset: 0,
            },
            Checkpoint {
                compressed_offset_in_bits: 800,
                uncompressed_offset_in_bytes: 1000,
                line_offset: 0,
            },
            Checkpoint {
                compressed_offset_in_bits: 1600,
                uncompressed_offset_in_bytes: 2000,
                line_offset: 0,
            },
        ];
        index.windows.insert(0, StoredWindow::empty());
        index.windows.insert(800, StoredWindow::empty());
        index.windows.insert(1600, StoredWindow::empty());
        let segments = build_segments(&index).unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].need, Some(1000));
        assert_eq!(segments[1].need, Some(1000));
        assert_eq!(segments[2].start_bit, 1600);
        assert_eq!(segments[2].need, None); // open-ended last block
    }
}
