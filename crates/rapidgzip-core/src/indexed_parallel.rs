//! Deciding whether an index can drive a parallel decode, and how to split it.
//!
//! The speculative grid exists because a worker landing on a guessed offset
//! knows neither its DEFLATE block boundary nor the history before it. An
//! index records both, so its checkpoints partition the file into spans that
//! plain zlib can decode independently.
//!
//! This module owns that decision and the resulting plan. Running the plan is
//! [`crate::backend`]'s business.

// The worker and the coordinator that consume this plan land in the next two
// commits on this branch. Until then nothing calls it, which is a state this
// allow makes explicit rather than something to discover from a red build.
#![allow(dead_code)]

use crate::index::{Checkpoint, GzipIndex, StoredWindow};

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
