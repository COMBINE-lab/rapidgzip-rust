//! Index construction during a decode.
//!
//! Decode paths offer checkpoints at boundaries they already compute, once the
//! associated predecessor window is resolved. Offers may arrive out of order,
//! because parallel paths discover boundaries concurrently, so the builder
//! orders and thins them when the index is finalized rather than requiring the
//! caller to serialize.

use super::{Checkpoint, GzipIndex, IndexError, StoredWindow};
use std::collections::{BTreeMap, BTreeSet};

/// Collects checkpoints into a [`GzipIndex`].
pub(crate) struct IndexBuilder {
    points: Vec<(Checkpoint, StoredWindow)>,
    spacing: u64,
    compress_windows: bool,
    compressed_size: u64,
    uncompressed_size: u64,
    total_line_count: Option<u64>,
    error: Option<IndexError>,
    /// Whether line offsets are being resolved for this index.
    annotate_lines: bool,
    /// Checkpoint offsets whose line offset the output has not reached yet.
    pending_lines: BTreeSet<u64>,
    /// Resolved line offset per checkpoint decompressed offset.
    resolved_lines: BTreeMap<u64, u64>,
}

impl IndexBuilder {
    /// Creates a builder targeting `spacing` decompressed bytes between
    /// interior checkpoints.
    pub(crate) const fn new(spacing: u64, compress_windows: bool) -> Self {
        Self {
            points: Vec::new(),
            spacing,
            compress_windows,
            compressed_size: 0,
            uncompressed_size: u64::MAX,
            total_line_count: None,
            error: None,
            annotate_lines: false,
            pending_lines: BTreeSet::new(),
            resolved_lines: BTreeMap::new(),
        }
    }

    /// Resolves a line offset for every checkpoint as the output passes it.
    pub(crate) const fn enable_line_annotation(&mut self) {
        self.annotate_lines = true;
    }

    /// Resolves the line offset of every checkpoint inside one output run.
    ///
    /// `start` is the decompressed offset of `bytes` and `lines_before` is the
    /// newline count preceding it. Offsets are visited in order, so the scan
    /// over `bytes` advances once rather than restarting per checkpoint.
    pub(crate) fn note_output(&mut self, start: u64, lines_before: u64, bytes: &[u8]) {
        if !self.annotate_lines || self.pending_lines.is_empty() {
            return;
        }
        let end = start.saturating_add(bytes.len() as u64);
        let reached: Vec<u64> = self.pending_lines.range(start..end).copied().collect();
        let mut scanned = 0_usize;
        let mut lines = lines_before;
        for offset in reached {
            let target = (offset - start) as usize;
            lines += bytes[scanned..target]
                .iter()
                .filter(|&&byte| byte == b'\n')
                .count() as u64;
            scanned = target;
            self.pending_lines.remove(&offset);
            self.resolved_lines.insert(offset, lines);
        }
    }

    /// Offers a checkpoint whose predecessor window is `window`.
    ///
    /// An empty window marks a point that needs no history, such as a member
    /// boundary or an independent BGZF block. Such points are always kept;
    /// interior points are thinned to the configured spacing.
    pub(crate) fn offer(&mut self, checkpoint: Checkpoint, window: &[u8]) {
        if self.error.is_some() {
            return;
        }
        let stored = if window.is_empty() {
            StoredWindow::empty()
        } else {
            match StoredWindow::from_raw_maybe_compress(window.to_vec(), self.compress_windows) {
                Ok(stored) => stored,
                Err(error) => {
                    self.error = Some(error);
                    return;
                }
            }
        };
        if self.annotate_lines {
            self.pending_lines
                .insert(checkpoint.uncompressed_offset_in_bytes);
        }
        self.points.push((checkpoint, stored));
    }

    /// Records the verified sizes and the final newline count of the decode.
    ///
    /// A checkpoint sitting exactly at the end of the output is resolved here,
    /// since no further run of bytes passes it.
    pub(crate) fn finish(
        &mut self,
        compressed_bytes: u64,
        uncompressed_bytes: u64,
        line_count: Option<u64>,
    ) {
        self.compressed_size = compressed_bytes;
        self.uncompressed_size = uncompressed_bytes;
        self.total_line_count = line_count;
        if let Some(lines) = line_count {
            if self.pending_lines.remove(&uncompressed_bytes) {
                self.resolved_lines.insert(uncompressed_bytes, lines);
            }
        }
    }

    /// Orders and thins the collected points into a validated index.
    pub(crate) fn into_index(mut self) -> Result<GzipIndex, IndexError> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }

        self.points.sort_by_key(|(checkpoint, _)| {
            (
                checkpoint.uncompressed_offset_in_bytes,
                checkpoint.compressed_offset_in_bits,
            )
        });

        let mut index = GzipIndex::new();
        index.compressed_size_in_bytes = self.compressed_size;
        index.uncompressed_size_in_bytes = self.uncompressed_size;
        index.checkpoint_spacing_in_bytes = self.spacing;

        // An index claims line counters only when every kept checkpoint got
        // one. Writing zeros for the rest is what gztool would silently
        // believe, so a partial annotation drops the claim entirely.
        let mut fully_annotated = self.annotate_lines;
        let mut last: Option<Checkpoint> = None;
        for (mut checkpoint, window) in self.points {
            if self.annotate_lines {
                match self
                    .resolved_lines
                    .get(&checkpoint.uncompressed_offset_in_bytes)
                {
                    Some(&lines) => checkpoint.line_offset = lines,
                    None => fully_annotated = false,
                }
            }
            if let Some(last) = last {
                if checkpoint.uncompressed_offset_in_bytes <= last.uncompressed_offset_in_bytes
                    || checkpoint.compressed_offset_in_bits <= last.compressed_offset_in_bits
                {
                    continue;
                }
                let gap =
                    checkpoint.uncompressed_offset_in_bytes - last.uncompressed_offset_in_bytes;
                if !window.is_empty() && gap < self.spacing {
                    continue;
                }
            }
            last = Some(checkpoint);
            index.push(checkpoint, window);
        }

        if fully_annotated {
            index.total_line_count = self.total_line_count;
        }

        index.validate()?;
        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::WINDOW_SIZE;

    fn checkpoint(compressed_bits: u64, uncompressed: u64) -> Checkpoint {
        Checkpoint {
            compressed_offset_in_bits: compressed_bits,
            uncompressed_offset_in_bytes: uncompressed,
            line_offset: 0,
        }
    }

    #[test]
    fn orders_points_offered_out_of_order() {
        let mut builder = IndexBuilder::new(1024, false);
        let window = vec![7u8; WINDOW_SIZE];
        builder.offer(checkpoint(8 * 2000, 4096), &window);
        builder.offer(checkpoint(0, 0), &[]);
        builder.offer(checkpoint(8 * 1000, 2048), &window);
        builder.finish(4096, 8192, None);

        let index = builder.into_index().expect("index");
        assert_eq!(
            index
                .checkpoints()
                .iter()
                .map(|point| point.uncompressed_offset_in_bytes)
                .collect::<Vec<_>>(),
            vec![0, 2048, 4096]
        );
    }

    #[test]
    fn thins_interior_points_below_the_spacing() {
        let mut builder = IndexBuilder::new(4096, false);
        let window = vec![1u8; WINDOW_SIZE];
        builder.offer(checkpoint(0, 0), &[]);
        builder.offer(checkpoint(80, 100), &window);
        builder.offer(checkpoint(160, 200), &window);
        builder.offer(checkpoint(240, 8192), &window);
        builder.finish(4096, 16384, None);

        let index = builder.into_index().expect("index");
        assert_eq!(
            index
                .checkpoints()
                .iter()
                .map(|point| point.uncompressed_offset_in_bytes)
                .collect::<Vec<_>>(),
            vec![0, 8192]
        );
    }

    #[test]
    fn keeps_every_point_that_needs_no_history() {
        let mut builder = IndexBuilder::new(1 << 30, false);
        builder.offer(checkpoint(0, 0), &[]);
        builder.offer(checkpoint(800, 100), &[]);
        builder.offer(checkpoint(1600, 200), &[]);
        builder.finish(4096, 300, None);

        let index = builder.into_index().expect("index");
        assert_eq!(index.checkpoint_count(), 3);
    }

    #[test]
    fn drops_duplicate_offers() {
        let mut builder = IndexBuilder::new(1024, false);
        builder.offer(checkpoint(0, 0), &[]);
        builder.offer(checkpoint(0, 0), &[]);
        builder.finish(4096, 100, None);

        let index = builder.into_index().expect("index");
        assert_eq!(index.checkpoint_count(), 1);
    }

    #[test]
    fn compresses_windows_when_asked() {
        let mut builder = IndexBuilder::new(0, true);
        builder.offer(checkpoint(0, 0), &[]);
        builder.offer(checkpoint(800, 100), &vec![0x2bu8; WINDOW_SIZE]);
        builder.finish(4096, 200, None);

        let index = builder.into_index().expect("index");
        assert!(index.windows().get(800).expect("window").is_compressed());
    }
}
