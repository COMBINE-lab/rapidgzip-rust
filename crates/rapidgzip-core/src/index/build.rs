//! Index construction during a decode.
//!
//! Decode paths offer checkpoints at boundaries they already compute, once the
//! associated predecessor window is resolved. Offers may arrive out of order,
//! because parallel paths discover boundaries concurrently, so the builder
//! orders and thins them when the index is finalized rather than requiring the
//! caller to serialize.

use super::{Checkpoint, GzipIndex, IndexError, StoredWindow};

/// Collects checkpoints into a [`GzipIndex`].
pub(crate) struct IndexBuilder {
    points: Vec<(Checkpoint, StoredWindow)>,
    spacing: u64,
    compress_windows: bool,
    compressed_size: u64,
    uncompressed_size: u64,
    error: Option<IndexError>,
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
            error: None,
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
        self.points.push((checkpoint, stored));
    }

    /// Records the verified sizes of the decode.
    pub(crate) const fn finish(&mut self, compressed_bytes: u64, uncompressed_bytes: u64) {
        self.compressed_size = compressed_bytes;
        self.uncompressed_size = uncompressed_bytes;
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

        let mut last: Option<Checkpoint> = None;
        for (checkpoint, window) in self.points {
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
        builder.finish(4096, 8192);

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
        builder.finish(4096, 16384);

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
        builder.finish(4096, 300);

        let index = builder.into_index().expect("index");
        assert_eq!(index.checkpoint_count(), 3);
    }

    #[test]
    fn drops_duplicate_offers() {
        let mut builder = IndexBuilder::new(1024, false);
        builder.offer(checkpoint(0, 0), &[]);
        builder.offer(checkpoint(0, 0), &[]);
        builder.finish(4096, 100);

        let index = builder.into_index().expect("index");
        assert_eq!(index.checkpoint_count(), 1);
    }

    #[test]
    fn compresses_windows_when_asked() {
        let mut builder = IndexBuilder::new(0, true);
        builder.offer(checkpoint(0, 0), &[]);
        builder.offer(checkpoint(800, 100), &vec![0x2bu8; WINDOW_SIZE]);
        builder.finish(4096, 200);

        let index = builder.into_index().expect("index");
        assert!(index.windows().get(800).expect("window").is_compressed());
    }
}
