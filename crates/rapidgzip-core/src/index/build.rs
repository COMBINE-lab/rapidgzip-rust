//! Ordered index construction shared by push and pull decode operations.

use super::{
    Checkpoint, CheckpointKind, GzipIndex, IndexError, IndexKind, IndexOptions, StoredWindow,
    WindowStorage,
};
use std::sync::{Arc, Mutex};

/// Mutable ordered state protected by [`IndexCollector`].
pub(crate) struct IndexBuilder {
    index: GzipIndex,
    options: IndexOptions,
    last_kept: Option<Checkpoint>,
    error: Option<IndexError>,
}

impl IndexBuilder {
    pub(crate) fn new(options: IndexOptions) -> Self {
        let mut index = GzipIndex::new();
        index.set_checkpoint_spacing(Some(options.checkpoint_spacing.get()));
        Self {
            index,
            options,
            last_kept: None,
            error: None,
        }
    }

    fn should_keep(&mut self, checkpoint: Checkpoint, has_window: bool) -> bool {
        if self.error.is_some() {
            return false;
        }
        let Some(last) = self.last_kept else {
            return true;
        };
        if checkpoint.compressed_offset_in_bits <= last.compressed_offset_in_bits
            || checkpoint.uncompressed_offset_in_bytes < last.uncompressed_offset_in_bytes
        {
            self.error = Some(IndexError::InvalidCheckpoint(
                "decode path offered checkpoints out of output order",
            ));
            return false;
        }
        if !matches!(checkpoint.kind, CheckpointKind::DeflateBlock) || !has_window {
            return true;
        }
        checkpoint
            .uncompressed_offset_in_bytes
            .saturating_sub(last.uncompressed_offset_in_bytes)
            >= self.options.checkpoint_spacing.get()
    }

    fn commit(&mut self, checkpoint: Checkpoint, window: StoredWindow) {
        if !self.should_keep(checkpoint, !window.is_empty()) {
            return;
        }
        match self.index.push(checkpoint, window) {
            Ok(()) => self.last_kept = Some(checkpoint),
            Err(error) => self.error = Some(error),
        }
    }

    fn record_error(&mut self, error: IndexError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    fn finish(
        mut self,
        kind: IndexKind,
        compressed_size: u64,
        uncompressed_size: u64,
    ) -> Result<GzipIndex, IndexError> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        self.index.set_kind(kind);
        self.index.set_compressed_size(Some(compressed_size));
        self.index.set_uncompressed_size(Some(uncompressed_size));
        self.index.validate()?;
        Ok(self.index)
    }
}

/// A decode-local index sink kept separate from runtime telemetry.
///
/// Offers are made only when a boundary becomes authoritative in output
/// order. The mutex bridges the different ownership models of push decoding,
/// background positional readers, and pull-driven stream readers; it is not
/// on the per-chunk telemetry path. Window compression happens before the
/// short commit lock is reacquired.
pub(crate) struct IndexCollector {
    builder: Mutex<Option<IndexBuilder>>,
    kind: Mutex<IndexKind>,
}

impl IndexCollector {
    pub(crate) fn new(options: IndexOptions) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(IndexBuilder::new(options))),
            kind: Mutex::new(IndexKind::Gzip),
        })
    }

    pub(crate) fn set_kind(&self, kind: IndexKind) {
        *self.kind.lock().expect("index kind mutex") = kind;
    }

    /// Offers an authoritative resume point and its expanded predecessor
    /// window. Empty windows represent points proven independent.
    pub(crate) fn offer(&self, checkpoint: Checkpoint, window: &[u8]) {
        let options = {
            let mut guard = self.builder.lock().expect("index builder mutex");
            let Some(builder) = guard.as_mut() else {
                return;
            };
            if !builder.should_keep(checkpoint, !window.is_empty()) {
                return;
            }
            builder.options
        };

        let stored = if window.is_empty() {
            Ok(StoredWindow::empty())
        } else {
            StoredWindow::from_raw_maybe_compress(
                window.to_vec(),
                options.window_storage == WindowStorage::Zlib,
            )
        };

        let mut guard = self.builder.lock().expect("index builder mutex");
        let Some(builder) = guard.as_mut() else {
            return;
        };
        match stored {
            Ok(window) => builder.commit(checkpoint, window),
            Err(error) => builder.record_error(error),
        }
    }

    /// Consumes the collector and validates final source sizes.
    pub(crate) fn finish(
        &self,
        compressed_size: u64,
        uncompressed_size: u64,
    ) -> Result<GzipIndex, IndexError> {
        let builder = self
            .builder
            .lock()
            .expect("index builder mutex")
            .take()
            .ok_or(IndexError::InvalidCheckpoint(
                "index collector was finalized more than once",
            ))?;
        let kind = *self.kind.lock().expect("index kind mutex");
        builder.finish(kind, compressed_size, uncompressed_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::WINDOW_SIZE;

    fn point(bits: u64, bytes: u64, kind: CheckpointKind) -> Checkpoint {
        Checkpoint {
            compressed_offset_in_bits: bits,
            uncompressed_offset_in_bytes: bytes,
            kind,
            line_offset: None,
        }
    }

    #[test]
    fn thins_interior_windows_before_storage() {
        let collector = IndexCollector::new(IndexOptions::default());
        let window = vec![7; WINDOW_SIZE];
        collector.offer(point(0, 0, CheckpointKind::MemberHeader), &[]);
        collector.offer(point(80, 1024, CheckpointKind::DeflateBlock), &window);
        collector.offer(
            point(160, 8 * 1024 * 1024, CheckpointKind::DeflateBlock),
            &window,
        );
        let index = collector.finish(1024, 9 * 1024 * 1024).expect("index");
        assert_eq!(index.checkpoint_count(), 2);
    }

    #[test]
    fn keeps_equal_output_offsets_for_empty_members() {
        let collector = IndexCollector::new(IndexOptions::default());
        collector.offer(point(0, 0, CheckpointKind::MemberHeader), &[]);
        collector.offer(point(160, 0, CheckpointKind::MemberHeader), &[]);
        let index = collector.finish(40, 0).expect("index");
        assert_eq!(index.checkpoint_count(), 2);
        assert_eq!(
            index
                .checkpoint_at_or_before(0)
                .expect("checkpoint")
                .compressed_offset_in_bits,
            160
        );
    }
}
