//! Ordered index construction shared by push and pull decode operations.

use super::{
    Checkpoint, CheckpointKind, DeflateIndex, IndexError, IndexKind, IndexOptions, StoredWindow,
    WindowStorage,
};
use crate::line::count_newlines;
use std::sync::{Arc, Mutex};

/// Mutable ordered state protected by [`IndexCollector`].
pub(crate) struct IndexBuilder {
    index: DeflateIndex,
    options: IndexOptions,
    last_kept: Option<Checkpoint>,
    error: Option<IndexError>,
    annotate_lines: bool,
    next_line_checkpoint: usize,
    line_metadata_incomplete: bool,
}

impl IndexBuilder {
    pub(crate) fn new(options: IndexOptions, annotate_lines: bool) -> Self {
        let mut index = DeflateIndex::new();
        index.set_checkpoint_spacing(Some(options.checkpoint_spacing.get()));
        Self {
            index,
            options,
            last_kept: None,
            error: None,
            annotate_lines,
            next_line_checkpoint: 0,
            line_metadata_incomplete: false,
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

    fn commit(&mut self, mut checkpoint: Checkpoint, window: StoredWindow) {
        if !self.should_keep(checkpoint, !window.is_empty()) {
            return;
        }
        if self.annotate_lines {
            checkpoint.line_offset = self
                .index
                .checkpoints
                .last()
                .filter(|previous| {
                    previous.uncompressed_offset_in_bytes == checkpoint.uncompressed_offset_in_bytes
                })
                .and_then(|previous| previous.line_offset);
        }
        match self.index.push(checkpoint, window) {
            Ok(()) => {
                if checkpoint.line_offset.is_some()
                    && self.next_line_checkpoint + 1 == self.index.checkpoints.len()
                {
                    self.next_line_checkpoint += 1;
                }
                self.last_kept = Some(checkpoint);
            }
            Err(error) => self.error = Some(error),
        }
    }

    fn record_error(&mut self, error: IndexError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    /// Resolves retained checkpoint line offsets covered by one output run.
    fn note_output(&mut self, start: u64, lines_before: u64, bytes: &[u8]) -> Option<u64> {
        if !self.annotate_lines {
            return None;
        }
        let end = start.saturating_add(bytes.len() as u64);
        let mut scanned = 0_usize;
        let mut lines = lines_before;
        while let Some(checkpoint) = self.index.checkpoints.get_mut(self.next_line_checkpoint) {
            if checkpoint.line_offset.is_some() {
                self.next_line_checkpoint += 1;
                continue;
            }
            let offset = checkpoint.uncompressed_offset_in_bytes;
            if offset < start {
                // A checkpoint arrived after ordered output had already passed
                // it. There is no retained byte history from which to recover
                // its exact line count, so preserve the existing all-or-none
                // fallback rather than publishing partial metadata.
                self.line_metadata_incomplete = true;
                self.next_line_checkpoint += 1;
                continue;
            }
            if offset >= end {
                break;
            }
            let target = usize::try_from(offset - start).expect("offset falls inside this slice");
            lines = lines.saturating_add(count_newlines(&bytes[scanned..target]));
            scanned = target;
            checkpoint.line_offset = Some(lines);
            self.next_line_checkpoint += 1;
        }
        Some(
            lines
                .saturating_sub(lines_before)
                .saturating_add(count_newlines(&bytes[scanned..])),
        )
    }

    fn finish(
        mut self,
        kind: IndexKind,
        compressed_size: u64,
        uncompressed_size: u64,
        line_count: Option<u64>,
    ) -> Result<DeflateIndex, IndexError> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        self.index.set_kind(kind);
        self.index.set_compressed_size(Some(compressed_size));
        self.index.set_uncompressed_size(Some(uncompressed_size));
        if let Some(lines) = line_count {
            while let Some(checkpoint) = self.index.checkpoints.get_mut(self.next_line_checkpoint) {
                if checkpoint.uncompressed_offset_in_bytes != uncompressed_size {
                    self.line_metadata_incomplete = true;
                    break;
                }
                checkpoint.line_offset = Some(lines);
                self.next_line_checkpoint += 1;
            }
        }
        let fully_annotated = self.annotate_lines
            && line_count.is_some()
            && !self.line_metadata_incomplete
            && self.next_line_checkpoint == self.index.checkpoints.len()
            && self
                .index
                .checkpoints
                .iter()
                .all(|checkpoint| checkpoint.line_offset.is_some());
        if fully_annotated {
            self.index.set_total_line_count(line_count);
        } else {
            for checkpoint in &mut self.index.checkpoints {
                checkpoint.line_offset = None;
            }
            self.index.set_total_line_count(None);
        }
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
    pub(crate) fn new(options: IndexOptions, annotate_lines: bool) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(IndexBuilder::new(options, annotate_lines))),
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

    /// Resolves line metadata as final ordered output passes checkpoints.
    pub(crate) fn note_output(&self, start: u64, lines_before: u64, bytes: &[u8]) -> Option<u64> {
        if let Some(builder) = self.builder.lock().expect("index builder mutex").as_mut() {
            return builder.note_output(start, lines_before, bytes);
        }
        None
    }

    /// Consumes the collector and validates final source sizes.
    pub(crate) fn finish(
        &self,
        compressed_size: u64,
        uncompressed_size: u64,
        line_count: Option<u64>,
    ) -> Result<DeflateIndex, IndexError> {
        let builder = self
            .builder
            .lock()
            .expect("index builder mutex")
            .take()
            .ok_or(IndexError::InvalidCheckpoint(
                "index collector was finalized more than once",
            ))?;
        let kind = *self.kind.lock().expect("index kind mutex");
        builder.finish(kind, compressed_size, uncompressed_size, line_count)
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
        let collector = IndexCollector::new(IndexOptions::default(), false);
        let window = vec![7; WINDOW_SIZE];
        collector.offer(point(0, 0, CheckpointKind::GzipMemberHeader), &[]);
        collector.offer(point(80, 1024, CheckpointKind::DeflateBlock), &window);
        collector.offer(
            point(160, 8 * 1024 * 1024, CheckpointKind::DeflateBlock),
            &window,
        );
        let index = collector
            .finish(1024, 9 * 1024 * 1024, None)
            .expect("index");
        assert_eq!(index.checkpoint_count(), 2);
    }

    #[test]
    fn keeps_equal_output_offsets_for_empty_members() {
        let collector = IndexCollector::new(IndexOptions::default(), false);
        collector.offer(point(0, 0, CheckpointKind::GzipMemberHeader), &[]);
        collector.offer(point(160, 0, CheckpointKind::GzipMemberHeader), &[]);
        let index = collector.finish(40, 0, None).expect("index");
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
