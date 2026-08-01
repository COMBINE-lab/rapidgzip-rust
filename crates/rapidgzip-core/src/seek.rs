//! Random-access gzip reading driven by a [`GzipIndex`].
//!
//! [`IndexedReader`] seeks to uncompressed offsets by resuming raw DEFLATE at
//! the nearest preceding checkpoint (with its predecessor window) and
//! discarding skip bytes. Member CRC32 is **not** verified: partial mid-member
//! reads cannot form a complete payload CRC. ISIZE is also not enforced for
//! M1 simplicity.
//!
//! Decoded windows are retained in an LRU cache (keyed by uncompressed start
//! offset) so repeated seeks into the same regions avoid re-inflate. After a
//! successful buffer fill, optional sequential **read-ahead** decodes the next
//! window into the cache when the active inflate session can continue without
//! a re-seek. When [`crate::DecoderBuilder::seek_prefetch_windows`] is non-zero
//! (and read-ahead is enabled), additional windows further ahead are inflated
//! on **background threads** from independent checkpoint resumes — without
//! sharing or advancing the consumer's live inflate session. Far seeks bump a
//! generation counter so in-flight workers discard stale inserts; dropping the
//! reader cancels and joins workers.
//!
//! When the index was built with line offsets, [`IndexedReader::seek_to_line`]
//! supports 1-based line seeks (gztool/rapidgzip style).

use crate::backend::RawInflater;
use crate::config::Config;
use crate::gzip::{SourceCursor, parse_member_header};
use crate::index::{
    GzipIndex, IndexError, StoredWindow, WindowCompression, INDEXED_GZIP_WINDOW_SIZE,
};
use crate::parallel::Window;
use crate::{DecodeError, DeflateErrorKind, ReadAt};
use libz_rs_sys as z;
use std::cmp::min;
use std::collections::HashSet;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

/// Snapshot of [`IndexedReader`] decoded-window cache counters.
///
/// Useful for tests and debugging. Hit/miss/insert counters are updated under
/// the cache mutex; the public reader remains single-consumer (`Read` +
/// `Seek`) from the caller's perspective.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SeekCacheStats {
    /// Number of times a seek/read found coverage in the cache.
    pub hits: u64,
    /// Number of times a buffer fill had to inflate from a checkpoint/session.
    pub misses: u64,
    /// Number of windows inserted (including replacements of the same start).
    pub inserts: u64,
    /// Number of successful sequential read-ahead fills.
    pub readaheads: u64,
    /// Number of successful background prefetch inserts.
    pub prefetches: u64,
    /// Windows currently retained.
    pub chunks: usize,
    /// Total decoded bytes currently retained.
    pub bytes: usize,
    /// Configured maximum windows (`0` = cache disabled).
    pub max_chunks: usize,
    /// Configured maximum total bytes (`0` = cache disabled).
    pub max_bytes: usize,
    /// Whether sequential read-ahead is enabled.
    pub readahead_enabled: bool,
    /// Configured background prefetch window count (`0` = disabled).
    pub prefetch_windows: usize,
    /// Hits on the expanded zlib index-window (predecessor history) LRU.
    pub window_expand_hits: u64,
    /// Misses that required zlib-decompress of a stored index window.
    pub window_expand_misses: u64,
    /// Expanded index windows currently retained.
    pub window_expand_chunks: usize,
    /// Configured max expanded index windows (`0` = disabled).
    pub window_expand_max_chunks: usize,
}

/// Random-access gzip reader driven by a [`GzipIndex`].
///
/// Implements [`Read`] and [`Seek`] over uncompressed output. Seeking restarts
/// inflate from a checkpoint when the target is not already covered by the
/// active lookahead buffer or the decoded-window LRU. Sequential reads after a
/// seek continue from the active inflate session when possible; sequential
/// read-ahead warms the next window into the cache, and optional background
/// workers warm further windows via independent checkpoint resumes.
///
/// When index windows are stored zlib-compressed
/// ([`crate::DecoderBuilder::compress_index_windows`]), expanded predecessor
/// history is retained in a second small LRU (keyed by compressed bit offset,
/// capacity tied to [`crate::DecoderBuilder::seek_cache_chunks`]) so repeated
/// seeks into the same checkpoints avoid re-decompressing 32 KiB windows.
///
/// The public type is single-threaded from the caller's view (`Read` + `Seek`);
/// background prefetch uses internal synchronization around the caches only.
///
/// # Limitations
///
/// - No CRC32 verification of member payloads (partial reads cannot complete a
///   full-member CRC).
/// - [`Self::seek_to_line`] requires an index built with
///   [`crate::DecoderBuilder::gather_line_offsets`].
/// - Background prefetch is best-effort: failures are ignored, and far seeks
///   cancel in-flight inserts via a generation counter.
/// - Requires a non-empty, validated index with checkpoints that cover the
///   requested uncompressed range.
pub struct IndexedReader<R: ReadAt + 'static> {
    source: Arc<R>,
    index: Arc<GzipIndex>,
    config: Config,
    /// Next uncompressed byte to return to the consumer.
    position: u64,
    /// Decoded lookahead starting at [`Self::buffer_base`].
    buffer: Vec<u8>,
    buffer_base: u64,
    /// Active raw-inflate session when sequential reads can continue.
    session: Option<InflateSession>,
    /// LRU of recently decoded windows (shared with prefetch workers).
    cache: Arc<Mutex<DecodedWindowCache>>,
    /// LRU of expanded zlib index windows (shared with prefetch workers).
    expand_cache: Arc<Mutex<ExpandedWindowCache>>,
    /// Bumped on far seeks / shutdown so workers discard stale results.
    generation: Arc<AtomicU64>,
    /// Set while shutting down so workers exit ASAP.
    cancelled: Arc<AtomicBool>,
    /// Uncompressed starts currently being prefetched (dedupe concurrent jobs).
    prefetch_inflight: Arc<Mutex<HashSet<u64>>>,
    /// In-flight background prefetch workers.
    prefetch_handles: Vec<JoinHandle<()>>,
}

/// Live raw-inflate state after a successful seek/setup.
struct InflateSession {
    inflater: RawInflater,
    /// Absolute compressed **byte** offset for the next inflate input.
    compressed_byte: u64,
    /// Absolute uncompressed offset of the next byte the inflater will emit.
    uncompressed_pos: u64,
}

/// One cached decoded span starting at `start`.
struct CachedChunk {
    start: u64,
    data: Vec<u8>,
}

/// LRU of decoded uncompressed windows.
///
/// Index 0 is the least-recently used entry; the last element is MRU.
/// Disabled when `max_chunks == 0` or `max_bytes == 0`.
struct DecodedWindowCache {
    chunks: Vec<CachedChunk>,
    max_chunks: usize,
    max_bytes: usize,
    total_bytes: usize,
    hits: u64,
    misses: u64,
    inserts: u64,
    readaheads: u64,
    prefetches: u64,
    readahead_enabled: bool,
    prefetch_windows: usize,
}

impl DecodedWindowCache {
    fn new(
        max_chunks: usize,
        max_bytes: usize,
        readahead_enabled: bool,
        prefetch_windows: usize,
    ) -> Self {
        Self {
            chunks: Vec::new(),
            max_chunks,
            max_bytes,
            total_bytes: 0,
            hits: 0,
            misses: 0,
            inserts: 0,
            readaheads: 0,
            prefetches: 0,
            readahead_enabled,
            prefetch_windows,
        }
    }

    const fn enabled(&self) -> bool {
        self.max_chunks > 0 && self.max_bytes > 0
    }

    fn stats(&self, expand: &ExpandedWindowCache) -> SeekCacheStats {
        SeekCacheStats {
            hits: self.hits,
            misses: self.misses,
            inserts: self.inserts,
            readaheads: self.readaheads,
            prefetches: self.prefetches,
            chunks: self.chunks.len(),
            bytes: self.total_bytes,
            max_chunks: self.max_chunks,
            max_bytes: self.max_bytes,
            readahead_enabled: self.readahead_enabled,
            prefetch_windows: self.prefetch_windows,
            window_expand_hits: expand.hits,
            window_expand_misses: expand.misses,
            window_expand_chunks: expand.entries.len(),
            window_expand_max_chunks: expand.max_entries,
        }
    }

    fn set_capacity(&mut self, max_chunks: usize, max_bytes: usize) {
        self.max_chunks = max_chunks;
        self.max_bytes = max_bytes;
        if !self.enabled() {
            self.chunks.clear();
            self.total_bytes = 0;
            return;
        }
        self.evict_to_limits();
    }

    fn set_readahead(&mut self, enabled: bool) {
        self.readahead_enabled = enabled;
    }

    fn set_prefetch_windows(&mut self, count: usize) {
        self.prefetch_windows = count;
    }

    /// Returns a clone of the chunk covering `offset`, promoting it to MRU.
    fn get_covering(&mut self, offset: u64) -> Option<(u64, Vec<u8>)> {
        if !self.enabled() {
            return None;
        }
        let index = self.chunks.iter().position(|chunk| {
            let end = chunk.start.saturating_add(chunk.data.len() as u64);
            offset >= chunk.start && offset < end
        })?;
        let chunk = self.chunks.remove(index);
        let start = chunk.start;
        let data = chunk.data.clone();
        self.chunks.push(chunk);
        self.hits = self.hits.saturating_add(1);
        Some((start, data))
    }

    fn covers(&self, offset: u64) -> bool {
        self.covering_end(offset).is_some()
    }

    /// Exclusive end of the cached chunk covering `offset`, if any.
    fn covering_end(&self, offset: u64) -> Option<u64> {
        if !self.enabled() {
            return None;
        }
        self.chunks.iter().find_map(|chunk| {
            let end = chunk.start.saturating_add(chunk.data.len() as u64);
            if offset >= chunk.start && offset < end {
                Some(end)
            } else {
                None
            }
        })
    }

    fn insert(&mut self, start: u64, data: Vec<u8>) {
        if !self.enabled() || data.is_empty() {
            return;
        }

        // Replace existing entry with the same start (refresh MRU).
        if let Some(index) = self.chunks.iter().position(|chunk| chunk.start == start) {
            let old = self.chunks.remove(index);
            self.total_bytes = self.total_bytes.saturating_sub(old.data.len());
        }

        let len = data.len();
        // Evict LRU until the new window fits under both limits. A single window
        // larger than `max_bytes` is still admitted alone (after clearing).
        while !self.chunks.is_empty() {
            let would_exceed_chunks = self.chunks.len() + 1 > self.max_chunks;
            let would_exceed_bytes = self.total_bytes.saturating_add(len) > self.max_bytes;
            if !would_exceed_chunks && !would_exceed_bytes {
                break;
            }
            let removed = self.chunks.remove(0);
            self.total_bytes = self.total_bytes.saturating_sub(removed.data.len());
        }

        self.chunks.push(CachedChunk { start, data });
        self.total_bytes = self.total_bytes.saturating_add(len);
        self.inserts = self.inserts.saturating_add(1);
    }

    /// Evicts until current occupancy fits `max_chunks` / `max_bytes`.
    ///
    /// A sole window larger than `max_bytes` is retained so capacity shrinks
    /// do not drop the only useful entry without a replacement insert.
    fn evict_to_limits(&mut self) {
        if !self.enabled() {
            self.chunks.clear();
            self.total_bytes = 0;
            return;
        }
        while !self.chunks.is_empty() {
            let over_chunks = self.chunks.len() > self.max_chunks;
            let over_bytes = self.total_bytes > self.max_bytes && self.chunks.len() > 1;
            if !over_chunks && !over_bytes {
                break;
            }
            let removed = self.chunks.remove(0);
            self.total_bytes = self.total_bytes.saturating_sub(removed.data.len());
        }
    }

    fn note_miss(&mut self) {
        self.misses = self.misses.saturating_add(1);
    }

    fn note_readahead(&mut self) {
        self.readaheads = self.readaheads.saturating_add(1);
    }

    fn note_prefetch(&mut self) {
        self.prefetches = self.prefetches.saturating_add(1);
    }
}

/// LRU of expanded zlib-compressed index windows (predecessor history).
///
/// Keyed by compressed bit offset. Index 0 is LRU; the last element is MRU.
/// Disabled when `max_entries == 0`. Raw (uncompressed) stored windows are
/// never inserted — they need no expand step.
struct ExpandedWindowCache {
    entries: Vec<(u64, Vec<u8>)>,
    max_entries: usize,
    hits: u64,
    misses: u64,
}

impl ExpandedWindowCache {
    const fn new(max_entries: usize) -> Self {
        Self {
            entries: Vec::new(),
            max_entries,
            hits: 0,
            misses: 0,
        }
    }

    const fn enabled(&self) -> bool {
        self.max_entries > 0
    }

    fn set_capacity(&mut self, max_entries: usize) {
        self.max_entries = max_entries;
        if !self.enabled() {
            self.entries.clear();
            return;
        }
        while self.entries.len() > self.max_entries {
            self.entries.remove(0);
        }
    }

    /// Returns a clone of the expanded window for `bit_offset`, promoting to MRU.
    fn get(&mut self, bit_offset: u64) -> Option<Vec<u8>> {
        if !self.enabled() {
            return None;
        }
        let index = self.entries.iter().position(|(key, _)| *key == bit_offset)?;
        let entry = self.entries.remove(index);
        let data = entry.1.clone();
        self.entries.push(entry);
        self.hits = self.hits.saturating_add(1);
        Some(data)
    }

    fn insert(&mut self, bit_offset: u64, data: Vec<u8>) {
        if !self.enabled() || data.is_empty() {
            return;
        }
        if let Some(index) = self.entries.iter().position(|(key, _)| *key == bit_offset) {
            self.entries.remove(index);
        }
        while self.entries.len() >= self.max_entries {
            self.entries.remove(0);
        }
        self.entries.push((bit_offset, data));
    }

    fn note_miss(&mut self) {
        self.misses = self.misses.saturating_add(1);
    }
}

impl<R: ReadAt + 'static> IndexedReader<R> {
    /// Opens a seekable reader for `source` using a prebuilt or imported index.
    ///
    /// Validates the index, requires at least one checkpoint, and when the
    /// index records a known compressed size (`!= u64::MAX`) checks it against
    /// `source.len()`. Prefer [`crate::Decoder::reader_with_index`] or
    /// [`crate::Decoder::open_with_index`] from outside this crate.
    pub(crate) fn new(source: R, index: GzipIndex, config: Config) -> Result<Self, DecodeError> {
        index
            .validate()
            .map_err(DecodeError::InvalidIndex)?;
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

        let cache = DecodedWindowCache::new(
            config.seek_cache_max_chunks,
            config.seek_cache_max_bytes,
            config.seek_readahead,
            config.seek_prefetch_windows,
        );
        // Expanded zlib index windows share the same entry budget as the
        // decoded-window cache (each entry ≤ 32 KiB of history).
        let expand_cache = ExpandedWindowCache::new(config.seek_cache_max_chunks);

        Ok(Self {
            source: Arc::new(source),
            index: Arc::new(index),
            config,
            position: 0,
            buffer: Vec::new(),
            buffer_base: 0,
            session: None,
            cache: Arc::new(Mutex::new(cache)),
            expand_cache: Arc::new(Mutex::new(expand_cache)),
            generation: Arc::new(AtomicU64::new(0)),
            cancelled: Arc::new(AtomicBool::new(false)),
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_handles: Vec::new(),
        })
    }

    /// Current uncompressed read position.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Total uncompressed size from the index, when known (`!= u64::MAX`).
    #[must_use]
    pub fn len(&self) -> Option<u64> {
        let size = self.index.uncompressed_size_in_bytes;
        if size == u64::MAX {
            None
        } else {
            Some(size)
        }
    }

    /// Returns `true` when the index reports a known zero-length payload.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == Some(0)
    }

    /// Borrows the driving index.
    #[must_use]
    pub fn index(&self) -> &GzipIndex {
        &self.index
    }

    /// Decoded-window and expanded index-window cache counters for tests and
    /// debugging.
    #[must_use]
    pub fn cache_stats(&self) -> SeekCacheStats {
        let cache = self.lock_cache();
        let expand = self.lock_expand_cache();
        cache.stats(&expand)
    }

    /// Overrides the decoded-window LRU capacity.
    ///
    /// Either limit at zero disables the cache. Existing entries are evicted
    /// as needed to fit the new bounds. Prefer
    /// [`crate::DecoderBuilder::seek_cache_chunks`] /
    /// [`crate::DecoderBuilder::seek_cache_bytes`] when opening the reader.
    /// Changing `max_chunks` also resizes the expanded zlib index-window LRU.
    #[must_use]
    pub fn with_cache_capacity(mut self, max_chunks: usize, max_bytes: usize) -> Self {
        self.config.seek_cache_max_chunks = max_chunks;
        self.config.seek_cache_max_bytes = max_bytes;
        self.lock_cache().set_capacity(max_chunks, max_bytes);
        self.lock_expand_cache().set_capacity(max_chunks);
        self
    }

    /// Enables or disables sequential read-ahead into the decoded-window cache.
    ///
    /// Background prefetch also requires read-ahead to be enabled.
    #[must_use]
    pub fn with_readahead(mut self, enabled: bool) -> Self {
        self.config.seek_readahead = enabled;
        self.lock_cache().set_readahead(enabled);
        self
    }

    /// Sets the number of windows to warm via background prefetch.
    ///
    /// Zero disables background prefetch. Sequential read-ahead is unchanged.
    #[must_use]
    pub fn with_prefetch_windows(mut self, count: usize) -> Self {
        self.config.seek_prefetch_windows = count;
        self.lock_cache().set_prefetch_windows(count);
        self
    }

    /// Consumes the reader and returns ownership of the index.
    #[must_use]
    pub fn into_index(mut self) -> GzipIndex {
        self.shutdown_prefetch();
        let index = Arc::clone(&self.index);
        drop(self);
        Arc::try_unwrap(index).unwrap_or_else(|arc| (*arc).clone())
    }

    /// Consumes the reader and returns the source and index.
    #[must_use]
    pub fn into_inner(mut self) -> (R, GzipIndex) {
        self.shutdown_prefetch();
        let source = Arc::clone(&self.source);
        let index = Arc::clone(&self.index);
        drop(self);
        let source = Arc::try_unwrap(source).unwrap_or_else(|_| {
            unreachable!("prefetch workers joined before into_inner; source Arc is unique")
        });
        let index = Arc::try_unwrap(index).unwrap_or_else(|arc| (*arc).clone());
        (source, index)
    }

    /// Seeks to the start of a **1-based** line number.
    ///
    /// Line `1` is the first line of the uncompressed stream (byte offset 0
    /// when the stream is non-empty). Line `N` is the byte immediately after
    /// the `(N - 1)`-th Unix newline (`\n`). This matches gztool/rapidgzip
    /// `-L` / line-range conventions.
    ///
    /// Requires [`GzipIndex::has_line_offsets`]. When `line` is past the last
    /// line, the reader is positioned at the known uncompressed EOF (or the
    /// end of available data).
    ///
    /// Returns the uncompressed byte position at the start of the requested
    /// line (or EOF).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] when `line == 0` or the index
    /// lacks line offsets. Inflate failures map to I/O errors as with
    /// [`Seek::seek`].
    pub fn seek_to_line(&mut self, line: u64) -> io::Result<u64> {
        if line == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "line numbers are 1-based; use 1 for the first line",
            ));
        }
        if !self.index.has_line_offsets {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "index does not contain line offsets; rebuild with gather_line_offsets",
            ));
        }

        // Newlines strictly before the start of this 1-based line.
        let target_newlines = line - 1;
        if target_newlines == 0 {
            // First line starts at uncompressed offset 0.
            self.seek_to(0).map_err(|error| error.to_io_error())?;
            return Ok(self.position);
        }

        // Resume from the latest checkpoint with *strictly fewer* newlines than
        // the target. A checkpoint with `line_offset == target_newlines` may
        // sit anywhere after the start of that line (including EOF), so it is
        // not a safe resume point for "start of line".
        let checkpoint = {
            let idx = self
                .index
                .checkpoints
                .partition_point(|cp| cp.line_offset < target_newlines);
            if idx == 0 {
                self.index.checkpoints.first()
            } else {
                Some(&self.index.checkpoints[idx - 1])
            }
        }
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "no checkpoint at or before target line",
            )
        })?;

        let start_byte = checkpoint.uncompressed_offset_in_bytes;
        let mut newlines = checkpoint.line_offset;
        self.seek_to(start_byte).map_err(|error| error.to_io_error())?;

        // Scan forward from the checkpoint, counting `\n` until the target
        // line start (the byte after the (target_newlines)-th newline overall).
        let chunk_size = self.config.decoded_chunk_size.max(1);
        let mut buf = vec![0_u8; chunk_size];
        loop {
            let n = self.read(&mut buf)?;
            if n == 0 {
                // EOF before the requested line: stay at end.
                return Ok(self.position);
            }
            for (i, &byte) in buf[..n].iter().enumerate() {
                if byte != b'\n' {
                    continue;
                }
                newlines = newlines.saturating_add(1);
                if newlines == target_newlines {
                    // Start of the target line is the byte after this newline.
                    // `read` advanced `position` by `n`; rewind to after `\n`.
                    let after_newline = self.position - (n - i - 1) as u64;
                    self.seek_to(after_newline)
                        .map_err(|error| error.to_io_error())?;
                    return Ok(self.position);
                }
            }
        }
    }

    fn lock_cache(&self) -> std::sync::MutexGuard<'_, DecodedWindowCache> {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_expand_cache(&self) -> std::sync::MutexGuard<'_, ExpandedWindowCache> {
        self.expand_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Cancels in-flight prefetch and joins all workers.
    fn shutdown_prefetch(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        self.bump_generation();
        for handle in self.prefetch_handles.drain(..) {
            let _ = handle.join();
        }
        self.prefetch_inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// Joins finished workers and drops their handles.
    fn reap_prefetch(&mut self) {
        let handles = std::mem::take(&mut self.prefetch_handles);
        for handle in handles {
            if handle.is_finished() {
                let _ = handle.join();
            } else {
                self.prefetch_handles.push(handle);
            }
        }
    }

    fn uncompressed_len_or_max(&self) -> u64 {
        let size = self.index.uncompressed_size_in_bytes;
        if size == u64::MAX {
            u64::MAX
        } else {
            size
        }
    }

    fn buffer_contains(&self, absolute: u64) -> bool {
        let end = self.buffer_base.saturating_add(self.buffer.len() as u64);
        absolute >= self.buffer_base && absolute < end
    }

    fn clear_buffer_and_session(&mut self) {
        self.buffer.clear();
        self.buffer_base = self.position;
        self.session = None;
    }

    /// Loads the consumer lookahead from the LRU when `absolute` is covered.
    ///
    /// Returns `true` on a hit (buffer replaced). Does not change `position`.
    fn load_buffer_from_cache(&mut self, absolute: u64) -> bool {
        let hit = self.lock_cache().get_covering(absolute);
        if let Some((base, data)) = hit {
            self.buffer_base = base;
            self.buffer = data;
            true
        } else {
            false
        }
    }

    /// Loads predecessor history for a checkpoint, expanding zlib windows via
    /// the shared LRU when configured.
    fn window_for_bit_offset(&self, start_bit: u64) -> Result<Window, DecodeError> {
        window_from_stored_cached(
            self.index.window_for(start_bit),
            start_bit,
            Some(&self.expand_cache),
        )
    }

    fn is_eof_checkpoint(&self, checkpoint_uncompressed: u64, start_bit: u64) -> bool {
        is_eof_checkpoint(&self.index, checkpoint_uncompressed, start_bit)
    }

    /// Positions inflate at `target` uncompressed offset (buffer starts empty at target).
    fn seek_inflate_to(&mut self, target: u64) -> Result<(), DecodeError> {
        self.buffer.clear();
        self.buffer_base = target;
        self.session = None;

        let total = self.uncompressed_len_or_max();
        if target >= total {
            // Past or at known EOF: nothing to inflate.
            return Ok(());
        }

        let checkpoint = self
            .index
            .checkpoint_at_or_before(target)
            .ok_or(DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
                "no checkpoint at or before target offset",
            )))?;

        let start_bit = checkpoint.compressed_offset_in_bits;
        let checkpoint_u = checkpoint.uncompressed_offset_in_bytes;

        if self.is_eof_checkpoint(checkpoint_u, start_bit) {
            // Target should already be >= total when only EOF remains; treat as empty.
            return Ok(());
        }

        let window = self.window_for_bit_offset(start_bit)?;
        let (inflater, compressed_byte) = RawInflater::prepare_at_bit_offset(
            start_bit,
            &window,
            self.source.as_ref(),
            self.config.input_page_size,
            true, // empty-window BGZI/header starts → skip gzip header to DEFLATE
        )?;

        self.session = Some(InflateSession {
            inflater,
            compressed_byte,
            uncompressed_pos: checkpoint_u,
        });

        let mut skip = target.saturating_sub(checkpoint_u);
        while skip > 0 {
            let chunk = min(skip, self.config.decoded_chunk_size as u64) as usize;
            let produced = self.inflate_into_discard(chunk)?;
            if produced == 0 {
                // Cannot reach target from this checkpoint.
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: start_bit,
                    reason: DeflateErrorKind::Truncated,
                });
            }
            skip -= produced as u64;
        }

        debug_assert!(
            self.session
                .as_ref()
                .map(|s| s.uncompressed_pos == target)
                .unwrap_or(target >= total)
        );
        self.buffer_base = target;
        Ok(())
    }

    /// Inflates up to `max_out` bytes into a temporary discard buffer.
    ///
    /// Returns the number of uncompressed bytes discarded.
    fn inflate_into_discard(&mut self, max_out: usize) -> Result<usize, DecodeError> {
        if max_out == 0 {
            return Ok(0);
        }
        let mut discard = vec![0_u8; max_out];
        let n = self.inflate_fill(&mut discard)?;
        Ok(n)
    }

    /// Fills `out` from the active session (or starts nothing if session is `None`).
    ///
    /// Returns bytes written. May be short on true EOF.
    fn inflate_fill(&mut self, out: &mut [u8]) -> Result<usize, DecodeError> {
        if out.is_empty() {
            return Ok(0);
        }
        let mut filled = 0usize;

        while filled < out.len() {
            if self.session.is_none() {
                break;
            }
            let produced = self.inflate_once(&mut out[filled..])?;
            if produced == 0 {
                // `inflate_once` only returns 0 when the session is finished.
                break;
            }
            filled += produced;
        }

        Ok(filled)
    }

    /// Inflates until at least one output byte is produced, the stream ends, or
    /// an error occurs. Zero is returned only when the session is exhausted.
    fn inflate_once(&mut self, out: &mut [u8]) -> Result<usize, DecodeError> {
        if out.is_empty() {
            return Ok(0);
        }

        let mut session = match self.session.take() {
            Some(session) => session,
            None => return Ok(0),
        };

        let page_size = self.config.input_page_size;
        let mut total_produced = 0usize;

        loop {
            let mut cursor = SourceCursor::new(self.source.as_ref(), page_size)?;
            cursor.seek(session.compressed_byte)?;

            if cursor.at_end() {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: session.compressed_byte.saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
                });
            }

            let (input_pointer, input_length) = {
                let input = cursor.available()?;
                (input.as_ptr(), input.len().min(u32::MAX as usize))
            };

            let dest = &mut out[total_produced..];
            if dest.is_empty() {
                self.session = Some(session);
                return Ok(total_produced);
            }

            session.inflater.stream.next_in = input_pointer;
            session.inflater.stream.avail_in = input_length as u32;
            session.inflater.stream.next_out = dest.as_mut_ptr();
            session.inflater.stream.avail_out = dest.len() as u32;
            let input_before = session.inflater.stream.avail_in;
            let output_before = session.inflater.stream.avail_out;

            // SAFETY:
            // - `session.inflater.stream` was initialized (optionally primed + dict).
            // - `next_in/avail_in` describe the current immutable cursor page.
            // - `next_out/avail_out` describe the remaining caller-owned `out`
            //   slice; zlib only reduces avail_out after initializing those bytes.
            let status = unsafe { z::inflate(&mut session.inflater.stream, z::Z_NO_FLUSH) };

            let consumed = usize::try_from(input_before - session.inflater.stream.avail_in)
                .expect("zlib uInt fits usize");
            let produced = usize::try_from(output_before - session.inflater.stream.avail_out)
                .expect("zlib uInt fits usize");
            cursor.advance(consumed);
            session.compressed_byte = cursor.position();
            session.uncompressed_pos = session
                .uncompressed_pos
                .saturating_add(produced as u64);
            total_produced += produced;

            match status {
                z::Z_STREAM_END => {
                    // Skip gzip footer (CRC32 + ISIZE); do not verify for seek reads.
                    if cursor.position() + 8 > cursor.length() {
                        return Err(DecodeError::InvalidGzip {
                            offset: cursor.position(),
                            reason: crate::GzipErrorKind::Truncated,
                        });
                    }
                    let _footer = cursor.read_exact::<8>(cursor.position())?;
                    session.compressed_byte = cursor.position();

                    if cursor.at_end() {
                        // True EOF after last member; drop session.
                        return Ok(total_produced);
                    }

                    // Next gzip member: parse header and start raw inflate with empty window.
                    let header = parse_member_header(&mut cursor, false)?;
                    debug_assert_eq!(header.deflate_start, cursor.position());
                    let mut inflater = RawInflater::new()?;
                    let empty = Window::empty();
                    inflater.set_dictionary(&empty, header.deflate_start.saturating_mul(8))?;
                    session.inflater = inflater;
                    session.compressed_byte = cursor.position();

                    if total_produced > 0 {
                        self.session = Some(session);
                        return Ok(total_produced);
                    }
                    // Member produced no bytes yet (empty member); continue into the next.
                    continue;
                }
                z::Z_OK | z::Z_BUF_ERROR if consumed != 0 || produced != 0 => {
                    if total_produced > 0 {
                        self.session = Some(session);
                        return Ok(total_produced);
                    }
                    // Consumed only compressed input; keep inflating into `out`.
                    continue;
                }
                z::Z_DATA_ERROR => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: session.compressed_byte.saturating_mul(8),
                        reason: DeflateErrorKind::InvalidData,
                    });
                }
                z::Z_NEED_DICT => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: session.compressed_byte.saturating_mul(8),
                        reason: DeflateErrorKind::UnexpectedDictionary,
                    });
                }
                z::Z_OK | z::Z_BUF_ERROR => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: session.compressed_byte.saturating_mul(8),
                        reason: DeflateErrorKind::Stalled,
                    });
                }
                other => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: session.compressed_byte.saturating_mul(8),
                        reason: DeflateErrorKind::BackendStatus(other),
                    });
                }
            }
        }
    }

    /// Target size for the next decoded window at `start`.
    ///
    /// Prefer the span to the next checkpoint when that is denser than
    /// `decoded_chunk_size`; otherwise use the configured chunk size, clipped
    /// to known uncompressed EOF.
    fn window_capacity_at(&self, start: u64) -> usize {
        window_capacity_at(&self.index, self.config.decoded_chunk_size, start)
    }

    /// Best-effort sequential read-ahead of the window after the current buffer.
    ///
    /// Only runs when readahead is enabled, the session sits at the buffer end,
    /// and the next offset is not already cached. Failures are ignored so the
    /// consumer path is not blocked by speculative work.
    fn maybe_readahead(&mut self) {
        {
            let cache = self.lock_cache();
            if !cache.readahead_enabled || !cache.enabled() {
                return;
            }
        }
        if self.buffer.is_empty() {
            return;
        }

        let next = self.buffer_base.saturating_add(self.buffer.len() as u64);
        let total = self.uncompressed_len_or_max();
        if next >= total || self.lock_cache().covers(next) {
            return;
        }

        let can_continue = self
            .session
            .as_ref()
            .is_some_and(|s| s.uncompressed_pos == next);
        if !can_continue {
            return;
        }

        let capacity = self.window_capacity_at(next);
        if capacity == 0 {
            return;
        }

        let mut chunk = vec![0_u8; capacity];
        match self.inflate_fill(&mut chunk) {
            Ok(n) if n > 0 => {
                chunk.truncate(n);
                let mut cache = self.lock_cache();
                cache.insert(next, chunk);
                cache.note_readahead();
            }
            _ => {
                // Best-effort: leave cache unchanged. Session may be None or
                // advanced partially; subsequent ensure_buffered will recover.
            }
        }
    }

    /// Schedules background workers to decode windows ahead of the active buffer.
    ///
    /// Prefetch starts after the current buffer (and any sequential readahead
    /// already in the cache). Each worker independently resumes inflate from
    /// the nearest checkpoint and never touches the consumer session.
    fn maybe_prefetch(&mut self) {
        self.reap_prefetch();

        let (prefetch_windows, readahead_enabled, cache_enabled) = {
            let cache = self.lock_cache();
            (
                cache.prefetch_windows,
                cache.readahead_enabled,
                cache.enabled(),
            )
        };
        if !readahead_enabled || !cache_enabled || prefetch_windows == 0 {
            return;
        }
        if self.buffer.is_empty() {
            return;
        }

        let max_in_flight = self.config.decoder_threads.max(1).min(prefetch_windows);
        if self.prefetch_handles.len() >= max_in_flight {
            return;
        }

        let total = self.uncompressed_len_or_max();
        let mut cursor = self.buffer_base.saturating_add(self.buffer.len() as u64);

        // Sequential readahead owns the immediate next window when the live
        // session can continue (or that window is already cached). Background
        // workers start one window further so they do not race readahead.
        let session_owns_next = self
            .session
            .as_ref()
            .is_some_and(|s| s.uncompressed_pos == cursor);
        if session_owns_next || self.lock_cache().covers(cursor) {
            if let Some(end) = self.lock_cache().covering_end(cursor) {
                cursor = end;
            } else {
                let next_cap = self.window_capacity_at(cursor);
                if next_cap == 0 {
                    return;
                }
                cursor = cursor.saturating_add(next_cap as u64);
            }
        }

        let mut targets: Vec<(u64, usize)> = Vec::new();

        // Walk ahead up to `prefetch_windows` window starts, skipping already
        // cached spans.
        let mut planned = 0usize;
        while planned < prefetch_windows && cursor < total {
            if let Some(end) = self.lock_cache().covering_end(cursor) {
                cursor = end;
                continue;
            }

            let capacity = self.window_capacity_at(cursor);
            if capacity == 0 {
                break;
            }
            targets.push((cursor, capacity));
            cursor = cursor.saturating_add(capacity as u64);
            planned += 1;
        }

        if targets.is_empty() {
            return;
        }

        let slots = max_in_flight.saturating_sub(self.prefetch_handles.len());
        let generation = self.generation.load(Ordering::Relaxed);

        for (start, capacity) in targets.into_iter().take(slots) {
            // Re-check coverage under the lock at schedule time.
            if self.lock_cache().covers(start) {
                continue;
            }
            {
                let mut inflight = self
                    .prefetch_inflight
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !inflight.insert(start) {
                    // Another worker is already decoding this window.
                    continue;
                }
            }

            let source = Arc::clone(&self.source);
            let index = Arc::clone(&self.index);
            let cache = Arc::clone(&self.cache);
            let expand_cache = Arc::clone(&self.expand_cache);
            let gen_arc = Arc::clone(&self.generation);
            let cancelled = Arc::clone(&self.cancelled);
            let inflight = Arc::clone(&self.prefetch_inflight);
            let page_size = self.config.input_page_size;
            let decoded_chunk_size = self.config.decoded_chunk_size;

            let handle = thread::Builder::new()
                .name("rapidgzip-seek-prefetch".to_owned())
                .spawn(move || {
                    struct ClearInflight {
                        set: Arc<Mutex<HashSet<u64>>>,
                        start: u64,
                    }
                    impl Drop for ClearInflight {
                        fn drop(&mut self) {
                            self.set
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .remove(&self.start);
                        }
                    }
                    let _clear = ClearInflight {
                        set: inflight,
                        start,
                    };

                    if cancelled.load(Ordering::Relaxed) {
                        return;
                    }
                    if gen_arc.load(Ordering::Relaxed) != generation {
                        return;
                    }
                    // Confirm capacity with the same policy as the consumer path.
                    let capacity = window_capacity_at(&index, decoded_chunk_size, start).min(capacity);
                    if capacity == 0 {
                        return;
                    }
                    match decode_window_independent(
                        source.as_ref(),
                        &index,
                        start,
                        capacity,
                        page_size,
                        &cancelled,
                        Some(&expand_cache),
                    ) {
                        Ok(data) if !data.is_empty() => {
                            if cancelled.load(Ordering::Relaxed) {
                                return;
                            }
                            if gen_arc.load(Ordering::Relaxed) != generation {
                                return;
                            }
                            let mut cache = cache
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            if !cache.covers(start) {
                                cache.insert(start, data);
                                cache.note_prefetch();
                            }
                        }
                        _ => {
                            // Best-effort: ignore prefetch failures.
                        }
                    }
                });

            match handle {
                Ok(handle) => self.prefetch_handles.push(handle),
                Err(_) => {
                    self.prefetch_inflight
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&start);
                    break;
                }
            }
        }
    }

    /// Ensures the buffer covers `self.position` for at least one byte when possible.
    fn ensure_buffered(&mut self) -> Result<(), DecodeError> {
        if self.buffer_contains(self.position) {
            return Ok(());
        }

        let total = self.uncompressed_len_or_max();
        if self.position >= total {
            self.clear_buffer_and_session();
            return Ok(());
        }

        if self.load_buffer_from_cache(self.position) {
            self.maybe_readahead();
            self.maybe_prefetch();
            return Ok(());
        }

        self.lock_cache().note_miss();

        // Prefer continuing the active session when it sits at position.
        let can_continue = self
            .session
            .as_ref()
            .is_some_and(|s| s.uncompressed_pos == self.position);

        if !can_continue {
            self.seek_inflate_to(self.position)?;
        }

        // Fill a lookahead buffer at position (checkpoint-aligned when denser).
        let capacity = self.window_capacity_at(self.position);
        if capacity == 0 {
            self.clear_buffer_and_session();
            return Ok(());
        }
        let mut chunk = vec![0_u8; capacity];
        let filled = self.inflate_fill(&mut chunk)?;
        chunk.truncate(filled);
        self.buffer_base = self.position;
        self.buffer = chunk;
        if !self.buffer.is_empty() {
            self.lock_cache()
                .insert(self.buffer_base, self.buffer.clone());
        }
        self.maybe_readahead();
        self.maybe_prefetch();
        Ok(())
    }

    fn seek_to(&mut self, target: u64) -> Result<u64, DecodeError> {
        if self.buffer_contains(target) {
            self.position = target;
            return Ok(self.position);
        }

        if self.load_buffer_from_cache(target) {
            self.position = target;
            return Ok(self.position);
        }

        // Far seek: invalidate in-flight prefetch inserts so workers do not
        // thrash the LRU with windows from a previous region.
        self.bump_generation();

        // Drop session unless we can reuse it for a forward seek within the
        // same inflate stream without re-seeking (optional optimization).
        // M1: re-init from checkpoint unless target is exactly session position.
        if self
            .session
            .as_ref()
            .is_some_and(|s| s.uncompressed_pos == target)
        {
            self.position = target;
            self.buffer.clear();
            self.buffer_base = target;
            return Ok(self.position);
        }

        self.position = target;
        self.seek_inflate_to(target)?;
        Ok(self.position)
    }
}

impl<R: ReadAt + 'static> Drop for IndexedReader<R> {
    fn drop(&mut self) {
        self.shutdown_prefetch();
    }
}

impl<R: ReadAt + 'static> Read for IndexedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        self.ensure_buffered()
            .map_err(|error| error.to_io_error())?;

        if !self.buffer_contains(self.position) {
            return Ok(0);
        }

        let relative = (self.position - self.buffer_base) as usize;
        let available = self.buffer.len() - relative;
        let count = min(buf.len(), available);
        buf[..count].copy_from_slice(&self.buffer[relative..relative + count]);
        self.position += count as u64;
        Ok(count)
    }
}

impl<R: ReadAt + 'static> Seek for IndexedReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let target = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(delta) => {
                let len = self.len().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::Unsupported,
                        "cannot seek from end without a known uncompressed size in the index",
                    )
                })?;
                if delta >= 0 {
                    len.checked_add(delta as u64).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "seek offset overflow")
                    })?
                } else {
                    let back = (-delta) as u64;
                    len.checked_sub(back).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "seek before start of uncompressed stream",
                        )
                    })?
                }
            }
            SeekFrom::Current(delta) => {
                if delta >= 0 {
                    self.position.checked_add(delta as u64).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "seek offset overflow")
                    })?
                } else {
                    let back = (-delta) as u64;
                    self.position.checked_sub(back).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "seek before start of uncompressed stream",
                        )
                    })?
                }
            }
        };

        self.seek_to(target).map_err(|error| error.to_io_error())
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position)
    }
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

fn window_capacity_at(index: &GzipIndex, decoded_chunk_size: usize, start: u64) -> usize {
    let total = {
        let size = index.uncompressed_size_in_bytes;
        if size == u64::MAX {
            u64::MAX
        } else {
            size
        }
    };
    if start >= total {
        return 0;
    }
    let remaining = total - start;
    let mut capacity = decoded_chunk_size as u64;

    // Align to the next denser checkpoint boundary when available.
    if let Some(next) = index
        .checkpoints
        .iter()
        .find(|cp| cp.uncompressed_offset_in_bytes > start)
    {
        let span = next
            .uncompressed_offset_in_bytes
            .saturating_sub(start);
        if span > 0 && span < capacity {
            capacity = span;
        }
    }

    min(capacity, remaining) as usize
}

/// Builds a [`Window`] from a stored index window, optionally using the
/// expanded-window LRU for zlib-compressed history.
///
/// `start_bit` is the compressed bit offset key for the cache (checkpoint key).
fn window_from_stored_cached(
    stored: Option<&StoredWindow>,
    start_bit: u64,
    expand_cache: Option<&Arc<Mutex<ExpandedWindowCache>>>,
) -> Result<Window, DecodeError> {
    let Some(window) = stored else {
        return Ok(Window::empty());
    };
    if window.is_empty() {
        return Ok(Window::empty());
    }

    let raw = if window.compression() == WindowCompression::Zlib {
        if let Some(cache_arc) = expand_cache {
            let mut cache = cache_arc
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(hit) = cache.get(start_bit) {
                hit
            } else {
                cache.note_miss();
                let owned = window
                    .decompressed()
                    .map_err(DecodeError::InvalidIndex)?
                    .into_owned();
                cache.insert(start_bit, owned.clone());
                owned
            }
        } else {
            window
                .decompressed()
                .map_err(DecodeError::InvalidIndex)?
                .into_owned()
        }
    } else {
        window
            .decompressed()
            .map_err(DecodeError::InvalidIndex)?
            .into_owned()
    };

    if raw.is_empty() {
        return Ok(Window::empty());
    }
    let window_size = INDEXED_GZIP_WINDOW_SIZE as usize;
    let truncated = if raw.len() > window_size {
        raw[raw.len() - window_size..].to_vec()
    } else {
        raw
    };
    Window::new(truncated).map_err(|_| {
        DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
            "checkpoint window exceeds 32 KiB",
        ))
    })
}

/// Independently inflate `capacity` uncompressed bytes starting at `target`.
///
/// Resumes from the nearest preceding checkpoint, discards skip bytes, then
/// fills the requested window. Crosses multi-member boundaries without CRC
/// verification (same policy as the consumer path).
fn decode_window_independent<R: ReadAt + ?Sized>(
    source: &R,
    index: &GzipIndex,
    target: u64,
    capacity: usize,
    page_size: usize,
    cancelled: &AtomicBool,
    expand_cache: Option<&Arc<Mutex<ExpandedWindowCache>>>,
) -> Result<Vec<u8>, DecodeError> {
    if capacity == 0 {
        return Ok(Vec::new());
    }
    if cancelled.load(Ordering::Relaxed) {
        return Err(DecodeError::Cancelled);
    }

    let total = {
        let size = index.uncompressed_size_in_bytes;
        if size == u64::MAX {
            u64::MAX
        } else {
            size
        }
    };
    if target >= total {
        return Ok(Vec::new());
    }

    let checkpoint = index
        .checkpoint_at_or_before(target)
        .ok_or(DecodeError::InvalidIndex(IndexError::InvalidCheckpoint(
            "no checkpoint at or before target offset",
        )))?;

    let start_bit = checkpoint.compressed_offset_in_bits;
    let checkpoint_u = checkpoint.uncompressed_offset_in_bytes;

    if is_eof_checkpoint(index, checkpoint_u, start_bit) {
        return Ok(Vec::new());
    }

    let window = window_from_stored_cached(index.window_for(start_bit), start_bit, expand_cache)?;
    let (mut inflater, mut compressed_byte) =
        RawInflater::prepare_at_bit_offset(start_bit, &window, source, page_size, true)?;

    let mut uncompressed_pos = checkpoint_u;

    // Discard bytes until `target`.
    let mut skip = target.saturating_sub(checkpoint_u);
    while skip > 0 {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }
        let chunk = min(skip, capacity.max(1) as u64) as usize;
        let mut discard = vec![0_u8; chunk];
        let produced = inflate_into(
            source,
            &mut inflater,
            &mut compressed_byte,
            &mut uncompressed_pos,
            &mut discard,
            page_size,
        )?;
        if produced == 0 {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: start_bit,
                reason: DeflateErrorKind::Truncated,
            });
        }
        skip -= produced as u64;
    }

    debug_assert_eq!(uncompressed_pos, target);

    let mut out = vec![0_u8; capacity];
    let mut filled = 0usize;
    while filled < capacity {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }
        let produced = inflate_into(
            source,
            &mut inflater,
            &mut compressed_byte,
            &mut uncompressed_pos,
            &mut out[filled..],
            page_size,
        )?;
        if produced == 0 {
            break;
        }
        filled += produced;
    }
    out.truncate(filled);
    Ok(out)
}

/// Inflate until at least one output byte is produced, the stream ends, or an
/// error occurs. Zero is returned only when the inflate session is exhausted.
fn inflate_into<R: ReadAt + ?Sized>(
    source: &R,
    inflater: &mut RawInflater,
    compressed_byte: &mut u64,
    uncompressed_pos: &mut u64,
    out: &mut [u8],
    page_size: usize,
) -> Result<usize, DecodeError> {
    if out.is_empty() {
        return Ok(0);
    }

    let mut total_produced = 0usize;

    loop {
        let mut cursor = SourceCursor::new(source, page_size)?;
        cursor.seek(*compressed_byte)?;

        if cursor.at_end() {
            if total_produced > 0 {
                return Ok(total_produced);
            }
            return Err(DecodeError::InvalidDeflate {
                bit_offset: compressed_byte.saturating_mul(8),
                reason: DeflateErrorKind::Truncated,
            });
        }

        let (input_pointer, input_length) = {
            let input = cursor.available()?;
            (input.as_ptr(), input.len().min(u32::MAX as usize))
        };

        let dest = &mut out[total_produced..];
        if dest.is_empty() {
            return Ok(total_produced);
        }

        inflater.stream.next_in = input_pointer;
        inflater.stream.avail_in = input_length as u32;
        inflater.stream.next_out = dest.as_mut_ptr();
        inflater.stream.avail_out = dest.len() as u32;
        let input_before = inflater.stream.avail_in;
        let output_before = inflater.stream.avail_out;

        // SAFETY:
        // - `inflater.stream` was initialized (optionally primed + dictionary).
        // - `next_in/avail_in` describe the current immutable cursor page.
        // - `next_out/avail_out` describe the remaining caller-owned `out` slice.
        let status = unsafe { z::inflate(&mut inflater.stream, z::Z_NO_FLUSH) };

        let consumed = usize::try_from(input_before - inflater.stream.avail_in)
            .expect("zlib uInt fits usize");
        let produced = usize::try_from(output_before - inflater.stream.avail_out)
            .expect("zlib uInt fits usize");
        cursor.advance(consumed);
        *compressed_byte = cursor.position();
        *uncompressed_pos = uncompressed_pos.saturating_add(produced as u64);
        total_produced += produced;

        match status {
            z::Z_STREAM_END => {
                if cursor.position() + 8 > cursor.length() {
                    return Err(DecodeError::InvalidGzip {
                        offset: cursor.position(),
                        reason: crate::GzipErrorKind::Truncated,
                    });
                }
                let _footer = cursor.read_exact::<8>(cursor.position())?;
                *compressed_byte = cursor.position();

                if cursor.at_end() {
                    return Ok(total_produced);
                }

                let header = parse_member_header(&mut cursor, false)?;
                debug_assert_eq!(header.deflate_start, cursor.position());
                *inflater = RawInflater::new()?;
                let empty = Window::empty();
                inflater.set_dictionary(&empty, header.deflate_start.saturating_mul(8))?;
                *compressed_byte = cursor.position();

                if total_produced > 0 {
                    return Ok(total_produced);
                }
                continue;
            }
            z::Z_OK | z::Z_BUF_ERROR if consumed != 0 || produced != 0 => {
                if total_produced > 0 {
                    return Ok(total_produced);
                }
                continue;
            }
            z::Z_DATA_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: compressed_byte.saturating_mul(8),
                    reason: DeflateErrorKind::InvalidData,
                });
            }
            z::Z_NEED_DICT => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: compressed_byte.saturating_mul(8),
                    reason: DeflateErrorKind::UnexpectedDictionary,
                });
            }
            z::Z_OK | z::Z_BUF_ERROR => {
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
    fn checkpoint_at_or_before_picks_predecessor() {
        let index = sample_index();
        assert_eq!(
            index
                .checkpoint_at_or_before(0)
                .unwrap()
                .uncompressed_offset_in_bytes,
            0
        );
        assert_eq!(
            index
                .checkpoint_at_or_before(499)
                .unwrap()
                .uncompressed_offset_in_bytes,
            0
        );
        assert_eq!(
            index
                .checkpoint_at_or_before(500)
                .unwrap()
                .uncompressed_offset_in_bytes,
            500
        );
        assert_eq!(
            index
                .checkpoint_at_or_before(999)
                .unwrap()
                .uncompressed_offset_in_bytes,
            500
        );
        assert_eq!(
            index
                .checkpoint_at_or_before(1000)
                .unwrap()
                .uncompressed_offset_in_bytes,
            1000
        );
    }

    #[test]
    fn cache_lru_evicts_oldest_and_covers_offsets() {
        let mut cache = DecodedWindowCache::new(2, 1024, true, 2);
        cache.insert(0, vec![1, 2, 3, 4]);
        cache.insert(100, vec![5, 6]);
        assert!(cache.covers(2));
        assert!(cache.covers(100));

        // Touch first chunk (promote to MRU), then insert third → evict 100.
        let hit = cache.get_covering(1).expect("hit");
        assert_eq!(hit.0, 0);
        assert_eq!(hit.1, vec![1, 2, 3, 4]);

        cache.insert(200, vec![7, 8, 9]);
        assert!(cache.covers(0));
        assert!(!cache.covers(100));
        assert!(cache.covers(200));
        let empty_expand = ExpandedWindowCache::new(0);
        assert_eq!(cache.stats(&empty_expand).chunks, 2);
        assert_eq!(cache.stats(&empty_expand).hits, 1);
        assert_eq!(cache.stats(&empty_expand).inserts, 3);
    }

    #[test]
    fn cache_disabled_with_zero_capacity() {
        let mut cache = DecodedWindowCache::new(0, 1024, true, 0);
        cache.insert(0, vec![1, 2, 3]);
        assert!(!cache.covers(0));
        assert!(cache.get_covering(0).is_none());

        let mut cache = DecodedWindowCache::new(4, 0, true, 0);
        cache.insert(0, vec![1, 2, 3]);
        assert!(!cache.covers(0));
    }

    #[test]
    fn cache_admits_single_oversized_chunk() {
        let mut cache = DecodedWindowCache::new(4, 8, true, 0);
        cache.insert(0, vec![0; 4]);
        cache.insert(10, vec![1; 16]); // exceeds max_bytes alone
        assert!(!cache.covers(0));
        assert!(cache.covers(10));
        let empty_expand = ExpandedWindowCache::new(0);
        assert_eq!(cache.stats(&empty_expand).bytes, 16);
        assert_eq!(cache.stats(&empty_expand).chunks, 1);
    }

    #[test]
    fn expanded_window_cache_lru_and_hit_miss() {
        let mut cache = ExpandedWindowCache::new(2);
        cache.note_miss();
        cache.insert(10, vec![1, 2, 3]);
        cache.note_miss();
        cache.insert(20, vec![4, 5]);
        assert_eq!(cache.get(10).as_deref(), Some(&[1, 2, 3][..]));
        cache.note_miss();
        cache.insert(30, vec![6]); // evicts 20 (LRU after promoting 10)
        assert!(cache.get(20).is_none());
        assert_eq!(cache.get(10).as_deref(), Some(&[1, 2, 3][..]));
        assert_eq!(cache.get(30).as_deref(), Some(&[6][..]));
        assert_eq!(cache.hits, 3);
        assert_eq!(cache.misses, 3);
        assert_eq!(cache.entries.len(), 2);
    }
}
