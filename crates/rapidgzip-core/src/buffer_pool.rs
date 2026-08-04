//! Decode-local, capacity-bounded recycling for byte buffers.
//!
//! This is deliberately private. Different decoder paths have different
//! ownership and sizing behavior, so a path opts in only after its allocation
//! profile shows a benefit.

use crossbeam_deque::{Injector, Steal};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A small size-classed pool whose accounting is charged before publication.
pub(crate) struct ByteBufferPool {
    buckets: Box<[Injector<Vec<u8>>]>,
    class_ceilings: Box<[usize]>,
    retained_bytes: AtomicUsize,
    retained_entries: AtomicUsize,
    maximum_bytes: usize,
    maximum_entries: usize,
    minimum_capacity: usize,
    maximum_capacity: usize,
    #[cfg(test)]
    hits: AtomicUsize,
    #[cfg(test)]
    misses: AtomicUsize,
    #[cfg(test)]
    rejected: AtomicUsize,
}

impl ByteBufferPool {
    /// Builds the deliberately small pool used by one positional reader.
    pub(crate) fn for_reader(decoded_chunk_size: usize, in_flight_chunks: usize) -> Self {
        let minimum_capacity = decoded_chunk_size.div_ceil(2);
        let maximum_capacity = decoded_chunk_size.saturating_mul(2);
        let maximum_entries = in_flight_chunks.saturating_add(1).min(4);
        Self::new(
            decoded_chunk_size,
            minimum_capacity,
            maximum_capacity,
            maximum_capacity,
            maximum_entries,
        )
    }

    fn new(
        target_capacity: usize,
        minimum_capacity: usize,
        maximum_capacity: usize,
        maximum_bytes: usize,
        maximum_entries: usize,
    ) -> Self {
        debug_assert!(target_capacity != 0);
        debug_assert!(minimum_capacity != 0);
        debug_assert!(minimum_capacity <= target_capacity);
        debug_assert!(target_capacity <= maximum_capacity);

        let target_class = target_capacity
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX);
        let mut classes = Vec::with_capacity(3);
        for ceiling in [
            target_class.div_ceil(2),
            target_class,
            target_class.saturating_mul(2),
        ] {
            if classes.last().copied() != Some(ceiling) {
                classes.push(ceiling);
            }
        }
        let buckets = (0..classes.len())
            .map(|_| Injector::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            buckets,
            class_ceilings: classes.into_boxed_slice(),
            retained_bytes: AtomicUsize::new(0),
            retained_entries: AtomicUsize::new(0),
            maximum_bytes,
            maximum_entries,
            minimum_capacity,
            maximum_capacity,
            #[cfg(test)]
            hits: AtomicUsize::new(0),
            #[cfg(test)]
            misses: AtomicUsize::new(0),
            #[cfg(test)]
            rejected: AtomicUsize::new(0),
        }
    }

    /// Returns an empty allocation that can satisfy `minimum_capacity`.
    ///
    /// Empty or unsuitable pools fall back to a zero-capacity vector. The
    /// caller retains ordinary `Vec` growth semantics in that case.
    pub(crate) fn take(&self, minimum_capacity: usize) -> Vec<u8> {
        let first_bucket = self
            .class_ceilings
            .iter()
            .position(|&ceiling| ceiling >= minimum_capacity);
        let Some(first_bucket) = first_bucket else {
            #[cfg(test)]
            self.misses.fetch_add(1, Ordering::Relaxed);
            return Vec::new();
        };

        for bucket in &self.buckets[first_bucket..] {
            loop {
                match bucket.steal() {
                    Steal::Success(buffer) => {
                        debug_assert!(buffer.is_empty());
                        let capacity = buffer.capacity();
                        self.retained_bytes.fetch_sub(capacity, Ordering::AcqRel);
                        self.retained_entries.fetch_sub(1, Ordering::AcqRel);
                        if capacity >= minimum_capacity {
                            #[cfg(test)]
                            self.hits.fetch_add(1, Ordering::Relaxed);
                            return buffer;
                        }
                        // A non-power-of-two capacity can share the request's
                        // ceiling while still being slightly too small. Drop
                        // it instead of making the caller grow a retained
                        // allocation immediately.
                        #[cfg(test)]
                        self.rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    Steal::Retry => continue,
                    Steal::Empty => break,
                }
            }
        }

        #[cfg(test)]
        self.misses.fetch_add(1, Ordering::Relaxed);
        Vec::new()
    }

    /// Clears and retains `buffer` when both resource ceilings permit it.
    pub(crate) fn recycle(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        let capacity = buffer.capacity();
        if capacity < self.minimum_capacity || capacity > self.maximum_capacity {
            #[cfg(test)]
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }

        if !reserve(&self.retained_bytes, capacity, self.maximum_bytes) {
            #[cfg(test)]
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if !reserve(&self.retained_entries, 1, self.maximum_entries) {
            self.retained_bytes.fetch_sub(capacity, Ordering::AcqRel);
            #[cfg(test)]
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let bucket = self
            .class_ceilings
            .iter()
            .position(|&ceiling| capacity <= ceiling)
            .unwrap_or(self.buckets.len() - 1);
        self.buckets[bucket].push(buffer);
    }

    #[cfg(test)]
    fn snapshot(&self) -> PoolSnapshot {
        PoolSnapshot {
            retained_bytes: self.retained_bytes.load(Ordering::Acquire),
            retained_entries: self.retained_entries.load(Ordering::Acquire),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
        }
    }
}

fn reserve(counter: &AtomicUsize, amount: usize, maximum: usize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current
                .checked_add(amount)
                .filter(|&updated| updated <= maximum)
        })
        .is_ok()
}

#[cfg(test)]
#[derive(Debug, Eq, PartialEq)]
struct PoolSnapshot {
    retained_bytes: usize,
    retained_entries: usize,
    hits: usize,
    misses: usize,
    rejected: usize,
}

#[cfg(test)]
mod tests {
    use super::ByteBufferPool;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn reuses_a_sufficient_buffer_and_releases_accounting() {
        let pool = ByteBufferPool::new(1024, 512, 2048, 2048, 2);
        pool.recycle(Vec::with_capacity(1024));
        assert_eq!(pool.snapshot().retained_bytes, 1024);

        let buffer = pool.take(1024);
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 1024);
        assert_eq!(pool.snapshot().retained_bytes, 0);
        assert_eq!(pool.snapshot().retained_entries, 0);
        assert_eq!(pool.snapshot().hits, 1);
    }

    #[test]
    fn enforces_byte_entry_and_capacity_bounds() {
        let pool = ByteBufferPool::new(1024, 512, 2048, 2048, 2);
        pool.recycle(Vec::with_capacity(1024));
        pool.recycle(Vec::with_capacity(1024));
        pool.recycle(Vec::with_capacity(1024));
        pool.recycle(Vec::with_capacity(256));
        pool.recycle(Vec::with_capacity(4096));

        let snapshot = pool.snapshot();
        assert_eq!(snapshot.retained_bytes, 2048);
        assert_eq!(snapshot.retained_entries, 2);
        assert_eq!(snapshot.rejected, 3);
    }

    #[test]
    fn searches_larger_classes_and_rejects_an_undersized_peer() {
        let pool = ByteBufferPool::new(1000, 500, 2000, 4000, 4);
        pool.recycle(Vec::with_capacity(900));
        pool.recycle(Vec::with_capacity(1500));

        let buffer = pool.take(1000);
        assert_eq!(buffer.capacity(), 1500);
        let snapshot = pool.snapshot();
        assert_eq!(snapshot.retained_bytes, 0);
        assert_eq!(snapshot.retained_entries, 0);
        assert_eq!(snapshot.hits, 1);
        assert_eq!(snapshot.rejected, 1);
    }

    #[test]
    fn concurrent_round_trips_stay_within_bounds() {
        let pool = Arc::new(ByteBufferPool::new(1024, 512, 2048, 4096, 4));
        thread::scope(|scope| {
            for _ in 0..8 {
                let pool = Arc::clone(&pool);
                scope.spawn(move || {
                    let mut buffer = Vec::with_capacity(1024);
                    for _ in 0..1000 {
                        pool.recycle(buffer);
                        buffer = pool.take(1024);
                        if buffer.capacity() < 1024 {
                            buffer.reserve(1024);
                        }
                    }
                });
            }
        });

        let snapshot = pool.snapshot();
        assert!(snapshot.retained_bytes <= 4096);
        assert!(snapshot.retained_entries <= 4);
    }
}
