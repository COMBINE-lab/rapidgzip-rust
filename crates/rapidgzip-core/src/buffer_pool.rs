//! Soft-capped free list for reusable `Vec<u8>` capacity.
//!
//! Used by the estimated/marker coordinator (post-`emit_reusable` recycle into
//! worker `clean_scratch`) and by [`crate::reader::DecoderReader`] (consumer
//! recycles fully-read channel chunks; `ChannelOutput::emit_reusable` steals
//! for the next coordinator fill). The two pools are intentionally separate:
//! the reader pool is created at spawn time, while the estimated pool lives
//! only inside the estimated decode scopes.

use crossbeam_deque::{Injector, Steal};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Soft-capped free list of empty `Vec<u8>` buffers with retained capacity.
///
/// Soft-capped so RSS cannot grow unbounded if producers outrun consumers.
/// Zero-capacity vecs are dropped, not stored.
pub(crate) struct ByteBufferFreeList {
    buffers: Injector<Vec<u8>>,
    count: AtomicUsize,
    cap: usize,
}

impl ByteBufferFreeList {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            buffers: Injector::new(),
            count: AtomicUsize::new(0),
            cap: cap.max(1),
        }
    }

    /// Soft capacity (at least 1).
    #[cfg(test)]
    pub(crate) fn cap(&self) -> usize {
        self.cap
    }

    /// Approximate number of buffers currently on the free list.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// Push a cleared buffer when under the soft cap; otherwise drop it.
    pub(crate) fn recycle(&self, mut buf: Vec<u8>) {
        if buf.capacity() == 0 {
            return;
        }
        buf.clear();
        let prev = self.count.fetch_add(1, Ordering::AcqRel);
        if prev >= self.cap {
            self.count.fetch_sub(1, Ordering::AcqRel);
            return;
        }
        self.buffers.push(buf);
    }

    /// Steal one free-list buffer, or `None` if the list is empty.
    pub(crate) fn try_steal(&self) -> Option<Vec<u8>> {
        loop {
            match self.buffers.steal() {
                Steal::Success(buf) => {
                    self.count.fetch_sub(1, Ordering::AcqRel);
                    return Some(buf);
                }
                Steal::Retry => std::hint::spin_loop(),
                Steal::Empty => return None,
            }
        }
    }

    /// Steal one free-list buffer and keep the larger capacity in `dst`.
    ///
    /// The buffer that is not retained is recycled back (or dropped if it has
    /// zero capacity) so free-list entries are not lost when the worker already
    /// holds a larger scratch.
    pub(crate) fn try_steal_into(&self, dst: &mut Vec<u8>) {
        let Some(buf) = self.try_steal() else {
            return;
        };
        if buf.capacity() > dst.capacity() {
            let displaced = std::mem::replace(dst, buf);
            self.recycle(displaced);
        } else {
            self.recycle(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ByteBufferFreeList;

    #[test]
    fn recycle_increases_len_and_try_steal_returns_capacity() {
        let list = ByteBufferFreeList::new(4);
        assert_eq!(list.len(), 0);
        assert_eq!(list.cap(), 4);

        let mut buf = Vec::with_capacity(1024);
        buf.extend_from_slice(&[1, 2, 3]);
        list.recycle(buf);
        assert_eq!(list.len(), 1);

        let stolen = list.try_steal().expect("one buffer available");
        assert!(stolen.is_empty());
        assert!(stolen.capacity() >= 1024);
        assert_eq!(list.len(), 0);
        assert!(list.try_steal().is_none());
    }

    #[test]
    fn zero_capacity_is_not_stored() {
        let list = ByteBufferFreeList::new(4);
        list.recycle(Vec::new());
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn soft_cap_drops_excess() {
        let list = ByteBufferFreeList::new(2);
        list.recycle(Vec::with_capacity(8));
        list.recycle(Vec::with_capacity(16));
        list.recycle(Vec::with_capacity(32));
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn try_steal_into_keeps_larger_capacity() {
        let list = ByteBufferFreeList::new(4);
        list.recycle(Vec::with_capacity(64));
        let mut dst = Vec::with_capacity(8);
        list.try_steal_into(&mut dst);
        assert!(dst.capacity() >= 64);
        // Displaced smaller buffer is recycled back.
        assert_eq!(list.len(), 1);
    }
}
