//! Bounded cache of expanded predecessor windows.
//!
//! Repeated seeks into the same region reuse the same checkpoint window.
//! Expanding a zlib-compressed window costs a full inflate, so the reader
//! keeps recently used windows expanded under a byte budget and evicts the
//! least recently used entry first.

use std::collections::HashMap;

/// Default cache budget: eight full windows.
pub(crate) const DEFAULT_BUDGET: usize = 8 * super::super::index::WINDOW_SIZE;

pub(crate) struct WindowCache {
    entries: HashMap<u64, Entry>,
    budget: usize,
    used: usize,
    clock: u64,
}

struct Entry {
    window: Vec<u8>,
    last_used: u64,
}

impl WindowCache {
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            entries: HashMap::new(),
            budget,
            used: 0,
            clock: 0,
        }
    }

    /// Returns the expanded window stored for `key`, marking it as used.
    pub(crate) fn get(&mut self, key: u64) -> Option<&[u8]> {
        self.clock += 1;
        let clock = self.clock;
        let entry = self.entries.get_mut(&key)?;
        entry.last_used = clock;
        Some(&entry.window)
    }

    /// Stores `window` for `key`, evicting until it fits the budget.
    ///
    /// A window larger than the whole budget is dropped rather than stored.
    pub(crate) fn insert(&mut self, key: u64, window: Vec<u8>) {
        if window.len() > self.budget {
            return;
        }
        self.clock += 1;
        if let Some(previous) = self.entries.remove(&key) {
            self.used -= previous.window.len();
        }
        while self.used + window.len() > self.budget {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| *key)
            else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.used -= evicted.window.len();
            }
        }
        self.used += window.len();
        let clock = self.clock;
        self.entries.insert(
            key,
            Entry {
                window,
                last_used: clock,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_stored_windows() {
        let mut cache = WindowCache::new(64);
        cache.insert(10, vec![1u8; 8]);
        assert_eq!(cache.get(10), Some(&[1u8; 8][..]));
        assert_eq!(cache.get(11), None);
    }

    #[test]
    fn evicts_the_least_recently_used_entry() {
        let mut cache = WindowCache::new(16);
        cache.insert(1, vec![0u8; 8]);
        cache.insert(2, vec![0u8; 8]);
        // Touch entry 1 so entry 2 becomes the eviction candidate.
        assert!(cache.get(1).is_some());
        cache.insert(3, vec![0u8; 8]);
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_none());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn drops_windows_larger_than_the_budget() {
        let mut cache = WindowCache::new(4);
        cache.insert(1, vec![0u8; 8]);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn replacing_a_key_does_not_leak_budget() {
        let mut cache = WindowCache::new(16);
        for _ in 0..10 {
            cache.insert(1, vec![0u8; 8]);
        }
        cache.insert(2, vec![0u8; 8]);
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
    }
}
