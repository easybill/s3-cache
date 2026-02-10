use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::{
    borrow::Borrow,
    collections::VecDeque,
    hash::Hash,
    num::{NonZeroU64, NonZeroUsize},
};

use super::{CacheError, CacheStatistics, Cacheable, GhostList, ValueEntry};

/// Location of a key in the S3-FIFO cache queues.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueueLocation {
    Small,
    Main,
}

/// S3-FIFO cache: a cache that uses the S3-FIFO eviction strategy.
pub struct S3FiFoCache<K, V> {
    pub(crate) values: HashMap<K, ValueEntry<V>>,
    pub(crate) queue_location: HashMap<K, QueueLocation>,

    pub(crate) small_fifo: VecDeque<K>,
    pub(crate) main_fifo: VecDeque<K>,
    pub(crate) ghost: GhostList<K>,

    pub(crate) small_count: u64,
    pub(crate) small_size: usize,
    pub(crate) max_small_count: u64,
    pub(crate) max_small_size: usize,
    pub(crate) main_count: u64,
    pub(crate) main_size: usize,
    pub(crate) max_main_count: u64,
    pub(crate) max_main_size: usize,
    pub(crate) max_count: u64,
    pub(crate) max_size: usize,
}

impl<K: Clone + Eq + Hash, V: Cacheable> S3FiFoCache<K, V> {
    pub fn new(max_count: NonZeroU64, max_size: NonZeroUsize) -> Self {
        let max_count = max_count.get();
        let max_size = max_size.get();

        let max_small_count = std::cmp::max(1, max_count / 10);
        let max_small_size = std::cmp::max(1, max_size / 10);

        Self {
            values: HashMap::new(),
            queue_location: HashMap::new(),
            main_fifo: VecDeque::new(),
            small_fifo: VecDeque::new(),
            ghost: GhostList::new(max_count as usize - max_small_count as usize),
            small_count: 0,
            small_size: 0,
            max_small_count,
            max_small_size,
            main_count: 0,
            main_size: 0,
            max_main_count: max_count - max_small_count,
            max_main_size: max_size - max_small_size,
            max_count,
            max_size,
        }
    }

    pub fn statistics(&self) -> CacheStatistics {
        CacheStatistics {
            count: self.count(),
            max_count: self.max_count(),
            size: self.size(),
            max_size: self.max_size(),
        }
    }

    #[inline(always)]
    pub fn count(&self) -> u64 {
        self.small_count + self.main_count
    }

    #[inline(always)]
    pub fn size(&self) -> usize {
        self.small_size + self.main_size
    }

    #[inline(always)]
    pub fn max_count(&self) -> u64 {
        self.max_count
    }

    #[inline(always)]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    pub fn compact(&mut self) {
        self.ghost.compact();
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), CacheError> {
        let size = value.size();

        if self.values.contains_key(&key) {
            return Err(CacheError::ValueAlreadyPresent);
        }

        if size > self.max_small_size {
            return Err(CacheError::ValueTooBig);
        }

        if self.ghost.contains(&key) {
            self.ghost.remove(&key);

            while self.main_count >= self.max_main_count
                || self.main_size.saturating_add(size) > self.max_main_size
            {
                self.evict_m();
            }
            self.main_fifo.push_front(key.clone());
            self.queue_location.insert(key.clone(), QueueLocation::Main);
            self.main_count += 1;
            self.main_size += size;
        } else {
            while self.small_count >= self.max_small_count
                || self.small_size.saturating_add(size) > self.max_small_size
            {
                self.evict_s();
            }
            self.small_fifo.push_front(key.clone());
            self.queue_location.insert(key.clone(), QueueLocation::Small);
            self.small_count += 1;
            self.small_size += size;
        }

        self.values.insert(key, ValueEntry::new(value, size));

        Ok(())
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.values.get(key).map(|value_entry| {
            let _ = value_entry
                .freq
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |f| {
                    Some(std::cmp::min(f + 1, 3))
                });
            &value_entry.value
        })
    }

    /// Removes a specific key from the cache. Returns true if the key was present.
    pub fn remove(&mut self, key: &K) -> bool {
        if let Some(entry) = self.values.remove(key) {
            // Use queue_location to efficiently find which queue the key is in.
            // We leave tombstones in the FIFO queues; they'll be skipped during eviction.
            if let Some(location) = self.queue_location.remove(key) {
                match location {
                    QueueLocation::Small => {
                        self.small_count -= 1;
                        self.small_size -= entry.size;
                    }
                    QueueLocation::Main => {
                        self.main_count -= 1;
                        self.main_size -= entry.size;
                    }
                }
            }
            true
        } else {
            false
        }
    }

    /// Removes all entries matching the given predicate. Returns count of removed entries.
    pub fn remove_matching<F>(&mut self, mut predicate: F) -> usize
    where
        F: FnMut(&K) -> bool,
    {
        let keys_to_remove: Vec<K> = self
            .values
            .keys()
            .filter(|k| predicate(k))
            .cloned()
            .collect();

        let count = keys_to_remove.len();
        for key in &keys_to_remove {
            self.remove(key);
        }
        count
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.values.contains_key(key)
    }

    pub(crate) fn evict_s(&mut self) {
        while let Some(tail_key) = self.small_fifo.pop_back() {
            let Some(tail) = self.values.get(&tail_key) else {
                // Tombstone, remove from location map and continue
                self.queue_location.remove(&tail_key);
                continue;
            };

            self.small_count -= 1;
            self.small_size -= tail.size;

            if tail.freq.load(Ordering::Acquire) > 1 {
                let size = tail.size;

                while self.main_count >= self.max_main_count
                    || self.main_size.saturating_add(size) > self.max_main_size
                {
                    self.evict_m();
                }

                self.main_fifo.push_back(tail_key.clone());
                self.queue_location.insert(tail_key, QueueLocation::Main);
                self.main_count += 1;
                self.main_size += size;

                return;
            } else {
                let _ = self.values.remove(&tail_key).unwrap();
                self.queue_location.remove(&tail_key);
                self.ghost.insert(tail_key);
                return;
            }
        }
    }

    pub(crate) fn evict_m(&mut self) {
        while let Some(tail_key) = self.main_fifo.pop_back() {
            let Some(tail) = self.values.get(&tail_key) else {
                // Tombstone, remove from location map and continue
                self.queue_location.remove(&tail_key);
                continue;
            };

            self.main_count -= 1;
            self.main_size -= tail.size;

            if tail.freq.load(Ordering::Acquire) > 0 {
                self.main_count += 1;
                self.main_size += tail.size;
                tail.freq.fetch_sub(1, Ordering::AcqRel);
                self.main_fifo.push_front(tail_key);
                // Location stays Main, no need to update
            } else {
                let _ = self.values.remove(&tail_key).unwrap();
                self.queue_location.remove(&tail_key);
                return;
            }
        }
    }
}
