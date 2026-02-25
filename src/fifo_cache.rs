use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

pub use self::entry::ValueEntry;
pub use self::fifo::*;
pub use self::ghost_list::*;

mod entry;
mod fifo;
mod ghost_list;

type ValuesMap<K, V> = HashMap<K, ValueEntry<V>>;

/// S3-FIFO cache: a cache that uses the S3-FIFO eviction strategy.
///
/// Note: The `S3` in `S3FifoCache` refers to `S3-FIFO`'s three
/// internal static queues, not to Amazon's `S3` web service.
///
/// # Memory Estimates
///
/// Given a max length of `N`, the cache requires memory of approximately:
///
/// ```plain
/// APPROX_MIN_BYTES = N * (2 * sizeof(K) + sizeof(V) + 1)
/// APPROX_MAX_BYTES = N * (4 * sizeof(K) + sizeof(V) + 1)
/// ```
///
/// All sizes are in bytes. The `+1` accounts for the `AtomicU8` frequency counter
/// in the `ValueEntry<V>` wrapper. Use `std::mem::size_of::<T>()` to determine type sizes.
///
/// ## Scenarios
///
/// - **Minimum:** Cache starting to fill. Each entry's key appears in the values
///   `HashMap` and in one queue (small or main). No ghost entries yet.
///   - Key storage: 2 copies per entry (1 in HashMap + 1 in queue)
///   - Value storage: 1 copy per entry (in HashMap)
///
/// - **Maximum:** Cache at full capacity with active eviction. Ghost list is populated
///   with N evicted keys. Maximum key duplication across all internal structures.
///   - Active key storage: 2 copies per entry (1 in HashMap + 1 in queue)
///   - Evicted key storage: 2 copies per entry (1 in ghost HashSet + 1 in ghost VecDeque)
///   - Total: 4 key copies per unique key at maximum
///
/// ## Arc-Wrapped Types
///
/// For types wrapped in `Arc<T>`, note that `sizeof(Arc<T>)` returns the pointer size
/// (8 bytes on 64-bit systems), not the size of the wrapped data. When `Arc<T>` keys are
/// duplicated in the cache's internal structures, only the Arc pointer is copied—the
/// actual wrapped data is heap-allocated and shared among all clones.
///
/// ## Important Notes
///
/// - Formulas exclude `HashMap` and `VecDeque` internal overhead (buckets, capacity
///   over-allocation, etc.)
/// - Actual memory usage may be 1.5-2x higher due to allocator overhead and data
///   structure internals
/// - Padding and alignment may add additional bytes depending on your types
///
/// ## Examples
///
/// ```rust
/// use std::mem::size_of;
/// use std::sync::Arc;
///
/// // Example 1: String keys (24 bytes), u64 values (8 bytes)
/// // MIN = N * (2*24 + 8 + 1) = 57N bytes
/// // MAX = N * (4*24 + 8 + 1) = 105N bytes
///
/// // Example 2: Arc<String> keys (8 bytes pointer), u64 values (8 bytes)
/// // MIN = N * (2*8 + 8 + 1) = 25N bytes
/// // MAX = N * (4*8 + 8 + 1) = 41N bytes
/// // Note: The actual String data is allocated once; only Arc pointers are duplicated
/// ```
pub struct S3FifoCache<K, V> {
    values: ValuesMap<K, V>,

    // Fixed size: `N / 10`
    small: FiFoQueue<K>,
    // Fixed size: `N`
    main: FiFoQueue<K>,
    // Fixed size: `N`
    ghost: GhostList<K>,
}

impl<K: Clone + Eq + Hash, V> S3FifoCache<K, V> {
    /// The scale factor used to partition capacity between small and main queues.
    ///
    /// A value of 10 means the main queue gets 10x more capacity than the small queue.
    pub const SCALE_FACTOR: usize = 10;

    /// Creates a new S3-FIFO cache with automatic queue sizing.
    ///
    /// Partitions the total capacity between the small and main queues using a scale
    /// factor of 10:1. For small caches (≤20 entries), more aggressive partitioning is used.
    ///
    /// ```
    /// use s3_cache::S3FifoCache;
    ///
    /// let cache: S3FifoCache<String, Vec<u8>> = S3FifoCache::with_max_len(100);
    /// assert_eq!(cache.max_len(), 100);
    /// ```
    pub fn with_max_len(max_len: usize) -> Self {
        let scale_factor = Self::SCALE_FACTOR;

        let max_small_len: usize = match max_len {
            0 => 0,
            1 => 1,
            2..=10 => max_len / 2.max(scale_factor),
            11..=20 => max_len / 5.max(scale_factor),
            21.. => max_len / scale_factor,
        };
        let max_main_len: usize = max_len - max_small_len;

        Self::new(max_small_len, max_main_len)
    }

    /// Creates a new S3-FIFO cache with explicit queue sizes.
    ///
    /// Use this for fine-grained control over the small and main queue capacities.
    /// For most use cases, [`with_max_len`](Self::with_max_len) is recommended.
    ///
    /// ```
    /// use s3_cache::S3FifoCache;
    ///
    /// let cache: S3FifoCache<String, Vec<u8>> = S3FifoCache::new(10, 90);
    /// ```
    pub fn new(max_small_len: usize, max_main_len: usize) -> Self {
        let values = HashMap::new();

        let max_ghost_len = max_main_len;

        let small = FiFoQueue::new(max_small_len);
        let main = FiFoQueue::new(max_main_len);
        let ghost = GhostList::new(max_ghost_len);

        Self {
            values,
            small,
            main,
            ghost,
        }
    }

    /// Returns the maximum total number of entries the cache can hold.
    #[inline]
    pub fn max_len(&self) -> usize {
        self.small.max_len() + self.main.max_len()
    }

    /// Returns the current number of entries in the cache.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if the cache contains no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.small.is_empty() && self.main.is_empty()
    }

    /// Returns `true` if the cache is at maximum capacity.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() == self.max_len()
    }

    /// Compacts internal data structures to free unused memory.
    ///
    /// Useful after large-scale removals or when memory pressure is high.
    pub fn compact(&mut self) {
        self.ghost.compact();
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key exists, updates its value and increments the access counter.
    /// New keys enter the small queue, or main queue if recently evicted (in ghost list).
    /// Evicts entries as needed to maintain capacity.
    ///
    /// Returns the previous value if the key already existed.
    ///
    /// ```
    /// use s3_cache::S3FifoCache;
    ///
    /// let mut cache = S3FifoCache::with_max_len(2);
    /// assert_eq!(cache.insert("key1".to_string(), "value1".to_string()), None);
    /// assert_eq!(cache.insert("key1".to_string(), "updated".to_string()), Some("value1".to_string()));
    /// ```
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if let Some(entry) = self.values.get_mut(&key) {
            let old_value = std::mem::replace(entry.value_mut(), value);
            entry.increment_counter();

            return Some(old_value);
        }

        let entry = ValueEntry::new(value);

        // push_force allows the queue to temporarily exceed its target size.
        // The eviction loop below trims the cache back to capacity.
        if self.ghost.contains(&key) {
            self.ghost.remove(&key);
            self.main.push_force(key.clone());
        } else {
            self.small.push_force(key.clone());
        }

        self.values.insert(key, entry);

        // Evict until within capacity (matches paper: evict while |S|+|M| > n)
        while self.values.len() > self.max_len() {
            if self.evict().is_none() {
                break;
            }
        }

        None
    }

    /// Gets a reference to the value associated with the key.
    ///
    /// Increments the internal frequency counter, affecting eviction decisions.
    ///
    /// ```
    /// use s3_cache::S3FifoCache;
    ///
    /// let mut cache = S3FifoCache::with_max_len(10);
    /// cache.insert("key1".to_string(), "value1".to_string());
    /// assert_eq!(cache.get("key1"), Some(&"value1".to_string()));
    /// ```
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.values.get(key).map(|entry| {
            entry.increment_counter();
            entry.value()
        })
    }

    /// Removes a key from the cache, returning its value if present.
    ///
    /// ```
    /// use s3_cache::S3FifoCache;
    ///
    /// let mut cache = S3FifoCache::with_max_len(10);
    /// cache.insert("key1".to_string(), "value1".to_string());
    /// assert_eq!(cache.remove(&"key1".to_string()), Some("value1".to_string()));
    /// assert_eq!(cache.remove(&"key1".to_string()), None);
    /// ```
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let Some(entry) = self.values.remove(key) else {
            return None;
        };

        Some(entry.into_value())
    }

    /// Retains only entries satisfying the predicate.
    ///
    /// The predicate receives mutable references, allowing in-place value modifications.
    ///
    /// ```
    /// use s3_cache::S3FifoCache;
    ///
    /// let mut cache = S3FifoCache::with_max_len(10);
    /// cache.insert("keep".to_string(), 100);
    /// cache.insert("remove".to_string(), 50);
    ///
    /// cache.retain(|_key, value| *value >= 100);
    /// assert!(cache.contains_key("keep"));
    /// assert!(!cache.contains_key("remove"));
    /// ```
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.values.retain(|key, entry| f(key, entry.value_mut()));
    }

    /// Evicts one entry according to the S3-FIFO algorithm.
    ///
    /// Evicts from the small queue if it exceeds its target size, otherwise from main.
    /// Entries with non-zero frequency counters get a second chance.
    ///
    /// Returns the evicted key-value pair, or `None` if the cache is empty.
    pub fn evict(&mut self) -> Option<(K, V)> {
        // Per the paper: evict from small when it exceeds its target size, else from main.
        // pop_from_small may promote (return None) instead of evicting, so we loop.
        loop {
            if self.small.len() > self.small.max_len() {
                if let Some(key) = self.pop_from_small() {
                    return self.remove(&key).map(|v| (key, v));
                }
                // Entry was promoted from small to main (not evicted). Retry.
                continue;
            }

            let evicted_key = self.pop_from_main().or_else(|| self.pop_from_small())?;
            return self.remove(&evicted_key).map(|v| (evicted_key, v));
        }
    }

    /// Returns `true` if the cache contains the specified key.
    ///
    /// Unlike [`get`](Self::get), this does not increment the access counter.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.values.contains_key(key)
    }

    fn pop_from_small(&mut self) -> Option<K> {
        Self::pop_from_small_impl(
            &mut self.values,
            &mut self.small,
            &mut self.main,
            &mut self.ghost,
        )
    }

    fn pop_from_small_impl(
        values: &mut ValuesMap<K, V>,
        small: &mut FiFoQueue<K>,
        main: &mut FiFoQueue<K>,
        ghost: &mut GhostList<K>,
    ) -> Option<K> {
        loop {
            let key = small.pop()?;

            let Some(entry) = values.get(&key) else {
                continue; // skip tombstone
            };

            if entry.counter() > 0 {
                entry.decrement_counter();
                // Promote to main. push_force is safe: it never drops items.
                // Main may temporarily exceed its target; the eviction loop
                // in insert()/evict() drains it.
                main.push_force(key);
                return None; // promoted, not evicted
            }

            Self::push_to_ghost_impl(ghost, key.clone());
            return Some(key);
        }
    }

    fn pop_from_main(&mut self) -> Option<K> {
        Self::pop_from_main_impl(&mut self.values, &mut self.main)
    }

    fn pop_from_main_impl(values: &mut ValuesMap<K, V>, main: &mut FiFoQueue<K>) -> Option<K> {
        loop {
            let key = main.pop()?;

            let Some(entry) = values.get(&key) else {
                continue; // skip tombstone
            };

            if entry.counter() > 0 {
                entry.decrement_counter();
                // FIFO-Reinsertion: re-insert at head. push_force is safe here
                // because we just popped from the same queue, so the net queue
                // length doesn't increase.
                main.push_force(key);
                continue;
            }

            return Some(key);
        }
    }

    fn push_to_ghost_impl(ghost: &mut GhostList<K>, key: K) -> Option<K> {
        let evicted = if ghost.is_full() {
            Self::pop_from_ghost_impl(ghost)
        } else {
            None
        };

        ghost.insert(key);

        evicted
    }

    fn pop_from_ghost_impl(ghost: &mut GhostList<K>) -> Option<K> {
        ghost.evict_oldest()
    }
}

#[cfg(test)]
mod tests;
