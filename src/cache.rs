use std::{
    borrow::Borrow,
    collections::VecDeque,
    hash::Hash,
    num::{NonZeroU64, NonZeroUsize},
    sync::atomic::{AtomicU8, Ordering},
    time::Duration,
};

use hashbrown::{HashMap, HashSet};
use tokio::sync::Mutex;

use crate::{cache_entry::CachedObject, cache_key::CacheKey, telemetry};

/// Something that can be cached.
pub trait Cacheable {
    fn size(&self) -> usize;
}

impl Cacheable for Vec<u8> {
    fn size(&self) -> usize {
        self.len()
    }
}

impl Cacheable for bytes::Bytes {
    fn size(&self) -> usize {
        self.len()
    }
}

/// Statistics about the cache.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheStatistics {
    pub count: u64,
    pub max_count: u64,
    pub size: usize,
    pub max_size: usize,
}

/// Errors the cache can throw.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum CacheError {
    ValueTooBig,
    ValueAlreadyPresent,
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::ValueTooBig => write!(f, "Value is too big"),
            CacheError::ValueAlreadyPresent => write!(f, "Value is already present in cache"),
        }
    }
}

impl std::error::Error for CacheError {}

/// A FIFO-ordered ghost list that supports O(1) random access and removal.
struct GhostList<K> {
    map: HashSet<K>,
    queue: VecDeque<K>,
    max_count: usize,
}

impl<K: Clone + Eq + Hash> GhostList<K> {
    fn new(max_count: usize) -> Self {
        Self {
            map: HashSet::new(),
            queue: VecDeque::new(),
            max_count,
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.contains(key)
    }

    fn insert(&mut self, key: K) {
        if self.map.contains(&key) {
            return;
        }

        while self.len() >= self.max_count {
            self.evict_oldest();
        }

        self.map.insert(key.clone());
        self.queue.push_front(key);
    }

    fn remove<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.remove(key);
    }

    fn evict_oldest(&mut self) -> Option<K> {
        while let Some(key) = self.queue.pop_back() {
            if self.map.contains(&key) {
                self.map.remove(&key);
                return Some(key);
            }
        }
        None
    }

    fn should_compact(&self) -> bool {
        self.queue.len() > self.map.len() * 2
    }

    fn compact(&mut self) {
        if self.should_compact() {
            let mut new_queue = VecDeque::with_capacity(self.map.len());

            for key in self.queue.iter().rev() {
                if self.map.contains(key) {
                    new_queue.push_front(key.clone());
                }
            }

            self.queue = new_queue;
        }
    }
}

struct ValueEntry<V> {
    value: V,
    freq: AtomicU8,
    size: usize,
}

impl<V> ValueEntry<V> {
    fn new(value: V, size: usize) -> Self {
        Self {
            value,
            freq: AtomicU8::new(0),
            size,
        }
    }
}

/// S3-FIFO cache: a cache that uses the S3-FIFO eviction strategy.
pub struct S3FiFoCache<K, V> {
    values: HashMap<K, ValueEntry<V>>,

    small_fifo: VecDeque<K>,
    main_fifo: VecDeque<K>,
    ghost: GhostList<K>,

    small_count: u64,
    small_size: usize,
    max_small_count: u64,
    max_small_size: usize,
    main_count: u64,
    main_size: usize,
    max_main_count: u64,
    max_main_size: usize,
    max_count: u64,
    max_size: usize,
}

impl<K: Clone + Eq + Hash, V: Cacheable> S3FiFoCache<K, V> {
    pub fn new(max_count: NonZeroU64, max_size: NonZeroUsize) -> Self {
        let max_count = max_count.get();
        let max_size = max_size.get();

        let max_small_count = std::cmp::max(1, max_count / 10);
        let max_small_size = std::cmp::max(1, max_size / 10);

        Self {
            values: HashMap::new(),
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
            self.main_count += 1;
            self.main_size += size;
        } else {
            while self.small_count >= self.max_small_count
                || self.small_size.saturating_add(size) > self.max_small_size
            {
                self.evict_s();
            }
            self.small_fifo.push_front(key.clone());
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
            // We leave tombstones in the FIFO queues; they'll be skipped during eviction.
            // Adjust the size counters. We need to check which FIFO the key is in.
            // Since we can't easily tell, we check small first, then main.
            // The eviction logic already handles missing keys (tombstones) gracefully.
            //
            // We need to track whether this was in small or main. Since we can't
            // know without scanning, we use a simple approach: try to find and remove
            // from small_fifo first, then main_fifo. The VecDeque scan is acceptable
            // because remove() is called infrequently (only on writes).
            let mut found_in_small = false;
            if let Some(pos) = self.small_fifo.iter().position(|k| k == key) {
                self.small_fifo.remove(pos);
                self.small_count -= 1;
                self.small_size -= entry.size;
                found_in_small = true;
            }

            if !found_in_small {
                if let Some(pos) = self.main_fifo.iter().position(|k| k == key) {
                    self.main_fifo.remove(pos);
                    self.main_count -= 1;
                    self.main_size -= entry.size;
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

    fn evict_s(&mut self) {
        while let Some(tail_key) = self.small_fifo.pop_back() {
            let Some(tail) = self.values.get(&tail_key) else {
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

                self.main_fifo.push_back(tail_key);
                self.main_count += 1;
                self.main_size += size;

                return;
            } else {
                let _ = self.values.remove(&tail_key).unwrap();
                self.ghost.insert(tail_key);
                return;
            }
        }
    }

    fn evict_m(&mut self) {
        while let Some(tail_key) = self.main_fifo.pop_back() {
            let Some(tail) = self.values.get(&tail_key) else {
                continue;
            };

            self.main_count -= 1;
            self.main_size -= tail.size;

            if tail.freq.load(Ordering::Acquire) > 0 {
                self.main_count += 1;
                self.main_size += tail.size;
                tail.freq.fetch_sub(1, Ordering::AcqRel);
                self.main_fifo.push_front(tail_key);
            } else {
                let _ = self.values.remove(&tail_key).unwrap();
                return;
            }
        }
    }
}

/// Async wrapper around S3FiFoCache for use with tokio.
pub struct AsyncS3Cache {
    inner: Mutex<S3FiFoCache<CacheKey, CachedObject>>,
    ttl: Duration,
    max_cacheable_size: usize,
}

impl AsyncS3Cache {
    pub fn new(
        max_count: NonZeroU64,
        max_size: NonZeroUsize,
        ttl: Duration,
        max_cacheable_size: usize,
    ) -> Self {
        Self {
            inner: Mutex::new(S3FiFoCache::new(max_count, max_size)),
            ttl,
            max_cacheable_size,
        }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn max_cacheable_size(&self) -> usize {
        self.max_cacheable_size
    }

    /// Attempt to get a cached object. Returns None if not found or expired.
    pub async fn get(&self, key: &CacheKey) -> Option<CachedObject> {
        let cache = self.inner.lock().await;
        let entry = cache.get(key)?;

        if entry.is_expired(self.ttl) {
            drop(cache);
            // Expired — remove it.
            self.remove(key).await;
            return None;
        }

        Some(entry.clone())
    }

    /// Insert a cached object. Returns true if inserted, false if too large or already present.
    pub async fn insert(&self, key: CacheKey, value: CachedObject) -> bool {
        if value.size() > self.max_cacheable_size {
            return false;
        }

        let mut cache = self.inner.lock().await;

        // Remove existing entry first to allow re-insert.
        cache.remove(&key);

        match cache.insert(key, value) {
            Ok(()) => true,
            Err(_) => false,
        }
    }

    /// Remove a specific cache entry.
    pub async fn remove(&self, key: &CacheKey) -> bool {
        let mut cache = self.inner.lock().await;
        cache.remove(key)
    }

    /// Remove all entries matching a bucket and object key (all ranges).
    pub async fn invalidate_object(&self, bucket: &str, object_key: &str) -> usize {
        let mut cache = self.inner.lock().await;
        let count = cache.remove_matching(|k| k.matches_object(bucket, object_key));
        if count > 0 {
            cache.compact();
        }
        count
    }

    /// Get current cache statistics and report them as metrics.
    pub async fn report_stats(&self) {
        let cache = self.inner.lock().await;
        let stats = cache.statistics();
        telemetry::record_cache_stats(stats.count, stats.size as u64);
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct TestData {
        size: usize,
    }

    impl TestData {
        fn new(size: usize) -> Self {
            Self { size }
        }
    }

    impl Cacheable for TestData {
        fn size(&self) -> usize {
            self.size
        }
    }

    #[test]
    fn test_basic_insertion_and_retrieval() {
        let mut cache: S3FiFoCache<String, TestData> = S3FiFoCache::new(
            NonZeroU64::new(100).unwrap(),
            NonZeroUsize::new(10000).unwrap(),
        );

        let key1 = "test_key_1".to_string();
        let data1 = TestData::new(500);

        assert!(cache.insert(key1.clone(), data1.clone()).is_ok());
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.size(), 500);

        let retrieved = cache.get(&key1);
        assert!(retrieved.is_some());
        assert_eq!(*retrieved.unwrap(), data1);
    }

    #[test]
    fn test_remove() {
        let mut cache: S3FiFoCache<String, TestData> = S3FiFoCache::new(
            NonZeroU64::new(100).unwrap(),
            NonZeroUsize::new(10000).unwrap(),
        );

        let key = "test_key".to_string();
        let data = TestData::new(500);

        assert!(cache.insert(key.clone(), data).is_ok());
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.size(), 500);

        assert!(cache.remove(&key));
        assert_eq!(cache.count(), 0);
        assert_eq!(cache.size(), 0);
        assert!(cache.get(&key).is_none());

        // Removing again returns false.
        assert!(!cache.remove(&key));
    }

    #[test]
    fn test_remove_matching() {
        let mut cache: S3FiFoCache<String, TestData> = S3FiFoCache::new(
            NonZeroU64::new(100).unwrap(),
            NonZeroUsize::new(10000).unwrap(),
        );

        for i in 0..5 {
            let key = format!("prefix_a_{i}");
            assert!(cache.insert(key, TestData::new(100)).is_ok());
        }
        for i in 0..3 {
            let key = format!("prefix_b_{i}");
            assert!(cache.insert(key, TestData::new(100)).is_ok());
        }

        assert_eq!(cache.count(), 8);

        let removed = cache.remove_matching(|k| k.starts_with("prefix_a_"));
        assert_eq!(removed, 5);
        assert_eq!(cache.count(), 3);
    }

    #[test]
    fn test_cache_eviction_by_count() {
        let mut cache: S3FiFoCache<String, TestData> = S3FiFoCache::new(
            NonZeroU64::new(100).unwrap(),
            NonZeroUsize::new(100000).unwrap(),
        );

        for i in 0..20 {
            let key = format!("key_{i}");
            let data = TestData::new(100);
            assert!(cache.insert(key, data).is_ok());
        }

        assert_eq!(cache.count(), 10);
    }

    #[test]
    fn test_value_too_big_error() {
        let mut cache: S3FiFoCache<String, TestData> = S3FiFoCache::new(
            NonZeroU64::new(100).unwrap(),
            NonZeroUsize::new(5000).unwrap(),
        );

        let key = "big_item".to_string();
        let big_data = TestData::new(6000);

        let result = cache.insert(key, big_data);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CacheError::ValueTooBig));
    }
}
