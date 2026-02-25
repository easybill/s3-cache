use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

use crate::FifoCache;
use crate::telemetry;

pub use self::key::CacheKey;
pub use self::object::{CachedObject, CachedObjectBody};

mod key;
mod object;

/// Statistics snapshot for [`AsyncS3Cache`].
///
/// Provides information about cache utilization in terms of both entry count and byte size.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct AsyncS3CacheStatistics {
    pub len: usize,
    pub max_len: usize,
    pub size: usize,
    pub max_size: usize,
}

struct AsyncS3CacheShard {
    cache: RwLock<FifoCache<CacheKey, CachedObject>>,
    /// Per-shard size tracked atomically so other shards can read it without locking.
    size: AtomicUsize,
}

impl AsyncS3CacheShard {
    fn new(max_len: usize) -> Self {
        Self {
            cache: RwLock::new(FifoCache::with_max_len(max_len)),
            size: AtomicUsize::new(0),
        }
    }
}

/// Async sharded wrapper around `FifoCache` for use with tokio.
///
/// Keys are distributed across shards by hashing, each guarded by its own
/// `RwLock`. All shards share a single global size budget tracked via atomics.
///
/// # Memory Estimates
///
/// This cache uses `FifoCache<CacheKey, CachedObject>` internally, where `CacheKey`
/// wraps `Arc<CacheKeyState>` for efficient memory sharing.
///
/// ## Per-Entry Memory Breakdown
///
/// Given a max length of `N`, each entry requires approximately:
///
/// ```plain
/// APPROX_MIN_BYTES_PER_ENTRY =
///     16  (CacheKey Arc pointers: 2 copies × 8 bytes)
///   + 96  (CacheKeyState base struct)
///   + bucket.len() + key.len() + range.len() + version_id.len()
///   + 176 (CachedObject base size with metadata)
///   + content_length (actual body bytes)
///
/// APPROX_MAX_BYTES_PER_ENTRY =
///     32  (CacheKey Arc pointers: 4 copies × 8 bytes)
///   + 96  (CacheKeyState base struct, shared)
///   + bucket.len() + key.len() + range.len() + version_id.len()
///   + 176 (CachedObject base size with metadata)
///   + content_length (actual body bytes)
/// ```
///
/// All sizes are in bytes, rounded to the nearest full byte.
///
/// ## Arc-Based Memory Sharing
///
/// `CacheKey` stores an `Arc<CacheKeyState>` (8-byte pointer). When keys are duplicated
/// across internal queues (small, main, ghost), only the Arc pointer is copied—the
/// actual `CacheKeyState` (containing bucket, key, range, and version_id strings) is
/// stored once and shared among all clones.
///
/// Similarly, `CachedObject`'s `Bytes` body uses `Arc` internally (from the `bytes`
/// crate), enabling efficient sharing when objects are cloned.
///
/// ## Scenarios
///
/// - **Minimum:** Cache starting to fill. Each `CacheKey` appears 2 times (as Arc
///   pointers): once in the values HashMap and once in a queue. The actual
///   `CacheKeyState` data is stored once.
///
/// - **Maximum:** Cache at full capacity with active eviction. Each `CacheKey` appears
///   4 times (as Arc pointers): once in values, once in a queue, and twice in the ghost
///   list (HashSet + VecDeque). The actual `CacheKeyState` data is still stored once.
///
/// ## Examples
///
/// ```rust
/// // Example 1: Small objects with short keys
/// // bucket: "mybucket" (8 bytes), key: "file.txt" (8 bytes)
/// // range: None, version_id: None, body: 1KB
/// // MIN ≈ 16 + 96 + 16 + 176 + 1024 = 1,328 bytes per entry
/// // MAX ≈ 32 + 96 + 16 + 176 + 1024 = 1,344 bytes per entry
///
/// // Example 2: Large objects (body dominates)
/// // bucket: "prod" (4 bytes), key: "data/object.bin" (15 bytes)
/// // range: "bytes=0-10485759" (16 bytes), version_id: None
/// // body: 10MB (10,485,760 bytes)
/// // MIN ≈ 16 + 96 + 35 + 176 + 10,485,760 = ~10MB per entry
/// // MAX ≈ 32 + 96 + 35 + 176 + 10,485,760 = ~10MB per entry
/// // Note: For large objects, the 16-byte Arc pointer difference is negligible
/// ```
///
/// ## Capacity Planning
///
/// - **Small objects (<10KB):** Metadata and key duplication dominate memory usage
/// - **Large objects (>1MB):** Body size dominates; key duplication overhead is negligible
/// - Use `max_size` to control total memory by content length
/// - Use `max_len` to control entry count
/// - Practical memory budget: `average_object_size × max_len × 1.5`
///
/// ## Additional Overhead
///
/// Beyond per-entry costs, the cache includes:
/// - Sharding overhead: `num_shards × (RwLock + AtomicUsize)` (~48 bytes per shard)
/// - Global tracking: `AtomicU64 + AtomicUsize + Duration + usize` (~32 bytes total)
/// - See [`FifoCache`](crate::FifoCache) documentation for detailed formula explanation
///
/// Note: The `S3` in `AsyncS3Cache` refers to Amazon's `S3` web service.
pub struct AsyncS3Cache {
    shards: Vec<AsyncS3CacheShard>,
    // Time-to-live
    ttl: Duration,
    last_stats_report: AtomicU64,
    stats_report_interval_secs: u64,
    /// Maximum allowed total number of bytes in the cache (global across all shards).
    max_size: usize,
    /// Global size counter — sum of all per-shard sizes.
    global_size: AtomicUsize,
}

impl AsyncS3Cache {
    /// Creates a new async S3 cache with the specified configuration.
    ///
    /// The cache distributes entries across `num_shards` shards for concurrent access.
    /// Each shard has its own lock, allowing parallel operations on different keys.
    ///
    /// ```
    /// use s3_cache::AsyncS3Cache;
    /// use std::time::Duration;
    ///
    /// let cache = AsyncS3Cache::new(
    ///     10_000,                    // max 10,000 entries
    ///     1_073_741_824,             // max 1 GB total size
    ///     Duration::from_secs(3600), // 1 hour TTL
    ///     16,                        // 16 shards
    /// );
    /// ```
    pub fn new(max_len: usize, max_size: usize, ttl: Duration, num_shards: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be greater than 0");

        let per_shard_max_len = max_len / num_shards;
        let remainder = max_len % num_shards;

        let shards: Vec<_> = (0..num_shards)
            .map(|i| {
                // Distribute remainder across first `remainder` shards.
                let shard_max_len = per_shard_max_len + if i < remainder { 1 } else { 0 };
                AsyncS3CacheShard::new(shard_max_len)
            })
            .collect();

        Self {
            shards,
            ttl,
            last_stats_report: AtomicU64::new(0),
            stats_report_interval_secs: 1,
            max_size,
            global_size: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn shard_index(&self, key: &CacheKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.shards.len()
    }

    #[inline]
    fn shard_for(&self, key: &CacheKey) -> &AsyncS3CacheShard {
        &self.shards[self.shard_index(key)]
    }

    /// Returns the time-to-live duration for cached entries.
    #[inline]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Returns the current total number of entries across all shards.
    ///
    /// This operation acquires read locks on all shards sequentially.
    #[inline]
    pub async fn len(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            let cache = shard.cache.read().await;
            total += cache.len();
        }
        total
    }

    /// Returns `true` if all shards are empty.
    ///
    /// This operation acquires read locks on all shards sequentially.
    #[inline]
    pub async fn is_empty(&self) -> bool {
        for shard in &self.shards {
            let cache = shard.cache.read().await;
            if !cache.is_empty() {
                return false;
            }
        }
        true
    }

    /// Returns `true` if all shards are at maximum capacity.
    ///
    /// This operation acquires read locks on all shards sequentially.
    #[inline]
    pub async fn is_full(&self) -> bool {
        for shard in &self.shards {
            let cache = shard.cache.read().await;
            if !cache.is_full() {
                return false;
            }
        }
        true
    }

    /// Returns `true` if the cache contains the specified key.
    ///
    /// Unlike [`get`](Self::get), this does not check for expiration.
    pub async fn contains_key(&self, key: &CacheKey) -> bool {
        let shard = self.shard_for(key);
        let cache = shard.cache.read().await;
        cache.contains_key(key)
    }

    /// Gets a cached object if present and not expired.
    ///
    /// Expired entries are automatically removed when accessed.
    ///
    /// ```no_run
    /// # use s3_cache::{AsyncS3Cache, CacheKey};
    /// # use std::time::Duration;
    /// # async fn example() {
    /// # let cache = AsyncS3Cache::new(100, 1024, Duration::from_secs(3600), 4);
    /// let key = CacheKey::new(
    ///     "bucket".to_string(),
    ///     "key".to_string(),
    ///     None,
    ///     None,
    /// );
    /// if let Some(obj) = cache.get(&key).await {
    ///     // Use cached object
    /// }
    /// # }
    /// ```
    pub async fn get(&self, key: &CacheKey) -> Option<CachedObject> {
        let shard = self.shard_for(key);

        {
            let cache = shard.cache.read().await;
            let entry = cache.get(key)?;

            if !entry.is_expired(self.ttl) {
                return Some(entry.clone());
            }
        }

        // Read lock dropped — safe to acquire write lock for removal.
        self.remove(key).await;
        None
    }

    /// Inserts a cached object.
    ///
    /// If the object doesn't fit within size limits after eviction attempts,
    /// the insertion is skipped and `None` is returned.
    ///
    /// Evicts from the target shard first, then from other shards if needed to
    /// satisfy the global size constraint.
    ///
    /// Returns the previous value if the key already existed.
    pub async fn insert(&self, key: CacheKey, value: CachedObject) -> Option<CachedObject> {
        let size = value.content_length();
        let shard_idx = self.shard_index(&key);
        let shard = &self.shards[shard_idx];

        let mut cache = shard.cache.write().await;

        // Evict from own shard first.
        while self.global_size.load(Ordering::Relaxed) + size > self.max_size {
            let Some((_evicted_key, evicted_value)) = cache.evict() else {
                break;
            };

            let evicted_size = evicted_value.content_length();
            shard.size.fetch_sub(evicted_size, Ordering::Relaxed);
            self.global_size.fetch_sub(evicted_size, Ordering::Relaxed);
        }

        // Drop own shard's write lock before touching other shards.
        // We'll re-acquire it after cross-shard eviction if needed.
        let needs_cross_shard = self.global_size.load(Ordering::Relaxed) + size > self.max_size;

        if needs_cross_shard {
            drop(cache);
            self.evict_from_other_shards(shard_idx, size).await;
            cache = shard.cache.write().await;
        }

        // After eviction, if the item still doesn't fit, skip insertion.
        if self.global_size.load(Ordering::Relaxed) + size > self.max_size {
            return None;
        }

        let existing = cache.insert(key, value);

        shard.size.fetch_add(size, Ordering::Relaxed);
        self.global_size.fetch_add(size, Ordering::Relaxed);

        if let Some(existing) = &existing {
            let existing_size = existing.content_length();
            shard.size.fetch_sub(existing_size, Ordering::Relaxed);
            self.global_size.fetch_sub(existing_size, Ordering::Relaxed);
        }

        existing
    }

    /// Evict items from other shards until there's enough room or all shards are exhausted.
    async fn evict_from_other_shards(&self, skip_shard_idx: usize, needed: usize) {
        while self.global_size.load(Ordering::Relaxed) + needed > self.max_size {
            // Find the shard with the largest size (excluding the caller's shard).
            let target_idx = self
                .shards
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != skip_shard_idx)
                .max_by_key(|(_, s)| s.size.load(Ordering::Relaxed))
                .map(|(i, _)| i);

            let Some(target_idx) = target_idx else {
                break;
            };

            let target = &self.shards[target_idx];

            // Skip shards that are already empty (by size).
            if target.size.load(Ordering::Relaxed) == 0 {
                break;
            }

            let mut target_cache = target.cache.write().await;
            let Some((_evicted_key, evicted_value)) = target_cache.evict() else {
                break;
            };

            let evicted_size = evicted_value.content_length();
            target.size.fetch_sub(evicted_size, Ordering::Relaxed);
            self.global_size.fetch_sub(evicted_size, Ordering::Relaxed);
        }
    }

    /// Removes a cache entry, returning its value if present.
    pub async fn remove(&self, key: &CacheKey) -> Option<CachedObject> {
        let shard = self.shard_for(key);
        let mut cache = shard.cache.write().await;

        let removed = cache.remove(key);

        if let Some(removed) = &removed {
            let size = removed.content_length();
            shard.size.fetch_sub(size, Ordering::Relaxed);
            self.global_size.fetch_sub(size, Ordering::Relaxed);
        }

        removed
    }

    /// Removes all cache entries for a given bucket and object key.
    ///
    /// This invalidates all cached ranges and versions of the object by scanning
    /// all shards, since different ranges may hash to different shards.
    ///
    /// Returns the number of entries removed.
    pub async fn invalidate_object(&self, bucket: &str, object_key: &str) -> usize {
        let mut total_count: usize = 0;

        for shard in &self.shards {
            let mut cache = shard.cache.write().await;

            let mut count: usize = 0;
            let mut size: usize = 0;

            cache.retain(|key, value| {
                let is_match = key.matches_object(bucket, object_key);

                if is_match {
                    size += value.content_length();
                    count += 1;
                }

                !is_match
            });

            if count > 0 {
                cache.compact();
                shard.size.fetch_sub(size, Ordering::Relaxed);
                self.global_size.fetch_sub(size, Ordering::Relaxed);
                total_count += count;
            }
        }

        total_count
    }

    /// Reports current cache statistics to telemetry.
    ///
    /// Uses time-based throttling (1 second interval) to avoid overhead on hot paths.
    /// Multiple concurrent calls within the throttle window will be no-ops.
    pub async fn report_stats(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let last = self.last_stats_report.load(Ordering::Relaxed);
        if now - last < self.stats_report_interval_secs {
            // Too soon since last report, skip
            return;
        }

        // Try to update the timestamp (race is okay, we just want throttling)
        if self
            .last_stats_report
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            let mut total_len = 0;
            let mut total_max_len = 0;
            for shard in &self.shards {
                let cache = shard.cache.read().await;
                total_len += cache.len();
                total_max_len += cache.max_len();
            }

            let stats = AsyncS3CacheStatistics {
                len: total_len,
                max_len: total_max_len,
                size: self.global_size.load(Ordering::Relaxed),
                max_size: self.max_size,
            };

            telemetry::record_cache_stats(stats.len, stats.size);
        }
    }
}

#[cfg(test)]
mod tests;
