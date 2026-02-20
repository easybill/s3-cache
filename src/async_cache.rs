use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

use crate::S3FifoCache;
use crate::cache::{CacheKey, CachedObject};
use crate::telemetry;

/// Statistics about the cache.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct AsyncS3CacheStatistics {
    pub len: usize,
    pub max_len: usize,
    pub size: usize,
    pub max_size: usize,
}

struct AsyncS3CacheShard {
    cache: RwLock<S3FifoCache<CacheKey, CachedObject>>,
    /// Per-shard size tracked atomically so other shards can read it without locking.
    size: AtomicUsize,
}

impl AsyncS3CacheShard {
    fn new(max_len: usize) -> Self {
        Self {
            cache: RwLock::new(S3FifoCache::with_max_len(max_len)),
            size: AtomicUsize::new(0),
        }
    }
}

/// Async sharded wrapper around `S3FifoCache` for use with tokio.
///
/// Keys are distributed across shards by hashing, each guarded by its own
/// `RwLock`. All shards share a single global size budget tracked via atomics.
///
/// # Memory Estimates
///
/// Given a max length of `N` the cache requires memory of approximately:
///
/// ```plain
/// APPROX_MIN_BYTES = N * 6
/// APPROX_MAX_BYTES = N * (8 + sizeof(K) + sizeof(V))
/// ```
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

    #[inline]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    #[inline]
    pub async fn len(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            let cache = shard.cache.read().await;
            total += cache.len();
        }
        total
    }

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

    pub async fn contains_key(&self, key: &CacheKey) -> bool {
        let shard = self.shard_for(key);
        let cache = shard.cache.read().await;
        cache.contains_key(key)
    }

    /// Attempt to get a cached object.
    ///
    /// Returns None if not found or expired.
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

    /// Insert a cached object.
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

    /// Remove a specific cache entry.
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

    /// Remove all entries matching a bucket and object key (all ranges).
    ///
    /// This scans all shards since different ranges of the same object may
    /// hash to different shards.
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

    /// Get current cache statistics and report them as metrics.
    ///
    /// Uses time-based throttling to avoid overhead on hot path.
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
