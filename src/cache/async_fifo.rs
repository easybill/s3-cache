use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

use crate::cache::{CacheKey, CachedObject};
use crate::telemetry;

use super::S3FiFoCache;

/// Statistics about the cache.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct AsyncS3CacheStatistics {
    pub len: usize,
    pub max_len: usize,
    pub size: usize,
    pub max_size: usize,
}

struct AsyncS3CacheState {
    cache: S3FiFoCache<CacheKey, CachedObject>,
    /// Current total number of bytes in the cache.
    size: usize,
}

/// Async wrapper around S3FiFoCache for use with tokio.
pub struct AsyncS3Cache {
    state: RwLock<AsyncS3CacheState>,
    // Time-to-live
    ttl: Duration,
    last_stats_report: AtomicU64,
    stats_report_interval_secs: u64,
    /// Maximum allowed total number of bytes in the cache.
    max_size: usize,
}

impl AsyncS3Cache {
    pub fn new(max_len: usize, max_size: usize, ttl: Duration) -> Self {
        let cache = S3FiFoCache::with_max_len(max_len);
        let state = RwLock::new(AsyncS3CacheState { cache, size: 0 });
        let last_stats_report = AtomicU64::new(0);

        Self {
            state,
            ttl,
            last_stats_report,
            stats_report_interval_secs: 1,
            max_size,
        }
    }

    #[inline]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    #[inline]
    pub async fn len(&self) -> usize {
        let state = self.state.read().await;
        let cache = &state.cache;

        cache.len()
    }

    #[inline]
    pub async fn is_empty(&self) -> bool {
        let state = self.state.read().await;
        let cache = &state.cache;

        cache.is_empty()
    }

    #[inline]
    pub async fn is_full(&self) -> bool {
        let state = self.state.read().await;
        let cache = &state.cache;

        cache.is_full()
    }

    pub async fn contains_key<Q>(&self, key: &Q) -> bool
    where
        CacheKey: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let state = self.state.read().await;
        let cache = &state.cache;

        cache.contains_key(key)
    }

    /// Attempt to get a cached object.
    ///
    /// Returns None if not found or expired.
    pub async fn get(&self, key: &CacheKey) -> Option<CachedObject> {
        let expired;

        {
            let state = self.state.read().await;
            let entry = state.cache.get(key)?;
            expired = entry.is_expired(self.ttl);

            if !expired {
                return Some(entry.clone());
            }
        }

        // Read lock dropped — safe to acquire write lock for removal.
        self.remove(key).await;
        None
    }

    /// Insert a cached object.
    pub async fn insert(&self, key: CacheKey, value: CachedObject) -> Option<CachedObject> {
        let mut state = self.state.write().await;

        let size = value.size();

        while state.size + size > self.max_size {
            let Some((_evicted_key, evicted_value)) = state.cache.evict() else {
                break;
            };

            state.size -= evicted_value.size();
        }

        // After eviction, if the item still doesn't fit, skip insertion.
        if state.size + size > self.max_size {
            return None;
        }

        let existing = state.cache.insert(key, value);
        state.size += size;

        if let Some(existing) = &existing {
            state.size -= existing.size();
        }

        existing
    }

    /// Remove a specific cache entry.
    pub async fn remove(&self, key: &CacheKey) -> Option<CachedObject> {
        let mut state = self.state.write().await;
        let cache = &mut state.cache;

        let removed = cache.remove(key);

        if let Some(removed) = &removed {
            state.size -= removed.size();
        }

        removed
    }

    /// Remove all entries matching a bucket and object key (all ranges).
    pub async fn invalidate_object(&self, bucket: &str, object_key: &str) -> usize {
        let mut state = self.state.write().await;
        let cache = &mut state.cache;

        let mut count: usize = 0;
        let mut size: usize = 0;

        cache.retain(|key, value| {
            let is_match = key.matches_object(bucket, object_key);

            if is_match {
                size += value.size();
                count += 1;
            }

            !is_match
        });

        if count > 0 {
            cache.compact();
            state.size -= size;
        }

        count
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
            // We won the race, report stats
            let state = self.state.read().await;
            let cache = &state.cache;

            let stats = AsyncS3CacheStatistics {
                len: cache.len(),
                max_len: cache.max_len(),
                size: state.size,
                max_size: self.max_size,
            };

            telemetry::record_cache_stats(stats.len, stats.size);
        }
    }
}
