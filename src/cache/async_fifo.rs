use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::sync::RwLock;

use crate::cache::{CacheKey, CachedObject};
use crate::telemetry;

use super::{Cacheable as _, S3FiFoCache};

/// Async wrapper around S3FiFoCache for use with tokio.
pub struct AsyncS3Cache {
    inner: RwLock<S3FiFoCache<CacheKey, CachedObject>>,
    ttl: Duration,
    max_cacheable_size: usize,
    last_stats_report: AtomicU64,
    stats_report_interval_secs: u64,
}

impl AsyncS3Cache {
    pub fn new(
        max_count: NonZeroU64,
        max_size: NonZeroUsize,
        ttl: Duration,
        max_cacheable_size: usize,
    ) -> Self {
        Self {
            inner: RwLock::new(S3FiFoCache::new(max_count, max_size)),
            ttl,
            max_cacheable_size,
            last_stats_report: AtomicU64::new(0),
            stats_report_interval_secs: 1,
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
        let cache = self.inner.read().await;
        let entry = cache.get(key)?;

        if entry.is_expired(self.ttl) {
            drop(cache);
            // Expired — remove it (requires write lock).
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

        let mut cache = self.inner.write().await;

        // Remove existing entry first to allow re-insert.
        cache.remove(&key);

        match cache.insert(key, value) {
            Ok(()) => true,
            Err(_) => false,
        }
    }

    /// Remove a specific cache entry.
    pub async fn remove(&self, key: &CacheKey) -> bool {
        let mut cache = self.inner.write().await;
        cache.remove(key)
    }

    /// Remove all entries matching a bucket and object key (all ranges).
    pub async fn invalidate_object(&self, bucket: &str, object_key: &str) -> usize {
        let mut cache = self.inner.write().await;
        let count = cache.remove_matching(|k| k.matches_object(bucket, object_key));
        if count > 0 {
            cache.compact();
        }
        count
    }

    /// Get current cache statistics and report them as metrics.
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
            let cache = self.inner.read().await;
            let stats = cache.statistics();
            telemetry::record_cache_stats(stats.count, stats.size as u64);
        }
    }
}
