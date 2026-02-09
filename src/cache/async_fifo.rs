use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use tokio::sync::Mutex;

use crate::cache::{CacheKey, CachedObject};
use crate::telemetry;

use super::{Cacheable as _, S3FiFoCache};

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
