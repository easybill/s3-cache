mod common;

use bytes::Bytes;
use common::MockS3Backend;
use common::helpers::*;
use mock_instant::global::MockClock;
use minio_cache::proxy_service::CachingProxy;
use s3s::S3;
use std::time::Duration;

#[tokio::test]
async fn test_get_object_cache_miss_then_hit() {
    // Setup: MockS3Backend with test data
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "key.txt", b"test content")
        .await;

    // Setup: Cache + Proxy
    let cache = create_test_cache(100, 10_000_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone());

    // First request: cache miss
    let req = build_get_request("test-bucket", "key.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("test content"));
    assert_eq!(backend.get_request_count().await, 1);

    // Verify cached
    assert_cache_contains(&cache, "test-bucket", "key.txt").await;

    // Second request: cache hit (backend not called again)
    let req = build_get_request("test-bucket", "key.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("test content"));
    assert_eq!(backend.get_request_count().await, 1); // Still 1
}

#[tokio::test]
async fn test_cache_ttl_expiration() {
    MockClock::set_time(Duration::ZERO);

    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "expiring.txt", b"original")
        .await;

    // Cache with 60 second TTL
    let cache = create_test_cache(100, 10_000_000, 60);
    let proxy = CachingProxy::new(backend.clone(), cache.clone());

    // First request: populate cache
    let req = build_get_request("test-bucket", "expiring.txt", None);
    proxy.get_object(req).await.unwrap();
    assert_eq!(backend.get_request_count().await, 1);
    assert_cache_contains(&cache, "test-bucket", "expiring.txt").await;

    // Advance mock clock past TTL
    MockClock::advance(Duration::from_secs(61));

    // Update backend data
    backend
        .put_object_sync("test-bucket", "expiring.txt", b"updated")
        .await;

    // Request again: cache expired, should fetch fresh data
    let req = build_get_request("test-bucket", "expiring.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("updated"));
    assert_eq!(backend.get_request_count().await, 2); // New request made
}

#[tokio::test]
async fn test_cache_size_eviction() {
    let backend = MockS3Backend::new();

    // Populate backend with multiple objects
    for i in 0..10 {
        backend
            .put_object_sync("test-bucket", &format!("file{}.txt", i), b"data")
            .await;
    }

    // Cache with room for only 5 entries
    let cache = create_test_cache(5, 10_000_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone());

    // Fetch all 10 objects
    for i in 0..10 {
        let req = build_get_request("test-bucket", &format!("file{}.txt", i), None);
        proxy.get_object(req).await.unwrap();
    }

    assert_eq!(backend.get_request_count().await, 10);

    // Count cached entries - S3-FIFO should evict some but keep at most 5
    let mut cached_count = 0;
    for i in 0..10 {
        let cache_key =
            minio_cache::CacheKey::new("test-bucket".to_string(), format!("file{}.txt", i), None, None);
        if cache.get(&cache_key).await.is_some() {
            cached_count += 1;
        }
    }

    // Should respect the limit of 5 entries
    assert!(
        cached_count <= 5,
        "Expected at most 5 cached entries, found {}",
        cached_count
    );

    // Requesting file0 which may or may not be cached
    let req = build_get_request("test-bucket", "file0.txt", None);
    proxy.get_object(req).await.unwrap();
    // If it was evicted, backend count increases to 11
    assert!(backend.get_request_count().await >= 10 && backend.get_request_count().await <= 11);
}

#[tokio::test]
async fn test_cache_entry_count_limit() {
    let backend = MockS3Backend::new();

    // Populate backend
    for i in 0..20 {
        backend
            .put_object_sync("test-bucket", &format!("obj{}.txt", i), b"content")
            .await;
    }

    // Cache limited to 10 entries
    let cache = create_test_cache(10, 10_000_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone());

    // Fetch 15 objects
    for i in 0..15 {
        let req = build_get_request("test-bucket", &format!("obj{}.txt", i), None);
        proxy.get_object(req).await.unwrap();
    }

    // With S3-FIFO algorithm, at least some entries should be evicted
    // Count how many entries are still in cache
    let mut cached_count = 0;
    for i in 0..15 {
        let cache_key =
            minio_cache::CacheKey::new("test-bucket".to_string(), format!("obj{}.txt", i), None, None);
        if cache.get(&cache_key).await.is_some() {
            cached_count += 1;
        }
    }

    // Should have at most 10 entries (the limit)
    assert!(
        cached_count <= 10,
        "Expected at most 10 cached entries, found {}",
        cached_count
    );
    // Should have some entries cached
    assert!(cached_count > 0, "Expected some entries to be cached");
}

#[tokio::test]
async fn test_oversized_object_not_cached() {
    let backend = MockS3Backend::new();

    // Large object (1MB)
    let large_data = vec![b'x'; 1_000_000];
    backend
        .put_object_sync("test-bucket", "large.bin", &large_data)
        .await;

    // Cache with max size 100KB
    let cache = create_test_cache(100, 100_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone());

    // First request: object too large, streams through without caching
    let req = build_get_request("test-bucket", "large.bin", None);
    let result = proxy.get_object(req).await;
    assert!(result.is_ok(), "Oversized object should stream through successfully");
    assert_eq!(backend.get_request_count().await, 1);

    // Verify not cached
    assert_cache_missing(&cache, "test-bucket", "large.bin").await;

    // Second request should also hit backend (not cached)
    let req = build_get_request("test-bucket", "large.bin", None);
    proxy.get_object(req).await.unwrap();
    assert_eq!(backend.get_request_count().await, 2);
}

#[tokio::test]
async fn test_concurrent_cache_access() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "concurrent.txt", b"data")
        .await;

    let cache = create_test_cache(100, 10_000_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone());

    // Spawn multiple concurrent requests
    let mut handles = vec![];
    for _ in 0..10 {
        let proxy_clone = proxy.clone();
        let handle = tokio::spawn(async move {
            let req = build_get_request("test-bucket", "concurrent.txt", None);
            proxy_clone.get_object(req).await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Backend should be called at least once, but not necessarily 10 times
    // (some requests may hit cache if first request completes first)
    let count = backend.get_request_count().await;
    assert!(count >= 1 && count <= 10);

    // Object should be cached
    assert_cache_contains(&cache, "test-bucket", "concurrent.txt").await;
}

#[tokio::test]
async fn test_different_buckets_separate_cache() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("bucket-a", "key.txt", b"data-a")
        .await;
    backend
        .put_object_sync("bucket-b", "key.txt", b"data-b")
        .await;

    let cache = create_test_cache(100, 10_000_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone());

    // Fetch from both buckets
    let req = build_get_request("bucket-a", "key.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body_a = extract_body(resp.output.body).await;

    let req = build_get_request("bucket-b", "key.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body_b = extract_body(resp.output.body).await;

    assert_eq!(body_a, Bytes::from("data-a"));
    assert_eq!(body_b, Bytes::from("data-b"));
    assert_eq!(backend.get_request_count().await, 2);

    // Both should be cached separately
    assert_cache_contains(&cache, "bucket-a", "key.txt").await;
    assert_cache_contains(&cache, "bucket-b", "key.txt").await;
}
