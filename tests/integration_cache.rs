mod common;

use bytes::Bytes;
use common::MockS3Backend;
use common::helpers::*;
use s3_cache::CacheKey;
use s3_cache::proxy_service::CachingProxy;
use s3s::S3;
use std::sync::Arc;
use std::usize;

#[tokio::test]
async fn get_object_cache_miss_then_hit() {
    // Setup: MockS3Backend with test data
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "key.txt", b"test content")
        .await;

    // Setup: Cache + Proxy
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

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
#[cfg_attr(not(feature = "mock-clock"), ignore = "requires mock-clock feature")]
async fn cache_ttl_expiration() {
    #[cfg(feature = "mock-clock")]
    mock_instant::global::MockClock::set_time(std::time::Duration::ZERO);

    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "expiring.txt", b"original")
        .await;

    // Cache with 60 second TTL
    let cache = create_test_cache(100, usize::MAX, 60);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // First request: populate cache
    let req = build_get_request("test-bucket", "expiring.txt", None);
    proxy.get_object(req).await.unwrap();
    assert_eq!(backend.get_request_count().await, 1);
    assert_cache_contains(&cache, "test-bucket", "expiring.txt").await;

    // Advance mock clock past TTL
    #[cfg(feature = "mock-clock")]
    mock_instant::global::MockClock::advance(std::time::Duration::from_secs(61));

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
async fn cache_size_eviction() {
    let backend = MockS3Backend::new();

    // Populate backend with multiple objects
    for i in 0..10 {
        backend
            .put_object_sync("test-bucket", &format!("file{}.txt", i), b"data")
            .await;
    }

    // Cache with room for only 5 entries
    let cache = create_test_cache(5, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // Fetch all 10 objects
    for i in 0..10 {
        let req = build_get_request("test-bucket", &format!("file{}.txt", i), None);
        proxy.get_object(req).await.unwrap();
    }

    assert_eq!(backend.get_request_count().await, 10);

    // Count cached entries - S3-FIFO should evict some but keep at most 5
    let mut cached_count = 0;
    for i in 0..10 {
        let cache_key = s3_cache::CacheKey::new(
            "test-bucket".to_string(),
            format!("file{}.txt", i),
            None,
            None,
        );
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
async fn cache_object_count_limit() {
    let backend = MockS3Backend::new();

    // Populate backend
    for i in 0..20 {
        backend
            .put_object_sync("test-bucket", &format!("obj{}.txt", i), b"content")
            .await;
    }

    // Cache limited to 10 entries
    let cache = create_test_cache(10, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // Fetch 15 objects
    for i in 0..15 {
        let req = build_get_request("test-bucket", &format!("obj{}.txt", i), None);
        proxy.get_object(req).await.unwrap();
    }

    // With S3-FIFO algorithm, at least some entries should be evicted
    // Count how many entries are still in cache
    let mut cached_count = 0;
    for i in 0..15 {
        let cache_key = s3_cache::CacheKey::new(
            "test-bucket".to_string(),
            format!("obj{}.txt", i),
            None,
            None,
        );
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
async fn oversized_object_not_cached() {
    let backend = MockS3Backend::new();

    // Large object (1MB)
    let large_data = vec![b'x'; 1_000_000];
    backend
        .put_object_sync("test-bucket", "large.bin", &large_data)
        .await;

    // Cache with max size 100KB
    let cache = create_test_cache(100, 100_000, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // First request: object too large, streams through without caching
    let req = build_get_request("test-bucket", "large.bin", None);
    let result = proxy.get_object(req).await;
    assert!(
        result.is_ok(),
        "Oversized object should stream through successfully"
    );
    assert_eq!(backend.get_request_count().await, 1);

    // Verify not cached
    assert_cache_missing(&cache, "test-bucket", "large.bin").await;

    // Second request should also hit backend (not cached)
    let req = build_get_request("test-bucket", "large.bin", None);
    proxy.get_object(req).await.unwrap();
    assert_eq!(backend.get_request_count().await, 2);
}

#[tokio::test]
async fn concurrent_cache_access() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "concurrent.txt", b"data")
        .await;

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = Arc::new(CachingProxy::new(
        backend.clone(),
        Some(cache.clone()),
        usize::MAX,
        false,
    ));

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
async fn different_buckets_separate_cache() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("bucket-a", "key.txt", b"data-a")
        .await;
    backend
        .put_object_sync("bucket-b", "key.txt", b"data-b")
        .await;

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

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

#[tokio::test]
async fn cache_byte_size_eviction() {
    let backend = MockS3Backend::new();

    // Each object is 500 bytes
    let data = vec![b'a'; 500];
    for i in 0..10 {
        backend
            .put_object_sync("test-bucket", &format!("sized{}.bin", i), &data)
            .await;
    }

    // Cache with max_size of 2000 bytes (room for ~4 objects of 500 bytes)
    let cache = create_test_cache(100, 2000, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // Fetch all 10 objects
    for i in 0..10 {
        let req = build_get_request("test-bucket", &format!("sized{}.bin", i), None);
        proxy.get_object(req).await.unwrap();
    }

    // Count cached entries — at most 4 should fit (2000 / 500)
    let mut cached_count = 0;
    for i in 0..10 {
        let key = CacheKey::new(
            "test-bucket".to_string(),
            format!("sized{}.bin", i),
            None,
            None,
        );
        if cache.get(&key).await.is_some() {
            cached_count += 1;
        }
    }

    assert!(
        cached_count <= 4,
        "Expected at most 4 cached entries (2000B / 500B), found {}",
        cached_count
    );
    assert!(cached_count > 0, "Expected some entries to be cached");
}

#[tokio::test]
async fn backend_error_not_cached() {
    let backend = MockS3Backend::new();
    // Don't add the object — backend will return NoSuchKey

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // Request non-existent object
    let req = build_get_request("test-bucket", "missing.txt", None);
    let result = proxy.get_object(req).await;
    assert!(result.is_err(), "Should propagate backend error");

    // Nothing should be cached
    assert_cache_missing(&cache, "test-bucket", "missing.txt").await;
    assert!(cache.is_empty().await);
}

#[tokio::test]
async fn max_cacheable_size_rejects_large_objects() {
    let backend = MockS3Backend::new();

    let small_data = vec![b's'; 100];
    let large_data = vec![b'l'; 5000];
    backend
        .put_object_sync("test-bucket", "small.bin", &small_data)
        .await;
    backend
        .put_object_sync("test-bucket", "large.bin", &large_data)
        .await;

    // Proxy with max_cacheable_size = 1000 (rejects objects > 1KB)
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), 1000, false);

    // Small object: should be cached
    let req = build_get_request("test-bucket", "small.bin", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body.len(), 100);
    assert_cache_contains(&cache, "test-bucket", "small.bin").await;

    // Large object: should stream through without caching
    let req = build_get_request("test-bucket", "large.bin", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body.len(), 5000);
    assert_cache_missing(&cache, "test-bucket", "large.bin").await;

    // Second request for large object hits backend again
    let req = build_get_request("test-bucket", "large.bin", None);
    proxy.get_object(req).await.unwrap();
    assert_eq!(backend.get_request_count().await, 3); // small + large + large
}

#[tokio::test]
async fn cache_hit_preserves_metadata() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "meta.txt", b"hello")
        .await;

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // First request: cache miss
    let req = build_get_request("test-bucket", "meta.txt", None);
    let miss_resp = proxy.get_object(req).await.unwrap();
    let miss_body = extract_body(miss_resp.output.body).await;
    let miss_content_length = miss_resp.output.content_length;

    // Second request: cache hit
    let req = build_get_request("test-bucket", "meta.txt", None);
    let hit_resp = proxy.get_object(req).await.unwrap();
    let hit_body = extract_body(hit_resp.output.body).await;
    let hit_content_length = hit_resp.output.content_length;

    // Body and metadata should match between miss and hit responses
    assert_eq!(miss_body, hit_body);
    assert_eq!(miss_content_length, hit_content_length);
    assert_eq!(hit_content_length, Some(5));
    assert_eq!(backend.get_request_count().await, 1); // Only one backend call
}

#[tokio::test]
async fn head_object_does_not_populate_cache() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "head-test.txt", b"data")
        .await;

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // HEAD request — should be delegated, not cached
    let req = s3s::S3Request {
        input: s3s::dto::HeadObjectInput {
            bucket: "test-bucket".to_string(),
            key: "head-test.txt".to_string(),
            ..Default::default()
        },
        method: http::Method::HEAD,
        uri: http::Uri::from_static("/"),
        headers: http::HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    let resp = proxy.head_object(req).await.unwrap();
    assert_eq!(resp.output.content_length, Some(4));

    // Cache should still be empty
    assert!(cache.is_empty().await);
}

#[tokio::test]
async fn put_then_get_sees_new_content() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "mutable.txt", b"version1")
        .await;

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // GET: caches "version1"
    let req = build_get_request("test-bucket", "mutable.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    assert_eq!(
        extract_body(resp.output.body).await,
        Bytes::from("version1")
    );

    // PUT: updates to "version2" and invalidates cache
    let req = build_put_request("test-bucket", "mutable.txt", Bytes::from("version2"));
    proxy.put_object(req).await.unwrap();

    // GET: should fetch fresh "version2" from backend
    let req = build_get_request("test-bucket", "mutable.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    assert_eq!(
        extract_body(resp.output.body).await,
        Bytes::from("version2")
    );
    assert_eq!(backend.get_request_count().await, 2);
}
