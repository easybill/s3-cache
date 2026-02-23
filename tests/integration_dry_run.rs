mod common;

use bytes::Bytes;
use common::MockS3Backend;
use common::helpers::*;
use s3_cache::proxy_service::CachingProxy;
use s3s::S3;
use std::sync::Arc;

#[tokio::test]
async fn cache_miss_populates_with_hash() {
    // Setup: MockS3Backend with test data
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "key.txt", b"test content")
        .await;

    // Setup: Cache + Proxy in dry-run mode
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, true);

    // First request in dry-run mode: cache miss
    let req = build_get_request("test-bucket", "key.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("test content"));
    assert_eq!(backend.get_request_count().await, 1);

    // Verify object was cached (even in dry-run mode)
    assert_cache_contains(&cache, "test-bucket", "key.txt").await;
}

#[tokio::test]
async fn always_fetches_from_upstream() {
    // Setup: MockS3Backend with test data
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "key.txt", b"test content")
        .await;

    // Setup: Cache + Proxy in dry-run mode
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, true);

    // First request: cache miss
    let req = build_get_request("test-bucket", "key.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("test content"));
    assert_eq!(backend.get_request_count().await, 1);

    // Second request: even though cache hit, should still fetch from upstream in dry-run mode
    let req = build_get_request("test-bucket", "key.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("test content"));
    // Backend should be called again (dry-run always fetches fresh)
    assert_eq!(backend.get_request_count().await, 2);
}

#[tokio::test]
async fn returns_fresh_data() {
    // Setup: MockS3Backend with initial data
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "mutable.txt", b"version1")
        .await;

    // Setup: Cache + Proxy in dry-run mode
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, true);

    // First request: populates cache with "version1"
    let req = build_get_request("test-bucket", "mutable.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    assert_eq!(
        extract_body(resp.output.body).await,
        Bytes::from("version1")
    );

    // Update backend data directly (simulating upstream change)
    backend
        .put_object_sync("test-bucket", "mutable.txt", b"version2")
        .await;

    // Second request in dry-run mode: should return fresh "version2" from backend
    // even though cache has "version1"
    let req = build_get_request("test-bucket", "mutable.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    assert_eq!(
        extract_body(resp.output.body).await,
        Bytes::from("version2")
    );
    assert_eq!(backend.get_request_count().await, 2);
}

#[tokio::test]
async fn with_matching_cache_data() {
    // Setup: MockS3Backend with test data
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "stable.txt", b"stable content")
        .await;

    // Setup: Cache + Proxy in dry-run mode
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, true);

    // First request: populates cache
    let req = build_get_request("test-bucket", "stable.txt", None);
    proxy.get_object(req).await.unwrap();

    // Second request: cache hit with matching data (should not trigger mismatch)
    let req = build_get_request("test-bucket", "stable.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("stable content"));
    // Should not crash and should return correct data
    assert_eq!(backend.get_request_count().await, 2);
}

#[tokio::test]
async fn with_mismatched_cache_data() {
    // Setup: MockS3Backend with test data
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "changing.txt", b"original")
        .await;

    // Setup: Cache + Proxy in dry-run mode
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, true);

    // First request: populates cache with "original"
    let req = build_get_request("test-bucket", "changing.txt", None);
    proxy.get_object(req).await.unwrap();

    // Update backend data
    backend
        .put_object_sync("test-bucket", "changing.txt", b"modified")
        .await;

    // Second request: cache hit but data differs
    // Should detect mismatch, log warning, but not crash
    let req = build_get_request("test-bucket", "changing.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    // Should return fresh data from backend
    assert_eq!(body, Bytes::from("modified"));
    assert_eq!(backend.get_request_count().await, 2);
}

#[tokio::test]
async fn multiple_objects() {
    // Setup: MockS3Backend with multiple objects
    let backend = MockS3Backend::new();
    for i in 0..5 {
        backend
            .put_object_sync("test-bucket", &format!("file{}.txt", i), b"data")
            .await;
    }

    // Setup: Cache + Proxy in dry-run mode
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, true);

    // Fetch all objects
    for i in 0..5 {
        let req = build_get_request("test-bucket", &format!("file{}.txt", i), None);
        let resp = proxy.get_object(req).await.unwrap();
        let body = extract_body(resp.output.body).await;
        assert_eq!(body, Bytes::from("data"));
    }

    // First pass: 5 requests
    assert_eq!(backend.get_request_count().await, 5);

    // Fetch all again - in dry-run mode, should hit backend again for each
    for i in 0..5 {
        let req = build_get_request("test-bucket", &format!("file{}.txt", i), None);
        let resp = proxy.get_object(req).await.unwrap();
        let body = extract_body(resp.output.body).await;
        assert_eq!(body, Bytes::from("data"));
    }

    // Second pass: should have made 5 more requests (total 10)
    assert_eq!(backend.get_request_count().await, 10);
}

#[tokio::test]
async fn with_large_objects() {
    // Setup: MockS3Backend with large object
    let backend = MockS3Backend::new();
    let large_data = vec![b'x'; 1_000_000];
    backend
        .put_object_sync("test-bucket", "large.bin", &large_data)
        .await;

    // Setup: Cache + Proxy in dry-run mode with size limit
    let cache = create_test_cache(100, 100_000, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), 100_000, true);

    // Request large object in dry-run mode
    let req = build_get_request("test-bucket", "large.bin", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body.len(), 1_000_000);
    assert_eq!(backend.get_request_count().await, 1);

    // Object too large should not be cached (same as non-dry-run mode)
    assert_cache_missing(&cache, "test-bucket", "large.bin").await;

    // Second request should hit backend again
    let req = build_get_request("test-bucket", "large.bin", None);
    proxy.get_object(req).await.unwrap();
    assert_eq!(backend.get_request_count().await, 2);
}

#[tokio::test]
async fn concurrent_access() {
    // Setup: MockS3Backend with test data
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "concurrent.txt", b"data")
        .await;

    // Setup: Cache + Proxy in dry-run mode
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = Arc::new(CachingProxy::new(
        backend.clone(),
        Some(cache.clone()),
        usize::MAX,
        true,
    ));

    // Spawn multiple concurrent requests
    let mut handles = vec![];
    for _ in 0..10 {
        let proxy_clone = proxy.clone();
        let handle = tokio::spawn(async move {
            let req = build_get_request("test-bucket", "concurrent.txt", None);
            let resp = proxy_clone.get_object(req).await.unwrap();
            let body = extract_body(resp.output.body).await;
            assert_eq!(body, Bytes::from("data"));
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // In dry-run mode, every request hits the backend
    // So we should see 10 backend calls
    assert_eq!(backend.get_request_count().await, 10);

    // Object should be cached
    assert_cache_contains(&cache, "test-bucket", "concurrent.txt").await;
}

#[tokio::test]
async fn backend_error_not_cached() {
    // Setup: MockS3Backend without adding the object
    let backend = MockS3Backend::new();

    // Setup: Cache + Proxy in dry-run mode
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, true);

    // Request non-existent object
    let req = build_get_request("test-bucket", "missing.txt", None);
    let result = proxy.get_object(req).await;
    assert!(result.is_err(), "Should propagate backend error");

    // Nothing should be cached
    assert_cache_missing(&cache, "test-bucket", "missing.txt").await;
    assert!(cache.is_empty().await);
}

#[tokio::test]
async fn preserves_metadata() {
    // Setup: MockS3Backend with test data
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "meta.txt", b"hello")
        .await;

    // Setup: Cache + Proxy in dry-run mode
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, true);

    // First request
    let req = build_get_request("test-bucket", "meta.txt", None);
    let resp1 = proxy.get_object(req).await.unwrap();
    let body1 = extract_body(resp1.output.body).await;
    let content_length1 = resp1.output.content_length;

    // Second request (cache hit in dry-run mode)
    let req = build_get_request("test-bucket", "meta.txt", None);
    let resp2 = proxy.get_object(req).await.unwrap();
    let body2 = extract_body(resp2.output.body).await;
    let content_length2 = resp2.output.content_length;

    // Both responses should be identical
    assert_eq!(body1, body2);
    assert_eq!(body1, Bytes::from("hello"));
    assert_eq!(content_length1, content_length2);
    assert_eq!(content_length1, Some(5));
    assert_eq!(backend.get_request_count().await, 2);
}
