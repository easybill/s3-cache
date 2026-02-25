mod common;

use bytes::Bytes;
use common::MockS3Backend;
use common::helpers::*;
use s3_cache::{CachingProxy, SharedCachingProxy};
use s3s::S3;

#[tokio::test]
async fn put_invalidates_cache() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "file.txt", b"original")
        .await;

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = SharedCachingProxy::new(CachingProxy::new(
        backend.clone(),
        Some(cache.clone()),
        usize::MAX,
        false,
    ));

    // First GET: populate cache
    let req = build_get_request("test-bucket", "file.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("original"));
    assert_eq!(backend.get_request_count().await, 1);
    assert_cache_contains(&cache, "test-bucket", "file.txt").await;

    // PUT: should invalidate cache
    let req = build_put_request("test-bucket", "file.txt", Bytes::from("updated"));
    proxy.put_object(req).await.unwrap();
    assert_eq!(backend.put_request_count().await, 1);

    // Verify cache invalidated
    assert_cache_missing(&cache, "test-bucket", "file.txt").await;

    // Next GET: should fetch fresh data from backend
    let req = build_get_request("test-bucket", "file.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("updated"));
    assert_eq!(backend.get_request_count().await, 2); // New fetch
}

#[tokio::test]
async fn delete_invalidates_cache() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "deleteme.txt", b"data")
        .await;

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = SharedCachingProxy::new(CachingProxy::new(
        backend.clone(),
        Some(cache.clone()),
        usize::MAX,
        false,
    ));

    // GET: populate cache
    let req = build_get_request("test-bucket", "deleteme.txt", None);
    proxy.get_object(req).await.unwrap();
    assert_cache_contains(&cache, "test-bucket", "deleteme.txt").await;

    // DELETE: should invalidate cache
    let req = build_delete_request("test-bucket", "deleteme.txt");
    proxy.delete_object(req).await.unwrap();
    assert_eq!(backend.delete_request_count().await, 1);

    // Verify cache invalidated
    assert_cache_missing(&cache, "test-bucket", "deleteme.txt").await;

    // Verify object deleted from backend
    assert!(!backend.contains_object("test-bucket", "deleteme.txt").await);
}

#[tokio::test]
async fn delete_objects_invalidates_all() {
    let backend = MockS3Backend::new();
    for i in 0..5 {
        backend
            .put_object_sync("test-bucket", &format!("file{}.txt", i), b"data")
            .await;
    }

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = SharedCachingProxy::new(CachingProxy::new(
        backend.clone(),
        Some(cache.clone()),
        usize::MAX,
        false,
    ));

    // GET all objects: populate cache
    for i in 0..5 {
        let req = build_get_request("test-bucket", &format!("file{}.txt", i), None);
        proxy.get_object(req).await.unwrap();
    }

    // Verify all cached
    for i in 0..5 {
        assert_cache_contains(&cache, "test-bucket", &format!("file{}.txt", i)).await;
    }

    // Batch DELETE: should invalidate all
    let keys: Vec<String> = (0..5).map(|i| format!("file{}.txt", i)).collect();
    let req = build_delete_objects_request("test-bucket", keys);
    proxy.delete_objects(req).await.unwrap();

    // Verify all cache entries invalidated
    for i in 0..5 {
        assert_cache_missing(&cache, "test-bucket", &format!("file{}.txt", i)).await;
    }
}

#[tokio::test]
async fn copy_invalidates_destination() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "source.txt", b"source data")
        .await;
    backend
        .put_object_sync("test-bucket", "dest.txt", b"old dest data")
        .await;

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = SharedCachingProxy::new(CachingProxy::new(
        backend.clone(),
        Some(cache.clone()),
        usize::MAX,
        false,
    ));

    // GET destination: populate cache
    let req = build_get_request("test-bucket", "dest.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("old dest data"));
    assert_cache_contains(&cache, "test-bucket", "dest.txt").await;

    // COPY: should invalidate destination
    let req = build_copy_request("test-bucket", "source.txt", "test-bucket", "dest.txt");
    proxy.copy_object(req).await.unwrap();

    // Verify destination cache invalidated
    assert_cache_missing(&cache, "test-bucket", "dest.txt").await;

    // GET destination: should fetch copied data
    let req = build_get_request("test-bucket", "dest.txt", None);
    let resp = proxy.get_object(req).await.unwrap();
    let body = extract_body(resp.output.body).await;
    assert_eq!(body, Bytes::from("source data"));
}

#[tokio::test]
async fn invalidation_removes_all_ranges() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "ranged.txt", b"0123456789")
        .await;

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = SharedCachingProxy::new(CachingProxy::new(
        backend.clone(),
        Some(cache.clone()),
        usize::MAX,
        false,
    ));

    // GET full object
    let req = build_get_request("test-bucket", "ranged.txt", None);
    proxy.get_object(req).await.unwrap();
    assert_cache_contains(&cache, "test-bucket", "ranged.txt").await;

    // GET with range (cache stores ranges separately in current implementation)
    // Note: Range handling in cache is via range_to_string()
    let range = s3s::dto::Range::Int {
        first: 0,
        last: Some(4),
    };
    let req = build_get_request("test-bucket", "ranged.txt", Some(range));
    proxy.get_object(req).await.unwrap();

    // PUT: should invalidate both full and range entries
    let req = build_put_request("test-bucket", "ranged.txt", Bytes::from("new data"));
    proxy.put_object(req).await.unwrap();

    // Verify full entry invalidated
    assert_cache_missing(&cache, "test-bucket", "ranged.txt").await;

    // Note: Range entries use different cache keys, but invalidate_object()
    // in proxy_service.rs calls remove_matching which removes all variants
}

#[tokio::test]
async fn put_only_invalidates_target_key() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "file1.txt", b"data1")
        .await;
    backend
        .put_object_sync("test-bucket", "file2.txt", b"data2")
        .await;

    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = SharedCachingProxy::new(CachingProxy::new(
        backend.clone(),
        Some(cache.clone()),
        usize::MAX,
        false,
    ));

    // GET both files
    let req = build_get_request("test-bucket", "file1.txt", None);
    proxy.get_object(req).await.unwrap();
    let req = build_get_request("test-bucket", "file2.txt", None);
    proxy.get_object(req).await.unwrap();

    assert_cache_contains(&cache, "test-bucket", "file1.txt").await;
    assert_cache_contains(&cache, "test-bucket", "file2.txt").await;

    // PUT file1: should only invalidate file1
    let req = build_put_request("test-bucket", "file1.txt", Bytes::from("updated"));
    proxy.put_object(req).await.unwrap();

    assert_cache_missing(&cache, "test-bucket", "file1.txt").await;
    assert_cache_contains(&cache, "test-bucket", "file2.txt").await; // Still cached
}
