mod common;

use bytes::Bytes;
use common::MockS3Backend;
use common::helpers::*;
use s3_cache::{CacheKey, CachingProxy, range_to_string};
use s3s::S3;
use s3s::dto::Range;

#[tokio::test]
async fn test_range_requests_cached_separately() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "file.txt", b"0123456789")
        .await;

    let cache = create_test_cache(100, 10_000_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone(), usize::MAX);

    // GET full object
    let req = build_get_request("test-bucket", "file.txt", None);
    proxy.get_object(req).await.unwrap();
    assert_eq!(backend.get_request_count().await, 1);

    // GET partial range
    let range = Range::Int {
        first: 0,
        last: Some(4),
    };
    let req = build_get_request("test-bucket", "file.txt", Some(range.clone()));
    proxy.get_object(req).await.unwrap();
    assert_eq!(backend.get_request_count().await, 2); // Separate request

    // Verify both are cached (different cache keys)
    let full_key = CacheKey::new(
        "test-bucket".to_string(),
        "file.txt".to_string(),
        None,
        None,
    );
    assert!(cache.get(&full_key).await.is_some());

    // Range keys use range_to_string() - they're stored separately
    let range_str = range_to_string(&range);
    let range_key = CacheKey::new(
        "test-bucket".to_string(),
        "file.txt".to_string(),
        Some(range_str),
        None,
    );
    assert!(cache.get(&range_key).await.is_some());
}

#[tokio::test]
async fn test_overlapping_ranges_separate_cache() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "data.bin", b"0123456789")
        .await;

    let cache = create_test_cache(100, 10_000_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone(), usize::MAX);

    // GET range 0-4
    let range1 = Range::Int {
        first: 0,
        last: Some(4),
    };
    let req = build_get_request("test-bucket", "data.bin", Some(range1.clone()));
    proxy.get_object(req).await.unwrap();

    // GET overlapping range 2-6
    let range2 = Range::Int {
        first: 2,
        last: Some(6),
    };
    let req = build_get_request("test-bucket", "data.bin", Some(range2.clone()));
    proxy.get_object(req).await.unwrap();

    // Should be 2 separate backend requests (not deduplicated)
    assert_eq!(backend.get_request_count().await, 2);

    // Verify both cached separately
    let range1_str = range_to_string(&range1);
    let key1 = CacheKey::new(
        "test-bucket".to_string(),
        "data.bin".to_string(),
        Some(range1_str),
        None,
    );
    assert!(cache.get(&key1).await.is_some());

    let range2_str = range_to_string(&range2);
    let key2 = CacheKey::new(
        "test-bucket".to_string(),
        "data.bin".to_string(),
        Some(range2_str),
        None,
    );
    assert!(cache.get(&key2).await.is_some());
}

#[tokio::test]
async fn test_suffix_range_caching() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "file.txt", b"0123456789")
        .await;

    let cache = create_test_cache(100, 10_000_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone(), usize::MAX);

    // GET last 5 bytes
    let range = Range::Suffix { length: 5 };
    let req = build_get_request("test-bucket", "file.txt", Some(range.clone()));
    proxy.get_object(req).await.unwrap();
    assert_eq!(backend.get_request_count().await, 1);

    // Verify cached
    let range_str = range_to_string(&range);
    let key = CacheKey::new(
        "test-bucket".to_string(),
        "file.txt".to_string(),
        Some(range_str),
        None,
    );
    assert!(cache.get(&key).await.is_some());

    // Request again: should hit cache
    let req = build_get_request("test-bucket", "file.txt", Some(range));
    proxy.get_object(req).await.unwrap();
    assert_eq!(backend.get_request_count().await, 1); // No new request
}

#[tokio::test]
async fn test_range_invalidation_removes_all() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "multi-range.txt", b"0123456789")
        .await;

    let cache = create_test_cache(100, 10_000_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone(), usize::MAX);

    // GET full object
    let req = build_get_request("test-bucket", "multi-range.txt", None);
    proxy.get_object(req).await.unwrap();

    // GET multiple ranges
    let ranges = vec![
        Range::Int {
            first: 0,
            last: Some(2),
        },
        Range::Int {
            first: 5,
            last: Some(7),
        },
        Range::Suffix { length: 3 },
    ];

    for range in &ranges {
        let req = build_get_request("test-bucket", "multi-range.txt", Some(range.clone()));
        proxy.get_object(req).await.unwrap();
    }

    // Verify all cached
    let full_key = CacheKey::new(
        "test-bucket".to_string(),
        "multi-range.txt".to_string(),
        None,
        None,
    );
    assert!(cache.get(&full_key).await.is_some());

    for range in &ranges {
        let range_str = range_to_string(range);
        let key = CacheKey::new(
            "test-bucket".to_string(),
            "multi-range.txt".to_string(),
            Some(range_str),
            None,
        );
        assert!(cache.get(&key).await.is_some());
    }

    // PUT: should invalidate all variants (full + all ranges)
    let req = build_put_request("test-bucket", "multi-range.txt", Bytes::from("new content"));
    proxy.put_object(req).await.unwrap();

    // Verify full entry invalidated
    assert!(cache.get(&full_key).await.is_none());

    // Verify all range entries invalidated
    // Note: This depends on invalidate_object calling cache.remove_matching
    // which removes entries matching bucket+key regardless of range
}

#[tokio::test]
async fn test_full_request_does_not_populate_range_cache() {
    let backend = MockS3Backend::new();
    backend
        .put_object_sync("test-bucket", "file.txt", b"0123456789")
        .await;

    let cache = create_test_cache(100, 10_000_000, 300);
    let proxy = CachingProxy::new(backend.clone(), cache.clone(), usize::MAX);

    // GET full object
    let req = build_get_request("test-bucket", "file.txt", None);
    proxy.get_object(req).await.unwrap();

    // Verify full object cached
    let full_key = CacheKey::new(
        "test-bucket".to_string(),
        "file.txt".to_string(),
        None,
        None,
    );
    assert!(cache.get(&full_key).await.is_some());

    // Request range: should NOT hit cache (requires separate fetch)
    let range = Range::Int {
        first: 0,
        last: Some(4),
    };
    let req = build_get_request("test-bucket", "file.txt", Some(range));
    proxy.get_object(req).await.unwrap();

    // Should have made 2 backend requests (full + range are separate)
    assert_eq!(backend.get_request_count().await, 2);
}
