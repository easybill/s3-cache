mod common;

use bytes::Bytes;
use common::MockS3Backend;
use common::helpers::*;
use s3_cache::proxy_service::CachingProxy;
use s3s::S3;

/// Test that create_bucket passes through without "not implemented" error
#[tokio::test]
async fn create_bucket_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    let req = build_create_bucket_request("test-bucket");
    let result = proxy.create_bucket(req).await;

    assert!(
        result.is_ok(),
        "create_bucket should not return 'not implemented' error"
    );
}

/// Test that delete_bucket passes through without "not implemented" error
#[tokio::test]
async fn delete_bucket_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // Create bucket first
    let req = build_create_bucket_request("test-bucket");
    proxy.create_bucket(req).await.unwrap();

    // Now delete it
    let req = build_delete_bucket_request("test-bucket");
    let result = proxy.delete_bucket(req).await;

    assert!(
        result.is_ok(),
        "delete_bucket should not return 'not implemented' error"
    );
}

/// Test that head_bucket passes through without "not implemented" error
#[tokio::test]
async fn head_bucket_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // Create bucket first
    let req = build_create_bucket_request("test-bucket");
    proxy.create_bucket(req).await.unwrap();

    // Now head it
    let req = build_head_bucket_request("test-bucket");
    let result = proxy.head_bucket(req).await;

    assert!(
        result.is_ok(),
        "head_bucket should not return 'not implemented' error"
    );
}

/// Test that list_buckets passes through without "not implemented" error
#[tokio::test]
async fn list_buckets_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    let req = build_list_buckets_request();
    let result = proxy.list_buckets(req).await;

    assert!(
        result.is_ok(),
        "list_buckets should not return 'not implemented' error"
    );
}

/// Test that list_objects passes through without "not implemented" error
#[tokio::test]
async fn list_objects_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // Create bucket first
    let req = build_create_bucket_request("test-bucket");
    proxy.create_bucket(req).await.unwrap();

    // Now list objects
    let req = build_list_objects_request("test-bucket");
    let result = proxy.list_objects(req).await;

    assert!(
        result.is_ok(),
        "list_objects should not return 'not implemented' error"
    );
}

/// Test that list_objects_v2 passes through without "not implemented" error
#[tokio::test]
async fn list_objects_v2_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // Create bucket first
    let req = build_create_bucket_request("test-bucket");
    proxy.create_bucket(req).await.unwrap();

    // Now list objects v2
    let req = build_list_objects_v2_request("test-bucket");
    let result = proxy.list_objects_v2(req).await;

    assert!(
        result.is_ok(),
        "list_objects_v2 should not return 'not implemented' error"
    );
}

/// Test that get_bucket_location passes through without "not implemented" error
#[tokio::test]
async fn get_bucket_location_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    // Create bucket first
    let req = build_create_bucket_request("test-bucket");
    proxy.create_bucket(req).await.unwrap();

    // Now get bucket location
    let req = build_get_bucket_location_request("test-bucket");
    let result = proxy.get_bucket_location(req).await;

    assert!(
        result.is_ok(),
        "get_bucket_location should not return 'not implemented' error"
    );
}

/// Test that create_multipart_upload passes through without "not implemented" error
#[tokio::test]
async fn create_multipart_upload_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    let req = build_create_multipart_upload_request("test-bucket", "test-key.txt");
    let result = proxy.create_multipart_upload(req).await;

    assert!(
        result.is_ok(),
        "create_multipart_upload should not return 'not implemented' error"
    );
}

/// Test that complete_multipart_upload passes through without "not implemented" error
#[tokio::test]
async fn complete_multipart_upload_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    let req = build_complete_multipart_upload_request("test-bucket", "test-key.txt", "upload-123");
    let result = proxy.complete_multipart_upload(req).await;

    assert!(
        result.is_ok(),
        "complete_multipart_upload should not return 'not implemented' error"
    );
}

/// Test that abort_multipart_upload passes through without "not implemented" error
#[tokio::test]
async fn abort_multipart_upload_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    let req = build_abort_multipart_upload_request("test-bucket", "test-key.txt", "upload-123");
    let result = proxy.abort_multipart_upload(req).await;

    assert!(
        result.is_ok(),
        "abort_multipart_upload should not return 'not implemented' error"
    );
}

/// Test that list_multipart_uploads passes through without "not implemented" error
#[tokio::test]
async fn list_multipart_uploads_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    let req = build_list_multipart_uploads_request("test-bucket");
    let result = proxy.list_multipart_uploads(req).await;

    assert!(
        result.is_ok(),
        "list_multipart_uploads should not return 'not implemented' error"
    );
}

/// Test that list_parts passes through without "not implemented" error
#[tokio::test]
async fn list_parts_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    let req = build_list_parts_request("test-bucket", "test-key.txt", "upload-123");
    let result = proxy.list_parts(req).await;

    assert!(
        result.is_ok(),
        "list_parts should not return 'not implemented' error"
    );
}

/// Test that upload_part passes through without "not implemented" error
#[tokio::test]
async fn upload_part_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    let req = build_upload_part_request(
        "test-bucket",
        "test-key.txt",
        "upload-123",
        1,
        Bytes::from("part data"),
    );
    let result = proxy.upload_part(req).await;

    assert!(
        result.is_ok(),
        "upload_part should not return 'not implemented' error"
    );
}

/// Test that upload_part_copy passes through without "not implemented" error
#[tokio::test]
async fn upload_part_copy_passthrough() {
    let backend = MockS3Backend::new();
    let cache = create_test_cache(100, usize::MAX, 300);
    let proxy = CachingProxy::new(backend.clone(), Some(cache.clone()), usize::MAX, false);

    let req = build_upload_part_copy_request(
        "source-bucket",
        "source-key.txt",
        "dest-bucket",
        "dest-key.txt",
        "upload-123",
        1,
    );
    let result = proxy.upload_part_copy(req).await;

    assert!(
        result.is_ok(),
        "upload_part_copy should not return 'not implemented' error"
    );
}
