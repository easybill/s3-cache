use bytes::Bytes;
use http::{HeaderMap, Method, Uri};
use s3_cache::{S3Cache, CacheKey};
use s3s::dto::*;
use s3s::{Body, S3Request};
use std::sync::Arc;
use std::time::Duration;

/// Build a GetObject request
pub fn build_get_request(
    bucket: &str,
    key: &str,
    range: Option<Range>,
) -> S3Request<GetObjectInput> {
    S3Request {
        input: GetObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            range,
            ..Default::default()
        },
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a PutObject request
pub fn build_put_request(bucket: &str, key: &str, body: Bytes) -> S3Request<PutObjectInput> {
    S3Request {
        input: PutObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            body: Some(StreamingBlob::from(Body::from(body))),
            ..Default::default()
        },
        method: Method::PUT,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a DeleteObject request
pub fn build_delete_request(bucket: &str, key: &str) -> S3Request<DeleteObjectInput> {
    S3Request {
        input: DeleteObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        },
        method: Method::DELETE,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a DeleteObjects request
pub fn build_delete_objects_request(
    bucket: &str,
    keys: Vec<String>,
) -> S3Request<DeleteObjectsInput> {
    let objects: Vec<ObjectIdentifier> = keys
        .into_iter()
        .map(|key| ObjectIdentifier {
            key,
            ..Default::default()
        })
        .collect();

    S3Request {
        input: DeleteObjectsInput {
            bucket: bucket.to_string(),
            delete: Delete {
                objects,
                quiet: None,
            },
            bypass_governance_retention: None,
            checksum_algorithm: None,
            expected_bucket_owner: None,
            mfa: None,
            request_payer: None,
        },
        method: Method::POST,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a CopyObject request
pub fn build_copy_request(
    src_bucket: &str,
    src_key: &str,
    dest_bucket: &str,
    dest_key: &str,
) -> S3Request<CopyObjectInput> {
    let copy_source = CopySource::Bucket {
        bucket: src_bucket.to_string().into(),
        key: src_key.to_string().into(),
        version_id: None,
    };

    S3Request {
        input: CopyObjectInput {
            bucket: dest_bucket.to_string(),
            copy_source,
            key: dest_key.to_string(),
            acl: None,
            bucket_key_enabled: None,
            cache_control: None,
            checksum_algorithm: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            content_type: None,
            copy_source_if_match: None,
            copy_source_if_modified_since: None,
            copy_source_if_none_match: None,
            copy_source_if_unmodified_since: None,
            copy_source_sse_customer_algorithm: None,
            copy_source_sse_customer_key: None,
            copy_source_sse_customer_key_md5: None,
            expected_bucket_owner: None,
            expected_source_bucket_owner: None,
            expires: None,
            grant_full_control: None,
            grant_read: None,
            grant_read_acp: None,
            grant_write_acp: None,
            metadata: None,
            metadata_directive: None,
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_date: None,
            request_payer: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            ssekms_encryption_context: None,
            ssekms_key_id: None,
            server_side_encryption: None,
            storage_class: None,
            tagging: None,
            tagging_directive: None,
            website_redirect_location: None,
        },
        method: Method::PUT,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Create a test cache with specified parameters
pub fn create_test_cache(max_entries: usize, max_size: usize, ttl_secs: u64) -> Arc<S3Cache> {
    Arc::new(S3Cache::new(
        max_entries,
        max_size,
        Duration::from_secs(ttl_secs),
        4,
    ))
}

/// Check if cache contains an entry
pub async fn assert_cache_contains(cache: &Arc<S3Cache>, bucket: &str, key: &str) {
    let cache_key = CacheKey::new(bucket.to_string(), key.to_string(), None, None);
    assert!(
        cache.get(&cache_key).await.is_some(),
        "Expected cache to contain {}/{}",
        bucket,
        key
    );
}

/// Check if cache does not contain an entry
pub async fn assert_cache_missing(cache: &Arc<S3Cache>, bucket: &str, key: &str) {
    let cache_key = CacheKey::new(bucket.to_string(), key.to_string(), None, None);
    assert!(
        cache.get(&cache_key).await.is_none(),
        "Expected cache to NOT contain {}/{}",
        bucket,
        key
    );
}

/// Extract body bytes from GetObjectOutput
pub async fn extract_body(body: Option<StreamingBlob>) -> Bytes {
    match body {
        Some(blob) => {
            let mut s3s_body: Body = Body::from(blob);
            // Try to get bytes directly if available, otherwise buffer the stream
            if let Some(bytes) = s3s_body.bytes() {
                bytes
            } else {
                s3s_body
                    .store_all_limited(10_000_000)
                    .await
                    .expect("Failed to read body")
            }
        }
        None => Bytes::new(),
    }
}

/// Build a CreateBucket request
pub fn build_create_bucket_request(bucket: &str) -> S3Request<CreateBucketInput> {
    S3Request {
        input: CreateBucketInput {
            bucket: bucket.to_string(),
            ..Default::default()
        },
        method: Method::PUT,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a DeleteBucket request
pub fn build_delete_bucket_request(bucket: &str) -> S3Request<DeleteBucketInput> {
    S3Request {
        input: DeleteBucketInput {
            bucket: bucket.to_string(),
            expected_bucket_owner: None,
        },
        method: Method::DELETE,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a HeadBucket request
pub fn build_head_bucket_request(bucket: &str) -> S3Request<HeadBucketInput> {
    S3Request {
        input: HeadBucketInput {
            bucket: bucket.to_string(),
            expected_bucket_owner: None,
        },
        method: Method::HEAD,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a ListBuckets request
pub fn build_list_buckets_request() -> S3Request<ListBucketsInput> {
    S3Request {
        input: ListBucketsInput::default(),
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a ListObjects request
pub fn build_list_objects_request(bucket: &str) -> S3Request<ListObjectsInput> {
    S3Request {
        input: ListObjectsInput {
            bucket: bucket.to_string(),
            ..Default::default()
        },
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a ListObjectsV2 request
pub fn build_list_objects_v2_request(bucket: &str) -> S3Request<ListObjectsV2Input> {
    S3Request {
        input: ListObjectsV2Input {
            bucket: bucket.to_string(),
            ..Default::default()
        },
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a GetBucketLocation request
pub fn build_get_bucket_location_request(bucket: &str) -> S3Request<GetBucketLocationInput> {
    S3Request {
        input: GetBucketLocationInput {
            bucket: bucket.to_string(),
            expected_bucket_owner: None,
        },
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a CreateMultipartUpload request
pub fn build_create_multipart_upload_request(
    bucket: &str,
    key: &str,
) -> S3Request<CreateMultipartUploadInput> {
    S3Request {
        input: CreateMultipartUploadInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        },
        method: Method::POST,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a CompleteMultipartUpload request
pub fn build_complete_multipart_upload_request(
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> S3Request<CompleteMultipartUploadInput> {
    S3Request {
        input: CompleteMultipartUploadInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            ..Default::default()
        },
        method: Method::POST,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build an AbortMultipartUpload request
pub fn build_abort_multipart_upload_request(
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> S3Request<AbortMultipartUploadInput> {
    S3Request {
        input: AbortMultipartUploadInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            expected_bucket_owner: None,
            request_payer: None,
            if_match_initiated_time: None,
        },
        method: Method::DELETE,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a ListMultipartUploads request
pub fn build_list_multipart_uploads_request(bucket: &str) -> S3Request<ListMultipartUploadsInput> {
    S3Request {
        input: ListMultipartUploadsInput {
            bucket: bucket.to_string(),
            ..Default::default()
        },
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build a ListParts request
pub fn build_list_parts_request(
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> S3Request<ListPartsInput> {
    S3Request {
        input: ListPartsInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            ..Default::default()
        },
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build an UploadPart request
pub fn build_upload_part_request(
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Bytes,
) -> S3Request<UploadPartInput> {
    S3Request {
        input: UploadPartInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            part_number,
            body: Some(StreamingBlob::from(Body::from(body))),
            ..Default::default()
        },
        method: Method::PUT,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Build an UploadPartCopy request
pub fn build_upload_part_copy_request(
    src_bucket: &str,
    src_key: &str,
    dest_bucket: &str,
    dest_key: &str,
    upload_id: &str,
    part_number: i32,
) -> S3Request<UploadPartCopyInput> {
    let copy_source = CopySource::Bucket {
        bucket: src_bucket.to_string().into(),
        key: src_key.to_string().into(),
        version_id: None,
    };

    S3Request {
        input: UploadPartCopyInput {
            bucket: dest_bucket.to_string(),
            key: dest_key.to_string(),
            copy_source,
            upload_id: upload_id.to_string(),
            part_number,
            copy_source_if_match: None,
            copy_source_if_modified_since: None,
            copy_source_if_none_match: None,
            copy_source_if_unmodified_since: None,
            copy_source_range: None,
            copy_source_sse_customer_algorithm: None,
            copy_source_sse_customer_key: None,
            copy_source_sse_customer_key_md5: None,
            expected_bucket_owner: None,
            expected_source_bucket_owner: None,
            request_payer: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
        },
        method: Method::PUT,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}
