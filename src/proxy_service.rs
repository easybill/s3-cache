use std::sync::Arc;

use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use s3s_aws::Proxy;
use tracing::{debug, error, warn};

use crate::async_cache::AsyncS3Cache;
use crate::cache::CacheKey;
use crate::cache::CachedObject;
use crate::telemetry;

/// Generic caching proxy that wraps any S3 implementation
#[derive(Clone)]
pub struct CachingProxy<T = Proxy> {
    inner: T,
    cache: Option<Arc<AsyncS3Cache>>,
    max_cacheable_size: usize,
    /// Dry-run mode: the cache is populated and checked, but get_object always
    /// returns the fresh upstream response. On cache hit the cached body is
    /// compared against the fresh body and a `cache.mismatch` event is emitted
    /// when they differ.
    dryrun: bool,
}

impl<T> CachingProxy<T> {
    pub fn new(
        inner: T,
        cache: Option<Arc<AsyncS3Cache>>,
        max_cacheable_size: usize,
        dryrun: bool,
    ) -> Self {
        Self {
            inner,
            cache,
            max_cacheable_size,
            dryrun,
        }
    }
}

impl CachingProxy<Proxy> {
    /// Convenience constructor for the common case of wrapping s3s_aws::Proxy
    pub fn from_aws_proxy(
        inner: Proxy,
        cache: Option<Arc<AsyncS3Cache>>,
        max_cacheable_size: usize,
        dryrun: bool,
    ) -> Self {
        Self::new(inner, cache, max_cacheable_size, dryrun)
    }
}

/// Convert an s3s Range to a string for use as a cache key component.
pub fn range_to_string(range: &Range) -> String {
    match range {
        Range::Int {
            first,
            last: Some(last),
        } => format!("bytes={first}-{last}"),
        Range::Int { first, last: None } => format!("bytes={first}-"),
        Range::Suffix { length } => format!("bytes=-{length}"),
    }
}

#[async_trait::async_trait]
impl<T: S3 + Send + Sync> S3 for CachingProxy<T> {
    // === Cached operation ===

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let range = req.input.range;
        let range_str = range.as_ref().map(range_to_string);
        let version_id = req.input.version_id.clone();

        let cache_key = CacheKey::new(bucket.clone(), key.clone(), range_str.clone(), version_id);

        // Check cache
        let cached_hit = if let Some(cache) = &self.cache {
            if let Some(cached) = cache.get(&cache_key).await {
                debug!(bucket = %bucket, key = %key, "cache hit");
                telemetry::record_cache_hit();
                cache.report_stats().await;

                if !self.dryrun {
                    let body = StreamingBlob::from(s3s::Body::from(cached.body.clone()));
                    let output = GetObjectOutput {
                        body: Some(body),
                        content_length: Some(cached.content_length),
                        content_type: cached.content_type.clone(),
                        e_tag: cached.e_tag.clone(),
                        last_modified: cached.last_modified.clone(),
                        ..Default::default()
                    };
                    return Ok(S3Response::new(output));
                }

                Some(cached)
            } else {
                debug!(bucket = %bucket, key = %key, "cache miss");
                telemetry::record_cache_miss();
                None
            }
        } else {
            debug!(bucket = %bucket, key = %key, "cache miss");
            telemetry::record_cache_miss();
            None
        };

        // Forward to upstream — reconstruct request since we moved range out
        let get_req = req.map_input(|mut input| {
            input.range = range;
            input
        });

        let resp = self.inner.get_object(get_req).await.map_err(|err| {
            error!(bucket = %bucket, key = %key, error = %err, "upstream error on get_object");
            telemetry::record_upstream_error();
            err
        })?;
        let output = resp.output;

        let max_cacheable_size = self.max_cacheable_size;

        // Check if object is too large to cache based on Content-Length
        if let Some(content_length) = output.content_length {
            if (content_length as u64) > (max_cacheable_size as u64) {
                debug!(
                    bucket = %bucket,
                    key = %key,
                    size = content_length,
                    max_cacheable_size,
                    "object too large to cache, streaming through"
                );
                // Stream through without caching
                return Ok(S3Response::new(output));
            }
        }

        // Try to buffer and cache the response body
        let Some(body_blob) = output.body else {
            return Ok(S3Response::new(output));
        };

        let mut body = s3s::Body::from(body_blob);

        match body.store_all_limited(max_cacheable_size).await {
            Ok(bytes) => {
                let content_length = bytes.len() as i64;
                let cached = CachedObject::new(
                    bytes.clone(),
                    output.content_type.clone(),
                    output.e_tag.clone(),
                    output.last_modified.clone(),
                    content_length,
                );

                // In dryrun mode, compare the fresh body against the cached one
                if self.dryrun {
                    if let Some(cached_hit) = &cached_hit {
                        if cached_hit.body != bytes
                            || cached_hit.content_type != output.content_type
                            || cached_hit.e_tag != output.e_tag
                            || cached_hit.last_modified != output.last_modified
                        {
                            warn!(
                                bucket = %cache_key.bucket,
                                key = %cache_key.key,
                                range = ?cache_key.range,
                                version_id = ?cache_key.version_id,
                                cached_len = cached_hit.body.len(),
                                fresh_len = bytes.len(),
                                "cache mismatch: cached object differs from upstream"
                            );
                            telemetry::record_cache_mismatch();
                        } else {
                            debug!(bucket = %bucket, key = %key, "dryrun: cached object matches upstream");
                        }
                    }
                }

                if let Some(cache) = &self.cache {
                    let _existing = cache.insert(cache_key, cached).await;
                    debug!(bucket = %bucket, key = %key, size = content_length, "cached object");
                    cache.report_stats().await;
                }

                let new_body = StreamingBlob::from(s3s::Body::from(bytes));
                let new_output = GetObjectOutput {
                    body: Some(new_body),
                    content_length: Some(content_length),
                    content_type: output.content_type,
                    e_tag: output.e_tag,
                    last_modified: output.last_modified,
                    accept_ranges: output.accept_ranges,
                    cache_control: output.cache_control,
                    content_disposition: output.content_disposition,
                    content_encoding: output.content_encoding,
                    content_language: output.content_language,
                    content_range: output.content_range,
                    delete_marker: output.delete_marker,
                    expiration: output.expiration,
                    expires: output.expires,
                    metadata: output.metadata,
                    version_id: output.version_id,
                    storage_class: output.storage_class,
                    ..Default::default()
                };
                Ok(S3Response::new(new_output))
            }
            Err(_) => {
                // Body exceeds max cacheable size and stream is consumed.
                // Stream through without caching (though body is consumed).
                warn!(
                    bucket = %bucket,
                    key = %key,
                    "object exceeded size limit during buffering, stream consumed"
                );
                telemetry::record_buffering_error();
                Err(s3_error!(
                    InternalError,
                    "Object exceeded size limit during buffering"
                ))
            }
        }
    }

    // === Write-through invalidation ===

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();

        let resp = self.inner.put_object(req).await.map_err(|err| {
            error!(bucket = %bucket, key = %key, error = %err, "upstream error on put_object");
            telemetry::record_upstream_error();
            err
        })?;

        if let Some(cache) = &self.cache {
            let count = cache.invalidate_object(&bucket, &key).await;

            if count > 0 {
                debug!(bucket = %bucket, key = %key, count, "invalidated cache entries on put");
                telemetry::record_cache_invalidation();
                cache.report_stats().await;
            }
        }

        Ok(resp)
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();

        let resp = self.inner.delete_object(req).await.map_err(|err| {
            error!(bucket = %bucket, key = %key, error = %err, "upstream error on delete_object");
            telemetry::record_upstream_error();
            err
        })?;

        if let Some(cache) = &self.cache {
            let count = cache.invalidate_object(&bucket, &key).await;

            if count > 0 {
                debug!(bucket = %bucket, key = %key, count, "invalidated cache entries on delete");
                telemetry::record_cache_invalidation();
                cache.report_stats().await;
            }
        }

        Ok(resp)
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let bucket = req.input.bucket.clone();
        let keys: Vec<String> = req
            .input
            .delete
            .objects
            .iter()
            .map(|o| o.key.clone())
            .collect();

        let resp = self.inner.delete_objects(req).await.map_err(|err| {
            error!(bucket = %bucket, error = %err, "upstream error on delete_objects");
            telemetry::record_upstream_error();
            err
        })?;

        if let Some(cache) = &self.cache {
            for key in &keys {
                let count = cache.invalidate_object(&bucket, key).await;

                if count > 0 {
                    debug!(bucket = %bucket, key = %key, count, "invalidated cache entries on batch delete");
                    telemetry::record_cache_invalidation();
                }
            }
            cache.report_stats().await;
        }

        Ok(resp)
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let dest_bucket = req.input.bucket.clone();
        let dest_key = req.input.key.clone();

        let resp = self.inner.copy_object(req).await.map_err(|err| {
            error!(bucket = %dest_bucket, key = %dest_key, error = %err, "upstream error on copy_object");
            telemetry::record_upstream_error();
            err
        })?;

        if let Some(cache) = &self.cache {
            let count = cache.invalidate_object(&dest_bucket, &dest_key).await;

            if count > 0 {
                debug!(bucket = %dest_bucket, key = %dest_key, count, "invalidated cache entries on copy");
                telemetry::record_cache_invalidation();
                cache.report_stats().await;
            }
        }

        Ok(resp)
    }

    // === Delegated operations ===

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.inner.abort_multipart_upload(req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();

        let resp = self.inner.complete_multipart_upload(req).await.map_err(|err| {
            error!(bucket = %bucket, key = %key, error = %err, "upstream error on complete_multipart_upload");
            telemetry::record_upstream_error();
            err
        })?;

        if let Some(cache) = &self.cache {
            let count = cache.invalidate_object(&bucket, &key).await;

            if count > 0 {
                debug!(bucket = %bucket, key = %key, count, "invalidated cache entries on multipart upload completion");
                telemetry::record_cache_invalidation();
                cache.report_stats().await;
            }
        }

        Ok(resp)
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.inner.create_bucket(req).await
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.inner.create_multipart_upload(req).await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.inner.delete_bucket(req).await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.inner.get_bucket_location(req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.inner.head_bucket(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        self.inner.head_object(req).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        self.inner.list_buckets(req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        self.inner.list_multipart_uploads(req).await
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        self.inner.list_objects(req).await
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.inner.list_objects_v2(req).await
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        self.inner.list_parts(req).await
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.inner.upload_part(req).await
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        self.inner.upload_part_copy(req).await
    }
}
