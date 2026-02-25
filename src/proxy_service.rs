use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;

use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use s3s_aws::Proxy;
use tracing::{debug, error, warn};

use crate::s3_cache::{AsyncS3Cache, CacheKey, CachedObject, CachedObjectBody};
use crate::telemetry;

use self::counter::CachingCounter;

mod counter;

/// Generic caching proxy that wraps any S3 implementation.
///
/// Intercepts S3 requests to cache `GetObject` responses and invalidate cache
/// entries on mutations (`PutObject`, `DeleteObject`, etc.).
///
/// The type parameter `T` defaults to [`s3s_aws::Proxy`] but can be any type
/// implementing the [`S3`] trait.
pub struct CachingProxy<T = Proxy> {
    inner: T,
    cache: Option<Arc<AsyncS3Cache>>,
    max_cacheable_size: usize,
    counter: CachingCounter,
    /// Dry-run mode: the cache is populated and checked, but get_object always
    /// returns the fresh upstream response. On cache hit the cached body is
    /// compared against the fresh body and a `cache.mismatch` event is emitted
    /// when they differ.
    dry_run: bool,
}

impl<T> CachingProxy<T> {
    /// Creates a new caching proxy wrapping an S3 implementation.
    ///
    /// Pass `None` for `cache` to disable caching (passthrough mode).
    /// Set `dry_run` to `true` to validate cache correctness without serving cached data.
    pub fn new(
        inner: T,
        cache: Option<Arc<AsyncS3Cache>>,
        max_cacheable_size: usize,
        dry_run: bool,
    ) -> Self {
        let counter = CachingCounter::default();
        Self {
            inner,
            cache,
            max_cacheable_size,
            counter,
            dry_run,
        }
    }

    /// Returns the estimated number of unique objects accessed.
    ///
    /// Uses a HyperLogLog probabilistic counter for memory-efficient estimation.
    pub fn estimated_unique_count(&self) -> usize {
        self.counter.estimated_count()
    }

    /// Returns the estimated total bytes of unique objects accessed.
    ///
    /// Uses a HyperLogLog probabilistic counter for memory-efficient estimation.
    pub fn estimated_unique_bytes(&self) -> usize {
        self.counter.estimated_bytes()
    }
}

impl CachingProxy<Proxy> {
    /// Convenience constructor for wrapping [`s3s_aws::Proxy`].
    ///
    /// This is equivalent to calling [`new`](Self::new) with a [`Proxy`] type parameter.
    pub fn from_aws_proxy(
        inner: Proxy,
        cache: Option<Arc<AsyncS3Cache>>,
        max_cacheable_size: usize,
        dry_run: bool,
    ) -> Self {
        Self::new(inner, cache, max_cacheable_size, dry_run)
    }
}

/// Converts an S3 range to a string for use as a cache key component.
///
/// Formats the range according to HTTP Range header syntax.
///
/// # Examples
///
/// ```
/// use s3_cache::range_to_string;
/// use s3s::dto::Range;
///
/// assert_eq!(range_to_string(&Range::Int { first: 0, last: Some(99) }), "bytes=0-99");
/// assert_eq!(range_to_string(&Range::Int { first: 100, last: None }), "bytes=100-");
/// assert_eq!(range_to_string(&Range::Suffix { length: 500 }), "bytes=-500");
/// ```
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

                if !self.dry_run {
                    self.counter.insert(&key, cached.content_length());
                    telemetry::record_counter_estimates(
                        self.counter.estimated_count(),
                        self.counter.estimated_bytes(),
                    );

                    let Some(output) = cached.to_s3_object() else {
                        panic!("expected bytes, found hash");
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

        self.counter
            .insert(&key, output.content_length.unwrap_or(0) as usize);
        telemetry::record_counter_estimates(
            self.counter.estimated_count(),
            self.counter.estimated_bytes(),
        );

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
                let content_length = bytes.len();

                let body = if self.dry_run {
                    let hash = RandomState::new().hash_one(&bytes);
                    CachedObjectBody::Hash { hash }
                } else {
                    CachedObjectBody::Bytes {
                        bytes: bytes.clone(),
                    }
                };

                // In dry-run mode, compare the fresh body against the cached one
                if self.dry_run {
                    if let Some(cached_hit) = &cached_hit {
                        if cached_hit.content_type() != output.content_type.as_ref()
                            || cached_hit.e_tag() != output.e_tag.as_ref()
                            || cached_hit.last_modified() != output.last_modified.as_ref()
                            || cached_hit.body() != &body
                        {
                            warn!(
                                bucket = %cache_key.bucket(),
                                key = %cache_key.key(),
                                range = ?cache_key.range(),
                                version_id = ?cache_key.version_id(),
                                cached_len = cached_hit.content_length(),
                                fresh_len = bytes.len(),
                                "cache mismatch: cached object differs from upstream"
                            );
                            telemetry::record_cache_mismatch();
                        } else {
                            debug!(bucket = %bucket, key = %key, "dry-run: cached object matches upstream");
                        }
                    }
                }

                let cached = CachedObject::new(
                    body,
                    output.content_type.clone(),
                    output.e_tag.clone(),
                    output.last_modified.clone(),
                    content_length,
                    output.accept_ranges.clone(),
                    output.cache_control.clone(),
                    output.content_disposition.clone(),
                    output.content_encoding.clone(),
                    output.content_language.clone(),
                    output.content_range.clone(),
                    output.metadata.clone(),
                );

                if let Some(cache) = &self.cache {
                    let _existing = cache.insert(cache_key, cached).await;
                    debug!(bucket = %bucket, key = %key, size = content_length, "cached object");
                    cache.report_stats().await;
                }

                let new_body = StreamingBlob::from(s3s::Body::from(bytes));
                let new_output = GetObjectOutput {
                    body: Some(new_body),
                    content_length: Some(content_length as i64),
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

/// Thread-safe shared wrapper around [`CachingProxy`].
///
/// Wraps the proxy in an [`Arc`] to allow sharing between the S3 service handler
/// and other components like the metrics writer.
pub struct SharedCachingProxy<T>(Arc<CachingProxy<T>>);

impl<T> SharedCachingProxy<T> {
    /// Wraps a caching proxy for shared access.
    pub fn new(proxy: CachingProxy<T>) -> Self {
        Self(Arc::new(proxy))
    }

    /// Returns a clone of the inner [`Arc<CachingProxy<T>>`].
    pub fn clone_arc(&self) -> Arc<CachingProxy<T>> {
        self.0.clone()
    }
}

impl<T> Clone for SharedCachingProxy<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[async_trait::async_trait]
impl<T: S3 + Send + Sync> S3 for SharedCachingProxy<T> {
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        self.0.get_object(req).await
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        self.0.put_object(req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.0.delete_object(req).await
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        self.0.delete_objects(req).await
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        self.0.copy_object(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        self.0.head_object(req).await
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.0.create_multipart_upload(req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.0.complete_multipart_upload(req).await
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.0.abort_multipart_upload(req).await
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        self.0.list_objects(req).await
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.0.list_objects_v2(req).await
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        self.0.list_parts(req).await
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.0.upload_part(req).await
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        self.0.upload_part_copy(req).await
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.0.create_bucket(req).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        self.0.list_buckets(req).await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.0.delete_bucket(req).await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.0.get_bucket_location(req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.0.head_bucket(req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        self.0.list_multipart_uploads(req).await
    }
}
