use std::hash::{BuildHasher, RandomState};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use s3s_aws::Proxy;
use tracing::{debug, error, warn};

use crate::s3_cache::{CacheKey, CachedObject, CachedObjectBody, S3Cache};
use crate::statistics::UniqueRequestedObjectsStatisticsTracker;
use crate::telemetry::{self, RequestDuration};

/// Generic caching proxy that wraps any S3 implementation.
///
/// Intercepts S3 requests to cache `GetObject` responses and invalidate cache
/// entries on mutations (`PutObject`, `DeleteObject`, etc.).
///
/// The type parameter `T` defaults to [`s3s_aws::Proxy`] but can be any type
/// implementing the [`S3`] trait.
pub struct S3CachingProxy<T = Proxy> {
    inner: T,
    cache: Option<Arc<S3Cache>>,
    max_cacheable_size: usize,
    statistics: UniqueRequestedObjectsStatisticsTracker,
    hash_builder: RandomState,
    /// Dry-run mode: the cache is populated and checked, but get_object always
    /// returns the fresh upstream response. On cache hit the cached body is
    /// compared against the fresh body and a `cache.mismatch` event is emitted
    /// when they differ.
    dry_run: bool,
}

impl<T> S3CachingProxy<T> {
    /// Creates a new caching proxy wrapping an S3 implementation.
    ///
    /// Pass `None` for `cache` to disable caching (passthrough mode).
    /// Set `dry_run` to `true` to validate cache correctness without serving cached data.
    pub fn new(
        inner: T,
        cache: Option<Arc<S3Cache>>,
        max_cacheable_size: usize,
        dry_run: bool,
    ) -> Self {
        let statistics = UniqueRequestedObjectsStatisticsTracker::new();
        let hash_builder = RandomState::new();

        Self {
            inner,
            cache,
            max_cacheable_size,
            statistics,
            hash_builder,
            dry_run,
        }
    }

    /// Returns the estimated number of unique objects accessed.
    ///
    /// Uses a HyperLogLog probabilistic counter for memory-efficient estimation.
    pub fn estimated_unique_count(&self) -> usize {
        self.statistics.estimated_count()
    }

    /// Returns the estimated total bytes of unique objects accessed.
    ///
    /// Uses a HyperLogLog probabilistic counter for memory-efficient estimation.
    pub fn estimated_unique_bytes(&self) -> usize {
        self.statistics.estimated_bytes()
    }
}

impl S3CachingProxy<Proxy> {
    /// Convenience constructor for wrapping [`s3s_aws::Proxy`].
    ///
    /// This is equivalent to calling [`new`](Self::new) with a [`Proxy`] type parameter.
    pub fn from_aws_proxy(
        inner: Proxy,
        cache: Option<Arc<S3Cache>>,
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

macro_rules! make_client_request {
    ($client:expr, $req:expr, $op_name:expr, $op_fn:ident) => {{
        let client = $client;
        let req = $req;
        let op_name = $op_name;

        let method = req.method.to_string();
        let scheme = req.uri.scheme_str().map(str::to_owned);

        let start = Instant::now();
        let result: S3Result<S3Response<_>> = client.$op_fn(req).await;
        let duration = start.elapsed();

        let status_code: Option<u16> = match &result {
            Ok(res) => res.status,
            Err(err) => err.status_code(),
        }
        .map(|status| status.as_u16());

        let data = RequestDuration {
            version: "1.1",
            method,
            scheme,
            status_code,
            duration,
        };

        telemetry::record_client_request_duration(data, op_name);

        result
    }};
}

macro_rules! impl_s3_methods {
    ($op_fn:ident => [$op_name:expr, $op_input:ident, $op_output:ident]) => {
        // This function signature mirrors the expanded method signature
        // as obtained from `#[async_trait::async_trait] impl ... { ... }`,
        fn $op_fn<'life0, 'async_trait>(&'life0 self, req: S3Request<$op_input>) -> Pin<Box<dyn Future<Output = S3Result<S3Response<$op_output>>> + Send + 'async_trait>>
        where
            Self: 'async_trait,
            'life0: 'async_trait,
        {
            Box::into_pin(Box::new(async move {
                make_client_request!(&self.inner, req, $op_name, $op_fn)
            }))
        }
    };

    ( $($op_fn:ident => [$op_name:expr, $op_input:ident, $op_output:ident]),* $(,)? ) => {
        $(
            impl_s3_methods!($op_fn => [$op_name, $op_input, $op_output]);
        )*
    };
}

#[async_trait::async_trait]
impl<T: S3 + Send + Sync> S3 for S3CachingProxy<T> {
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let start = Instant::now();
        let method = req.method.to_string();
        let scheme = req.uri.scheme_str().map(str::to_owned);
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
                telemetry::record_cache_hit(cached.content_length() as u64);
                cache.report_stats().await;

                if !self.dry_run {
                    let bytes_len = cached.content_length();

                    if self.statistics.insert(&key, bytes_len) {
                        telemetry::record_unique_requested(bytes_len as u64);
                    }

                    let Some(output) = cached.to_s3_object() else {
                        panic!("expected bytes, found hash");
                    };

                    telemetry::record_server_request_duration(telemetry::RequestDuration {
                        version: "1.1",
                        method: method.clone(),
                        scheme: scheme.clone(),
                        status_code: Some(200),
                        duration: start.elapsed(),
                    });
                    telemetry::record_response_body_size(telemetry::ResponseBodySize {
                        version: "1.1",
                        method: method.clone(),
                        scheme: scheme.clone(),
                        status_code: 200,
                        size: cached.content_length() as u64,
                    });
                    return Ok(S3Response::new(output));
                }

                Some(cached)
            } else {
                debug!(bucket = %bucket, key = %key, "cache miss");
                None
            }
        } else {
            debug!(bucket = %bucket, key = %key, "cache miss");
            None
        };

        // Forward to upstream — reconstruct request since we moved range out
        let req = req.map_input(|mut input| {
            input.range = range;
            input
        });

        let result = make_client_request!(&self.inner, req, "GetObject", get_object);

        let resp = result.map_err(|err| {
            error!(bucket = %bucket, key = %key, error = %err, "upstream error on get_object");
            telemetry::record_upstream_error();
            telemetry::record_server_request_duration(telemetry::RequestDuration {
                version: "1.1",
                method: method.clone(),
                scheme: scheme.clone(),
                status_code: Some(502),
                duration: start.elapsed(),
            });
            err
        })?;
        let output = resp.output;

        let max_cacheable_size = self.max_cacheable_size;

        let bytes_len = output.content_length.unwrap_or(0) as usize;

        if self.statistics.insert(&key, bytes_len) {
            telemetry::record_unique_requested(bytes_len as u64);
        }

        // Check if object is too large to cache based on Content-Length
        if let Some(content_length) = output.content_length
            && (content_length as u64) > (max_cacheable_size as u64)
        {
            debug!(
                bucket = %bucket,
                key = %key,
                size = content_length,
                max_cacheable_size,
                "object too large to cache, streaming through"
            );
            telemetry::record_cache_oversized(content_length as u64);
            // Stream through without caching
            telemetry::record_server_request_duration(telemetry::RequestDuration {
                version: "1.1",
                method: "GET".to_owned(),
                scheme: None,
                status_code: Some(200),
                duration: start.elapsed(),
            });
            telemetry::record_response_body_size(telemetry::ResponseBodySize {
                version: "1.1",
                method: "GET".to_owned(),
                scheme: None,
                status_code: 200,
                size: content_length as u64,
            });
            return Ok(S3Response::new(output));
        }

        let body_len = output.content_length.unwrap_or(0);

        // Try to buffer and cache the response body
        let Some(body_blob) = output.body else {
            telemetry::record_server_request_duration(telemetry::RequestDuration {
                version: "1.1",
                method: "GET".to_owned(),
                scheme: None,
                status_code: Some(200),
                duration: start.elapsed(),
            });
            return Ok(S3Response::new(output));
        };

        let mut body = s3s::Body::from(body_blob);

        match body.store_all_limited(max_cacheable_size).await {
            Ok(bytes) => {
                let content_length = bytes.len();
                if cached_hit.is_none() {
                    telemetry::record_cache_miss(content_length as u64);
                }

                let body = if self.dry_run {
                    let hash = self.hash_builder.hash_one(&bytes);
                    CachedObjectBody::Hash { hash }
                } else {
                    CachedObjectBody::Bytes {
                        bytes: bytes.clone(),
                    }
                };

                // In dry-run mode, compare the fresh body against the cached one
                if self.dry_run
                    && let Some(cached_hit) = &cached_hit
                {
                    if cached_hit.content_type() != output.content_type.as_ref()
                        || cached_hit.e_tag() != output.e_tag.as_ref()
                        || cached_hit.last_modified() != output.last_modified.as_ref()
                        || cached_hit.body() != &body
                    {
                        error!(
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
                    debug!(bucket = %bucket, key = %key, size = content_length, "object cached");
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
                telemetry::record_server_request_duration(telemetry::RequestDuration {
                    version: "1.1",
                    method: method.clone(),
                    scheme: scheme.clone(),
                    status_code: Some(200),
                    duration: start.elapsed(),
                });
                telemetry::record_response_body_size(telemetry::ResponseBodySize {
                    version: "1.1",
                    method: method.clone(),
                    scheme: scheme.clone(),
                    status_code: 200,
                    size: content_length as u64,
                });
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
                telemetry::record_cache_oversized(body_len as u64);
                telemetry::record_server_request_duration(telemetry::RequestDuration {
                    version: "1.1",
                    method: method.clone(),
                    scheme: scheme.clone(),
                    status_code: Some(500),
                    duration: start.elapsed(),
                });
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

        let result = make_client_request!(&self.inner, req, "PutObject", put_object);

        let resp = result.map_err(|err| {
            error!(bucket = %bucket, key = %key, error = %err, "upstream error on put_object");
            telemetry::record_upstream_error();
            err
        })?;

        if let Some(cache) = &self.cache {
            let count = cache.invalidate_object(&bucket, &key).await;

            if count > 0 {
                debug!(bucket = %bucket, key = %key, "{count} cache entries invalidated on put");
                telemetry::record_cache_invalidation();
                cache.report_stats().await;
            } else {
                debug!(bucket = %bucket, key = %key, "no cache entries invalidated on put");
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

        let result = make_client_request!(&self.inner, req, "DeleteObject", delete_object);

        let resp = result.map_err(|err| {
            error!(bucket = %bucket, key = %key, error = %err, "upstream error on delete_object");
            telemetry::record_upstream_error();
            err
        })?;

        if let Some(cache) = &self.cache {
            let count = cache.invalidate_object(&bucket, &key).await;

            if count > 0 {
                debug!(bucket = %bucket, key = %key, "{count} cache entries invalidated on delete");
                telemetry::record_cache_invalidation();
                cache.report_stats().await;
            } else {
                debug!(bucket = %bucket, key = %key, "no cache entries invalidated on delete");
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

        let result = make_client_request!(&self.inner, req, "DeleteObjects", delete_objects);

        let resp = result.map_err(|err| {
            error!(bucket = %bucket, error = %err, "upstream error on delete_objects");
            telemetry::record_upstream_error();
            err
        })?;

        if let Some(cache) = &self.cache {
            for key in &keys {
                let count = cache.invalidate_object(&bucket, key).await;

                if count > 0 {
                    debug!(bucket = %bucket, key = %key, "{count} cache entries invalidated on batch delete");
                    telemetry::record_cache_invalidation();
                } else {
                    debug!(bucket = %bucket, key = %key, "no cache entries invalidated on batch delete");
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

        let result = make_client_request!(&self.inner, req, "CopyObject", copy_object);

        let resp = result.map_err(|err| {
            error!(bucket = %dest_bucket, key = %dest_key, error = %err, "upstream error on copy_object");
            telemetry::record_upstream_error();
            err
        })?;

        if let Some(cache) = &self.cache {
            let count = cache.invalidate_object(&dest_bucket, &dest_key).await;

            if count > 0 {
                debug!(bucket = %dest_bucket, key = %dest_key, "{count} cache entries invalidated on copy");
                telemetry::record_cache_invalidation();
                cache.report_stats().await;
            } else {
                debug!(bucket = %dest_bucket, key = %dest_key, "no cache entries invalidated on copy");
            }
        }

        Ok(resp)
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();

        let result = make_client_request!(
            &self.inner,
            req,
            "CompleteMultipartUpload",
            complete_multipart_upload
        );

        let resp = result.map_err(|err| {
            error!(bucket = %bucket, key = %key, error = %err, "upstream error on complete_multipart_upload");
            telemetry::record_upstream_error();
            err
        })?;

        if let Some(cache) = &self.cache {
            let count = cache.invalidate_object(&bucket, &key).await;

            if count > 0 {
                debug!(bucket = %bucket, key = %key, "{count} cache entries invalidated on multipart upload completion");
                telemetry::record_cache_invalidation();
                cache.report_stats().await;
            } else {
                debug!(bucket = %bucket, key = %key, "no cache entries invalidated on multipart upload completion");
            }
        }

        Ok(resp)
    }

    impl_s3_methods!(
        abort_multipart_upload => ["AbortMultipartUpload", AbortMultipartUploadInput, AbortMultipartUploadOutput],
        create_bucket => ["CreateBucket", CreateBucketInput, CreateBucketOutput],
        create_multipart_upload => ["CreateMultipartUpload", CreateMultipartUploadInput, CreateMultipartUploadOutput],
        delete_bucket => ["DeleteBucket", DeleteBucketInput, DeleteBucketOutput],
        get_bucket_location => ["GetBucketLocation", GetBucketLocationInput, GetBucketLocationOutput],
        head_bucket => ["HeadBucket", HeadBucketInput, HeadBucketOutput],
        head_object => ["HeadObject", HeadObjectInput, HeadObjectOutput],
        list_buckets => ["ListBuckets", ListBucketsInput, ListBucketsOutput],
        list_multipart_uploads => ["ListMultipartUploads", ListMultipartUploadsInput, ListMultipartUploadsOutput],
        list_objects => ["ListObjects", ListObjectsInput, ListObjectsOutput],
        list_objects_v2 => ["ListObjectsV2", ListObjectsV2Input, ListObjectsV2Output],
        list_parts => ["ListParts", ListPartsInput, ListPartsOutput],
        upload_part => ["UploadPart", UploadPartInput, UploadPartOutput],
        upload_part_copy => ["UploadPartCopy", UploadPartCopyInput, UploadPartCopyOutput],
    );
}
