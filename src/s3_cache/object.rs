use std::time::Duration;

#[cfg(not(feature = "mock-clock"))]
use std::time::Instant;

#[cfg(feature = "mock-clock")]
use mock_instant::global::Instant;

use bytes::Bytes;
use s3s::dto::{
    AcceptRanges, CacheControl, ContentDisposition, ContentEncoding, ContentLanguage, ContentRange,
    ContentType, ETag, GetObjectOutput, LastModified, Metadata, StreamingBlob,
};

/// A cached S3 object with its body and metadata.
///
/// Stores all S3 object metadata needed to reconstruct a `GetObjectOutput` response,
/// plus timing information for TTL enforcement.
#[derive(Clone, Eq, PartialEq)]
pub struct CachedObject {
    body: Bytes,
    content_type: Option<ContentType>,
    e_tag: Option<ETag>,
    last_modified: Option<LastModified>,
    content_length: usize,
    accept_ranges: Option<AcceptRanges>,
    cache_control: Option<CacheControl>,
    content_disposition: Option<ContentDisposition>,
    content_encoding: Option<ContentEncoding>,
    content_language: Option<ContentLanguage>,
    content_range: Option<ContentRange>,
    metadata: Option<Metadata>,
    inserted_at: Instant,
}

impl CachedObject {
    /// Creates a new cached object with the given body and metadata.
    ///
    /// The `inserted_at` timestamp is set to the current time.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        body: Bytes,
        content_type: Option<ContentType>,
        e_tag: Option<ETag>,
        last_modified: Option<LastModified>,
        content_length: usize,
        accept_ranges: Option<AcceptRanges>,
        cache_control: Option<CacheControl>,
        content_disposition: Option<ContentDisposition>,
        content_encoding: Option<ContentEncoding>,
        content_language: Option<ContentLanguage>,
        content_range: Option<ContentRange>,
        metadata: Option<Metadata>,
    ) -> Self {
        Self {
            body,
            content_type,
            e_tag,
            last_modified,
            content_length,
            accept_ranges,
            cache_control,
            content_disposition,
            content_encoding,
            content_language,
            content_range,
            metadata,
            inserted_at: Instant::now(),
        }
    }

    /// Returns the timestamp when this object was inserted into the cache.
    pub fn inserted_at(&self) -> Instant {
        self.inserted_at
    }

    /// Returns `true` if this object has exceeded the given TTL.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.inserted_at.elapsed() > ttl
    }

    /// Returns a reference to the cached body bytes.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Returns the content type, if present.
    pub fn content_type(&self) -> Option<&String> {
        self.content_type.as_ref()
    }

    /// Returns the ETag, if present.
    pub fn e_tag(&self) -> Option<&ETag> {
        self.e_tag.as_ref()
    }

    /// Returns the last modified timestamp, if present.
    pub fn last_modified(&self) -> Option<&s3s::dto::Timestamp> {
        self.last_modified.as_ref()
    }

    /// Returns the content length in bytes.
    pub fn content_length(&self) -> usize {
        self.content_length
    }

    /// Converts this cached object to a `GetObjectOutput` for S3 responses.
    pub fn to_s3_object(&self) -> GetObjectOutput {
        let body = StreamingBlob::from(s3s::Body::from(self.body.clone()));

        GetObjectOutput {
            body: Some(body),
            content_length: Some(self.content_length as i64),
            content_type: self.content_type.clone(),
            e_tag: self.e_tag.clone(),
            last_modified: self.last_modified.clone(),
            accept_ranges: self.accept_ranges.clone(),
            cache_control: self.cache_control.clone(),
            content_disposition: self.content_disposition.clone(),
            content_encoding: self.content_encoding.clone(),
            content_language: self.content_language.clone(),
            content_range: self.content_range.clone(),
            metadata: self.metadata.clone(),
            ..Default::default()
        }
    }
}
