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

/// Body content of a cached S3 object.
///
/// In normal mode, stores the actual bytes. In dry-run mode, stores only a hash
/// for validation purposes.
#[derive(Clone, Eq, PartialEq)]
pub enum CachedObjectBody {
    /// The actual object body bytes.
    Bytes { bytes: Bytes },
    /// A hash of the body (dry-run mode only).
    Hash { hash: u64 },
}

/// A cached S3 object with its body and metadata.
///
/// Stores all S3 object metadata needed to reconstruct a `GetObjectOutput` response,
/// plus timing information for TTL enforcement.
#[derive(Clone, Eq, PartialEq)]
pub struct CachedObject {
    body: CachedObjectBody,
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
    pub fn new(
        body: CachedObjectBody,
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

    /// Returns a reference to the cached body.
    pub fn body(&self) -> &CachedObjectBody {
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
    ///
    /// Returns `None` if the body is a hash (dry-run mode) rather than actual bytes.
    pub fn to_s3_object(&self) -> Option<GetObjectOutput> {
        let Self {
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
            inserted_at: _,
        } = self;

        let CachedObjectBody::Bytes { bytes } = body else {
            return None;
        };

        let body = StreamingBlob::from(s3s::Body::from(bytes.clone()));

        Some(GetObjectOutput {
            body: Some(body),
            content_length: Some(*content_length as i64),
            content_type: content_type.clone(),
            e_tag: e_tag.clone(),
            last_modified: last_modified.clone(),
            accept_ranges: accept_ranges.clone(),
            cache_control: cache_control.clone(),
            content_disposition: content_disposition.clone(),
            content_encoding: content_encoding.clone(),
            content_language: content_language.clone(),
            content_range: content_range.clone(),
            metadata: metadata.clone(),
            ..Default::default()
        })
    }
}
