use std::time::Duration;

#[cfg(not(feature = "mock-clock"))]
use std::time::Instant;

#[cfg(feature = "mock-clock")]
use mock_instant::global::Instant;

use bytes::Bytes;
use s3s::dto::{
    AcceptRanges, CacheControl, ContentDisposition, ContentEncoding, ContentLanguage, ContentRange,
    ContentType, ETag, LastModified, Metadata,
};

/// A cached S3 object with its body and metadata.
pub struct CachedObject {
    pub body: Bytes,
    pub content_type: Option<ContentType>,
    pub e_tag: Option<ETag>,
    pub last_modified: Option<LastModified>,
    pub accept_ranges: Option<AcceptRanges>,
    pub cache_control: Option<CacheControl>,
    pub content_disposition: Option<ContentDisposition>,
    pub content_encoding: Option<ContentEncoding>,
    pub content_language: Option<ContentLanguage>,
    pub content_range: Option<ContentRange>,
    pub metadata: Option<Metadata>,
    content_length: usize,
    inserted_at: Instant,
}

impl CachedObject {
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

    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.inserted_at.elapsed() > ttl
    }
}

impl Clone for CachedObject {
    fn clone(&self) -> Self {
        Self {
            body: self.body.clone(),
            content_type: self.content_type.clone(),
            e_tag: self.e_tag.clone(),
            last_modified: self.last_modified.clone(),
            content_length: self.content_length,
            accept_ranges: self.accept_ranges.clone(),
            cache_control: self.cache_control.clone(),
            content_disposition: self.content_disposition.clone(),
            content_encoding: self.content_encoding.clone(),
            content_language: self.content_language.clone(),
            content_range: self.content_range.clone(),
            metadata: self.metadata.clone(),
            inserted_at: self.inserted_at,
        }

    pub fn content_length(&self) -> usize {
        self.content_length
    }
    }
}
