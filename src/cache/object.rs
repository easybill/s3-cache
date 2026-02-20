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

#[derive(Clone, Eq, PartialEq)]
pub enum CachedObjectBody {
    Bytes { bytes: Bytes },
    Hash { hash: u64 },
}

/// A cached S3 object with its body and metadata.
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

    pub fn inserted_at(&self) -> Instant {
        self.inserted_at
    }

    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.inserted_at.elapsed() > ttl
    }

    pub fn body(&self) -> &CachedObjectBody {
        &self.body
    }

    pub fn content_type(&self) -> Option<&String> {
        self.content_type.as_ref()
    }

    pub fn e_tag(&self) -> Option<&ETag> {
        self.e_tag.as_ref()
    }

    pub fn last_modified(&self) -> Option<&s3s::dto::Timestamp> {
        self.last_modified.as_ref()
    }

    pub fn content_length(&self) -> usize {
        self.content_length
    }

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
