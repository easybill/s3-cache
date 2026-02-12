use std::time::Duration;

#[cfg(not(feature = "mock-clock"))]
use std::time::Instant;

#[cfg(feature = "mock-clock")]
use mock_instant::global::Instant;

use bytes::Bytes;
use s3s::dto::{ContentType, ETag, LastModified};

/// A cached S3 object with its body and metadata.
pub struct CachedObject {
    pub body: Bytes,
    pub content_type: Option<ContentType>,
    pub e_tag: Option<ETag>,
    pub last_modified: Option<LastModified>,
    pub content_length: i64,
    inserted_at: Instant,
}

impl CachedObject {
    pub fn new(
        body: Bytes,
        content_type: Option<ContentType>,
        e_tag: Option<ETag>,
        last_modified: Option<LastModified>,
        content_length: i64,
    ) -> Self {
        Self {
            body,
            content_type,
            e_tag,
            last_modified,
            content_length,
            inserted_at: Instant::now(),
        }
    }

    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.inserted_at.elapsed() > ttl
    }

    pub fn size(&self) -> usize {
        self.body.len()
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
            inserted_at: self.inserted_at,
        }
    }
}
