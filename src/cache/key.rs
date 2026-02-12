use std::hash::{Hash, Hasher};

/// Cache key for S3 objects: (bucket, key, range_str, version_id).
/// Range is stored as a string representation to be hashable.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CacheKey {
    pub bucket: String,
    pub key: String,
    pub range: Option<String>,
    pub version_id: Option<String>,
}

impl CacheKey {
    pub fn new(
        bucket: String,
        key: String,
        range: Option<String>,
        version_id: Option<String>,
    ) -> Self {
        Self {
            bucket,
            key,
            range,
            version_id,
        }
    }

    /// Check if this key matches a given bucket and object key (ignoring range and version).
    pub fn matches_object(&self, bucket: &str, key: &str) -> bool {
        self.bucket == bucket && self.key == key
    }
}
