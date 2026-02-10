use std::hash::{Hash, Hasher};

/// Cache key for S3 objects: (bucket, key, range_str, version_id).
/// Range is stored as a string representation to be hashable.
#[derive(Clone, Debug, Eq)]
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

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.key == other.key
            && self.range == other.range
            && self.version_id == other.version_id
    }
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bucket.hash(state);
        self.key.hash(state);
        self.range.hash(state);
        self.version_id.hash(state);
    }
}
