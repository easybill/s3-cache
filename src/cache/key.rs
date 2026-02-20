use std::hash::Hash;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct CacheKeyState {
    pub bucket: String,
    pub key: String,
    pub range: Option<String>,
    pub version_id: Option<String>,
}

/// Cache key for S3 objects: (bucket, key, range_str, version_id).
/// Range is stored as a string representation to be hashable.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct CacheKey {
    state: Arc<CacheKeyState>,
}

impl CacheKey {
    pub fn new(
        bucket: String,
        key: String,
        range: Option<String>,
        version_id: Option<String>,
    ) -> Self {
        let state = CacheKeyState {
            bucket,
            key,
            range,
            version_id,
        };

        Self {
            state: Arc::new(state),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.state.bucket
    }

    pub fn key(&self) -> &str {
        &self.state.key
    }

    pub fn range(&self) -> Option<&str> {
        self.state.range.as_ref().map(|s| s.as_str())
    }

    pub fn version_id(&self) -> Option<&str> {
        self.state.version_id.as_ref().map(|s| s.as_str())
    }

    /// Checks if this key matches a given bucket and object key (ignoring range and version).
    pub fn matches_object(&self, bucket: &str, key: &str) -> bool {
        self.state.bucket == bucket && self.state.key == key
    }
}

impl std::fmt::Debug for CacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.state.fmt(f)
    }
}
