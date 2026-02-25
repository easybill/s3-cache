use std::hash::Hash;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct CacheKeyState {
    pub bucket: String,
    pub key: String,
    pub range: Option<String>,
    pub version_id: Option<String>,
}

/// Cache key for S3 objects.
///
/// Uniquely identifies a cached object by bucket, object key, range, and version ID.
/// The range is stored as a string to make the key hashable.
///
/// Internally uses [`Arc`] for efficient cloning.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct CacheKey {
    state: Arc<CacheKeyState>,
}

impl CacheKey {
    /// Creates a new cache key.
    ///
    /// ```
    /// use s3_cache::CacheKey;
    ///
    /// let key = CacheKey::new(
    ///     "my-bucket".to_string(),
    ///     "path/to/object".to_string(),
    ///     Some("bytes=0-1023".to_string()),
    ///     None,
    /// );
    /// ```
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

    /// Returns the bucket name.
    pub fn bucket(&self) -> &str {
        &self.state.bucket
    }

    /// Returns the object key (path within the bucket).
    pub fn key(&self) -> &str {
        &self.state.key
    }

    /// Returns the range string, if any (e.g., `"bytes=0-1023"`).
    pub fn range(&self) -> Option<&str> {
        self.state.range.as_ref().map(|s| s.as_str())
    }

    /// Returns the object version ID, if any.
    pub fn version_id(&self) -> Option<&str> {
        self.state.version_id.as_ref().map(|s| s.as_str())
    }

    /// Returns `true` if this key matches the given bucket and object key.
    ///
    /// Ignores range and version ID when comparing.
    pub fn matches_object(&self, bucket: &str, key: &str) -> bool {
        self.state.bucket == bucket && self.state.key == key
    }
}

impl std::fmt::Debug for CacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.state.fmt(f)
    }
}
