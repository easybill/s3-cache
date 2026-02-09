use std::hash::Hash;

/// Errors the cache can throw.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum CacheError {
    ValueTooBig,
    ValueAlreadyPresent,
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::ValueTooBig => write!(f, "Value is too big"),
            CacheError::ValueAlreadyPresent => write!(f, "Value is already present in cache"),
        }
    }
}

impl std::error::Error for CacheError {}
