pub use self::async_fifo::*;
pub use self::entry::ValueEntry;
pub use self::error::*;
pub use self::fifo::*;
pub use self::ghost_list::*;
pub use self::key::CacheKey;
pub use self::object::CachedObject;

mod async_fifo;
mod entry;
mod error;
mod fifo;
mod ghost_list;
mod key;
mod object;

/// Something that can be cached.
pub trait Cacheable {
    fn size(&self) -> usize;
}

impl Cacheable for Vec<u8> {
    fn size(&self) -> usize {
        self.len()
    }
}

impl Cacheable for bytes::Bytes {
    fn size(&self) -> usize {
        self.len()
    }
}

/// Statistics about the cache.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheStatistics {
    pub count: u64,
    pub max_count: u64,
    pub size: usize,
    pub max_size: usize,
}

#[cfg(test)]
mod tests;
