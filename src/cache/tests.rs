use std::num::{NonZeroU64, NonZeroUsize};

use super::*;

#[derive(Debug, Clone, PartialEq)]
struct TestData {
    size: usize,
}

impl TestData {
    fn new(size: usize) -> Self {
        Self { size }
    }
}

impl Cacheable for TestData {
    fn size(&self) -> usize {
        self.size
    }
}

#[test]
fn test_basic_insertion_and_retrieval() {
    let mut cache: fifo::S3FiFoCache<String, TestData> = fifo::S3FiFoCache::new(
        NonZeroU64::new(100).unwrap(),
        NonZeroUsize::new(10000).unwrap(),
    );

    let key1 = "test_key_1".to_string();
    let data1 = TestData::new(500);

    assert!(cache.insert(key1.clone(), data1.clone()).is_ok());
    assert_eq!(cache.count(), 1);
    assert_eq!(cache.size(), 500);

    let retrieved = cache.get(&key1);
    assert!(retrieved.is_some());
    assert_eq!(*retrieved.unwrap(), data1);
}

#[test]
fn test_remove() {
    let mut cache: fifo::S3FiFoCache<String, TestData> = fifo::S3FiFoCache::new(
        NonZeroU64::new(100).unwrap(),
        NonZeroUsize::new(10000).unwrap(),
    );

    let key = "test_key".to_string();
    let data = TestData::new(500);

    assert!(cache.insert(key.clone(), data).is_ok());
    assert_eq!(cache.count(), 1);
    assert_eq!(cache.size(), 500);

    assert!(cache.remove(&key));
    assert_eq!(cache.count(), 0);
    assert_eq!(cache.size(), 0);
    assert!(cache.get(&key).is_none());

    // Removing again returns false.
    assert!(!cache.remove(&key));
}

#[test]
fn test_remove_matching() {
    let mut cache: fifo::S3FiFoCache<String, TestData> = fifo::S3FiFoCache::new(
        NonZeroU64::new(100).unwrap(),
        NonZeroUsize::new(10000).unwrap(),
    );

    for i in 0..5 {
        let key = format!("prefix_a_{i}");
        assert!(cache.insert(key, TestData::new(100)).is_ok());
    }
    for i in 0..3 {
        let key = format!("prefix_b_{i}");
        assert!(cache.insert(key, TestData::new(100)).is_ok());
    }

    assert_eq!(cache.count(), 8);

    let removed = cache.remove_matching(|k| k.starts_with("prefix_a_"));
    assert_eq!(removed, 5);
    assert_eq!(cache.count(), 3);
}

#[test]
fn test_cache_eviction_by_count() {
    let mut cache: fifo::S3FiFoCache<String, TestData> = fifo::S3FiFoCache::new(
        NonZeroU64::new(100).unwrap(),
        NonZeroUsize::new(100000).unwrap(),
    );

    for i in 0..20 {
        let key = format!("key_{i}");
        let data = TestData::new(100);
        assert!(cache.insert(key, data).is_ok());
    }

    assert_eq!(cache.count(), 10);
}

#[test]
fn test_value_too_big_error() {
    let mut cache: fifo::S3FiFoCache<String, TestData> = fifo::S3FiFoCache::new(
        NonZeroU64::new(100).unwrap(),
        NonZeroUsize::new(5000).unwrap(),
    );

    let key = "big_item".to_string();
    let big_data = TestData::new(6000);

    let result = cache.insert(key, big_data);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CacheError::ValueTooBig));
}
