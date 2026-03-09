use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};

use hyperloglockless::AtomicHyperLogLog;

/// Statistics tracker over unique objects
///
/// Tracks approximate cache statistics using HyperLogLog probabilistic counter
/// and an online size-distribution estimator.
pub struct UniqueRequestedObjectsStatisticsTracker {
    hll: AtomicHyperLogLog,
    bytes: AtomicUsize,
}

impl Default for UniqueRequestedObjectsStatisticsTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl UniqueRequestedObjectsStatisticsTracker {
    /// Target false-positive rate used when sizing the HyperLogLog sketch.
    pub const DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.005;

    /// Creates a new tracker with a deterministic seed and [`DEFAULT_FALSE_POSITIVE_RATE`](Self::DEFAULT_FALSE_POSITIVE_RATE) precision.
    pub fn new() -> Self {
        let seed_bytes: [u8; 16] = core::array::from_fn(|i| (i + 1) as u8);
        let seed = u128::from_ne_bytes(seed_bytes);

        let precision = hyperloglockless::precision_for_error(Self::DEFAULT_FALSE_POSITIVE_RATE);
        let hll = AtomicHyperLogLog::seeded(precision, seed);

        Self {
            hll,
            bytes: AtomicUsize::new(0),
        }
    }

    /// Records an object with the given `key` and size in `bytes`, skipping duplicate keys
    /// to avoid double-counting bytes or skewing the size distribution.
    ///
    /// Returns `true` if the key has increased the count estimate, otherwise `false`.
    pub fn insert<T>(&self, key: &T, bytes: usize) -> bool
    where
        T: Hash + ?Sized,
    {
        let count_before = self.hll.raw_count();
        self.hll.insert(key);
        let count_after = self.hll.raw_count();

        let count_has_increased = count_before < count_after;

        if count_has_increased {
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
        }

        count_has_increased
    }

    /// Total bytes accumulated across all uniquely inserted objects, returning an estimate
    /// subject to the HyperLogLog false-positive rate.
    pub fn estimated_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Approximate number of distinct keys seen so far, returning a HyperLogLog estimate
    /// within the configured false-positive rate.
    pub fn estimated_count(&self) -> usize {
        self.hll.count()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn default_creation() {
        let counter = UniqueRequestedObjectsStatisticsTracker::default();
        assert_eq!(counter.estimated_bytes(), 0);
        assert_eq!(counter.estimated_count(), 0);
    }

    #[test]
    fn insert_unique_keys() {
        let counter = UniqueRequestedObjectsStatisticsTracker::default();
        let initial_bytes = counter.estimated_bytes();

        counter.insert(&"key1", 100);
        counter.insert(&"key2", 200);
        counter.insert(&"key3", 150);

        let final_bytes = counter.estimated_bytes();
        assert!(
            final_bytes > initial_bytes,
            "Total bytes should increase after inserting unique keys"
        );
        assert!(
            final_bytes <= 450,
            "Total bytes should not exceed sum of all unique inserts"
        );
    }

    #[test]
    fn duplicate_key_does_not_add_extra_estimated_bytes() {
        let counter = UniqueRequestedObjectsStatisticsTracker::default();

        counter.insert(&"duplicate_key", 100);
        let bytes_after_first = counter.estimated_bytes();

        counter.insert(&"duplicate_key", 100);
        counter.insert(&"duplicate_key", 100);

        let bytes_after_duplicates = counter.estimated_bytes();
        assert_eq!(
            bytes_after_first, bytes_after_duplicates,
            "Duplicate key inserts should not add bytes"
        );
    }

    #[test]
    fn mixed_unique_and_duplicate_keys() {
        let counter = UniqueRequestedObjectsStatisticsTracker::default();

        counter.insert(&"key1", 100);
        counter.insert(&"key2", 200);
        let bytes_after_key2 = counter.estimated_bytes();

        counter.insert(&"key1", 100);
        counter.insert(&"key2", 200);
        let bytes_after_duplicates = counter.estimated_bytes();

        assert_eq!(bytes_after_key2, bytes_after_duplicates);

        counter.insert(&"key3", 300);
        let final_bytes = counter.estimated_bytes();

        assert!(
            final_bytes > bytes_after_duplicates,
            "Bytes should increase with new unique key"
        );
        assert!(
            final_bytes <= 600,
            "Total bytes should not exceed sum of unique keys"
        );
    }

    #[test]
    fn different_types_as_keys() {
        let counter = UniqueRequestedObjectsStatisticsTracker::default();

        counter.insert(&42i32, 50);
        counter.insert(&"string_key", 100);
        counter.insert(&(1, 2, 3), 75);

        let bytes = counter.estimated_bytes();
        assert!(bytes > 0, "Should have tracked some bytes from inserts");
        assert!(
            bytes <= 225,
            "Total bytes should not exceed sum of all inserts"
        );
    }

    #[test]
    fn zero_byte_inserts() {
        let counter = UniqueRequestedObjectsStatisticsTracker::default();

        counter.insert(&"key1", 0);
        counter.insert(&"key2", 0);

        assert_eq!(counter.estimated_bytes(), 0);

        let count = counter.estimated_count();
        assert!(count <= 2, "Should have at most 2 unique keys");
    }

    #[test]
    fn large_number_of_unique_keys() {
        let counter = UniqueRequestedObjectsStatisticsTracker::default();
        let num_keys = 10_000;

        for i in 0..num_keys {
            counter.insert(&i, 10);
        }

        let bytes = counter.estimated_bytes();
        let expected_bytes = num_keys * 10;

        let byte_error_margin = (expected_bytes as f64 * 0.20) as usize;

        assert!(
            bytes >= expected_bytes - byte_error_margin,
            "Total bytes {} should be within 20% of expected {}",
            bytes,
            expected_bytes
        );

        let estimated = counter.estimated_count();
        let count_error_margin = (num_keys as f64 * 0.05) as usize;
        assert!(
            estimated >= num_keys - count_error_margin
                && estimated <= num_keys + count_error_margin,
            "Estimated count {} should be within 5% of {}",
            estimated,
            num_keys
        );
    }

    #[test]
    fn concurrent_inserts() {
        let counter = Arc::new(UniqueRequestedObjectsStatisticsTracker::default());
        let num_threads = 4;
        let inserts_per_thread = 2500;

        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let counter_clone = Arc::clone(&counter);
            let handle = thread::spawn(move || {
                for i in 0..inserts_per_thread {
                    let key = format!("thread_{}_key_{}", thread_id, i);
                    counter_clone.insert(&key, 10);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let total_unique_keys = num_threads * inserts_per_thread;
        let expected_bytes = total_unique_keys * 10;
        let bytes = counter.estimated_bytes();

        let byte_error_margin = (expected_bytes as f64 * 0.20) as usize;

        assert!(
            bytes >= expected_bytes - byte_error_margin,
            "Total bytes {} should be within 20% of expected {}",
            bytes,
            expected_bytes
        );

        let estimated = counter.estimated_count();
        let count_error_margin = (total_unique_keys as f64 * 0.05) as usize;
        assert!(
            estimated >= total_unique_keys - count_error_margin
                && estimated <= total_unique_keys + count_error_margin,
            "Estimated count {} should be within 5% of {}",
            estimated,
            total_unique_keys
        );
    }

    #[test]
    fn concurrent_duplicate_inserts() {
        let counter = Arc::new(UniqueRequestedObjectsStatisticsTracker::default());
        let num_threads = 4;
        let inserts_per_thread = 2500;

        let mut handles = vec![];

        for _ in 0..num_threads {
            let counter_clone = Arc::clone(&counter);
            let handle = thread::spawn(move || {
                for i in 0..inserts_per_thread {
                    counter_clone.insert(&i, 10);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let expected_bytes = inserts_per_thread * 10;
        let bytes = counter.estimated_bytes();

        let byte_error_margin = (expected_bytes as f64 * 0.20) as usize;

        assert!(
            bytes >= expected_bytes - byte_error_margin,
            "Total bytes {} should be within 20% of expected {} (each key inserted once despite multiple threads)",
            bytes,
            expected_bytes
        );

        let estimated = counter.estimated_count();
        let count_error_margin = (inserts_per_thread as f64 * 0.05) as usize;
        assert!(
            estimated >= inserts_per_thread - count_error_margin
                && estimated <= inserts_per_thread + count_error_margin,
            "Estimated count {} should be within 5% of {}",
            estimated,
            inserts_per_thread
        );
    }

    #[test]
    fn duplicate_detection_with_varying_byte_sizes() {
        let counter = UniqueRequestedObjectsStatisticsTracker::default();

        counter.insert(&"key1", 100);
        let bytes_after_first = counter.estimated_bytes();

        counter.insert(&"key1", 500);
        counter.insert(&"key1", 1000);

        let final_bytes = counter.estimated_bytes();
        assert_eq!(
            bytes_after_first, final_bytes,
            "Duplicate key inserts should not add bytes regardless of byte size parameter"
        );
    }
}
