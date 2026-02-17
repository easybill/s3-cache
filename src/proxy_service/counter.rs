use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};

use hyperloglockless::AtomicHyperLogLog;

pub struct CachingCounter {
    hll: AtomicHyperLogLog,
    bytes: AtomicUsize,
}

impl Default for CachingCounter {
    fn default() -> Self {
        Self::new(Self::DEFAULT_FALSE_POSITIVE_RATE)
    }
}

impl CachingCounter {
    pub const DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.005;

    pub fn new(false_positive_rate: f64) -> Self {
        let seed_bytes: [u8; 16] = core::array::from_fn(|i| (i + 1) as u8);
        let seed = u128::from_ne_bytes(seed_bytes);

        let precision = hyperloglockless::precision_for_error(false_positive_rate);
        let hll = AtomicHyperLogLog::seeded(precision, seed);

        let bytes = AtomicUsize::new(0);

        Self { hll, bytes }
    }

    pub fn insert<T>(&self, key: &T, bytes: usize)
    where
        T: Hash + ?Sized,
    {
        let count_before = self.hll.raw_count();
        self.hll.insert(key);
        let count_after = self.hll.raw_count();

        if count_before < count_after {
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn estimated_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

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
    fn test_default_creation() {
        let counter = CachingCounter::default();
        assert_eq!(counter.estimated_bytes(), 0);
        assert_eq!(counter.estimated_count(), 0);
    }

    #[test]
    fn test_custom_false_positive_rate() {
        // Use 0.05 which should give a valid precision (within 4..=18)
        let counter = CachingCounter::new(0.05);
        assert_eq!(counter.estimated_bytes(), 0);
        assert_eq!(counter.estimated_count(), 0);
    }

    #[test]
    fn test_insert_unique_keys() {
        let counter = CachingCounter::default();
        let initial_bytes = counter.estimated_bytes();

        // Insert multiple unique keys
        counter.insert(&"key1", 100);
        counter.insert(&"key2", 200);
        counter.insert(&"key3", 150);

        // Total bytes should increase by sum of all inserts
        // Note: Due to HyperLogLog's probabilistic nature, some keys might not
        // increment the raw_count, so we check that bytes increased but not necessarily by exact amount
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
    fn test_duplicate_key_does_not_add_extra_estimated_bytes() {
        let counter = CachingCounter::default();

        // Insert the same key multiple times
        counter.insert(&"duplicate_key", 100);
        let bytes_after_first = counter.estimated_bytes();

        // Second and third inserts of same key should not add more bytes
        counter.insert(&"duplicate_key", 100);
        counter.insert(&"duplicate_key", 100);

        let bytes_after_duplicates = counter.estimated_bytes();
        assert_eq!(
            bytes_after_first, bytes_after_duplicates,
            "Duplicate key inserts should not add bytes"
        );
    }

    #[test]
    fn test_mixed_unique_and_duplicate_keys() {
        let counter = CachingCounter::default();

        counter.insert(&"key1", 100);
        counter.insert(&"key2", 200);
        let bytes_after_key2 = counter.estimated_bytes();

        // Duplicate inserts
        counter.insert(&"key1", 100);
        counter.insert(&"key2", 200);
        let bytes_after_duplicates = counter.estimated_bytes();

        // Bytes should not increase from duplicates
        assert_eq!(bytes_after_key2, bytes_after_duplicates);

        // Insert another unique key
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
    fn test_different_types_as_keys() {
        let counter = CachingCounter::default();

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
    fn test_zero_byte_inserts() {
        let counter = CachingCounter::default();

        counter.insert(&"key1", 0);
        counter.insert(&"key2", 0);

        // Even with 0-byte inserts, bytes should stay at 0
        assert_eq!(counter.estimated_bytes(), 0);

        // The counter should still track cardinality
        let count = counter.estimated_count();
        assert!(count <= 2, "Should have at most 2 unique keys");
    }

    #[test]
    fn test_large_number_of_unique_keys() {
        let counter = CachingCounter::default();
        let num_keys = 10_000;

        for i in 0..num_keys {
            counter.insert(&i, 10);
        }

        // HyperLogLog's raw_count only increments when the hash lands in a bucket
        // that needs updating, so it won't track every unique key insertion.
        // This is expected behavior for the probabilistic data structure.
        let bytes = counter.estimated_bytes();
        let expected_bytes = num_keys * 10;

        // Allow significant margin (~20%) as HLL may not increment raw_count for all unique keys
        let byte_error_margin = (expected_bytes as f64 * 0.20) as usize;

        assert!(
            bytes >= expected_bytes - byte_error_margin,
            "Total bytes {} should be within 20% of expected {}",
            bytes,
            expected_bytes
        );

        // HLL count estimation should be more accurate than raw_count
        let estimated = counter.estimated_count();
        let count_error_margin = (num_keys as f64 * 0.05) as usize; // 5% margin
        assert!(
            estimated >= num_keys - count_error_margin
                && estimated <= num_keys + count_error_margin,
            "Estimated count {} should be within 5% of {}",
            estimated,
            num_keys
        );
    }

    #[test]
    fn test_concurrent_inserts() {
        let counter = Arc::new(CachingCounter::default());
        let num_threads = 4;
        let inserts_per_thread = 2500;

        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let counter_clone = Arc::clone(&counter);
            let handle = thread::spawn(move || {
                for i in 0..inserts_per_thread {
                    // Each thread inserts unique keys
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

        // Allow 20% margin due to HyperLogLog's probabilistic nature
        let byte_error_margin = (expected_bytes as f64 * 0.20) as usize;

        assert!(
            bytes >= expected_bytes - byte_error_margin,
            "Total bytes {} should be within 20% of expected {}",
            bytes,
            expected_bytes
        );

        // HLL estimation should be more accurate
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
    fn test_concurrent_duplicate_inserts() {
        let counter = Arc::new(CachingCounter::default());
        let num_threads = 4;
        let inserts_per_thread = 2500;

        let mut handles = vec![];

        for _ in 0..num_threads {
            let counter_clone = Arc::clone(&counter);
            let handle = thread::spawn(move || {
                for i in 0..inserts_per_thread {
                    // All threads insert the same keys
                    counter_clone.insert(&i, 10);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Only unique keys should contribute, so only inserts_per_thread unique keys
        let expected_bytes = inserts_per_thread * 10;
        let bytes = counter.estimated_bytes();

        // Allow 20% margin due to HyperLogLog's probabilistic nature
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
    fn test_duplicate_detection_with_varying_byte_sizes() {
        let counter = CachingCounter::default();

        // First insert with 100 bytes
        counter.insert(&"key1", 100);
        let bytes_after_first = counter.estimated_bytes();

        // Duplicate insert but with different byte size - should still not add bytes
        counter.insert(&"key1", 500);
        counter.insert(&"key1", 1000);

        let final_bytes = counter.estimated_bytes();
        assert_eq!(
            bytes_after_first, final_bytes,
            "Duplicate key inserts should not add bytes regardless of byte size parameter"
        );
    }
}
