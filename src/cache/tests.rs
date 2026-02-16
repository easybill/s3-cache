use super::*;

#[derive(Debug, Clone, PartialEq)]
struct TestData {
    size: usize,
}

#[test]
fn test_basic_insertion_and_retrieval() {
    let mut cache: S3FifoCache<String, TestData> = S3FifoCache::new(1000, 10000);

    let key1 = "test_key_1".to_string();
    let data1 = TestData { size: 500 };

    assert!(cache.insert(key1.clone(), data1.clone()).is_none());
    assert_eq!(cache.len(), 1);

    let retrieved = cache.get(&key1);
    assert!(retrieved.is_some());
    assert_eq!(*retrieved.unwrap(), data1);
}

#[test]
fn test_remove() {
    let mut cache: S3FifoCache<String, TestData> = S3FifoCache::new(1000, 10000);

    let key = "test_key".to_string();
    let data = TestData { size: 500 };

    assert!(cache.insert(key.clone(), data).is_none());
    assert_eq!(cache.len(), 1);

    assert!(cache.remove(&key).is_some());
    assert_eq!(cache.len(), 0);
    assert!(cache.get(&key).is_none());

    // Removing again returns None.
    assert!(cache.remove(&key).is_none());
}

#[test]
fn test_retain() {
    let mut cache: S3FifoCache<String, TestData> = S3FifoCache::new(1000, 10000);

    for i in 0..5 {
        let key = format!("prefix_a_{i}");
        assert!(cache.insert(key, TestData { size: 100 }).is_none());
    }
    for i in 0..3 {
        let key = format!("prefix_b_{i}");
        assert!(cache.insert(key, TestData { size: 100 }).is_none());
    }

    assert_eq!(cache.len(), 8);

    let len_before = cache.len();
    cache.retain(|key, _value| !key.starts_with("prefix_a_"));
    let len_after = cache.len();
    assert_eq!(len_before - len_after, 5);
    assert_eq!(cache.len(), 3);
}

#[test]
fn test_cache_eviction_by_len() {
    let mut cache: S3FifoCache<String, TestData> = S3FifoCache::with_max_len(10);

    for i in 0..20 {
        let key = format!("key_{i}");
        let data = TestData { size: 100 };
        assert!(cache.insert(key, data).is_none());
    }

    assert_eq!(cache.len(), 10);
}

#[test]
fn test_small_to_main_promotion() {
    // small=2, main=3 → total capacity 5
    let mut cache: S3FifoCache<String, u32> = S3FifoCache::new(2, 3);

    // Insert A, B (fill small)
    cache.insert("A".into(), 1);
    cache.insert("B".into(), 2);

    // Access A to give it lives (so it gets promoted on eviction from small)
    cache.get("A");

    // Insert C — small is full, evicts tail (A has lives → promoted to main, no eviction)
    // Then C enters small. If A was promoted, it's now in main.
    cache.insert("C".into(), 3);
    cache.insert("D".into(), 4);
    cache.insert("E".into(), 5);

    // A was accessed, so it should have been promoted to main and survive
    assert!(
        cache.contains_key("A"),
        "Accessed item A should survive via promotion to main"
    );
}

#[test]
fn test_ghost_list_promotion() {
    // small=2, main=3 → total 5
    let mut cache: S3FifoCache<String, u32> = S3FifoCache::new(2, 3);

    // Fill cache with 5 items (A..E)
    for (i, name) in ["A", "B", "C", "D", "E"].iter().enumerate() {
        cache.insert(name.to_string(), i as u32);
    }
    assert_eq!(cache.len(), 5);

    // Insert F — this evicts something from small to ghost
    cache.insert("F".into(), 5);

    // Now re-insert one of the evicted keys. If it's in the ghost list,
    // it should go to main directly (promoted).
    // We need to find which key was evicted. Try all early keys.
    let ghost_key = ["A", "B"]
        .iter()
        .find(|k| !cache.contains_key(**k))
        .expect("At least one of A,B should have been evicted");

    cache.insert(ghost_key.to_string(), 99);
}

#[test]
fn test_fifo_reinsertion_in_main() {
    // small=1, main=3 → total 4
    let mut cache: S3FifoCache<String, u32> = S3FifoCache::new(1, 3);

    // Insert A (goes to small)
    cache.insert("A".into(), 1);
    // Access A so it gets promoted to main when evicted from small
    cache.get("A");

    // Insert B — evicts A from small (promoted to main), B in small
    cache.insert("B".into(), 2);

    // Insert C — evicts B from small → ghost. C in small. A in main.
    cache.insert("C".into(), 3);
    // Access C so it gets promoted
    cache.get("C");

    // Insert D — evicts C from small (promoted to main). D in small. A,C in main.
    cache.insert("D".into(), 4);

    // Access A frequently so it has high lives in main
    cache.get("A");
    cache.get("A");

    // Now main has A (high lives) and C. Fill up to trigger main eviction.
    // Access D so it gets promoted
    cache.get("D");
    cache.insert("E".into(), 5); // evicts D from small → main, triggers main eviction

    // A should survive FIFO-Reinsertion because it has lives
    assert!(
        cache.contains_key("A"),
        "Frequently accessed A should survive main eviction via FIFO-Reinsertion"
    );
}

#[test]
fn test_eviction_after_remove_tombstones() {
    // small=3, main=7 → total 10
    let mut cache: S3FifoCache<String, u32> = S3FifoCache::new(3, 7);

    // Fill cache
    for i in 0..10 {
        cache.insert(format!("key_{i}"), i as u32);
    }
    assert_eq!(cache.len(), 10);

    // Remove several entries (creates tombstones in queues)
    cache.remove(&"key_0".into());
    cache.remove(&"key_1".into());
    cache.remove(&"key_2".into());
    assert_eq!(cache.len(), 7);

    // Insert new entries — should not panic, should handle tombstones gracefully
    for i in 10..15 {
        cache.insert(format!("key_{i}"), i as u32);
    }

    assert!(
        cache.len() <= cache.max_len(),
        "Cache len {} exceeds max_len {}",
        cache.len(),
        cache.max_len()
    );
}

#[test]
fn test_eviction_after_retain_tombstones() {
    // small=3, main=7 → total 10
    let mut cache: S3FifoCache<String, u32> = S3FifoCache::new(3, 7);

    // Fill cache
    for i in 0..10 {
        cache.insert(format!("key_{i}"), i as u32);
    }
    assert_eq!(cache.len(), 10);

    // Retain only even keys (removes ~half, creates tombstones)
    cache.retain(|key, _| {
        let num: usize = key.strip_prefix("key_").unwrap().parse().unwrap();
        num.is_multiple_of(2)
    });
    assert_eq!(cache.len(), 5);

    // Insert new entries on top of tombstones
    for i in 20..30 {
        cache.insert(format!("key_{i}"), i as u32);
    }

    assert!(
        cache.len() <= cache.max_len(),
        "Cache len {} exceeds max_len {}",
        cache.len(),
        cache.max_len()
    );
}

#[test]
fn test_no_panic_on_main_reinsertion() {
    // Regression test for Bug 1: pop_from_main_impl must loop.
    // With small=1 and main=3, fill main with accessed items, then trigger eviction.
    let mut cache: S3FifoCache<String, u32> = S3FifoCache::new(1, 3);

    // Insert and promote items into main
    cache.insert("A".into(), 1);
    cache.get("A"); // give lives
    cache.insert("B".into(), 2); // A promoted to main
    cache.get("B");
    cache.insert("C".into(), 3); // B promoted to main
    cache.get("C");
    cache.insert("D".into(), 4); // C promoted to main, main now full with [A,B,C] all having lives

    // All items in main have lives>0. Triggering main eviction must loop
    // through all of them (decrementing lives) until one reaches 0.
    // Before the fix, this would panic.
    cache.get("D");
    cache.insert("E".into(), 5); // D promoted to main → must evict from main

    // Should not panic, and cache should remain within bounds
    assert!(cache.len() <= cache.max_len());
}

#[test]
fn test_small_queue_filters_one_hit_wonders() {
    // small=2, main=8 → total 10
    let mut cache: S3FifoCache<String, u32> = S3FifoCache::new(2, 8);

    // Insert 20 unique keys, never accessing any of them again (one-hit wonders).
    // They should flow through small and get evicted without reaching main.
    for i in 0..20 {
        cache.insert(format!("onehit_{i}"), i as u32);
    }

    // Now insert some keys and access them so they get promoted to main
    cache.insert("hot_A".into(), 100);
    cache.get("hot_A");
    cache.insert("hot_B".into(), 101);
    // hot_A should have been promoted to main when evicted from small

    // Insert more one-hit wonders
    for i in 20..40 {
        cache.insert(format!("onehit_{i}"), i as u32);
    }

    // hot_A should still be in cache (in main) since it was accessed
    assert!(
        cache.contains_key("hot_A"),
        "Frequently accessed key should survive in main"
    );
    assert!(cache.len() <= cache.max_len());
}

#[test]
fn test_cache_len_invariant() {
    let mut cache: S3FifoCache<String, u32> = S3FifoCache::new(3, 7);

    // Interleave inserts, removes, retains, and evictions
    for i in 0u32..50 {
        cache.insert(format!("key_{i}"), i);
        assert!(
            cache.len() <= cache.max_len(),
            "After insert {i}: len {} > max_len {}",
            cache.len(),
            cache.max_len()
        );

        if i % 7 == 0 {
            cache.remove(&format!("key_{i}"));
        }
        if i % 13 == 0 {
            cache.retain(|_, v| *v % 3 != 0);
        }
        if i % 5 == 0 {
            // Access some keys to exercise promotion paths
            cache.get(&format!("key_{}", i.saturating_sub(1)));
            cache.get(&format!("key_{}", i.saturating_sub(2)));
        }

        assert!(
            cache.len() <= cache.max_len(),
            "After ops at {i}: len {} > max_len {}",
            cache.len(),
            cache.max_len()
        );
    }
}
