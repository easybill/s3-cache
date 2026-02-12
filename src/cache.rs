use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

pub use self::async_fifo::*;
pub use self::entry::ValueEntry;
pub use self::fifo::*;
pub use self::ghost_list::*;
pub use self::key::CacheKey;
pub use self::object::CachedObject;

mod async_fifo;
mod entry;
mod fifo;
mod ghost_list;
mod key;
mod object;

type ValuesMap<K, V> = HashMap<K, ValueEntry<V>>;

/// S3-FIFO cache: a cache that uses the S3-FIFO eviction strategy.
pub struct S3FiFoCache<K, V> {
    values: ValuesMap<K, V>,

    small: FiFoQueue<K>,
    main: FiFoQueue<K>,
    ghost: GhostList<K>,
}

impl<K: Clone + Eq + Hash, V> S3FiFoCache<K, V> {
    pub fn with_max_len(max_len: usize) -> Self {
        let max_small_len: usize = match max_len {
            0 => 0,
            1 => 1,
            2..=10 => max_len / 2,
            11..=20 => max_len / 5,
            21.. => max_len / 10,
        };
        let max_main_len: usize = max_len - max_small_len;

        Self::new(max_small_len, max_main_len)
    }

    pub fn new(max_small_len: usize, max_main_len: usize) -> Self {
        let values = HashMap::new();

        let max_ghost_len = max_main_len;

        let small = FiFoQueue::new(max_small_len);
        let main = FiFoQueue::new(max_main_len);
        let ghost = GhostList::new(max_ghost_len);

        Self {
            values,
            small,
            main,
            ghost,
        }
    }

    #[inline]
    pub fn max_len(&self) -> usize {
        self.small.max_len() + self.main.max_len()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.small.is_empty() && self.main.is_empty()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() == self.max_len()
    }

    pub fn compact(&mut self) {
        self.ghost.compact();
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if let Some(entry) = self.values.get_mut(&key) {
            let old_value = std::mem::replace(entry.value_mut(), value);
            entry.add_live();

            return Some(old_value);
        }

        let mut entry = ValueEntry::new(value);

        // push_force allows the queue to temporarily exceed its target size.
        // The eviction loop below trims the cache back to capacity.
        if self.ghost.contains(&key) {
            entry.promote();
            self.ghost.remove(&key);
            self.main.push_force(key.clone());
        } else {
            self.small.push_force(key.clone());
        }

        self.values.insert(key, entry);

        // Evict until within capacity (matches paper: evict while |S|+|M| > n)
        while self.values.len() > self.max_len() {
            if self.evict().is_none() {
                break;
            }
        }

        None
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.values.get(key).map(|entry| {
            entry.add_live();
            entry.value()
        })
    }

    /// Removes a specific key from the cache. Returns true if the key was present.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let Some(entry) = self.values.remove(key) else {
            return None;
        };

        Some(entry.into_value())
    }

    /// Removes all entries matching the given predicate. Returns count of removed entries.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.values.retain(|key, entry| f(key, entry.value_mut()));
    }

    pub fn evict(&mut self) -> Option<(K, V)> {
        // Per the paper: evict from small when it exceeds its target size, else from main.
        // pop_from_small may promote (return None) instead of evicting, so we loop.
        loop {
            if self.small.len() > self.small.max_len() {
                if let Some(key) = self.pop_from_small() {
                    return self.remove(&key).map(|v| (key, v));
                }
                // Entry was promoted from small to main (not evicted). Retry.
                continue;
            }

            let evicted_key = self.pop_from_main().or_else(|| self.pop_from_small())?;
            return self.remove(&evicted_key).map(|v| (evicted_key, v));
        }
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.values.contains_key(key)
    }

    fn pop_from_small(&mut self) -> Option<K> {
        Self::pop_from_small_impl(
            &mut self.values,
            &mut self.small,
            &mut self.main,
            &mut self.ghost,
        )
    }

    fn pop_from_small_impl(
        values: &mut ValuesMap<K, V>,
        small: &mut FiFoQueue<K>,
        main: &mut FiFoQueue<K>,
        ghost: &mut GhostList<K>,
    ) -> Option<K> {
        loop {
            let key = small.pop()?;

            let Some(entry) = values.get(&key) else {
                continue; // skip tombstone
            };

            if entry.lives() > 0 {
                entry.remove_live();
                // Promote to main. push_force is safe: it never drops items.
                // Main may temporarily exceed its target; the eviction loop
                // in insert()/evict() drains it.
                main.push_force(key);
                return None; // promoted, not evicted
            }

            Self::push_to_ghost_impl(ghost, key.clone());
            return Some(key);
        }
    }

    fn pop_from_main(&mut self) -> Option<K> {
        Self::pop_from_main_impl(&mut self.values, &mut self.main)
    }

    fn pop_from_main_impl(values: &mut ValuesMap<K, V>, main: &mut FiFoQueue<K>) -> Option<K> {
        loop {
            let key = main.pop()?;

            let Some(entry) = values.get(&key) else {
                continue; // skip tombstone
            };

            if entry.lives() > 0 {
                entry.remove_live();
                // FIFO-Reinsertion: re-insert at head. push_force is safe here
                // because we just popped from the same queue, so the net queue
                // length doesn't increase.
                main.push_force(key);
                continue;
            }

            return Some(key);
        }
    }

    fn push_to_ghost_impl(ghost: &mut GhostList<K>, key: K) -> Option<K> {
        let evicted = if ghost.is_full() {
            Self::pop_from_ghost_impl(ghost)
        } else {
            None
        };

        ghost.insert(key);

        evicted
    }

    fn pop_from_ghost_impl(ghost: &mut GhostList<K>) -> Option<K> {
        ghost.evict_oldest()
    }
}

#[cfg(test)]
mod tests;
