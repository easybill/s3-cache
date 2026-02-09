use std::collections::HashSet;
use std::{borrow::Borrow, collections::VecDeque, hash::Hash};

/// A FIFO-ordered ghost list that supports O(1) random access and removal.
pub struct GhostList<K> {
    map: HashSet<K>,
    queue: VecDeque<K>,
    max_count: usize,
}

impl<K: Clone + Eq + Hash> GhostList<K> {
    pub fn new(max_count: usize) -> Self {
        Self {
            map: HashSet::new(),
            queue: VecDeque::new(),
            max_count,
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.contains(key)
    }

    pub fn insert(&mut self, key: K) {
        if self.map.contains(&key) {
            return;
        }

        while self.len() >= self.max_count {
            self.evict_oldest();
        }

        self.map.insert(key.clone());
        self.queue.push_front(key);
    }

    pub fn remove<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.remove(key);
    }

    pub fn evict_oldest(&mut self) -> Option<K> {
        while let Some(key) = self.queue.pop_back() {
            if self.map.contains(&key) {
                self.map.remove(&key);
                return Some(key);
            }
        }
        None
    }

    pub fn should_compact(&self) -> bool {
        self.queue.len() > self.map.len() * 2
    }

    pub fn compact(&mut self) {
        if self.should_compact() {
            let mut new_queue = VecDeque::with_capacity(self.map.len());

            for key in self.queue.iter().rev() {
                if self.map.contains(key) {
                    new_queue.push_front(key.clone());
                }
            }

            self.queue = new_queue;
        }
    }
}
