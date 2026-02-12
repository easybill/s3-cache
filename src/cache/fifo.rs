use std::collections::VecDeque;

pub struct FiFoQueue<K> {
    queue: VecDeque<K>,
    max_len: usize,
}

impl<K> FiFoQueue<K> {
    #[inline]
    pub fn new(max_len: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(max_len),
            max_len,
        }
    }

    #[inline]
    pub fn max_len(&self) -> usize {
        self.max_len
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= self.max_len()
    }

    #[inline]
    pub fn push(&mut self, key: K) -> Option<K> {
        let evicted = if self.is_full() { self.pop() } else { None };

        self.queue.push_front(key);

        evicted
    }

    /// Push without auto-eviction. Allows the queue to temporarily exceed `max_len`.
    /// The caller is responsible for draining the queue back to its target size.
    #[inline]
    pub fn push_force(&mut self, key: K) {
        self.queue.push_front(key);
    }

    #[inline]
    pub fn pop(&mut self) -> Option<K> {
        self.queue.pop_back()
    }
}
