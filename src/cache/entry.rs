use std::sync::atomic::{AtomicU8, Ordering};

pub struct ValueEntry<V> {
    value: V,
    counter: AtomicU8,
}

impl<V> ValueEntry<V> {
    const MAX_COUNT: u8 = 3;

    #[inline]
    pub fn new(value: V) -> Self {
        Self {
            value,
            counter: AtomicU8::new(0),
        }
    }

    #[inline]
    pub fn into_value(self) -> V {
        self.value
    }

    #[inline]
    pub fn value(&self) -> &V {
        &self.value
    }

    #[inline]
    pub fn value_mut(&mut self) -> &mut V {
        &mut self.value
    }

    #[inline]
    pub(crate) fn counter(&self) -> u8 {
        self.counter.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn increment_counter(&self) {
        self.counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |f| {
                Some(std::cmp::min(f + 1, Self::MAX_COUNT))
            })
            .unwrap();
    }

    #[inline]
    pub(crate) fn decrement_counter(&self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}
