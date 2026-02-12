use std::sync::atomic::{AtomicU8, Ordering};

pub struct ValueEntry<V> {
    value: V,
    lives: AtomicU8,
    is_promoted: bool,
}

impl<V> ValueEntry<V> {
    const MAX_LIVES: u8 = 3;

    #[inline]
    pub fn new(value: V) -> Self {
        Self {
            value,
            lives: AtomicU8::new(0),
            is_promoted: false,
        }
    }

    #[inline]
    pub fn promote(&mut self) {
        self.is_promoted = true;
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
    pub(crate) fn lives(&self) -> u8 {
        self.lives.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn add_live(&self) {
        self.lives
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |f| {
                Some(std::cmp::min(f + 1, Self::MAX_LIVES))
            })
            .unwrap();
    }

    #[inline]
    pub(crate) fn remove_live(&self) {
        self.lives.fetch_sub(1, Ordering::AcqRel);
    }

    #[inline]
    pub fn is_promoted(&self) -> bool {
        self.is_promoted
    }
}
