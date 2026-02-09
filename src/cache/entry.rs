use std::sync::atomic::AtomicU8;

pub struct ValueEntry<V> {
    pub value: V,
    pub freq: AtomicU8,
    pub size: usize,
}

impl<V> ValueEntry<V> {
    pub fn new(value: V, size: usize) -> Self {
        Self {
            value,
            freq: AtomicU8::new(0),
            size,
        }
    }
}
