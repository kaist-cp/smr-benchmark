use crossbeam_epoch::Guard;

pub trait ConcurrentMap<K, V> {
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V>;
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool;
    fn remove(&self, key: &K, guard: &Guard) -> Option<V>;
}
