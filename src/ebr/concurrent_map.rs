use crossbeam_ebr::Guard;

// TODO: impls in this file & re-export
pub trait ConcurrentMap<K, V> {
    fn new() -> Self;
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V>;
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool;
    fn remove(&self, key: &K, guard: &Guard) -> Option<V>;
}

// TODO: move test codes here
