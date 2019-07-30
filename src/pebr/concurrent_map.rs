use crossbeam_pebr::Guard;
use std::ops::Deref;

// TODO: impls in this file & re-export
pub trait ConcurrentMap<K, V> {
    type VRef: Deref<Target = V>;
    fn new() -> Self;
    fn get<'g>(&'g self, key: &'g K, guard: &'g mut Guard) -> Option<Self::VRef>;
    fn insert(&self, key: K, value: V, guard: &mut Guard) -> bool;
    fn remove(&self, key: &K, guard: &mut Guard) -> Option<V>;
}

// TODO: move test codes here
