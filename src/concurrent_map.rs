use crossbeam_epoch::Guard;
use std::ops::Deref;

// TODO: impls in this file & re-export
pub trait ConcurrentMap<K, V> {
    type Handle;

    fn new() -> Self;
    fn handle<'g>(guard: &'g Guard) -> Self::Handle;
    fn clear(handle: &mut Self::Handle);

    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &'g K,
        guard: &'g mut Guard,
    ) -> Option<&'g V>;
    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool;
    fn remove(&self, handle: &mut Self::Handle, key: &K, guard: &mut Guard) -> Option<V>;
}

// TODO: move test codes here
