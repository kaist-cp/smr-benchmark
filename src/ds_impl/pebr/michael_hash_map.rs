use super::concurrent_map::{ConcurrentMap, OutputHolder};
use crossbeam_pebr::Guard;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub use super::list::Cursor;
use super::list::HHSList;

pub struct HashMap<K, V> {
    buckets: Vec<HHSList<K, V>>,
}

impl<K, V> HashMap<K, V>
where
    K: Ord + Hash + Clone,
{
    pub fn with_capacity(n: usize) -> Self {
        let mut buckets = Vec::with_capacity(n);
        for _ in 0..n {
            buckets.push(HHSList::new());
        }

        HashMap { buckets }
    }

    #[inline]
    pub fn get_bucket(&self, index: usize) -> &HHSList<K, V> {
        unsafe { self.buckets.get_unchecked(index % self.buckets.len()) }
    }

    // TODO(@jeehoonkang): we're converting u64 to usize, which may lose information.
    #[inline]
    fn hash(k: &K) -> usize {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize
    }
}

impl<K, V> ConcurrentMap<K, V> for HashMap<K, V>
where
    K: Ord + Hash + Clone,
{
    type Handle = Cursor<K, V>;

    fn new() -> Self {
        Self::with_capacity(30000)
    }

    fn handle(guard: &Guard) -> Self::Handle {
        Cursor::new(guard)
    }

    fn clear(handle: &mut Self::Handle) {
        handle.release();
    }

    #[inline(always)]
    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &'g K,
        guard: &'g mut Guard,
    ) -> Option<impl OutputHolder<V>> {
        let i = Self::hash(key);
        self.get_bucket(i).get(handle, key, guard)
    }
    #[inline(always)]
    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool {
        let i = Self::hash(&key);
        self.get_bucket(i).insert(handle, key, value, guard)
    }
    #[inline(always)]
    fn remove(
        &self,
        handle: &mut Self::Handle,
        key: &K,
        guard: &mut Guard,
    ) -> Option<impl OutputHolder<V>> {
        let i = Self::hash(&key);
        self.get_bucket(i).remove(handle, key, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::ds_impl::pebr::concurrent_map;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<_, HashMap<i32, String>, _>(&i32::to_string);
    }
}
