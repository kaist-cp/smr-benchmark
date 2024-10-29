use super::concurrent_map::{ConcurrentMap, OutputHolder};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::list::HHSList;

pub struct HashMap<K, V> {
    buckets: Vec<HHSList<K, V>>,
}

impl<K, V> HashMap<K, V>
where
    K: Ord + Hash + 'static,
    V: 'static,
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

    #[inline]
    fn hash(k: &K) -> usize {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize
    }
}

impl<K, V> ConcurrentMap<K, V> for HashMap<K, V>
where
    K: Ord + Hash + 'static,
    V: 'static,
{
    fn new() -> Self {
        Self::with_capacity(30000)
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<impl OutputHolder<V>> {
        let i = Self::hash(key);
        self.get_bucket(i).get(key)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V) -> bool {
        let i = Self::hash(&key);
        self.get_bucket(i).insert(key, value)
    }
    #[inline(always)]
    fn remove(&self, key: &K) -> Option<impl OutputHolder<V>> {
        let i = Self::hash(key);
        self.get_bucket(i).remove(key)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::ds_impl::nr::concurrent_map;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<HashMap<i32, String>>();
    }
}
