use super::concurrent_map::ConcurrentMap;
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

    // TODO(@jeehoonkang): we're converting u64 to usize, which may lose information.
    #[inline]
    fn hash(k: &K) -> usize {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize
    }

    pub fn get(&self, k: &K) -> Option<&'static V> {
        let i = Self::hash(k);
        self.get_bucket(i).get(k)
    }

    pub fn insert(&self, k: K, v: V) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(k, v)
    }

    pub fn remove(&self, k: &K) -> Option<&'static V> {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(k)
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
    fn get(&self, key: &K) -> Option<&'static V> {
        self.get(key)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V) -> bool {
        self.insert(key, value)
    }
    #[inline(always)]
    fn remove(&self, key: &K) -> Option<&'static V> {
        self.remove(key)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::nr::concurrent_map;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<HashMap<i32, String>>();
    }
}
