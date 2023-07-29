use super::concurrent_map::ConcurrentMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::list::HHSList;

pub struct HashMap<K, V> {
    buckets: Vec<HHSList<K, V>>,
}

impl<K, V> HashMap<K, V>
where
    K: Ord + Hash,
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

    pub fn get<'g>(&'g self, k: &'g K) -> Option<&'g V> {
        let i = Self::hash(k);
        self.get_bucket(i).get(k)
    }

    pub fn insert(&self, k: K, v: V) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(k, v)
    }

    pub fn remove<'g>(&'g self, k: &'g K) -> Option<&'g V> {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(k)
    }
}

impl<K, V> ConcurrentMap<K, V> for HashMap<K, V>
where
    K: Ord + Hash,
{
    fn new() -> Self {
        Self::with_capacity(30000)
    }

    #[inline(never)]
    fn get<'g>(&'g self, key: &'g K) -> Option<&'g V> {
        self.get(key)
    }
    #[inline(never)]
    fn insert(&self, key: K, value: V) -> bool {
        self.insert(key, value)
    }
    #[inline(never)]
    fn remove<'g>(&'g self, key: &'g K) -> Option<&'g V> {
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
