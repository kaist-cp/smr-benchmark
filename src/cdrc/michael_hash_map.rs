use super::concurrent_map::ConcurrentMap;
use cdrc_rs::AcquireRetire;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::list::HHSList;

pub struct HashMap<K, V, Guard>
where
    Guard: AcquireRetire,
{
    buckets: Vec<HHSList<K, V, Guard>>,
}

impl<K, V, Guard> HashMap<K, V, Guard>
where
    K: Ord + Hash + Default,
    V: Default,
    Guard: AcquireRetire,
{
    pub fn with_capacity(n: usize) -> Self {
        let mut buckets = Vec::with_capacity(n);
        for _ in 0..n {
            buckets.push(HHSList::new());
        }

        HashMap { buckets }
    }

    #[inline]
    pub fn get_bucket(&self, index: usize) -> &HHSList<K, V, Guard> {
        unsafe { self.buckets.get_unchecked(index % self.buckets.len()) }
    }

    // TODO(@jeehoonkang): we're converting u64 to usize, which may lose information.
    #[inline]
    fn hash(k: &K) -> usize {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize
    }

    pub fn get<'g>(&'g self, k: &'g K, guard: &'g Guard) -> Option<&'g V> {
        let i = Self::hash(k);
        self.get_bucket(i).get(k, guard)
    }

    pub fn insert(&self, k: K, v: V, guard: &Guard) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(k, v, guard)
    }

    pub fn remove<'g>(&'g self, k: &'g K, guard: &'g Guard) -> Option<&'g V> {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(k, guard)
    }
}

impl<K, V, Guard> ConcurrentMap<K, V, Guard> for HashMap<K, V, Guard>
where
    K: Ord + Hash + Default,
    V: Default,
    Guard: AcquireRetire,
{
    fn new() -> Self {
        Self::with_capacity(30000)
    }

    #[inline]
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, guard)
    }
    #[inline]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.insert(key, value, guard)
    }
    #[inline]
    fn remove<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        self.remove(key, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::cdrc::concurrent_map;
    use cdrc_rs::GuardEBR;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<GuardEBR, HashMap<i32, String, GuardEBR>>();
    }
}
