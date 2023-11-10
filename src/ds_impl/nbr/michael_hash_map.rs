use super::concurrent_map::ConcurrentMap;
use nbr_rs::Guard;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::list::{HHSList, Handle};

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

    #[inline]
    pub fn get<'g>(&'g self, k: &'g K, handle: &mut Handle, guard: &'g Guard) -> Option<&'g V> {
        let i = Self::hash(k);
        self.get_bucket(i).get(k, handle, guard)
    }

    #[inline]
    pub fn insert(&self, k: K, v: V, handle: &mut Handle, guard: &Guard) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(k, v, handle, guard)
    }

    #[inline]
    pub fn remove<'g>(&'g self, k: &'g K, handle: &mut Handle, guard: &'g Guard) -> Option<&'g V> {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(k, handle, guard)
    }
}

impl<K, V> ConcurrentMap<K, V> for HashMap<K, V>
where
    K: Ord + Hash,
{
    type Handle = Handle;

    fn handle(guard: &mut Guard) -> Self::Handle {
        Self::Handle {
            prev: guard.acquire_shield().unwrap(),
            curr: guard.acquire_shield().unwrap(),
        }
    }

    fn new() -> Self {
        Self::with_capacity(30000)
    }

    #[inline(always)]
    fn get<'g>(&'g self, key: &'g K, handle: &mut Handle, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, handle, guard)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V, handle: &mut Handle, guard: &Guard) -> bool {
        self.insert(key, value, handle, guard)
    }
    #[inline(always)]
    fn remove<'g>(&'g self, key: &'g K, handle: &mut Handle, guard: &'g Guard) -> Option<&'g V> {
        self.remove(key, handle, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::ds_impl::nbr::concurrent_map;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<HashMap<i32, String>>();
    }
}
