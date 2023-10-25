use super::concurrent_map::ConcurrentMap;
use cdrc_rs::Cs;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::list::{Cursor, HMList};

pub struct HashMap<K, V, C: Cs> {
    buckets: Vec<HMList<K, V, C>>,
}

impl<K, V, C> HashMap<K, V, C>
where
    K: Ord + Hash + Default,
    V: Default,
    C: Cs,
{
    pub fn with_capacity(n: usize) -> Self {
        let mut buckets = Vec::with_capacity(n);
        for _ in 0..n {
            buckets.push(HMList::new());
        }

        HashMap { buckets }
    }

    #[inline]
    pub fn get_bucket(&self, index: usize) -> &HMList<K, V, C> {
        unsafe { self.buckets.get_unchecked(index % self.buckets.len()) }
    }

    // TODO(@jeehoonkang): we're converting u64 to usize, which may lose information.
    #[inline]
    fn hash(k: &K) -> usize {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize
    }

    pub fn get(&self, k: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        let i = Self::hash(k);
        self.get_bucket(i).get(k, cursor, cs)
    }

    pub fn insert(&self, k: K, v: V, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(k, v, cursor, cs)
    }

    pub fn remove(&self, k: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(k, cursor, cs)
    }
}

impl<K, V, C> ConcurrentMap<K, V, C> for HashMap<K, V, C>
where
    K: Ord + Hash + Default,
    V: Default,
    C: Cs,
{
    type Output = Cursor<K, V, C>;

    fn new() -> Self {
        Self::with_capacity(30000)
    }

    #[inline(always)]
    fn get(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        self.get(key, output, cs)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V, output: &mut Self::Output, cs: &C) -> bool {
        self.insert(key, value, output, cs)
    }
    #[inline(always)]
    fn remove(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        self.remove(key, output, cs)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::cdrc::concurrent_map;
    use cdrc_rs::{CsEBR, CsHP};

    #[test]
    fn smoke_hashmap_ebr() {
        concurrent_map::tests::smoke::<CsEBR, HashMap<i32, String, CsEBR>>();
    }

    #[test]
    fn smoke_hashmap_hp() {
        concurrent_map::tests::smoke::<CsHP, HashMap<i32, String, CsHP>>();
    }
}
