use super::concurrent_map::ConcurrentMap;
use circ::CsHP;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::list::{Cursor, HHSList};

pub struct HashMap<K, V> {
    buckets: Vec<HHSList<K, V>>,
}

impl<K, V> HashMap<K, V>
where
    K: Ord + Hash + Default,
    V: Default,
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

    pub fn get(&self, k: &K, cursor: &mut Cursor<K, V>, cs: &CsHP) -> bool {
        let i = Self::hash(k);
        self.get_bucket(i).get(k, cursor, cs)
    }

    pub fn insert(&self, k: K, v: V, cursor: &mut Cursor<K, V>, cs: &CsHP) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(k, v, cursor, cs)
    }

    pub fn remove(&self, k: &K, cursor: &mut Cursor<K, V>, cs: &CsHP) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(k, cursor, cs)
    }
}

impl<K, V> ConcurrentMap<K, V> for HashMap<K, V>
where
    K: Ord + Hash + Default,
    V: Default,
{
    type Output = Cursor<K, V>;

    fn new() -> Self {
        Self::with_capacity(30000)
    }

    #[inline(always)]
    fn get(&self, key: &K, output: &mut Self::Output, cs: &CsHP) -> bool {
        self.get(key, output, cs)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V, output: &mut Self::Output, cs: &CsHP) -> bool {
        self.insert(key, value, output, cs)
    }
    #[inline(always)]
    fn remove(&self, key: &K, output: &mut Self::Output, cs: &CsHP) -> bool {
        self.remove(key, output, cs)
    }
}

#[cfg(test)]
mod tests {
    // use super::HashMap;
    // use crate::circ_hp::concurrent_map;
    // use circ::CsEBR;

    #[test]
    fn smoke_hashmap() {
        // TODO: Implement CIRCL for HP and uncomment
        // concurrent_map::tests::smoke::<CsHP, HashMap<i32, String, CsHP>>();
    }
}
