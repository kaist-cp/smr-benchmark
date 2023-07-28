use super::concurrent_map::ConcurrentMap;
use super::list::Output;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::HHSList;

pub struct HashMap<K, V> {
    buckets: Vec<HHSList<K, V>>,
}

impl<K, V> HashMap<K, V>
where
    K: Ord + Default + Hash,
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

    #[inline]
    pub fn get(&self, k: &K, output: &mut Output<K, V>, thread: &mut hp_sharp::Thread) -> bool {
        let i = Self::hash(k);
        self.get_bucket(i).get(k, output, thread)
    }

    #[inline]
    pub fn insert(
        &self,
        k: K,
        v: V,
        output: &mut Output<K, V>,
        thread: &mut hp_sharp::Thread,
    ) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(k, v, output, thread)
    }

    #[inline]
    pub fn remove(&self, k: &K, output: &mut Output<K, V>, thread: &mut hp_sharp::Thread) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(k, output, thread)
    }
}

impl<K, V> ConcurrentMap<K, V> for HashMap<K, V>
where
    K: Ord + Default + Hash,
    V: Default,
{
    type Output = Output<K, V>;

    #[inline]
    fn new() -> Self {
        Self::with_capacity(30000)
    }

    #[inline]
    fn get(&self, key: &K, output: &mut Output<K, V>, thread: &mut hp_sharp::Thread) -> bool {
        self.get(key, output, thread)
    }
    #[inline]
    fn insert(
        &self,
        key: K,
        value: V,
        output: &mut Output<K, V>,
        thread: &mut hp_sharp::Thread,
    ) -> bool {
        self.insert(key, value, output, thread)
    }
    #[inline]
    fn remove(&self, key: &K, output: &mut Output<K, V>, thread: &mut hp_sharp::Thread) -> bool {
        self.remove(key, output, thread)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::hp_sharp::concurrent_map;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<HashMap<i32, String>>();
    }
}
