use vbr_rs::{Global, Local};

use super::concurrent_map::ConcurrentMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::list::{HHSList, Node};

pub struct HashMap<K, V>
where
    K: 'static + Ord + Hash + Copy,
    V: 'static + Copy,
{
    buckets: Vec<HHSList<K, V>>,
}

impl<K, V> HashMap<K, V>
where
    K: 'static + Ord + Hash + Copy,
    V: 'static + Copy,
{
    pub fn with_capacity(n: usize, local: &Local<Node<K, V>>) -> Self {
        let mut buckets = Vec::with_capacity(n);
        for _ in 0..n {
            buckets.push(HHSList::new(local));
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

    pub fn get<'g>(&'g self, k: &'g K, local: &Local<Node<K, V>>) -> Option<V> {
        let i = Self::hash(k);
        self.get_bucket(i).get(k, local)
    }

    pub fn insert(&self, k: K, v: V, local: &Local<Node<K, V>>) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(k, v, local)
    }

    pub fn remove<'g>(&'g self, k: &'g K, local: &Local<Node<K, V>>) -> Option<V> {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(k, local)
    }
}

impl<K, V> ConcurrentMap<K, V> for HashMap<K, V>
where
    K: 'static + Ord + Hash + Copy,
    V: 'static + Copy,
{
    type Global = Global<Node<K, V>>;

    type Local = Local<Node<K, V>>;

    fn global(key_range_hint: usize) -> Self::Global {
        Global::new(key_range_hint)
    }

    fn local(global: &Self::Global) -> Self::Local {
        Local::new(global)
    }

    fn new(local: &Self::Local) -> Self {
        Self::with_capacity(30000, local)
    }

    fn get(&self, key: &K, local: &Self::Local) -> Option<V> {
        self.get(key, local)
    }

    fn insert(&self, key: K, value: V, local: &Self::Local) -> bool {
        self.insert(key, value, local)
    }

    fn remove(&self, key: &K, local: &Self::Local) -> Option<V> {
        self.remove(key, local)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::ds_impl::vbr::concurrent_map;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<HashMap<i32, i32>>();
    }
}
