use super::concurrent_map::ConcurrentMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::list::HHSList;
pub use super::list::{Cursor, EraShields, Node};

use crystalline_l::Handle;

pub struct HashMap<K, V> {
    buckets: Vec<HHSList<K, V>>,
}

impl<K, V> HashMap<K, V>
where
    K: Ord + Hash + 'static,
    V: 'static,
{
    pub fn with_capacity<'d, 'h>(n: usize, handle: &'h Handle<'d, Node<K, V>>) -> Self {
        let mut buckets = Vec::with_capacity(n);
        for _ in 0..n {
            buckets.push(HHSList::new(handle));
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
    K: Ord + Hash + Send + 'static,
    V: Send + 'static,
{
    type Node = Node<K, V>;

    type Shields<'d, 'h> = EraShields<'d, 'h, K, V>
    where
        'd: 'h;

    fn shields<'d, 'h>(handle: &'h Handle<'d, Self::Node>) -> Self::Shields<'d, 'h> {
        EraShields::new(handle)
    }

    fn new<'d, 'h>(handle: &'h Handle<'d, Self::Node>) -> Self {
        Self::with_capacity(30000, handle)
    }

    #[inline(always)]
    fn get<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V> {
        let i = Self::hash(key);
        self.get_bucket(i).get(key, shields, handle)
    }

    #[inline(always)]
    fn insert<'d, 'h>(
        &self,
        key: K,
        value: V,
        shields: &mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> bool {
        let i = Self::hash(&key);
        self.get_bucket(i).insert(key, value, shields, handle)
    }

    #[inline(always)]
    fn remove<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V> {
        let i = Self::hash(key);
        self.get_bucket(i).remove(key, shields, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::ds_impl::crystalline_l::concurrent_map;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<HashMap<i32, String>>();
    }
}
