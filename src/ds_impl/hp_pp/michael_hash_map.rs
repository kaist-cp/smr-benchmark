use crate::ds_impl::hp::concurrent_map::{ConcurrentMap, OutputHolder};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::list::HHSList;
pub use super::list::{Cursor, Handle};

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
}

impl<K, V> ConcurrentMap<K, V> for HashMap<K, V>
where
    K: Ord + Hash + Send,
    V: Send,
{
    type Handle<'domain> = Handle<'domain>;

    fn new() -> Self {
        Self::with_capacity(30000)
    }

    fn handle() -> Self::Handle<'static> {
        Handle::default()
    }

    #[inline(always)]
    fn get<'hp>(
        &'hp self,
        handle: &'hp mut Self::Handle<'_>,
        key: &'hp K,
    ) -> Option<impl OutputHolder<V>> {
        let i = Self::hash(key);
        self.get_bucket(i).get(handle, key)
    }
    #[inline(always)]
    fn insert(&self, handle: &mut Self::Handle<'_>, key: K, value: V) -> bool {
        let i = Self::hash(&key);
        self.get_bucket(i).insert(handle, key, value)
    }
    #[inline(always)]
    fn remove<'hp>(
        &'hp self,
        handle: &'hp mut Self::Handle<'_>,
        key: &'hp K,
    ) -> Option<impl OutputHolder<V>> {
        let i = Self::hash(key);
        self.get_bucket(i).remove(handle, key)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::ds_impl::hp::concurrent_map;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<_, HashMap<i32, String>, _>(&i32::to_string);
    }
}
