use crate::ds_impl::hp::concurrent_map::ConcurrentMap;
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

    pub fn get<'domain, 'hp>(&self, handle: &'hp mut Handle<'domain>, k: &K) -> Option<&'hp V> {
        let i = Self::hash(k);
        self.get_bucket(i).get(handle, k)
    }

    pub fn insert<'domain, 'hp>(&self, handle: &'hp mut Handle<'domain>, k: K, v: V) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(handle, k, v)
    }

    pub fn remove<'domain, 'hp>(&self, handle: &'hp mut Handle<'domain>, k: &K) -> Option<&'hp V> {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(handle, k)
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
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        self.get(handle, key)
    }
    #[inline(always)]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.insert(handle, key, value)
    }
    #[inline(always)]
    fn remove<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: &K,
    ) -> Option<&'hp V> {
        self.remove(handle, key)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::ds_impl::hp::concurrent_map;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<HashMap<i32, String>>();
    }
}
