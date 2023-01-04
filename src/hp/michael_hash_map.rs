use super::concurrent_map::ConcurrentMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub use super::list::Cursor;
use super::list::HMList;

pub struct HashMap<K, V>
where
    K: Send,
    V: Send,
{
    buckets: Vec<HMList<K, V>>,
}

impl<K, V> HashMap<K, V>
where
    K: Ord + Hash + Send,
    V: Send,
{
    pub fn with_capacity(n: usize) -> Self {
        let mut buckets = Vec::with_capacity(n);
        for _ in 0..n {
            buckets.push(HMList::new());
        }

        HashMap { buckets }
    }

    #[inline]
    pub fn get_bucket(&self, index: usize) -> &HMList<K, V> {
        unsafe { self.buckets.get_unchecked(index % self.buckets.len()) }
    }

    // TODO(@jeehoonkang): we're converting u64 to usize, which may lose information.
    #[inline]
    fn hash(k: &K) -> usize {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize
    }

    pub fn get<'g, 'domain>(
        &'g self,
        cursor: &'g mut Cursor<'domain, K, V>,
        k: &'g K,
    ) -> Option<&'g V> {
        let i = Self::hash(k);
        self.get_bucket(i).get(cursor, k)
    }

    pub fn insert<'g, 'domain>(
        &'g self,
        cursor: &'g mut Cursor<'domain, K, V>,
        k: K,
        v: V,
    ) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(cursor, k, v)
    }

    pub fn remove<'g, 'domain>(
        &'g self,
        cursor: &'g mut Cursor<'domain, K, V>,
        k: &K,
    ) -> Option<V> {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(cursor, k)
    }
}

impl<K, V> ConcurrentMap<K, V> for HashMap<K, V>
where
    K: Ord + Hash + Send,
    V: Send,
{
    type Handle<'domain> = Cursor<'domain, K, V>;

    fn new() -> Self {
        Self::with_capacity(30000)
    }

    fn handle<'domain>() -> Self::Handle<'domain> {
        Cursor::new()
    }

    fn clear<'domain>(handle: &mut Self::Handle<'domain>) {
        handle.release();
    }

    #[inline]
    fn get<'g, 'domain>(
        &'g self,
        handle: &'g mut Self::Handle<'domain>,
        key: &'g K,
    ) -> Option<&'g V> {
        self.get(handle, key)
    }
    #[inline]
    fn insert<'g, 'domain>(
        &'g self,
        handle: &'g mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.insert(handle, key, value)
    }
    #[inline]
    fn remove<'g, 'domain>(&'g self, handle: &'g mut Self::Handle<'domain>, key: &K) -> Option<V> {
        self.remove(handle, key)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crate::hp::concurrent_map;

    #[test]
    fn smoke_hashmap() {
        concurrent_map::tests::smoke::<HashMap<i32, String>>();
    }
}
