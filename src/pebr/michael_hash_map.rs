use super::concurrent_map::ConcurrentMap;
use crossbeam_pebr::Guard;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub use super::harris_michael_list::Cursor;
use super::harris_michael_list::List;

pub struct HashMap<K, V> {
    buckets: Vec<List<K, V>>,
}

impl<K, V> HashMap<K, V>
where
    K: Ord + Hash,
{
    pub fn with_capacity(n: usize) -> Self {
        let mut buckets = Vec::with_capacity(n);
        for _ in 0..n {
            buckets.push(List::new());
        }

        HashMap { buckets }
    }

    #[inline]
    pub fn get_bucket(&self, index: usize) -> &List<K, V> {
        unsafe { self.buckets.get_unchecked(index % self.buckets.len()) }
    }

    // TODO(@jeehoonkang): we're converting u64 to usize, which may lose information.
    #[inline]
    fn hash(k: &K) -> usize {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize
    }

    pub fn get<'g>(
        &'g self,
        cursor: &'g mut Cursor<K, V>,
        k: &K,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        let i = Self::hash(k);
        self.get_bucket(i).get(cursor, k, guard)
    }

    pub fn insert(&self, cursor: &mut Cursor<K, V>, k: K, v: V, guard: &mut Guard) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(cursor, k, v, guard)
    }

    pub fn remove(&self, cursor: &mut Cursor<K, V>, k: &K, guard: &mut Guard) -> Option<V> {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(cursor, k, guard)
    }
}

impl<K, V> ConcurrentMap<K, V> for HashMap<K, V>
where
    K: Ord + Hash,
{
    type Handle = Cursor<K, V>;

    fn new() -> Self {
        Self::with_capacity(30000)
    }

    fn handle(guard: &Guard) -> Self::Handle {
        Cursor::new(guard)
    }

    fn clear(handle: &mut Self::Handle) {
        handle.release();
    }

    #[inline]
    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &'g K,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        self.get(handle, key, guard)
    }
    #[inline]
    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool {
        self.insert(handle, key, value, guard)
    }
    #[inline]
    fn remove(&self, handle: &mut Self::Handle, key: &K, guard: &mut Guard) -> Option<V> {
        self.remove(handle, key, guard)
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    use super::{Cursor, HashMap};
    use crossbeam_pebr::pin;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    #[test]
    fn smoke_hashmap() {
        let hash_map = &HashMap::with_capacity(10000);

        // insert
        thread::scope(|s| {
            for t in 0..10 {
                s.spawn(move |_| {
                    let mut cursor = Cursor::new(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(hash_map.insert(
                            &mut cursor,
                            i,
                            i.to_string(),
                            &mut crossbeam_pebr::pin()
                        ));
                    }
                });
            }
        })
        .unwrap();

        // remove
        thread::scope(|s| {
            for t in 0..5 {
                s.spawn(move |_| {
                    let mut cursor = Cursor::new(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            hash_map
                                .remove(&mut cursor, &i, &mut crossbeam_pebr::pin())
                                .unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();

        // get
        thread::scope(|s| {
            for t in 5..10 {
                s.spawn(move |_| {
                    let mut cursor = Cursor::new(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            *hash_map
                                .get(&mut cursor, &i, &mut crossbeam_pebr::pin())
                                .unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();
    }
}
