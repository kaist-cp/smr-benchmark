use crossbeam_epoch::Guard;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::harris_michael_list::List;

pub struct HashMap<K, V> {
    buckets: Vec<List<K, V>>,
}

impl<K, V> Drop for HashMap<K, V> {
    fn drop(&mut self) {
        unimplemented!()
    }
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
        unsafe { self.buckets.get_unchecked(index & self.buckets.len()) }
    }

    // TODO(@jeehoonkang): we're converting u64 to usize, which may lose information.
    #[inline]
    fn hash(k: &K) -> usize {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize
    }

    pub fn get<'g>(&'g self, k: &K, guard: &'g Guard) -> Option<&'g V> {
        let i = Self::hash(k);
        self.get_bucket(i).search(k, guard)
    }

    pub fn insert(&self, k: K, v: V) -> bool {
        let i = Self::hash(&k);
        let guard = crossbeam_epoch::pin();
        self.get_bucket(i).insert(k, v, &guard)
    }

    pub fn remove(&self, k: &K) -> Option<V> {
        let i = Self::hash(&k);
        let guard = crossbeam_epoch::pin();
        self.get_bucket(i).remove(k, &guard)
    }
}

#[cfg(test)]
mod tests {
    use super::HashMap;
    use crossbeam_utils::thread;

    #[test]
    fn smoke() {
        let hash_map = &HashMap::with_capacity(100);
        thread::scope(|s| {
            for t in 0..100 {
                s.spawn(move |_| {
                    for i in 0..1000 {
                        hash_map.insert(i, (i, t));
                    }
                });
            }
        })
        .unwrap();

        let guard = crossbeam_epoch::pin();
        assert_eq!(hash_map.get(&1, &guard).unwrap().0, 1);
        assert_eq!(hash_map.remove(&1).unwrap().0, 1);
        assert_eq!(hash_map.get(&1, &guard), None);
    }
}
