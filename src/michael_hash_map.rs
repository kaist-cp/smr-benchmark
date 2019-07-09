use crossbeam_epoch::Guard;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::harris_michael_list::List;

pub struct HashMap<K, V> {
    buckets: Vec<List<K, V>>,
}

impl<K, V> Drop for HashMap<K, V> {
    fn drop(&mut self) {
        for bucket in &mut self.buckets {
            drop(bucket);
        }
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
        unsafe { self.buckets.get_unchecked(index % self.buckets.len()) }
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
        self.get_bucket(i).get(k, guard)
    }

    pub fn insert(&self, k: K, v: V) -> bool {
        let i = Self::hash(&k);
        self.get_bucket(i).insert(k, v)
    }

    pub fn remove(&self, k: &K) -> Option<V> {
        let i = Self::hash(&k);
        self.get_bucket(i).remove(k)
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    use super::HashMap;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    #[test]
    fn smoke_hashmap() {
        let hash_map = &HashMap::with_capacity(10000);
        thread::scope(|s| {
            for t in 0..20 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..2000).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        hash_map.insert(i, (i, t));
                    }
                });
            }
        })
        .unwrap();

        println!("start removal");
        thread::scope(|s| {
            for _ in 0..20 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (1..2000).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        hash_map.remove(&i);
                    }
                });
            }
        })
        .unwrap();
        println!("done");

        let guard = crossbeam_epoch::pin();
        assert_eq!(hash_map.get(&0, &guard).unwrap().0, 0);
        assert_eq!(hash_map.remove(&0).unwrap().0, 0);
        assert_eq!(hash_map.get(&0, &guard), None);
    }
}
