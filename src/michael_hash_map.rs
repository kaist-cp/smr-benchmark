extern crate crossbeam;

use crossbeam::epoch;

use epoch::Guard;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::harris_michael_list::List;

// TODO: Drop

pub struct MichaelHashMap<K, V> {
    num_buckets: usize,
    buckets: Vec<List<K, V>>,
    // size: AtomicUsize, TODO
}

impl<K, V> MichaelHashMap<K, V>
where
    K: Ord + Hash,
{
    pub fn with_capacity(n: usize) -> Self {
        let mut b = Vec::with_capacity(n);
        for _ in 0..n {
            b.push(List::new());
        }
        MichaelHashMap {
            num_buckets: n,
            buckets: b,
        }
    }

    #[inline]
    fn hash(k: &K) -> usize {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize
    }

    pub fn get<'g, 'a: 'g>(&'a self, k: &K, guard: &'g Guard) -> Option<&'g V> {
        let i = Self::hash(k) % self.num_buckets;
        self.buckets[i].search(k, guard)
    }

    pub fn insert(&self, k: K, v: V) -> bool {
        let i = Self::hash(&k) % self.num_buckets;
        let guard = &epoch::pin();
        self.buckets[i].insert(k, v, guard)
    }

    pub fn remove(&self, k: &K) -> Option<V> {
        let i = Self::hash(&k) % self.num_buckets;
        let guard = &epoch::pin();
        self.buckets[i].remove(k, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::MichaelHashMap;
    use crossbeam::epoch;
    use crossbeam::thread::scope;

    #[test]
    fn test1() {
        let hash_map = &MichaelHashMap::with_capacity(100);
        scope(|s| {
            for t in 0..100 {
                s.spawn(move |_| {
                    for i in 0..1000 {
                        hash_map.insert(i, (i, t));
                    }
                });
            }
        })
        .unwrap();
        let guard = &epoch::pin();
        println!("asdfadsfasfsa");
        println!("{:?}", hash_map.get(&1, guard));
        println!("{:?}", hash_map.remove(&1));
        println!("{:?}", hash_map.get(&1, guard));
    }

}
