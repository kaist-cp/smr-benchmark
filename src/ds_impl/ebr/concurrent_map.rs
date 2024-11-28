use crossbeam_ebr::Guard;

pub trait OutputHolder<V> {
    fn output(&self) -> &V;
}

impl<'g, V> OutputHolder<V> for &'g V {
    fn output(&self) -> &V {
        self
    }
}

impl<V> OutputHolder<V> for V {
    fn output(&self) -> &V {
        self
    }
}

pub trait ConcurrentMap<K, V> {
    fn new() -> Self;
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<impl OutputHolder<V>>;
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool;
    fn remove<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<impl OutputHolder<V>>;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::{ConcurrentMap, OutputHolder};
    use crossbeam_ebr::pin;
    use crossbeam_utils::thread;
    use rand::prelude::*;
    use std::fmt::Debug;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    pub fn smoke<V, M, F>(to_value: &F)
    where
        V: Eq + Debug,
        M: ConcurrentMap<i32, V> + Send + Sync,
        F: Sync + Fn(&i32) -> V,
    {
        let map = &M::new();

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, to_value(&i), &pin()));
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in 0..(THREADS / 2) {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(to_value(&i), *map.remove(&i, &pin()).unwrap().output());
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in (THREADS / 2)..THREADS {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(to_value(&i), *map.get(&i, &pin()).unwrap().output());
                    }
                });
            }
        })
        .unwrap();
    }
}
