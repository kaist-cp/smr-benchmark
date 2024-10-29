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
    fn get(&self, key: &K) -> Option<impl OutputHolder<V>>;
    fn insert(&self, key: K, value: V) -> bool;
    fn remove(&self, key: &K) -> Option<impl OutputHolder<V>>;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::{ConcurrentMap, OutputHolder};
    use crossbeam_utils::thread;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    pub fn smoke<M: ConcurrentMap<i32, String> + Send + Sync>() {
        let map = &M::new();

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, i.to_string()));
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
                        assert_eq!(i.to_string(), *map.remove(&i).unwrap().output());
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
                        assert_eq!(i.to_string(), *map.get(&i).unwrap().output());
                    }
                });
            }
        })
        .unwrap();
    }
}
