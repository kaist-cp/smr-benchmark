use circ::CsHP;

pub trait OutputHolder<V> {
    fn default() -> Self;
    fn output(&self) -> &V;
}

pub trait ConcurrentMap<K, V> {
    type Output: OutputHolder<V>;

    fn empty_output() -> Self::Output {
        <Self::Output as OutputHolder<V>>::default()
    }

    fn new() -> Self;
    fn get(&self, key: &K, output: &mut Self::Output, cs: &CsHP) -> bool;
    fn insert(&self, key: K, value: V, output: &mut Self::Output, cs: &CsHP) -> bool;
    fn remove(&self, key: &K, output: &mut Self::Output, cs: &CsHP) -> bool;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::{ConcurrentMap, OutputHolder};
    use circ::{Cs, CsHP};
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
                    let output = &mut M::empty_output();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, to_value(&i), output, &CsHP::new()));
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in 0..(THREADS / 2) {
                s.spawn(move |_| {
                    let output = &mut M::empty_output();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    let cs = &mut CsHP::new();
                    for i in keys {
                        assert!(map.remove(&i, output, cs));
                        assert_eq!(to_value(&i), *output.output());
                        cs.clear();
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in (THREADS / 2)..THREADS {
                s.spawn(move |_| {
                    let output = &mut M::empty_output();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    let cs = &mut CsHP::new();
                    for i in keys {
                        assert!(map.get(&i, output, cs));
                        assert_eq!(to_value(&i), *output.output());
                        cs.clear();
                    }
                });
            }
        })
        .unwrap();
    }
}
