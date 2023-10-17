use circ::CsEBR;

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
    fn get(&self, key: &K, output: &mut Self::Output, cs: &CsEBR) -> bool;
    fn insert(&self, key: K, value: V, output: &mut Self::Output, cs: &CsEBR) -> bool;
    fn remove(&self, key: &K, output: &mut Self::Output, cs: &CsEBR) -> bool;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::{ConcurrentMap, OutputHolder};
    use circ::{Cs, CsEBR};
    use crossbeam_utils::thread;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    pub fn smoke<M: ConcurrentMap<i32, String> + Send + Sync>() {
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
                        assert!(map.insert(i, i.to_string(), output, &CsEBR::new()));
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
                    let cs = &mut CsEBR::new();
                    for i in keys {
                        assert!(map.remove(&i, output, cs));
                        assert_eq!(i.to_string(), *output.output());
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
                    let cs = &mut CsEBR::new();
                    for i in keys {
                        assert!(map.get(&i, output, cs));
                        assert_eq!(i.to_string(), *output.output());
                        cs.clear();
                    }
                });
            }
        })
        .unwrap();
    }
}
