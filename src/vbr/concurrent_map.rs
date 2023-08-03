pub trait ConcurrentMap<K, V> {
    type Global: Sync;
    type Local;

    fn global(key_range_hint: usize) -> Self::Global;
    fn local(global: &Self::Global) -> Self::Local;
    fn new(local: &Self::Local) -> Self;
    fn get(&self, key: &K, local: &Self::Local) -> Option<V>;
    fn insert(&self, key: K, value: V, local: &Self::Local) -> bool;
    fn remove(&self, key: &K, local: &Self::Local) -> Option<V>;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::ConcurrentMap;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    pub fn smoke<M: ConcurrentMap<i32, i32> + Send + Sync>() {
        let global = &M::global(30 * 1000);
        let local = &M::local(global);
        let map = &M::new(local);

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    let local = &M::local(global);
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, i, local));
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in 0..(THREADS / 2) {
                s.spawn(move |_| {
                    let local = &M::local(global);
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(i, map.remove(&i, local).unwrap());
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in (THREADS / 2)..THREADS {
                s.spawn(move |_| {
                    let local = &M::local(global);
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(i, map.get(&i, local).unwrap());
                    }
                });
            }
        })
        .unwrap();
    }
}
