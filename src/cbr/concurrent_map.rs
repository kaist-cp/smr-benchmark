use crossbeam_cbr::EpochGuard;

pub trait Shields<V> {
    fn default(guard: &EpochGuard) -> Self;
    fn result_value(&self) -> &V;
}

pub trait ConcurrentMap<K, V> {
    type Handle: Shields<V>;

    fn new() -> Self;
    fn get(&self, key: &K, handle: &mut Self::Handle, guard: &mut EpochGuard) -> bool;
    fn insert(&self, key: K, value: V, handle: &mut Self::Handle, guard: &mut EpochGuard) -> bool;
    fn remove(&self, key: &K, handle: &mut Self::Handle, guard: &mut EpochGuard) -> bool;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::Shields;
    use super::ConcurrentMap;
    use crossbeam_cbr::pin;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    pub fn smoke<M: ConcurrentMap<i32, String> + Send + Sync>() {
        let map = &M::new();

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    let mut handle = M::Handle::default(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, i.to_string(), &mut handle, &mut pin()));
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in 0..(THREADS / 2) {
                s.spawn(move |_| {
                    let mut handle = M::Handle::default(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    let mut guard = pin();
                    for i in keys {
                        assert!(map.remove(&i, &mut handle, &mut guard));
                        assert_eq!(i.to_string(), *handle.result_value());
                        guard.repin();
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in (THREADS / 2)..THREADS {
                s.spawn(move |_| {
                    let mut handle = M::Handle::default(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    let mut guard = pin();
                    for i in keys {
                        assert!(map.get(&i, &mut handle, &mut guard));
                        assert_eq!(i.to_string(), *handle.result_value());
                        guard.repin();
                    }
                });
            }
        })
        .unwrap();
    }
}
