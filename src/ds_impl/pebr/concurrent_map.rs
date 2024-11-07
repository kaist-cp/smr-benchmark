use crossbeam_pebr::Guard;

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
    type Handle;

    fn new() -> Self;
    fn handle<'g>(guard: &'g Guard) -> Self::Handle;
    fn clear(handle: &mut Self::Handle);

    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &'g K,
        guard: &'g mut Guard,
    ) -> Option<impl OutputHolder<V>>;
    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool;
    fn remove(
        &self,
        handle: &mut Self::Handle,
        key: &K,
        guard: &mut Guard,
    ) -> Option<impl OutputHolder<V>>;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::{ConcurrentMap, OutputHolder};
    use crossbeam_pebr::pin;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    pub fn smoke<M: ConcurrentMap<i32, String> + Send + Sync>() {
        let map = &M::new();

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    let mut handle = M::handle(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(&mut handle, i, i.to_string(), &mut pin()));
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in 0..(THREADS / 2) {
                s.spawn(move |_| {
                    let mut handle = M::handle(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            *map.remove(&mut handle, &i, &mut pin()).unwrap().output()
                        );
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in (THREADS / 2)..THREADS {
                s.spawn(move |_| {
                    let mut handle = M::handle(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            *map.get(&mut handle, &i, &mut pin()).unwrap().output()
                        );
                    }
                });
            }
        })
        .unwrap();
    }
}
