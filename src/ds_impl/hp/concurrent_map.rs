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
    type Handle<'domain>;

    fn new() -> Self;

    fn handle() -> Self::Handle<'static>;

    fn get<'hp>(
        &'hp self,
        handle: &'hp mut Self::Handle<'_>,
        key: &'hp K,
    ) -> Option<impl OutputHolder<V>>;

    fn insert(&self, handle: &mut Self::Handle<'_>, key: K, value: V) -> bool;

    fn remove<'hp>(
        &'hp self,
        handle: &'hp mut Self::Handle<'_>,
        key: &'hp K,
    ) -> Option<impl OutputHolder<V>>;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::{ConcurrentMap, OutputHolder};
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
                    let mut handle = M::handle();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(&mut handle, i, to_value(&i)));
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in 0..(THREADS / 2) {
                s.spawn(move |_| {
                    let mut handle = M::handle();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(to_value(&i), *map.remove(&mut handle, &i).unwrap().output());
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in (THREADS / 2)..THREADS {
                s.spawn(move |_| {
                    let mut handle = M::handle();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(to_value(&i), *map.get(&mut handle, &i).unwrap().output());
                    }
                });
            }
        })
        .unwrap();
    }
}
