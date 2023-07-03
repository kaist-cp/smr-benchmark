use hp_sharp::Handle;

pub trait OutputHolder<V> {
    fn default(handle: &mut Handle) -> Self;
    fn output(&self) -> &V;
}

pub trait ConcurrentMap<K, V> {
    type Output: OutputHolder<V>;

    fn empty_output(handle: &mut Handle) -> Self::Output {
        <Self::Output as OutputHolder<V>>::default(handle)
    }

    fn new() -> Self;
    fn get(&self, key: &K, output: &mut Self::Output, handle: &mut Handle) -> bool;
    fn insert(&self, key: K, value: V, output: &mut Self::Output, handle: &mut Handle) -> bool;
    fn remove<'domain, 'hp>(&self, key: &K, output: &mut Self::Output, handle: &mut Handle)
        -> bool;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::{ConcurrentMap, OutputHolder};
    use crossbeam_utils::thread;
    use hp_sharp::HANDLE;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    pub fn smoke<M: ConcurrentMap<i32, String> + Send + Sync>() {
        let map = &M::new();

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    HANDLE.with(|handle| {
                        let handle = &mut **handle.borrow_mut();
                        let output = &mut M::empty_output(handle);
                        let mut rng = rand::thread_rng();
                        let mut keys: Vec<i32> =
                            (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                        keys.shuffle(&mut rng);
                        for i in keys {
                            assert!(map.insert(i, i.to_string(), output, handle));
                        }
                    });
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in 0..(THREADS / 2) {
                s.spawn(move |_| {
                    HANDLE.with(|handle| {
                        let handle = &mut **handle.borrow_mut();
                        let output = &mut M::empty_output(handle);
                        let mut rng = rand::thread_rng();
                        let mut keys: Vec<i32> =
                            (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                        keys.shuffle(&mut rng);
                        for i in keys {
                            assert!(map.remove(&i, output, handle));
                            assert_eq!(i.to_string(), *output.output());
                        }
                    });
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in (THREADS / 2)..THREADS {
                s.spawn(move |_| {
                    HANDLE.with(|handle| {
                        let handle = &mut **handle.borrow_mut();
                        let output = &mut M::empty_output(handle);
                        let mut rng = rand::thread_rng();
                        let mut keys: Vec<i32> =
                            (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                        keys.shuffle(&mut rng);
                        for i in keys {
                            assert!(map.get(&i, output, handle));
                            assert_eq!(i.to_string(), *output.output());
                        }
                    });
                });
            }
        })
        .unwrap();
    }
}
