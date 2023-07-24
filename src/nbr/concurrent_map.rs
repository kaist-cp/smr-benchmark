use nbr_rs::Guard;

pub trait ConcurrentMap<K, V> {
    fn new() -> Self;
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V>;
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool;
    fn remove<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V>;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::ConcurrentMap;
    use crossbeam_utils::thread;
    use nbr_rs::Collector;
    use rand::prelude::*;
    use std::sync::Arc;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    /// `max_hazptr_per_thread` depends on the data structure.
    pub fn smoke<M: ConcurrentMap<i32, String> + Send + Sync>(max_hazptr_per_thread: usize) {
        let map = &M::new();
        let collector = Arc::new(Collector::new(
            THREADS as usize,
            max_hazptr_per_thread,
            256,
            32,
        ));

        thread::scope(|s| {
            for t in 0..THREADS {
                let collector = Arc::clone(&collector);
                s.spawn(move |_| {
                    let guard = collector.register();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, i.to_string(), &guard));
                    }
                });
            }
        })
        .unwrap();

        let mut collector = Arc::try_unwrap(collector).unwrap_or_else(|_| panic!());
        collector.reset_registrations();
        let collector = Arc::new(collector);

        thread::scope(|s| {
            for t in 0..THREADS {
                let collector = Arc::clone(&collector);
                s.spawn(move |_| {
                    let guard = collector.register();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    if t < THREADS / 2 {
                        for i in keys {
                            assert_eq!(i.to_string(), *map.remove(&i, &guard).unwrap());
                        }
                    } else {
                        for i in keys {
                            assert_eq!(i.to_string(), *map.get(&i, &guard).unwrap());
                        }
                    }
                });
            }
        })
        .unwrap();
    }
}
