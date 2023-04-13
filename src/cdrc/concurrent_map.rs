pub trait ConcurrentMap<K, V, Guard> {
    fn new() -> Self;
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V>;
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool;
    fn remove<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V>;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::ConcurrentMap;
    use cdrc_rs::Handle;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    pub fn smoke<H: Handle, M: ConcurrentMap<i32, String, H::Guard> + Send + Sync>() {
        let map = &M::new();

        unsafe { H::set_max_threads(THREADS as usize) };

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    let handle = H::register();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, i.to_string(), &handle.pin()));
                    }
                });
            }
        })
        .unwrap();

        unsafe { H::reset_registrations() };

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    let handle = H::register();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    if t < THREADS / 2 {
                        for i in keys {
                            assert_eq!(i.to_string(), *map.remove(&i, &handle.pin()).unwrap());
                        }
                    } else {
                        for i in keys {
                            assert_eq!(i.to_string(), *map.get(&i, &handle.pin()).unwrap());
                        }
                    }
                });
            }
        })
        .unwrap();
    }
}
