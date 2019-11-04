use crossbeam_ebr::Guard;

// TODO: impls in this file & re-export
pub trait ConcurrentMap<K, V> {
    fn new() -> Self;
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V>;
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool;
    fn remove(&self, key: &K, guard: &Guard) -> Option<V>;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::ConcurrentMap;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    pub fn smoke<M: ConcurrentMap<i32, String> + Send + Sync>() {
        let map = &M::new();

        // insert
        thread::scope(|s| {
            for t in 0..40 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..1000).map(|k| k * 40 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, i.to_string(), &crossbeam_ebr::pin()));
                    }
                });
            }
        })
        .unwrap();

        // remove
        thread::scope(|s| {
            for t in 0..20 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..1000).map(|k| k * 40 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            map.remove(&i, &crossbeam_ebr::pin()).unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();

        // get
        thread::scope(|s| {
            for t in 20..40 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..1000).map(|k| k * 40 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(i.to_string(), *map.get(&i, &crossbeam_ebr::pin()).unwrap());
                    }
                });
            }
        })
        .unwrap();
    }
}
