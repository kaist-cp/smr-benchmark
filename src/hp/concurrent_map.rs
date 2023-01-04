pub trait ConcurrentMap<K, V> {
    type Handle<'domain>;

    fn new() -> Self;
    fn handle<'domain>() -> Self::Handle<'domain>;
    fn clear<'domain>(handle: &mut Self::Handle<'domain>);

    fn get<'g, 'domain>(
        &'g self,
        handle: &'g mut Self::Handle<'domain>,
        key: &'g K,
    ) -> Option<&'g V>;

    fn insert<'g, 'domain>(
        &'g self,
        handle: &'g mut Self::Handle<'domain>,
        key: K,
        value: V
    ) -> bool;

    fn remove<'g, 'domain>(
        &'g self,
        handle: &'g mut Self::Handle<'domain>,
        key: &K
    ) -> Option<V>;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::ConcurrentMap;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    pub fn smoke<M: ConcurrentMap<i32, String> + Send + Sync>() {
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
                        assert!(map.insert(&mut handle, i, i.to_string()));
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
                        assert_eq!(
                            i.to_string(),
                            map.remove(&mut handle, &i).unwrap()
                        );
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
                        assert_eq!(
                            i.to_string(),
                            *map.get(&mut handle, &i).unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();
    }
}
