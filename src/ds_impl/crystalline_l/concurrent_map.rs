use crystalline_l::Handle;

pub trait ConcurrentMap<K, V> {
    type Node;
    type Shields<'d, 'h>
    where
        'd: 'h;

    fn shields<'d, 'h>(handle: &'h Handle<'d, Self::Node>) -> Self::Shields<'d, 'h>;
    fn new<'d, 'h>(handle: &'h Handle<'d, Self::Node>) -> Self;
    fn get<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V>;
    fn insert<'d, 'h>(
        &self,
        key: K,
        value: V,
        shields: &mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> bool;
    fn remove<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V>;
}

#[cfg(test)]
pub mod tests {
    extern crate rand;
    use super::ConcurrentMap;
    use crossbeam_utils::thread;
    use crystalline_l::Domain;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    pub fn smoke<M: ConcurrentMap<i32, String> + Send + Sync>() {
        let domain = &Domain::new((THREADS + 1) as usize);
        // Drop the map before the domain.
        smoke_inner::<M>(domain);
    }

    fn smoke_inner<M: ConcurrentMap<i32, String> + Send + Sync>(domain: &Domain<M::Node>) {
        let mut handle = domain.register();
        let map = &M::new(&mut handle);

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    let handle = domain.register();
                    let mut shields = M::shields(&handle);
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, i.to_string(), &mut shields, &handle));
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in 0..(THREADS / 2) {
                s.spawn(move |_| {
                    let handle = domain.register();
                    let mut shields = M::shields(&handle);
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            *map.remove(&i, &mut shields, &handle).unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in (THREADS / 2)..THREADS {
                s.spawn(move |_| {
                    let handle = domain.register();
                    let mut shields = M::shields(&handle);
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(i.to_string(), *map.get(&i, &mut shields, &handle).unwrap());
                    }
                });
            }
        })
        .unwrap();
    }
}
