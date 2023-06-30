use hp_sharp::Handle;

pub trait OutputHolder<V> {
    fn default(handle: &mut Handle);
    fn output(&self) -> &V;
}

pub trait ConcurrentMap<K, V> {
    type Protector: OutputHolder<V>;

    fn new() -> Self;
    fn protector() -> Self::Protector;
    fn get(&self, key: &K, protector: &mut Self::Protector) -> bool;
    fn insert(&self, handle: &mut Self::Protector, key: K, value: V) -> bool;
    fn remove<'domain, 'hp>(&self, handle: &mut Self::Protector, key: &K) -> bool;
}
