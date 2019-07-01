extern crate crossbeam;

use crossbeam::epoch;

use epoch::{Atomic, Guard, Owned, Shared};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::Ordering::{AcqRel, Acquire,Release};

struct Node<K, V> {
    key: K,
    value: ManuallyDrop<V>,
    // Mark: tag()
    // Tag: not needed
    next: Atomic<Node<K, V>>,
}
struct List<K, V> {
    head: Atomic<Node<K, V>>,
}

impl<K, V> List<K, V>
where
    K: Ord,
{
    fn new() -> Self {
        List {
            head: Atomic::null(),
        }
    }

    fn find<'g, 'a: 'g>(
        &'a self,
        key: &K,
        guard: &'g Guard,
        prev: &mut &'g Atomic<Node<K, V>>,
        cur: &mut Shared<'g, Node<K, V>>,
        next: &mut Shared<'g, Node<K, V>>,
    ) -> bool {
        loop {
            *prev = &self.head;
            *cur = prev.load(Acquire, guard);
            loop {
                match unsafe { cur.as_ref() } {
                    None => return false,
                    Some(cur_node) => {
                        if prev.load(Acquire, guard) != cur.with_tag(0) {
                            break;
                        }
                        *next = cur_node.next.load(Acquire, guard);
                        let ckey = &cur_node.key;
                        if next.tag() == 0 {
                            if ckey >= key {
                                return ckey == key;
                            }
                            *prev = &cur_node.next;
                        } else {
                            match prev.compare_and_set(cur.with_tag(0), *next, AcqRel, guard) {
                                Ok(_) => unsafe {
                                    guard.defer_destroy(*cur);
                                },
                                Err(_) => break,
                            }
                        }
                        *cur = *next;
                    }
                }
            }
        }
    }

    fn search<'g, 'a: 'g>(&'a self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let mut prev: &'g Atomic<Node<K, V>> = &self.head;
        let mut cur: Shared<'g, Node<K, V>> = Shared::null();
        let mut next: Shared<'g, Node<K, V>> = Shared::null();
        if self.find(key, guard, &mut prev, &mut cur, &mut next) {
            return unsafe { cur.as_ref().map(|n| &*n.value) };
        } else {
            return None;
        }
    }

    // mut? handle?
    fn insert<'g, 'a: 'g>(&'a self, key: K, value: V, guard: &'g Guard) -> bool {
        let mut prev: &'g Atomic<Node<K, V>> = &self.head;
        let mut cur: Shared<'g, Node<K, V>> = Shared::null();
        let mut next: Shared<'g, Node<K, V>> = Shared::null();
        let mut node = Owned::new(Node {
            key,
            value: ManuallyDrop::new(value),
            next: Atomic::null(),
        });
        loop {
            if self.find(&node.key, guard, &mut prev, &mut cur, &mut next) {
                return false;
            }
            node.next.store(cur, Release);
            match prev.compare_and_set(cur, node, AcqRel, guard) {
                Ok(_) => return true,
                Err(e) => node = e.new,
            }
        }
    }

    fn remove<'g, 'a: 'g>(&'a self, key: &K, guard: &'g Guard) -> Option<V> {
        let mut prev: &'g Atomic<Node<K, V>> = &self.head;
        let mut cur: Shared<'g, Node<K, V>> = Shared::null();
        let mut next: Shared<'g, Node<K, V>> = Shared::null();
        loop {
            if !self.find(key, guard, &mut prev, &mut cur, &mut next) {
                return None;
            }
            let cur_node = unsafe { cur.as_ref()? }; // TODO: ?
            if let Err(_) = cur_node
                .next
                .compare_and_set(next, next.with_tag(1), AcqRel, guard)
            {
                continue;
            }
            match prev.compare_and_set(cur, next, AcqRel, guard) {
                Ok(_) => unsafe {
                    guard.defer_destroy(cur);
                },
                Err(_) => {} // TODO ??
            }
            return unsafe { Some(ManuallyDrop::into_inner(ptr::read(&cur_node.value))) };
        }
    }
}

// TODO: Drop

pub struct MichaelHashMap<K, V> {
    num_buckets: usize,
    buckets: Vec<List<K, V>>,
    // size: AtomicUsize, TODO
}

impl<K, V> MichaelHashMap<K, V>
where
    K: Ord + Hash,
{
    pub fn with_capacity(n: usize) -> Self {
        let mut b = Vec::with_capacity(n);
        for _ in 0..n {
            b.push(List::new());
        }
        MichaelHashMap {
            num_buckets: n,
            buckets: b,
        }
    }

    #[inline]
    fn hash(k: &K) -> usize {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize
    }

    pub fn get<'g, 'a: 'g>(&'a self, k: &K, guard: &'g Guard) -> Option<&'g V> {
        let i = Self::hash(k) % self.num_buckets;
        self.buckets[i].search(k, guard)
    }

    pub fn insert(&self, k: K, v: V) -> bool {
        let i = Self::hash(&k) % self.num_buckets;
        let guard = &epoch::pin();
        self.buckets[i].insert(k, v, guard)
    }

    pub fn remove(&self, k: &K) -> Option<V> {
        let i = Self::hash(&k) % self.num_buckets;
        let guard = &epoch::pin();
        self.buckets[i].remove(k, guard)
    }
}

mod tests {
    use crossbeam::thread::{scope};
    use crossbeam::epoch;
    use super::{MichaelHashMap};

    #[test]
    fn test1() {
        let hash_map = &MichaelHashMap::with_capacity(100);
        scope(|s| {
            for t in 0..100 {
                s.spawn(move |_| {
                    for i in 0..1000 {
                        hash_map.insert(i, (i, t));
                    }
                });
            }
        })
        .unwrap();
        let guard = &epoch::pin();
        println!("asdfadsfasfsa");
        println!("{:?}", hash_map.get(&1, guard));
        println!("{:?}", hash_map.remove(&1));
        println!("{:?}", hash_map.get(&1, guard));
    }

}
