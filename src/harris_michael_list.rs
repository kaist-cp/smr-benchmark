use crossbeam_epoch::{Atomic, Guard, Owned, Shared};

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};

struct Node<K, V> {
    // TODO(@jeehoonkang): why not `ManuallyDrop<K>`?
    key: K,

    value: ManuallyDrop<V>,

    // Mark: tag()
    // Tag: not needed
    next: Atomic<Node<K, V>>,
}

pub struct List<K, V> {
    head: Atomic<Node<K, V>>,
}

impl<K, V> Default for List<K, V>
where
    K: Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for List<K, V> {
    fn drop(&mut self) {
        unimplemented!()
    }
}

impl<K, V> List<K, V>
where
    K: Ord,
{
    pub fn new() -> Self {
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

    pub fn search<'g, 'a: 'g>(&'a self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let mut prev: &'g Atomic<Node<K, V>> = &self.head;
        let mut cur: Shared<'g, Node<K, V>> = Shared::null();
        let mut next: Shared<'g, Node<K, V>> = Shared::null();

        if self.find(key, guard, &mut prev, &mut cur, &mut next) {
            unsafe { cur.as_ref().map(|n| &*n.value) }
        } else {
            None
        }
    }

    pub fn insert<'g, 'a: 'g>(&'a self, key: K, value: V, guard: &'g Guard) -> bool {
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

    pub fn remove<'g, 'a: 'g>(&'a self, key: &K, guard: &'g Guard) -> Option<V> {
        let mut prev: &'g Atomic<Node<K, V>> = &self.head;
        let mut cur: Shared<'g, Node<K, V>> = Shared::null();
        let mut next: Shared<'g, Node<K, V>> = Shared::null();
        loop {
            if !self.find(key, guard, &mut prev, &mut cur, &mut next) {
                return None;
            }
            let cur_node = unsafe { cur.as_ref()? }; // TODO: ?
            if cur_node
                .next
                .compare_and_set(next, next.with_tag(1), AcqRel, guard)
                .is_err()
            {
                continue;
            }
            match prev.compare_and_set(cur, next, AcqRel, guard) {
                Ok(_) => unsafe {
                    guard.defer_destroy(cur);
                },
                Err(_e) => unimplemented!(),
            }
            return unsafe { Some(ManuallyDrop::into_inner(ptr::read(&cur_node.value))) };
        }
    }
}
