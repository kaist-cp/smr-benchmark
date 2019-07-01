use crossbeam_epoch::{Atomic, Guard, Owned, Shared};

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::Ordering;

struct Node<K, V> {
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

struct Cursor<'g, K, V> {
    prev: &'g Atomic<Node<K, V>>,
    curr: Shared<'g, Node<K, V>>,
    next: Shared<'g, Node<K, V>>,
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

    /// Returns (1) whether it found an entry, and (2) a cursor.
    #[inline]
    fn find_inner<'g>(&'g self, key: &K, guard: &'g Guard) -> Result<(bool, Cursor<'g, K, V>), ()> {
        let mut cursor = Cursor {
            prev: &self.head,
            curr: self.head.load(Ordering::Acquire, guard),
            next: Shared::null(),
        };

        loop {
            let curr_node = match unsafe { cursor.curr.as_ref() } {
                None => return Ok((false, cursor)),
                Some(c) => c,
            };

            if cursor.prev.load(Ordering::Acquire, guard) != cursor.curr.with_tag(0) {
                return Err(());
            }

            cursor.next = curr_node.next.load(Ordering::Acquire, guard);

            let curr_key = &curr_node.key;
            if cursor.next.tag() == 0 {
                if curr_key >= key {
                    return Ok((curr_key == key, cursor));
                }
                cursor.prev = &curr_node.next;
            } else {
                match cursor.prev.compare_and_set(
                    cursor.curr.with_tag(0),
                    cursor.next,
                    Ordering::AcqRel,
                    guard,
                ) {
                    Err(_) => return Err(()),
                    Ok(_) => unsafe { guard.defer_destroy(cursor.curr) },
                }
            }
            cursor.curr = cursor.next;
        }
    }

    fn find<'g>(&'g self, key: &K, guard: &'g Guard) -> (bool, Cursor<'g, K, V>) {
        loop {
            if let Ok(r) = self.find_inner(key, guard) {
                return r;
            }
        }
    }

    pub fn search<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let (found, cursor) = self.find(key, guard);

        if found {
            unsafe { cursor.curr.as_ref().map(|n| &*n.value) }
        } else {
            None
        }
    }

    pub fn insert<'g>(&'g self, key: K, value: V, guard: &'g Guard) -> bool {
        let mut node = Owned::new(Node {
            key,
            value: ManuallyDrop::new(value),
            next: Atomic::null(),
        });

        loop {
            let (found, cursor) = self.find(&node.key, guard);
            if found {
                return false;
            }

            node.next.store(cursor.curr, Ordering::Relaxed);
            match cursor
                .prev
                .compare_and_set(cursor.curr, node, Ordering::AcqRel, guard)
            {
                Ok(_) => return true,
                Err(e) => node = e.new,
            }
        }
    }

    pub fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<V> {
        loop {
            let (found, cursor) = self.find(key, guard);
            if !found {
                return None;
            }

            let curr_node = unsafe { cursor.curr.as_ref() }.unwrap();
            let value = unsafe { ptr::read(&curr_node.value) };

            if curr_node
                .next
                .compare_and_set(
                    cursor.next,
                    cursor.next.with_tag(1),
                    Ordering::AcqRel,
                    guard,
                )
                .is_err()
            {
                continue;
            }

            match cursor
                .prev
                .compare_and_set(cursor.curr, cursor.next, Ordering::AcqRel, guard)
            {
                Err(_) => {
                    self.find(key, guard);
                }
                Ok(_) => unsafe { guard.defer_destroy(cursor.curr) },
            }

            return Some(ManuallyDrop::into_inner(value));
        }
    }
}
