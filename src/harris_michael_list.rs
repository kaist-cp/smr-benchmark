use crate::concurrent_map::ConcurrentMap;
use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Pointer, Shared, Shield, ShieldError};

use std::mem::{self, ManuallyDrop};
use std::ptr;
use std::sync::atomic::Ordering;
use std::ops::Deref;

#[derive(Debug)]
struct Node<K, V> {
    // Mark: tag()
    // Tag: not needed
    next: Atomic<Node<K, V>>,

    key: K,

    value: ManuallyDrop<V>,
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
        unsafe {
            let mut curr = self.head.load(Ordering::Relaxed, unprotected());

            while !curr.is_null() {
                let curr_ref = curr.deref_mut();
                ManuallyDrop::drop(&mut curr_ref.value);
                let next = curr_ref.next.load(Ordering::Relaxed, unprotected());
                drop(curr.into_owned());
                curr = next;
            }
        }
    }
}

struct Cursor<'g, K, V> {
    prev: Shield<Node<K, V>>,
    curr: Shield<Node<K, V>>,
    next: Shared<'g, Node<K, V>>,
}

impl<'g, K, V> Cursor<'g, K, V> {
    fn new(prev: Shared<'g, Node<K, V>>, curr: Shared<'g, Node<K, V>>, guard: &Guard) -> Self {
        Self {
            prev: Shield::new(prev, guard),
            curr: Shield::new(curr, guard),
            next: Shared::null(),
        }
    }
}

struct VRef<K, V> {
    shield: Shield<Node<K, V>>,
}

impl<K, V> VRef<K, V> {
    fn new(shield: Shield<Node<K, V>>) -> Self {
        Self { shield }
    }
}

impl<K, V> Deref for VRef<K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &unsafe { self.shield.deref() }.value
    }
}

enum FindError {
    Retry,
    ShieldError(ShieldError),
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
    fn find_inner<'g>(&'g self, key: &K, guard: &'g Guard) -> Result<(bool, Cursor<'g, K, V>), FindError> {
        let mut cursor = Cursor::new(
            // HACK(@jeehoonkang): we're unsafely assuming the first 8 bytes of both `Node<K, V>`
            // and `List<K, V>` are `Atomic<Node<K, V>>`.
            unsafe { Shared::from_usize(&self.head as *const _ as usize) },
            self.head.load(Ordering::Acquire, guard),
            guard,
        );

        loop {
            let curr_node = match unsafe { cursor.curr.as_ref() } {
                None => return Ok((false, cursor)),
                Some(c) => c,
            };

            if unsafe { cursor.prev.deref() }
                .next
                .load(Ordering::Acquire, guard)
                != cursor.curr.shared().with_tag(0)
            {
                return Err(FindError::Retry);
            }

            cursor.next = curr_node.next.load(Ordering::Acquire, guard);

            let curr_key = &curr_node.key;
            if cursor.next.tag() == 0 {
                if curr_key >= key {
                    return Ok((curr_key == key, cursor));
                }
                mem::swap(&mut cursor.prev, &mut cursor.curr);
            } else {
                match unsafe { cursor.prev.deref() }.next.compare_and_set(
                    cursor.curr.shared().with_tag(0),
                    cursor.next.with_tag(0),
                    Ordering::AcqRel,
                    guard,
                ) {
                    Err(_) => return Err(FindError::Retry),
                    Ok(_) => unsafe { guard.defer_destroy(cursor.curr.shared()) },
                }
            }
            cursor.curr.defend(cursor.next, guard).map_err(|e| FindError::ShieldError(e))?;
        }
    }

    fn find<'g>(&'g self, key: &K, guard: &'g mut Guard) -> (bool, Cursor<'g, K, V>) {
        loop {
            match self.find_inner(key, guard) {
                Ok(r) => return r,
                Err(FindError::Retry) => continue,
                Err(FindError::ShieldError(e)) => guard.repin(),
            }
        }
    }

    pub fn get<'g>(&'g self, key: &K, mut guard: &'g mut Guard) -> Option<VRef<K, V>> {
        let (found, cursor) = self.find(key, guard);

        if found {
            Some(VRef::new(cursor.curr))
        } else {
            None
        }
    }

    pub fn insert(&self, key: K, value: V, guard: &mut Guard) -> bool {
        let mut node = Owned::new(Node {
            key,
            value: ManuallyDrop::new(value),
            next: Atomic::null(),
        });

        loop {
            // TODO: create cursor in this function.
            let (found, cursor) = self.find(&node.key, guard);
            if found {
                return false;
            }

            node.next.store(cursor.curr.shared(), Ordering::Relaxed);
            match unsafe { cursor.prev.deref() }.next.compare_and_set(
                cursor.curr.shared(),
                node,
                Ordering::AcqRel,
                guard,
            ) {
                Ok(_) => return true,
                Err(e) => node = e.new,
            }
        }
    }

    pub fn remove(&self, key: &K, guard: &mut Guard) -> Option<V> {
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

            match unsafe { cursor.prev.deref() }.next.compare_and_set(
                cursor.curr.shared(),
                cursor.next,
                Ordering::AcqRel,
                guard,
            ) {
                Ok(_) => unsafe { guard.defer_destroy(cursor.curr.shared()) },
                Err(_) => {
                    self.find(key, guard);
                }
            }

            return Some(ManuallyDrop::into_inner(value));
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for List<K, V>
where
    K: Ord,
{
    fn new() -> Self {
        Self::new()
    }

    #[inline]
    fn get<'g>(&'g self, key: &K, guard: &'g mut Guard) -> Option<&'g V> {
        self.get(key, guard)
    }
    #[inline]
    fn insert(&self, key: K, value: V, guard: &mut Guard) -> bool {
        self.insert(key, value, guard)
    }
    #[inline]
    fn remove(&self, key: &K, guard: &mut Guard) -> Option<V> {
        self.remove(key, guard)
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    use super::List;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    #[test]
    fn smoke_list() {
        let list = &List::new();

        // insert
        thread::scope(|s| {
            for t in 0..10 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..1000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(list.insert(i, i.to_string(), &mut crossbeam_epoch::pin()));
                    }
                });
            }
        })
        .unwrap();

        // remove
        thread::scope(|s| {
            for t in 0..5 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..1000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            list.remove(&i, &mut crossbeam_epoch::pin()).unwrap()
                    }
                });
            }
        })
        .unwrap();

        // get
        thread::scope(|s| {
            for t in 5..10 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..1000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            *list.get(&i, &mut crossbeam_epoch::pin()).unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();
    }
}
