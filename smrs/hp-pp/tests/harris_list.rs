//! An implementation of Harris's List with HP++

#![feature(strict_provenance_atomic_ptr)]
use std::{
    mem, ptr, slice,
    sync::atomic::{AtomicPtr, Ordering},
    thread::scope,
};

use hp_pp::*;

// `#[repr(C)]` is used to ensure the first field
// is also the first data in the memory alignment.
#[repr(C)]
pub struct Node<K, V> {
    /// tag 1: logically deleted, tag 2: invalidated
    next: AtomicPtr<Node<K, V>>,
    key: K,
    value: V,
}

pub struct List<K, V> {
    head: AtomicPtr<Node<K, V>>,
}

impl<K, V> Drop for List<K, V> {
    fn drop(&mut self) {
        let mut curr = untagged(*self.head.get_mut());

        while !curr.is_null() {
            curr = untagged(*unsafe { Box::from_raw(curr) }.next.get_mut());
        }
    }
}

pub struct Handle<'domain> {
    prev_h: HazardPointer<'domain>,
    curr_h: HazardPointer<'domain>,
    anchor_h: HazardPointer<'domain>,
    anchor_next_h: HazardPointer<'domain>,
}

impl Default for Handle<'static> {
    fn default() -> Self {
        Self {
            prev_h: HazardPointer::default(),
            curr_h: HazardPointer::default(),
            anchor_h: HazardPointer::default(),
            anchor_next_h: HazardPointer::default(),
        }
    }
}

impl<'domain> Handle<'domain> {
    // bypass E0499-E0503, etc that are supposed to be fixed by polonius
    #[inline]
    fn launder<'hp1, 'hp2>(&'hp1 mut self) -> &'hp2 mut Self {
        unsafe { mem::transmute(self) }
    }
}

/// `Cursor` is safe to dereference its members
/// only it the underlying hazard pointers are present.
pub struct Cursor<'domain, 'hp, K, V> {
    prev: *mut Node<K, V>,
    curr: *mut Node<K, V>,
    // Anchor is the last node that was not logically deleted, and anchor_next is anchor's next
    // node at that moment. These are used for unlinking the chain of logically deleted nodes. They
    // are non-null iff they exist.
    anchor: *mut Node<K, V>,
    anchor_next: *mut Node<K, V>,
    handle: &'hp mut Handle<'domain>,
}

impl<'domain, 'hp, K, V> Cursor<'domain, 'hp, K, V> {
    pub fn new(head: &AtomicPtr<Node<K, V>>, handle: &'hp mut Handle<'domain>) -> Self {
        Self {
            prev: head as *const _ as *mut _,
            curr: head.load(Ordering::Acquire),
            anchor: ptr::null_mut(),
            anchor_next: ptr::null_mut(),
            handle,
        }
    }
}

impl<K, V> hp_pp::Invalidate for Node<K, V> {
    fn invalidate(&self) {
        let next = self.next.load(Ordering::Acquire);
        self.next.store(tagged(next, 1 | 2), Ordering::Release);
    }
}

/// Physical unlink in the traverse function of Harris's list
struct HarrisUnlink<'c, 'domain, 'hp, K, V> {
    cursor: &'c Cursor<'domain, 'hp, K, V>,
}

impl<'r, 'domain, 'hp, K, V> hp_pp::Unlink<Node<K, V>> for HarrisUnlink<'r, 'domain, 'hp, K, V> {
    fn do_unlink(&self) -> Result<Vec<*mut Node<K, V>>, ()> {
        if unsafe { &*self.cursor.anchor }
            .next
            .compare_exchange(
                self.cursor.anchor_next,
                self.cursor.curr,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            let mut collected = Vec::with_capacity(16);
            let mut node = self.cursor.anchor_next;
            loop {
                if untagged(node) == self.cursor.curr {
                    break;
                }
                let node_ref = unsafe { node.as_ref() }.unwrap();
                let next_base = untagged(node_ref.next.load(Ordering::Relaxed));
                collected.push(node);
                node = next_base;
            }
            Ok(collected)
        } else {
            Err(())
        }
    }
}

struct RemoveUnlink<'c, 'domain, 'hp, K, V> {
    cursor: &'c Cursor<'domain, 'hp, K, V>,
    next_base: *mut Node<K, V>,
}

impl<'r, 'domain, 'hp, K, V> hp_pp::Unlink<Node<K, V>> for RemoveUnlink<'r, 'domain, 'hp, K, V> {
    fn do_unlink(&self) -> Result<Vec<*mut Node<K, V>>, ()> {
        let prev = unsafe { &(*self.cursor.prev).next };
        if prev
            .compare_exchange(
                self.cursor.curr,
                self.next_base,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            Ok(vec![self.cursor.curr])
        } else {
            Err(())
        }
    }
}

impl<'domain, 'hp, K, V> Cursor<'domain, 'hp, K, V>
where
    K: Ord,
{
    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn try_search(&mut self, key: &K) -> Result<bool, ()> {
        let found = loop {
            if self.curr.is_null() {
                break false;
            }
            if self
                .handle
                .curr_h
                .try_protect_pp(
                    self.curr,
                    unsafe { &*self.prev },
                    &unsafe { &*self.prev }.next,
                    &|node| node.next.load(Ordering::Acquire) as usize & 2 == 2,
                )
                .is_err()
            {
                return Err(());
            }

            let curr_node = unsafe { &*self.curr };
            let (next_base, next_tag) = decompose_ptr(curr_node.next.load(Ordering::Acquire));
            if next_tag == 0 {
                if curr_node.key < *key {
                    self.prev = self.curr;
                    self.curr = next_base;
                    self.anchor = ptr::null_mut();
                    HazardPointer::swap(&mut self.handle.curr_h, &mut self.handle.prev_h);
                } else {
                    break curr_node.key == *key;
                }
            } else {
                if self.anchor.is_null() {
                    self.anchor = self.prev;
                    self.anchor_next = self.curr;
                    HazardPointer::swap(&mut self.handle.anchor_h, &mut self.handle.prev_h);
                } else if self.anchor_next == self.prev {
                    HazardPointer::swap(&mut self.handle.anchor_next_h, &mut self.handle.prev_h);
                }
                self.prev = self.curr;
                self.curr = next_base;
                HazardPointer::swap(&mut self.handle.prev_h, &mut self.handle.curr_h);
            }
        };

        if self.anchor.is_null() {
            Ok(found)
        } else {
            let unlink = HarrisUnlink { cursor: &self };
            if unsafe { try_unlink(unlink, slice::from_ref(&self.curr)) } {
                self.prev = self.anchor;
                Ok(found)
            } else {
                Err(())
            }
        }
    }
}

impl<K, V> List<K, V>
where
    K: Ord,
{
    /// Creates a new list.
    pub fn new() -> Self {
        List {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[inline]
    pub fn get<'domain, 'hp>(&self, key: &K, handle: &'hp mut Handle<'domain>) -> Option<&'hp V> {
        loop {
            let mut cursor = Cursor::new(&self.head, handle.launder());
            match cursor.try_search(key) {
                Ok(true) => return unsafe { Some(&((*cursor.curr).value)) },
                Ok(false) => return None,
                Err(_) => continue,
            }
        }
    }

    fn insert_inner<'domain, 'hp>(
        &self,
        node: *mut Node<K, V>,
        handle: &'hp mut Handle<'domain>,
    ) -> Result<bool, ()> {
        loop {
            let mut cursor = Cursor::new(&self.head, handle.launder());
            let found = cursor.try_search(unsafe { &(*node).key })?;
            if found {
                drop(unsafe { Box::from_raw(node) });
                return Ok(false);
            }

            unsafe { &*node }.next.store(cursor.curr, Ordering::Relaxed);
            if unsafe { &*cursor.prev }
                .next
                .compare_exchange(cursor.curr, node, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(true);
            }
        }
    }

    #[inline]
    pub fn insert<'domain, 'hp>(&self, key: K, value: V, handle: &'hp mut Handle<'domain>) -> bool {
        let node = Box::into_raw(Box::new(Node {
            key,
            value,
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        loop {
            match self.insert_inner(node, handle.launder()) {
                Ok(r) => return r,
                Err(()) => continue,
            }
        }
    }

    fn remove_inner<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Result<Option<&'hp V>, ()> {
        loop {
            let mut cursor = Cursor::new(&self.head, handle.launder());
            let found = cursor.try_search(key)?;
            if !found {
                return Ok(None);
            }

            let curr_node = unsafe { &*cursor.curr };
            let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
            let next_tag = tag(next);
            if next_tag == 1 {
                continue;
            }

            let links = slice::from_ref(&next);
            let unlink = RemoveUnlink {
                cursor: &cursor,
                next_base: next,
            };
            unsafe { try_unlink(unlink, links) };

            return Ok(Some(&curr_node.value));
        }
    }

    #[inline]
    pub fn remove<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V> {
        loop {
            match self.remove_inner(key, handle.launder()) {
                Ok(r) => return r,
                Err(_) => continue,
            }
        }
    }

    pub fn handle() -> Handle<'static> {
        Handle::default()
    }
}

#[test]
fn smoke_harris() {
    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;
    use rand::prelude::*;

    let map = &List::new();

    scope(|s| {
        for t in 0..THREADS {
            s.spawn(move || {
                let mut handle = List::<i32, String>::handle();
                let mut rng = rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(&mut rng);
                for i in keys {
                    assert!(map.insert(i, i.to_string(), &mut handle));
                }
            });
        }
    });

    scope(|s| {
        for t in 0..(THREADS / 2) {
            s.spawn(move || {
                let mut handle = List::<i32, String>::handle();
                let mut rng = rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(&mut rng);
                for i in keys {
                    assert_eq!(i.to_string(), *map.remove(&i, &mut handle).unwrap());
                }
            });
        }
    });

    scope(|s| {
        for t in (THREADS / 2)..THREADS {
            s.spawn(move || {
                let mut handle = List::<i32, String>::handle();
                let mut rng = rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(&mut rng);
                for i in keys {
                    assert_eq!(i.to_string(), *map.get(&i, &mut handle).unwrap());
                }
            });
        }
    });
}
