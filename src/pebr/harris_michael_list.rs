use super::concurrent_map::ConcurrentMap;
use crossbeam_pebr::{unprotected, Atomic, Guard, Owned, Pointer, Shared, Shield, ShieldError};

use std::mem::{self, ManuallyDrop};
use std::ptr;
use std::sync::atomic::Ordering;

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

pub struct Cursor<K, V> {
    prev: Shield<Node<K, V>>,
    curr: Shield<Node<K, V>>,
}

impl<K, V> Cursor<K, V> {
    pub fn new(guard: &Guard) -> Self {
        Self {
            prev: Shield::null(guard),
            curr: Shield::null(guard),
        }
    }

    pub fn release(&mut self) {
        self.prev.release();
        self.curr.release();
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
    #[inline(always)]
    fn find_inner<'g>(
        &'g self,
        key: &K,
        c: &mut Cursor<K, V>,
        guard: &'g Guard,
    ) -> Result<bool, FindError> {
        let mut cursor = mem::replace(c, Cursor::new(unsafe { unprotected() }));

        // HACK(@jeehoonkang): we're unsafely assuming the first 8 bytes of both `Node<K, V>` and
        // `List<K, V>` are `Atomic<Node<K, V>>`.
        cursor
            .prev
            .defend(
                unsafe { Shared::from_usize(&self.head as *const _ as usize) },
                guard,
            )
            .map_err(FindError::ShieldError)?;
        cursor
            .curr
            .defend(self.head.load(Ordering::Acquire, guard), guard)
            .map_err(FindError::ShieldError)?;

        let result = loop {
            debug_assert_eq!(cursor.curr.tag(), 0);

            let curr_node = match unsafe { cursor.curr.as_ref() } {
                None => break Ok(false),
                Some(c) => c,
            };

            if unsafe { cursor.prev.deref() }
                .next
                .load(Ordering::Acquire, guard)
                != cursor.curr.shared()
            {
                break Err(FindError::Retry);
            }

            let mut next = curr_node.next.load(Ordering::Acquire, guard);

            let curr_key = &curr_node.key;
            if next.tag() == 0 {
                if curr_key >= key {
                    break Ok(curr_key == key);
                }
                let tmp = cursor.prev;
                cursor.prev = cursor.curr;
                cursor.curr = tmp;
            } else {
                next = next.with_tag(0);
                match unsafe { cursor.prev.deref() }.next.compare_and_set(
                    cursor.curr.shared(),
                    next,
                    Ordering::AcqRel,
                    guard,
                ) {
                    Err(_) => break Err(FindError::Retry),
                    Ok(_) => unsafe { guard.defer_destroy(cursor.curr.shared()) },
                }
            }
            cursor
                .curr
                .defend(next, guard)
                .map_err(|e| FindError::ShieldError(e))?;
        };

        drop(mem::replace(c, cursor));
        result
    }

    fn find<'g>(&'g self, key: &K, cursor: &mut Cursor<K, V>, guard: &'g mut Guard) -> bool {
        // TODO(@jeehoonkang): we want to use `FindError::retry`, but it requires higher-kinded
        // things...
        loop {
            match self.find_inner(key, cursor, guard) {
                Ok(r) => return r,
                Err(FindError::Retry) => continue,
                Err(FindError::ShieldError(ShieldError::Ejected)) => {
                    unsafe {
                        // HACK(@jeehoonkang): We wanted to say `guard.repin()`, which is totally
                        // fine, but the current Rust's type checker cannot verify it.
                        (&mut *(guard as &_ as *const _ as *mut Guard)).repin();
                    }
                }
            }
        }
    }

    pub fn get<'g>(
        &'g self,
        cursor: &'g mut Cursor<K, V>,
        key: &K,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        let found = self.find(key, cursor, guard);

        if found {
            Some(unsafe { &cursor.curr.deref().value })
        } else {
            None
        }
    }

    fn insert_inner<'g>(
        &'g self,
        mut node: Shared<'g, Node<K, V>>,
        cursor: &mut Cursor<K, V>,
        guard: &'g Guard,
    ) -> Result<bool, FindError> {
        loop {
            // TODO: create cursor in this function.
            let found = self.find_inner(&unsafe { node.deref() }.key, cursor, guard)?;
            if found {
                unsafe {
                    ManuallyDrop::drop(&mut node.deref_mut().value);
                }
                return Ok(false);
            }

            unsafe { node.deref() }
                .next
                .store(cursor.curr.shared(), Ordering::Relaxed);
            if unsafe { cursor.prev.deref() }
                .next
                .compare_and_set(cursor.curr.shared(), node, Ordering::AcqRel, guard)
                .is_ok()
            {
                return Ok(true);
            }
        }
    }

    pub fn insert(&self, cursor: &mut Cursor<K, V>, key: K, value: V, guard: &mut Guard) -> bool {
        let node = Owned::new(Node {
            key: key,
            value: ManuallyDrop::new(value),
            next: Atomic::null(),
        })
        .into_shared(unsafe { unprotected() });

        loop {
            match self.insert_inner(node, cursor, guard) {
                Ok(r) => return r,
                Err(FindError::Retry) => continue,
                Err(FindError::ShieldError(ShieldError::Ejected)) => {
                    unsafe {
                        // HACK(@jeehoonkang): We wanted to say `guard.repin()`, which is totally
                        // fine, but the current Rust's type checker cannot verify it.
                        (&mut *(guard as &_ as *const _ as *mut Guard)).repin();
                    }
                }
            }
        }
    }

    fn remove_inner<'g>(
        &'g self,
        key: &K,
        cursor: &mut Cursor<K, V>,
        guard: &'g Guard,
    ) -> Result<Option<V>, FindError> {
        loop {
            let found = self.find_inner(key, cursor, guard)?;
            if !found {
                return Ok(None);
            }

            let curr_node = unsafe { cursor.curr.as_ref() }.unwrap();
            let value = unsafe { ptr::read(&curr_node.value) };

            let next = curr_node.next.fetch_or(1, Ordering::AcqRel, guard);
            if next.tag() == 1 {
                continue;
            }

            match unsafe { cursor.prev.deref() }.next.compare_and_set(
                cursor.curr.shared(),
                next,
                Ordering::AcqRel,
                guard,
            ) {
                Ok(_) => unsafe { guard.defer_destroy(cursor.curr.shared()) },
                Err(_) => {
                    self.find_inner(key, cursor, guard)?;
                }
            }

            return Ok(Some(ManuallyDrop::into_inner(value)));
        }
    }

    pub fn remove(&self, cursor: &mut Cursor<K, V>, key: &K, guard: &mut Guard) -> Option<V> {
        loop {
            match self.remove_inner(key, cursor, guard) {
                Ok(r) => return r,
                Err(FindError::Retry) => continue,
                Err(FindError::ShieldError(ShieldError::Ejected)) => {
                    unsafe {
                        // HACK(@jeehoonkang): We wanted to say `guard.repin()`, which is totally
                        // fine, but the current Rust's type checker cannot verify it.
                        (&mut *(guard as &_ as *const _ as *mut Guard)).repin();
                    }
                }
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for List<K, V>
where
    K: Ord + 'static,
    V: 'static,
{
    type Handle = Cursor<K, V>;

    fn new() -> Self {
        Self::new()
    }

    fn handle(guard: &Guard) -> Self::Handle {
        Cursor::new(guard)
    }

    fn clear(handle: &mut Self::Handle) {
        handle.release();
    }

    #[inline]
    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &K,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        self.get(handle, key, guard)
    }
    #[inline]
    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool {
        self.insert(handle, key, value, guard)
    }
    #[inline]
    fn remove(&self, handle: &mut Self::Handle, key: &K, guard: &mut Guard) -> Option<V> {
        self.remove(handle, key, guard)
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    use super::{Cursor, List};
    use crossbeam_pebr::pin;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    #[test]
    fn smoke_list() {
        let list = &List::new();

        // insert
        thread::scope(|s| {
            for t in 0..10 {
                s.spawn(move |_| {
                    let mut cursor = Cursor::new(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..1000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(list.insert(&mut cursor, i, i.to_string(), &mut pin()));
                    }
                });
            }
        })
        .unwrap();

        // remove
        thread::scope(|s| {
            for t in 0..5 {
                s.spawn(move |_| {
                    let mut cursor = Cursor::new(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..1000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            list.remove(&mut cursor, &i, &mut pin()).unwrap()
                        )
                    }
                });
            }
        })
        .unwrap();

        // get
        thread::scope(|s| {
            for t in 5..10 {
                s.spawn(move |_| {
                    let mut cursor = Cursor::new(&pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..1000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            *list.get(&mut cursor, &i, &mut pin()).unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();
    }
}
