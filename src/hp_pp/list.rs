use super::concurrent_map::ConcurrentMap;

use std::cmp::Ordering::{Equal, Greater, Less};
use std::mem::{self, ManuallyDrop};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::{ptr, slice};

use haphazard::{decompose_ptr, retire, tag, tagged, try_unlink, HazardPointer};

#[derive(Debug)]
struct Node<K, V>
where
    K: Send,
    V: Send,
{
    /// tag 1: logically deleted, tag 2: stopped
    next: AtomicPtr<Node<K, V>>,
    key: K,
    value: ManuallyDrop<V>,
}

pub struct List<K, V>
where
    K: Send,
    V: Send,
{
    head: AtomicPtr<Node<K, V>>,
}

impl<K, V> Default for List<K, V>
where
    K: Ord + Send,
    V: Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for List<K, V>
where
    K: Send,
    V: Send,
{
    fn drop(&mut self) {
        unsafe {
            let mut curr = self.head.load(Ordering::Relaxed);

            while !curr.is_null() {
                let (next, next_tag) = decompose_ptr((*curr).next.load(Ordering::Relaxed));
                if next_tag == 0 {
                    ManuallyDrop::drop(&mut (*curr).value);
                }
                // unsafe { Domain::global().retire_ptr::<_, Box<_>>(curr) };
                retire(curr);
                curr = next;
            }
        }
    }
}

pub struct Cursor<'g, K, V>
where
    K: Send,
    V: Send,
{
    prev: *mut Node<K, V>, // not &AtomicPtr because we can't construct the cursor out of thin air
    prev_h: HazardPointer<'g>,
    curr: *mut Node<K, V>,
    curr_h: HazardPointer<'g>,
}

impl<'g, 'h, K, V> Cursor<'g, K, V>
where
    K: Send,
    V: Send,
{
    pub fn new() -> Self {
        Self {
            prev: ptr::null_mut(),
            prev_h: HazardPointer::new(),
            curr: ptr::null_mut(),
            curr_h: HazardPointer::new(),
        }
    }

    pub fn release(&mut self) {
        self.prev_h.reset_protection();
        self.curr_h.reset_protection();
    }

    fn init_find(&mut self, head: &AtomicPtr<Node<K, V>>) {
        self.prev = head as *const _ as *mut _;
        self.curr = head.load(Ordering::Acquire);
    }
}

impl<'g, K, V> Cursor<'g, K, V>
where
    K: Ord + Send,
    V: Send,
{
    #[inline]
    fn find_harris_michael(&mut self, key: &K) -> Result<bool, ()> {
        loop {
            debug_assert_eq!(tag(self.curr), 0);
            if self.curr.is_null() {
                return Ok(false);
            }

            let prev = unsafe { &(*self.prev).next };
            self.curr_h
                .try_protect_pp(
                    self.curr,
                    unsafe { &*self.prev },
                    &|prev| &prev.next,
                    &|prev| tag(prev.next.load(Ordering::Acquire)) & 2 == 2,
                )
                .map_err(|_| ())?;

            let curr_node = unsafe { &*self.curr };

            let next = curr_node.next.load(Ordering::Acquire);
            let (next_base, next_tag) = decompose_ptr(next);

            if next_tag == 0 {
                match curr_node.key.cmp(key) {
                    Less => {
                        mem::swap(&mut self.prev, &mut self.curr);
                        mem::swap(&mut self.prev_h, &mut self.curr_h);
                    }
                    Equal => return Ok(true),
                    Greater => return Ok(false),
                }
            } else {
                let links = slice::from_ref(&next_base);
                let to_be_unlinked = slice::from_ref(&self.curr);
                if !try_unlink(
                    links,
                    to_be_unlinked,
                    || {
                        prev.compare_exchange(
                            self.curr,
                            next_base,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    },
                    |node| {
                        let node = unsafe { &*node };
                        let next = node.next.load(Ordering::Acquire);
                        node.next.store(tagged(next, 1 | 2), Ordering::Release);
                    },
                ) {
                    return Err(());
                }
            }
            self.curr = next_base;
        }
    }
}

impl<K, V> List<K, V>
where
    K: Ord + Send,
    V: Send,
{
    pub fn new() -> Self {
        List {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[inline]
    fn find<'g, 'domain, F>(&'g self, key: &K, find: &F, cursor: &mut Cursor<'domain, K, V>) -> bool
    where
        F: Fn(&mut Cursor<'domain, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            cursor.init_find(&self.head);
            match find(cursor, key) {
                Ok(r) => return r,
                Err(_) => continue,
            }
        }
    }

    #[inline]
    fn get<'g, 'domain, F>(
        &'g self,
        key: &K,
        find: F,
        cursor: &'g mut Cursor<'domain, K, V>,
    ) -> Option<&'g V>
    where
        F: Fn(&mut Cursor<'domain, K, V>, &K) -> Result<bool, ()>,
    {
        let found = self.find(key, &find, cursor);

        if found {
            Some(unsafe { &((*cursor.curr).value) })
        } else {
            None
        }
    }

    fn insert_inner<'g, 'domain, F>(
        &'g self,
        node: *mut Node<K, V>,
        find: &F,
        cursor: &mut Cursor<'domain, K, V>,
    ) -> Result<bool, ()>
    where
        F: Fn(&mut Cursor<'domain, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            cursor.init_find(&self.head);
            let found = find(cursor, unsafe { &(*node).key })?;
            if found {
                unsafe {
                    ManuallyDrop::drop(&mut (*node).value);
                    drop(Box::from_raw(node));
                }
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
    fn insert<'g, 'domain, F>(
        &'g self,
        key: K,
        value: V,
        find: F,
        cursor: &mut Cursor<'domain, K, V>,
    ) -> bool
    where
        F: Fn(&mut Cursor<'domain, K, V>, &K) -> Result<bool, ()>,
    {
        let node = Box::into_raw(Box::new(Node {
            key,
            value: ManuallyDrop::new(value),
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        loop {
            match self.insert_inner(node, &find, cursor) {
                Ok(r) => return r,
                Err(()) => continue,
            }
        }
    }

    fn remove_inner<'g, 'domain, F>(
        &'g self,
        key: &K,
        find: &F,
        cursor: &mut Cursor<'domain, K, V>,
    ) -> Result<Option<V>, ()>
    where
        F: Fn(&mut Cursor<'domain, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            cursor.init_find(&self.head);
            let found = find(cursor, key)?;
            if !found {
                return Ok(None);
            }

            let curr_node = unsafe { &*cursor.curr };
            let next = curr_node.next.fetch_or(1, Ordering::Relaxed);
            let next_tag = tag(next);
            if next_tag == 1 {
                continue;
            }

            let value = unsafe { ptr::read(&curr_node.value) };
            let prev = unsafe { &(*cursor.prev).next };

            let links = slice::from_ref(&next);
            let to_be_unlinked = slice::from_ref(&cursor.curr);
            try_unlink(
                links,
                to_be_unlinked,
                || {
                    prev.compare_exchange(cursor.curr, next, Ordering::Release, Ordering::Relaxed)
                        .is_ok()
                },
                |node| {
                    let node = unsafe { &*node };
                    let next = node.next.load(Ordering::Acquire);
                    node.next.store(tagged(next, 1 | 2), Ordering::Release);
                },
            );

            return Ok(Some(ManuallyDrop::into_inner(value)));
        }
    }

    #[inline]
    fn remove<'g, 'domain, F>(
        &'g self,
        key: &K,
        find: F,
        cursor: &mut Cursor<'domain, K, V>,
    ) -> Option<V>
    where
        F: Fn(&mut Cursor<'domain, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            match self.remove_inner(key, &find, cursor) {
                Ok(r) => return r,
                Err(_) => continue,
            }
        }
    }

    pub fn harris_michael_get<'g>(
        &'g self,
        key: &K,
        cursor: &'g mut Cursor<K, V>,
    ) -> Option<&'g V> {
        self.get(key, Cursor::find_harris_michael, cursor)
    }

    pub fn harris_michael_insert(&self, key: K, value: V, cursor: &mut Cursor<K, V>) -> bool {
        self.insert(key, value, Cursor::find_harris_michael, cursor)
    }

    pub fn harris_michael_remove(&self, key: &K, cursor: &mut Cursor<K, V>) -> Option<V> {
        self.remove(key, Cursor::find_harris_michael, cursor)
    }
}

pub struct HMList<K, V>
where
    K: Send,
    V: Send,
{
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HMList<K, V>
where
    K: Ord + Send,
    V: Send,
{
    type Handle<'domain> = Cursor<'domain, K, V>;

    fn handle<'domain>() -> Self::Handle<'domain> {
        Cursor::new()
    }

    fn new() -> Self {
        HMList { inner: List::new() }
    }

    fn clear<'domain>(handle: &mut Self::Handle<'domain>) {
        handle.release();
    }

    #[inline]
    fn get<'g, 'domain>(&'g self, handle: &'g mut Self::Handle<'domain>, key: &K) -> Option<&'g V> {
        self.inner.harris_michael_get(key, handle)
    }
    #[inline]
    fn insert<'g, 'domain>(
        &'g self,
        handle: &'g mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.inner.harris_michael_insert(key, value, handle)
    }
    #[inline]
    fn remove<'g, 'domain>(&'g self, handle: &'g mut Self::Handle<'domain>, key: &K) -> Option<V> {
        self.inner.harris_michael_remove(key, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::HMList;
    use crate::hp_pp::concurrent_map;

    #[test]
    fn smoke_hm_list() {
        concurrent_map::tests::smoke::<HMList<i32, String>>();
    }
}
