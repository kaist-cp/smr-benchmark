use crate::hp::concurrent_map::ConcurrentMap;

use std::cmp::Ordering::{Equal, Greater, Less};
use std::mem;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::{ptr, slice};

use hp_pp::{
    decompose_ptr, light_membarrier, tag, tagged, try_unlink, untagged, HazardPointer, ProtectError,
};

#[derive(Debug)]
pub struct Node<K, V> {
    /// tag 1: logically deleted, tag 2: stopped
    next: AtomicPtr<Node<K, V>>,
    key: K,
    value: V,
}

pub struct List<K, V> {
    head: AtomicPtr<Node<K, V>>,
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
            let mut curr = *self.head.get_mut();

            while !curr.is_null() {
                let next = untagged(*(*curr).next.get_mut());
                drop(Box::from_raw(curr));
                curr = next;
            }
        }
    }
}

pub struct Handle<'domain> {
    prev_h: HazardPointer<'domain>,
    curr_h: HazardPointer<'domain>,
    // `anchor_h` is used for `find_harris`
    anchor_h: HazardPointer<'domain>,
}

impl Default for Handle<'static> {
    fn default() -> Self {
        Self {
            prev_h: HazardPointer::default(),
            curr_h: HazardPointer::default(),
            anchor_h: HazardPointer::default(),
        }
    }
}

impl<'domain> Handle<'domain> {
    // bypass E0499-E0503, etc that are supposed to be fixed by polonius
    #[inline]
    fn launder<'hp1, 'hp2>(&'hp1 mut self) -> &'hp2 mut Self {
        unsafe { core::mem::transmute(self) }
    }
}

pub struct Cursor<'domain, 'hp, K, V> {
    prev: *mut Node<K, V>, // not &AtomicPtr because we can't construct the cursor out of thin air
    curr: *mut Node<K, V>,
    // `anchor` is used for `find_harris`
    anchor: *mut Node<K, V>,
    handle: &'hp mut Handle<'domain>,
}

impl<'domain, 'hp, K, V> Cursor<'domain, 'hp, K, V> {
    pub fn new(head: &AtomicPtr<Node<K, V>>, handle: &'hp mut Handle<'domain>) -> Self {
        Self {
            prev: head as *const _ as *mut _,
            curr: head.load(Ordering::Acquire),
            anchor: head as *const _ as *mut _,
            handle,
        }
    }
}

impl<'domain, 'hp, K, V> Cursor<'domain, 'hp, K, V>
where
    K: Ord,
{
    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn find_harris(&mut self, key: &K) -> Result<bool, ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)

        let mut anchor_next = self.curr;
        let found = loop {
            if self.curr.is_null() {
                break false;
            }

            // Inlined version of hp++ protection, without duplicate load
            self.handle.curr_h.protect_raw(self.curr);
            light_membarrier();
            let (curr_new_base, curr_new_tag) =
                decompose_ptr(unsafe { &(*self.prev).next }.load(Ordering::Acquire));
            if curr_new_tag & 2 == 2 {
                // Stopped. Restart from head.
                return Err(());
            } else if curr_new_base != self.curr {
                // If link changed but not stopped, retry protecting the new node.
                if anchor_next == self.curr {
                    anchor_next = curr_new_base;
                }
                self.curr = curr_new_base;
                continue;
            }

            let curr_node = unsafe { &*self.curr };
            let (next_base, next_tag) = decompose_ptr(curr_node.next.load(Ordering::Acquire));
            // NOTE: next_base will be protected on next iteration.

            // - finding stage is done if cursor.curr advancement stops
            // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
            // - stop cursor.curr if (not marked) && (cursor.curr >= key)
            // - advance cursor.prev if not marked
            if next_tag == 0 {
                match curr_node.key.cmp(key) {
                    Less => {
                        self.anchor = self.curr;
                        anchor_next = next_base;
                        self.prev = self.curr;
                        self.curr = next_base;
                        self.handle.anchor_h.reset_protection();
                        mem::swap(&mut self.handle.curr_h, &mut self.handle.prev_h);
                    }
                    Equal => break true,
                    Greater => break false,
                }
            } else {
                // `next_tag` is dirty, if `anchor` is not set, assign `prev`
                if self.anchor == self.prev {
                    mem::swap(&mut self.handle.anchor_h, &mut self.handle.prev_h);
                }
                self.prev = self.curr;
                self.curr = next_base;
                mem::swap(&mut self.handle.prev_h, &mut self.handle.curr_h);
                // `curr_h` will be used for protecting on next iteration.
            }
        };

        // If `anchor` and `curr` WERE adjacent, no need to clean up
        if anchor_next == self.curr {
            return Ok(found);
        }

        // cleanup marked nodes between anchor and curr
        if unsafe {
            !try_unlink(
                slice::from_ref(&self.curr),
                || {
                    if (&*self.anchor)
                        .next
                        .compare_exchange(
                            anchor_next,
                            self.curr,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        let mut collected = Vec::with_capacity(16);
                        let mut node = anchor_next;
                        loop {
                            if untagged(node) == self.curr {
                                break;
                            }
                            let node_ref = node.as_ref().unwrap();
                            let next_base = untagged(node_ref.next.load(Ordering::Acquire));
                            collected.push(node);
                            node = next_base;
                        }
                        Ok(collected)
                    } else {
                        Err(())
                    }
                },
                |node| {
                    let node = &*node;
                    let next = node.next.load(Ordering::Acquire);
                    node.next.store(tagged(next, 1 | 2), Ordering::Release);
                },
            )
        } {
            return Err(());
        }

        self.prev = self.anchor;
        Ok(found)
    }

    #[inline]
    fn find_harris_michael(&mut self, key: &K) -> Result<bool, ()> {
        loop {
            debug_assert_eq!(tag(self.curr), 0);
            if self.curr.is_null() {
                return Ok(false);
            }

            let prev = unsafe { &(*self.prev).next };

            // Inlined version of hp++ protection, without duplicate load
            self.handle.curr_h.protect_raw(self.curr);
            light_membarrier();
            let (curr_new_base, curr_new_tag) = decompose_ptr(prev.load(Ordering::Acquire));
            if curr_new_tag & 2 == 2 {
                // Stopped. Restart from head.
                return Err(());
            } else if curr_new_base != self.curr {
                // If link changed but not stopped, retry protecting the new node.
                self.curr = curr_new_base;
                continue;
            }

            let curr_node = unsafe { &*self.curr };

            let next = curr_node.next.load(Ordering::Acquire);
            let (next_base, next_tag) = decompose_ptr(next);

            if next_tag == 0 {
                match curr_node.key.cmp(key) {
                    Less => {
                        mem::swap(&mut self.prev, &mut self.curr);
                        mem::swap(&mut self.handle.prev_h, &mut self.handle.curr_h);
                    }
                    Equal => return Ok(true),
                    Greater => return Ok(false),
                }
            } else {
                let links = slice::from_ref(&next_base);
                if unsafe {
                    !try_unlink(
                        links,
                        || {
                            if prev
                                .compare_exchange(
                                    self.curr,
                                    next_base,
                                    Ordering::Release,
                                    Ordering::Relaxed,
                                )
                                .is_ok()
                            {
                                Ok(vec![self.curr])
                            } else {
                                Err(())
                            }
                        },
                        |node| {
                            let node = &*node;
                            let next = node.next.load(Ordering::Acquire);
                            node.next.store(tagged(next, 1 | 2), Ordering::Release);
                        },
                    )
                } {
                    return Err(());
                }
            }
            self.curr = next_base;
        }
    }

    #[inline]
    fn find_harris_herlihy_shavit(&mut self, key: &K) -> Result<bool, ()> {
        if self.curr.is_null() {
            return Ok(false);
        }
        let mut curr_origin = unsafe { &(*self.prev).next };
        let mut curr = curr_origin.load(Ordering::Acquire);

        loop {
            if curr.is_null() {
                self.curr = curr;
                return Ok(false);
            }

            match self.handle.curr_h.try_protect_pp(
                curr,
                curr_origin,
                &|origin| origin,
                &|origin| tag(origin.load(Ordering::Acquire)) & 2 == 2,
            ) {
                Err(ProtectError::Changed(ptr_new)) => {
                    curr = ptr_new;
                    continue;
                }
                Err(ProtectError::Stopped) => return Err(()),
                Ok(_) => {}
            }

            self.curr = curr;
            let curr_node = unsafe { &*curr };

            match curr_node.key.cmp(key) {
                Less => {
                    curr = curr_node.next.load(Ordering::Acquire);
                    curr_origin = &curr_node.next;
                    mem::swap(&mut self.prev, &mut self.curr);
                    mem::swap(&mut self.handle.prev_h, &mut self.handle.curr_h);
                }
                Equal => return Ok(tag(curr_node.next.load(Ordering::Relaxed)) == 0),
                Greater => return Ok(false),
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
    fn get<'domain, 'hp, F>(
        &self,
        key: &K,
        find: F,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V>
    where
        F: Fn(&mut Cursor<'domain, 'hp, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            let mut cursor = Cursor::new(&self.head, handle.launder());
            match find(&mut cursor, key) {
                Ok(true) => return unsafe { Some(&((*cursor.curr).value)) },
                Ok(false) => return None,
                Err(_) => continue,
            }
        }
    }

    fn insert_inner<'domain, 'hp, F>(
        &self,
        node: *mut Node<K, V>,
        find: &F,
        handle: &'hp mut Handle<'domain>,
    ) -> Result<bool, ()>
    where
        F: Fn(&mut Cursor<'domain, 'hp, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            let mut cursor = Cursor::new(&self.head, handle.launder());
            let found = find(&mut cursor, unsafe { &(*node).key })?;
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
    fn insert<'domain, 'hp, F>(
        &self,
        key: K,
        value: V,
        find: F,
        handle: &'hp mut Handle<'domain>,
    ) -> bool
    where
        F: Fn(&mut Cursor<'domain, 'hp, K, V>, &K) -> Result<bool, ()>,
    {
        let node = Box::into_raw(Box::new(Node {
            key,
            value,
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        loop {
            match self.insert_inner(node, &find, handle.launder()) {
                Ok(r) => return r,
                Err(()) => continue,
            }
        }
    }

    fn remove_inner<'domain, 'hp, F>(
        &self,
        key: &K,
        find: &F,
        handle: &'hp mut Handle<'domain>,
    ) -> Result<Option<&'hp V>, ()>
    where
        F: Fn(&mut Cursor<'domain, 'hp, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            let mut cursor = Cursor::new(&self.head, handle.launder());
            let found = find(&mut cursor, key)?;
            if !found {
                return Ok(None);
            }

            let curr_node = unsafe { &*cursor.curr };
            let next = curr_node.next.fetch_or(1, Ordering::Relaxed);
            let next_tag = tag(next);
            if next_tag == 1 {
                continue;
            }

            let prev = unsafe { &(*cursor.prev).next };

            let links = slice::from_ref(&next);
            unsafe {
                try_unlink(
                    links,
                    || {
                        if prev
                            .compare_exchange(
                                cursor.curr,
                                next,
                                Ordering::Release,
                                Ordering::Relaxed,
                            )
                            .is_ok()
                        {
                            Ok(vec![cursor.curr])
                        } else {
                            Err(())
                        }
                    },
                    |node| {
                        let node = &*node;
                        let next = node.next.load(Ordering::Acquire);
                        node.next.store(tagged(next, 1 | 2), Ordering::Release);
                    },
                )
            };

            return Ok(Some(&curr_node.value));
        }
    }

    #[inline]
    fn remove<'domain, 'hp, F>(
        &self,
        key: &K,
        find: F,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V>
    where
        F: Fn(&mut Cursor<'domain, 'hp, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            match self.remove_inner(key, &find, handle.launder()) {
                Ok(r) => return r,
                Err(_) => continue,
            }
        }
    }

    pub fn harris_get<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V> {
        self.get(key, Cursor::find_harris, handle)
    }

    pub fn harris_insert<'domain, 'hp>(
        &self,
        key: K,
        value: V,
        handle: &'hp mut Handle<'domain>,
    ) -> bool {
        self.insert(key, value, Cursor::find_harris, handle)
    }

    pub fn harris_remove<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V> {
        self.remove(key, Cursor::find_harris, handle)
    }

    pub fn harris_michael_get<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V> {
        self.get(key, Cursor::find_harris_michael, handle)
    }

    pub fn harris_michael_insert<'domain, 'hp>(
        &self,
        key: K,
        value: V,
        handle: &'hp mut Handle<'domain>,
    ) -> bool {
        self.insert(key, value, Cursor::find_harris_michael, handle)
    }

    pub fn harris_michael_remove<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V> {
        self.remove(key, Cursor::find_harris_michael, handle)
    }

    pub fn harris_herlihy_shavit_get<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V> {
        self.get(key, Cursor::find_harris_herlihy_shavit, handle)
    }
}

pub struct HList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HList<K, V>
where
    K: Ord,
{
    type Handle<'domain> = Handle<'domain>;

    fn handle() -> Self::Handle<'static> {
        Handle::default()
    }

    fn new() -> Self {
        HList { inner: List::new() }
    }

    #[inline]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        self.inner.harris_get(key, handle)
    }
    #[inline]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.inner.harris_insert(key, value, handle)
    }
    #[inline]
    fn remove<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: &K,
    ) -> Option<&'hp V> {
        self.inner.harris_remove(key, handle)
    }
}

pub struct HMList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HMList<K, V>
where
    K: Ord,
{
    type Handle<'domain> = Handle<'domain>;

    fn handle() -> Self::Handle<'static> {
        Handle::default()
    }

    fn new() -> Self {
        HMList { inner: List::new() }
    }

    #[inline]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        self.inner.harris_michael_get(key, handle)
    }
    #[inline]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.inner.harris_michael_insert(key, value, handle)
    }
    #[inline]
    fn remove<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: &K,
    ) -> Option<&'hp V> {
        self.inner.harris_michael_remove(key, handle)
    }
}

pub struct HHSList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HHSList<K, V>
where
    K: Ord,
{
    type Handle<'domain> = Handle<'domain>;

    fn handle() -> Self::Handle<'static> {
        Handle::default()
    }

    fn new() -> Self {
        HHSList { inner: List::new() }
    }

    #[inline]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        self.inner.harris_herlihy_shavit_get(key, handle)
    }
    #[inline]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.inner.harris_insert(key, value, handle)
    }
    #[inline]
    fn remove<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: &K,
    ) -> Option<&'hp V> {
        self.inner.harris_remove(key, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::{HHSList, HList, HMList};
    use crate::hp::concurrent_map;

    #[test]
    fn smoke_h_list() {
        concurrent_map::tests::smoke::<HList<i32, String>>();
    }

    #[test]
    fn smoke_hm_list() {
        concurrent_map::tests::smoke::<HMList<i32, String>>();
    }

    #[test]
    fn smoke_hhs_list() {
        concurrent_map::tests::smoke::<HHSList<i32, String>>();
    }
}
