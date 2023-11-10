use crate::ds_impl::hp::concurrent_map::ConcurrentMap;

use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::{mem, ptr, slice};

use hp_pp::{decompose_ptr, light_membarrier, tag, tagged, try_unlink, untagged, HazardPointer};

// `#[repr(C)]` is used to ensure the first field
// is also the first data in the memory alignment.
#[repr(C)]
#[derive(Debug)]
pub struct Node<K, V> {
    /// tag 1: logically deleted, tag 2: invalidated
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
        let mut curr = untagged(*self.head.get_mut());

        while !curr.is_null() {
            curr = untagged(*unsafe { Box::from_raw(curr) }.next.get_mut());
        }
    }
}

pub struct Handle<'domain> {
    prev_h: HazardPointer<'domain>,
    curr_h: HazardPointer<'domain>,
    // `anchor_h` and `anchor_next_h` are used for `find_harris`
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

pub struct Cursor<'domain, 'hp, K, V> {
    prev: *mut Node<K, V>, // not &AtomicPtr because we can't construct the cursor out of thin air
    curr: *mut Node<K, V>,
    // `anchor` is used for `find_harris`
    // anchor and anchor_next are non-null iff exist
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

struct Unlink<'c, 'domain, 'hp, K, V> {
    cursor: &'c Cursor<'domain, 'hp, K, V>,
    next_base: *mut Node<K, V>,
}

impl<'r, 'domain, 'hp, K, V> hp_pp::Unlink<Node<K, V>> for Unlink<'r, 'domain, 'hp, K, V> {
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
    fn find_harris(&mut self, key: &K) -> Result<bool, ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)

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
            if curr_new_tag == 3 {
                // Invalidated. Restart from head.
                return Err(());
            } else if curr_new_base != self.curr {
                // If link changed but not invalidated, retry protecting the new node.
                self.curr = curr_new_base;
                continue;
            }

            let curr_node = unsafe { &*self.curr };

            let next = curr_node.next.load(Ordering::Acquire);
            let (next_base, next_tag) = decompose_ptr(next);

            if next_tag == 0 {
                match curr_node.key.cmp(key) {
                    Less => {
                        self.prev = self.curr;
                        HazardPointer::swap(&mut self.handle.prev_h, &mut self.handle.curr_h);
                    }
                    Equal => return Ok(true),
                    Greater => return Ok(false),
                }
            } else {
                let links = slice::from_ref(&next_base);
                let unlink = Unlink {
                    cursor: self,
                    next_base,
                };
                if unsafe { !try_unlink(unlink, links) } {
                    return Err(());
                }
            }
            self.curr = next_base;
        }
    }

    #[inline]
    fn find_harris_herlihy_shavit(&mut self, key: &K) -> Result<bool, ()> {
        loop {
            if self.curr.is_null() {
                return Ok(false);
            }

            let prev = unsafe { &(*self.prev).next };

            // Inlined version of hp++ protection, without duplicate load
            self.handle.curr_h.protect_raw(self.curr);
            light_membarrier();
            let (curr_new_base, curr_new_tag) = decompose_ptr(prev.load(Ordering::Acquire));
            if curr_new_tag == 3 {
                // Invalidated. Restart from head.
                return Err(());
            } else if curr_new_base != self.curr {
                // If link changed but not invalidated, retry protecting the new node.
                self.curr = curr_new_base;
                continue;
            }

            let curr_node = unsafe { &*self.curr };
            let next = curr_node.next.load(Ordering::Acquire);

            match curr_node.key.cmp(key) {
                Less => {
                    self.prev = self.curr;
                    self.curr = next;
                    HazardPointer::swap(&mut self.handle.prev_h, &mut self.handle.curr_h);
                    continue;
                }
                Equal => return Ok(tag(next) == 0),
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
            let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
            let next_tag = tag(next);
            if next_tag == 1 {
                continue;
            }

            let links = slice::from_ref(&next);
            let unlink = Unlink {
                cursor: &cursor,
                next_base: next,
            };
            unsafe { try_unlink(unlink, links) };

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

    #[inline]
    pub fn pop<'domain, 'hp>(&self, handle: &'hp mut Handle<'domain>) -> Option<(&'hp K, &'hp V)> {
        loop {
            let cursor = Cursor::new(&self.head, handle.launder());
            let prev = unsafe { &(*cursor.prev).next };

            // Inlined version of hp++ protection, without duplicate load
            handle.curr_h.protect_raw(cursor.curr);
            light_membarrier();
            let (curr_new_base, curr_new_tag) = decompose_ptr(prev.load(Ordering::Acquire));
            if curr_new_tag == 3 || curr_new_base != cursor.curr {
                // Invalidated or link changed. Restart from head.
                continue;
            }

            if cursor.curr.is_null() {
                return None;
            }

            let curr_node = unsafe { &*cursor.curr };
            let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
            let next_tag = tag(next);
            if (next_tag & 1) != 0 {
                continue;
            }

            let links = slice::from_ref(&next);
            let unlink = Unlink {
                cursor: &cursor,
                next_base: next,
            };
            unsafe { try_unlink(unlink, links) };

            return Some((&curr_node.key, &curr_node.value));
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

    #[inline(always)]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        self.inner.harris_get(key, handle)
    }
    #[inline(always)]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.inner.harris_insert(key, value, handle)
    }
    #[inline(always)]
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

impl<K, V> HMList<K, V>
where
    K: Ord,
{
    /// For optimistic search on HashMap
    #[inline(always)]
    pub fn get_harris_herlihy_shavit<'domain, 'hp>(
        &self,
        handle: &'hp mut Handle<'domain>,
        key: &K,
    ) -> Option<&'hp V> {
        self.inner.harris_herlihy_shavit_get(key, handle)
    }
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

    #[inline(always)]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        self.inner.harris_michael_get(key, handle)
    }
    #[inline(always)]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.inner.harris_michael_insert(key, value, handle)
    }
    #[inline(always)]
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

impl<K, V> HHSList<K, V>
where
    K: Ord,
{
    /// Pop the first element efficiently.
    /// This method is used for only the fine grained benchmark (src/bin/long_running).
    pub fn pop<'domain, 'hp>(&self, handle: &'hp mut Handle<'domain>) -> Option<(&'hp K, &'hp V)> {
        self.inner.pop(handle)
    }
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

    #[inline(always)]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        self.inner.harris_herlihy_shavit_get(key, handle)
    }
    #[inline(always)]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.inner.harris_insert(key, value, handle)
    }
    #[inline(always)]
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
    use crate::ds_impl::hp::concurrent_map;

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

    #[test]
    fn litmus_hhs_pop() {
        use concurrent_map::ConcurrentMap;
        let map = HHSList::new();

        let handle = &mut HHSList::<i32, String>::handle();
        map.insert(handle, 1, "1".to_string());
        map.insert(handle, 2, "2".to_string());
        map.insert(handle, 3, "3".to_string());

        fn assert_eq(a: (&i32, &String), b: (i32, String)) {
            assert_eq!(*a.0, b.0);
            assert_eq!(*a.1, b.1);
        }

        assert_eq(map.pop(handle).unwrap(), (1, "1".to_string()));
        assert_eq(map.pop(handle).unwrap(), (2, "2".to_string()));
        assert_eq(map.pop(handle).unwrap(), (3, "3".to_string()));
        assert_eq!(map.pop(handle), None);
    }
}
