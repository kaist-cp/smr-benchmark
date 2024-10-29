use super::concurrent_map::{ConcurrentMap, OutputHolder};

use super::pointers::{Atomic, Pointer, Shared};
use core::mem;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering;

use hp_pp::{light_membarrier, HazardPointer, Thread, DEFAULT_DOMAIN};

// `#[repr(C)]` is used to ensure the first field
// is also the first data in the memory alignment.
#[repr(C)]
pub struct Node<K, V> {
    /// Mark: tag(), Tag: not needed
    next: Atomic<Node<K, V>>,
    key: K,
    value: V,
}

pub struct List<K, V> {
    head: Atomic<Node<K, V>>,
}

impl<K, V> Default for List<K, V>
where
    K: Ord + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for List<K, V> {
    fn drop(&mut self) {
        let mut o_curr = mem::take(&mut self.head);

        while let Some(curr) = unsafe { o_curr.try_into_owned() } {
            o_curr = curr.next;
        }
    }
}

pub struct Handle<'domain> {
    prev_h: HazardPointer<'domain>,
    curr_h: HazardPointer<'domain>,
    // `anchor_h` and `anchor_next_h` are used for `find_harris`
    anchor_h: HazardPointer<'domain>,
    anchor_next_h: HazardPointer<'domain>,
    thread: Box<Thread<'domain>>,
}

impl Default for Handle<'static> {
    fn default() -> Self {
        let mut thread = Box::new(Thread::new(&DEFAULT_DOMAIN));
        Self {
            prev_h: HazardPointer::new(&mut thread),
            curr_h: HazardPointer::new(&mut thread),
            anchor_h: HazardPointer::new(&mut thread),
            anchor_next_h: HazardPointer::new(&mut thread),
            thread,
        }
    }
}

impl<'domain> Handle<'domain> {
    // bypass E0499-E0503, etc that are supposed to be fixed by polonius
    #[inline]
    fn launder<'hp2>(&mut self) -> &'hp2 mut Self {
        unsafe { core::mem::transmute(self) }
    }
}

pub struct Cursor<'domain, 'hp, K, V> {
    prev: Shared<Node<K, V>>, // not &Atomic because we can't construct the cursor out of thin air
    // For harris, this keeps the mark bit. Don't mix harris and harris-micheal.
    curr: Shared<Node<K, V>>,
    // `anchor` is used for `find_harris`
    // anchor and anchor_next are non-null iff exist
    anchor: Shared<Node<K, V>>,
    anchor_next: Shared<Node<K, V>>,
    handle: &'hp mut Handle<'domain>,
}

impl<'domain, 'hp, K, V> Cursor<'domain, 'hp, K, V> {
    pub fn new(head: &Atomic<Node<K, V>>, handle: &'hp mut Handle<'domain>) -> Self {
        Self {
            prev: unsafe { Shared::from_raw(head as *const _ as *mut _) },
            curr: head.load(Ordering::Acquire),
            anchor: Shared::null(),
            anchor_next: Shared::null(),
            handle,
        }
    }
}

impl<'domain, 'hp, K, V> Cursor<'domain, 'hp, K, V>
where
    K: Ord,
{
    /// Optimistically traverses while maintaining `anchor` and `anchor_next`.
    /// It is used for both Harris and Harris-Herlihy-Shavit traversals.
    #[inline]
    fn traverse_with_anchor(&mut self, key: &K) -> Result<bool, ()> {
        // Invariants:
        // anchor, anchor_next: protected if they are not null.
        // prev: always protected with prev_sh
        // curr: not protected.
        // curr: also has tag value when it is obtained from prev.
        Ok(loop {
            if self.curr.is_null() {
                break false;
            }

            let prev_next = unsafe { &self.prev.deref().next };
            self.handle
                .curr_h
                .protect_raw(self.curr.with_tag(0).into_raw());
            light_membarrier();

            // Validation depending on the state of `self.curr`.
            //
            // - If it is marked, validate on anchor.
            // - If it is not marked, validate on prev.

            if self.curr.tag() != 0 {
                // Validate on anchor.

                debug_assert!(!self.anchor.is_null());
                debug_assert!(!self.anchor_next.is_null());
                let an_new = unsafe { &self.anchor.deref().next }.load(Ordering::Acquire);

                if an_new.tag() != 0 {
                    return Err(());
                } else if an_new != self.anchor_next {
                    // Anchor is updated but clear, so can restart from anchor.

                    self.prev = self.anchor;
                    self.curr = an_new;
                    self.anchor = Shared::null();

                    // Set prev HP as anchor HP, since prev should always be protected.
                    HazardPointer::swap(&mut self.handle.prev_h, &mut self.handle.anchor_h);
                    continue;
                }
            } else {
                // Validate on prev.
                debug_assert!(self.anchor.is_null());

                let curr_new = prev_next.load(Ordering::Acquire);

                if curr_new.tag() != 0 {
                    // If prev is marked, then restart from head.
                    return Err(());
                } else if curr_new != self.curr {
                    // self.curr's tag was 0, so the above comparison ignores tags.

                    // In contrary to what HP04 paper does, it's fine to retry protecting the new node
                    // without restarting from head as long as prev is not logically deleted.
                    self.curr = curr_new;
                    continue;
                }
            }

            let curr_node = unsafe { self.curr.deref() };
            let next = curr_node.next.load(Ordering::Acquire);
            if next.tag() == 0 {
                if curr_node.key < *key {
                    self.prev = self.curr;
                    self.curr = next;
                    self.anchor = Shared::null();
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
                self.curr = next;
                HazardPointer::swap(&mut self.handle.prev_h, &mut self.handle.curr_h);
            }
        })
    }

    #[inline]
    fn find_harris(&mut self, key: &K) -> Result<bool, ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)

        let found = self.traverse_with_anchor(key)?;

        if self.anchor.is_null() {
            self.prev = self.prev.with_tag(0);
            self.curr = self.curr.with_tag(0);
            Ok(found)
        } else {
            debug_assert_eq!(self.anchor_next.tag(), 0);
            // TODO: on CAS failure, if anchor is not tagged, we can restart from anchor.
            unsafe { &self.anchor.deref().next }
                .compare_exchange(
                    self.anchor_next,
                    self.curr.with_tag(0),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .map_err(|_| {
                    self.curr = self.curr.with_tag(0);
                    ()
                })?;

            let mut node = self.anchor_next;
            while node.with_tag(0) != self.curr.with_tag(0) {
                // NOTE: It may seem like this can be done with a NA load, but we do a `fetch_or` in remove, which always does an write.
                // This can be a NA load if the `fetch_or` in delete is changed to a CAS, but it is not clear if it is worth it.
                let next = unsafe { node.deref().next.load(Ordering::Relaxed) };
                debug_assert!(next.tag() != 0);
                unsafe { self.handle.thread.retire(node.with_tag(0).into_raw()) };
                node = next;
            }
            self.prev = self.anchor.with_tag(0);
            self.curr = self.curr.with_tag(0);
            Ok(found)
        }
    }

    #[inline]
    fn find_harris_michael(&mut self, key: &K) -> Result<bool, ()> {
        loop {
            debug_assert_eq!(self.curr.tag(), 0);
            if self.curr.is_null() {
                return Ok(false);
            }

            let prev = unsafe { &self.prev.deref().next };

            self.handle
                .curr_h
                .protect_raw(self.curr.with_tag(0).into_raw());
            light_membarrier();
            let curr_new = prev.load(Ordering::Acquire);
            if curr_new.tag() != 0 {
                return Err(());
            } else if curr_new.with_tag(0) != self.curr {
                // In contrary to what HP04 paper does, it's fine to retry protecting the new node
                // without restarting from head as long as prev is not logically deleted.
                self.curr = curr_new.with_tag(0);
                continue;
            }

            let curr_node = unsafe { self.curr.deref() };

            let next = curr_node.next.load(Ordering::Acquire);

            if next.tag() == 0 {
                match curr_node.key.cmp(key) {
                    Less => {
                        self.prev = self.curr;
                        HazardPointer::swap(&mut self.handle.prev_h, &mut self.handle.curr_h);
                    }
                    Equal => return Ok(true),
                    Greater => return Ok(false),
                }
            } else if prev
                .compare_exchange(
                    self.curr,
                    next.with_tag(0),
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                unsafe { self.handle.thread.retire(self.curr.with_tag(0).into_raw()) };
            } else {
                return Err(());
            }
            self.curr = next.with_tag(0);
        }
    }

    fn find_harris_herlihy_shavit(&mut self, key: &K) -> Result<bool, ()> {
        let found = self.traverse_with_anchor(key)?;
        // Return only the found `curr` node.
        // Others are not necessary because we are not going to do insertion or deletion
        // with this Harris-Herlihy-Shavit traversal.
        self.curr = self.curr.with_tag(0);
        Ok(found)
    }
}

impl<K, V> List<K, V>
where
    K: Ord + 'static,
{
    /// Creates a new list.
    pub fn new() -> Self {
        List {
            head: Atomic::null(),
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
                Ok(true) => return Some(&unsafe { cursor.curr.deref() }.value),
                Ok(false) => return None,
                Err(_) => continue,
            }
        }
    }

    fn insert_inner<'domain, 'hp, F>(
        &self,
        mut node: Box<Node<K, V>>,
        find: &F,
        handle: &'hp mut Handle<'domain>,
    ) -> bool
    where
        F: Fn(&mut Cursor<'domain, 'hp, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            let mut cursor = Cursor::new(&self.head, handle.launder());
            let Ok(found) = find(&mut cursor, &node.key) else {
                continue;
            };
            if found {
                return false;
            }

            node.next = cursor.curr.into();
            match unsafe { cursor.prev.deref() }.next.compare_exchange(
                cursor.curr,
                node,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(e) => node = e.new,
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
        let node = Box::new(Node {
            key,
            value,
            next: Atomic::null(),
        });

        self.insert_inner(node, &find, handle.launder())
    }

    fn remove_inner<'domain, 'hp, F>(
        &self,
        key: &K,
        find: &F,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V>
    where
        F: Fn(&mut Cursor<'domain, 'hp, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            let mut cursor = Cursor::new(&self.head, handle.launder());
            let Ok(found) = find(&mut cursor, key) else {
                continue;
            };
            if !found {
                return None;
            }

            let curr_node = unsafe { cursor.curr.deref() };
            let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
            if next.tag() == 1 {
                continue;
            }

            let prev = unsafe { &cursor.prev.deref().next };

            if prev
                .compare_exchange(cursor.curr, next, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                unsafe {
                    cursor
                        .handle
                        .thread
                        .retire(cursor.curr.with_tag(0).into_raw())
                };
            }

            return Some(&curr_node.value);
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
        self.remove_inner(key, &find, handle.launder())
    }

    #[inline]
    fn pop_inner<'hp>(&self, handle: &'hp mut Handle<'_>) -> Result<Option<(&'hp K, &'hp V)>, ()> {
        let cursor = Cursor::new(&self.head, handle.launder());
        let prev = unsafe { &cursor.prev.deref().next };

        handle
            .curr_h
            .protect_raw(cursor.curr.with_tag(0).into_raw());
        light_membarrier();
        let curr_new = prev.load(Ordering::Acquire);
        if curr_new.tag() != 0 || curr_new.with_tag(0) != cursor.curr {
            return Err(());
        }

        if cursor.curr.is_null() {
            return Ok(None);
        }

        let curr_node = unsafe { cursor.curr.deref() };

        let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
        if next.tag() == 1 {
            return Err(());
        }

        if prev
            .compare_exchange(cursor.curr, next, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            unsafe { handle.thread.retire(cursor.curr.with_tag(0).into_raw()) };
        }

        Ok(Some((&curr_node.key, &curr_node.value)))
    }

    #[inline]
    pub fn pop<'hp>(&self, handle: &'hp mut Handle<'_>) -> Option<(&'hp K, &'hp V)> {
        loop {
            match self.pop_inner(handle.launder()) {
                Ok(r) => return r,
                Err(_) => continue,
            }
        }
    }

    pub fn harris_get<'hp>(&self, key: &K, handle: &'hp mut Handle<'_>) -> Option<&'hp V> {
        self.get(key, Cursor::find_harris, handle)
    }

    pub fn harris_insert(&self, key: K, value: V, handle: &mut Handle<'_>) -> bool {
        self.insert(key, value, Cursor::find_harris, handle)
    }

    pub fn harris_remove<'hp>(&self, key: &K, handle: &'hp mut Handle<'_>) -> Option<&'hp V> {
        self.remove(key, Cursor::find_harris, handle)
    }

    pub fn harris_michael_get<'hp>(&self, key: &K, handle: &'hp mut Handle<'_>) -> Option<&'hp V> {
        self.get(key, Cursor::find_harris_michael, handle)
    }

    pub fn harris_michael_insert(&self, key: K, value: V, handle: &mut Handle<'_>) -> bool {
        self.insert(key, value, Cursor::find_harris_michael, handle)
    }

    pub fn harris_michael_remove<'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'_>,
    ) -> Option<&'hp V> {
        self.remove(key, Cursor::find_harris_michael, handle)
    }

    pub fn harris_herlihy_shavit_get<'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'_>,
    ) -> Option<&'hp V> {
        self.get(key, Cursor::find_harris_herlihy_shavit, handle)
    }
}

pub struct HList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HList<K, V>
where
    K: Ord + 'static,
{
    type Handle<'domain> = Handle<'domain>;

    fn handle() -> Self::Handle<'static> {
        Handle::default()
    }

    fn new() -> Self {
        HList { inner: List::new() }
    }

    #[inline(always)]
    fn get<'hp>(
        &'hp self,
        handle: &'hp mut Self::Handle<'_>,
        key: &'hp K,
    ) -> Option<impl OutputHolder<V>> {
        self.inner.harris_get(key, handle)
    }
    #[inline(always)]
    fn insert(&self, handle: &mut Self::Handle<'_>, key: K, value: V) -> bool {
        self.inner.harris_insert(key, value, handle)
    }
    #[inline(always)]
    fn remove<'hp>(
        &'hp self,
        handle: &'hp mut Self::Handle<'_>,
        key: &'hp K,
    ) -> Option<impl OutputHolder<V>> {
        self.inner.harris_remove(key, handle)
    }
}

pub struct HMList<K, V> {
    inner: List<K, V>,
}

impl<K, V> HMList<K, V>
where
    K: Ord + 'static,
{
    /// Pop the first element efficiently.
    /// This method is used for only the fine grained benchmark (src/bin/long_running).
    pub fn pop<'hp>(&self, handle: &'hp mut Handle<'_>) -> Option<(&'hp K, &'hp V)> {
        self.inner.pop(handle)
    }
}

impl<K, V> ConcurrentMap<K, V> for HMList<K, V>
where
    K: Ord + 'static,
{
    type Handle<'domain> = Handle<'domain>;

    fn handle() -> Self::Handle<'static> {
        Handle::default()
    }

    fn new() -> Self {
        HMList { inner: List::new() }
    }

    #[inline(always)]
    fn get<'hp>(
        &'hp self,
        handle: &'hp mut Self::Handle<'_>,
        key: &'hp K,
    ) -> Option<impl OutputHolder<V>> {
        self.inner.harris_michael_get(key, handle)
    }
    #[inline(always)]
    fn insert(&self, handle: &mut Self::Handle<'_>, key: K, value: V) -> bool {
        self.inner.harris_michael_insert(key, value, handle)
    }
    #[inline(always)]
    fn remove<'hp>(
        &'hp self,
        handle: &'hp mut Self::Handle<'_>,
        key: &'hp K,
    ) -> Option<impl OutputHolder<V>> {
        self.inner.harris_michael_remove(key, handle)
    }
}

pub struct HHSList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HHSList<K, V>
where
    K: Ord + 'static,
{
    type Handle<'domain> = Handle<'domain>;

    fn handle() -> Self::Handle<'static> {
        Handle::default()
    }

    fn new() -> Self {
        HHSList { inner: List::new() }
    }

    #[inline(always)]
    fn get<'hp>(
        &'hp self,
        handle: &'hp mut Self::Handle<'_>,
        key: &'hp K,
    ) -> Option<impl OutputHolder<V>> {
        self.inner.harris_michael_get(key, handle)
    }
    #[inline(always)]
    fn insert(&self, handle: &mut Self::Handle<'_>, key: K, value: V) -> bool {
        self.inner.harris_michael_insert(key, value, handle)
    }
    #[inline(always)]
    fn remove<'hp>(
        &'hp self,
        handle: &'hp mut Self::Handle<'_>,
        key: &'hp K,
    ) -> Option<impl OutputHolder<V>> {
        self.inner.harris_michael_remove(key, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::{HHSList, HList, HMList};
    use crate::ds_impl::hp::concurrent_map;

    #[test]
    fn smoke_h_list() {
        concurrent_map::tests::smoke::<_, HList<i32, String>, _>(&i32::to_string);
    }

    #[test]
    fn smoke_hm_list() {
        concurrent_map::tests::smoke::<_, HMList<i32, String>, _>(&i32::to_string);
    }

    #[test]
    fn smoke_hhs_list() {
        concurrent_map::tests::smoke::<_, HHSList<i32, String>, _>(&i32::to_string);
    }

    #[test]
    fn litmus_hm_pop() {
        use concurrent_map::ConcurrentMap;
        let map = HMList::new();

        let handle = &mut HMList::<i32, String>::handle();
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
