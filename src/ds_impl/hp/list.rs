use super::concurrent_map::ConcurrentMap;

use super::pointers::{Atomic, Pointer, Shared};
use core::mem;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering;

use hp_pp::{light_membarrier, retire, HazardPointer, Thread, DEFAULT_DOMAIN};

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
    thread: Thread<'domain>,
}

impl Default for Handle<'static> {
    fn default() -> Self {
        Self {
            prev_h: HazardPointer::default(),
            curr_h: HazardPointer::default(),
            anchor_h: HazardPointer::default(),
            anchor_next_h: HazardPointer::default(),
            thread: Thread::new(&DEFAULT_DOMAIN),
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
    // Invariants:
    // anchor, anchor_next: protected if they are not null.
    // prev: always protected with prev_sh
    // curr: not protected.
    // curr: also has tag value when it is obtained from prev.
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

            let prev = unsafe { &self.prev.deref().next };
            self.handle
                .curr_h
                .protect_raw(self.curr.with_tag(0).into_raw());
            light_membarrier();

            // Validation depending on the state of self.curr.
            //
            // - If it is marked, validate on anchor.
            // - If it is not marked, validate on curr.

            if self.curr.tag() != 0 {
                // This means that prev -> curr was marked, hence persistent.
                debug_assert!(!self.anchor.is_null());
                debug_assert!(!self.anchor_next.is_null());
                let an_new = unsafe { &self.anchor.deref().next }.load(Ordering::Acquire);
                // Validate on anchor, which should still be the same and cleared.
                if an_new.tag() != 0 {
                    // Anchor dirty -> someone logically deleted it -> progress.
                    return Err(());
                } else if an_new != self.anchor_next {
                    // Anchor changed -> someone updated it.
                    // - Someone else cleared the logically deleted chain -> their find must return -> they must attempt CAS.

                    // TODO: optimization here, can restart from anchor, but need to setup some protection for prev & curr.
                    return Err(());
                }
            } else {
                // TODO: optimize here.
                if prev.load(Ordering::Acquire) != self.curr {
                    return Err(());
                }
            }

            let curr_node = unsafe { self.curr.deref() };
            let next = curr_node.next.load(Ordering::Acquire);
            // TODO: REALLY THINK HARD ABOUT THIS SHIELD STUFF.
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
        };

        if self.anchor.is_null() {
            self.prev = self.prev.with_tag(0);
            self.curr = self.curr.with_tag(0);
            Ok(found)
        } else {
            debug_assert_eq!(self.anchor_next.tag(), 0);
            // CAS
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
                // SAFETY: the fact that node is tagged means that it cannot be modified, hence we can safety do an non-atomic load.
                let next = unsafe { node.deref().next.as_shared() };
                debug_assert!(next.tag() != 0);
                unsafe { retire(node.with_tag(0).into_raw()) };
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

    // fn find_hhs(&mut self, key: &K) -> Result<bool, ()> {
    //     // Finding phase
    //     // - cursor.curr: first unmarked node w/ key >= search key (4)
    //     // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
    //     // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)

    //     loop {
    //         if self.curr.is_null() {
    //             return Ok(false);
    //         }

    //         let prev = unsafe { &(*self.prev).next };

    //         let (curr_base, curr_tag) = decompose_ptr(self.curr);

    //         self.handle.curr_h.protect_raw(curr_base);
    //         light_membarrier();

    //         // Validation depending on the state of self.curr.
    //         //
    //         // - If it is marked, validate on anchor.
    //         // - If it is not marked, validate on curr.

    //         if curr_tag != 0 {
    //             debug_assert!(!self.anchor.is_null());
    //             debug_assert!(!self.anchor_next.is_null());
    //             let (an_base, an_tag) =
    //                 decompose_ptr(unsafe { &(*self.anchor).next }.load(Ordering::Acquire));
    //             if an_tag != 0 {
    //                 return Err(());
    //             } else if an_base != self.anchor_next {
    //                 // TODO: optimization here, can restart from anchor, but need to setup some protection for prev & curr.
    //                 return Err(());
    //             }
    //         } else {
    //             let (curr_new_base, curr_new_tag) = decompose_ptr(prev.load(Ordering::Acquire));
    //             if curr_new_tag != 0 {
    //                 // TODO: this seems correct, but deadlocks? Might not be dealing with stuff correctly.
    //                 // self.curr = curr_new_base;
    //                 // continue;
    //                 return Err(());
    //             } else if curr_new_base != self.curr {
    //                 // In contrary to what HP04 paper does, it's fine to retry protecting the new node
    //                 // without restarting from head as long as prev is not logically deleted.
    //                 self.curr = curr_new_base;
    //                 continue;
    //             }
    //         }

    //         let curr_node = unsafe { &*curr_base };
    //         let (next_base, next_tag) = decompose_ptr(curr_node.next.load(Ordering::Acquire));
    //         // TODO: REALLY THINK HARD ABOUT THIS SHIELD STUFF.
    //         // TODO: check key first, then traversal.
    //         if next_tag == 0 {
    //             if curr_node.key < *key {
    //                 self.prev = self.curr;
    //                 self.curr = next_base;
    //                 self.anchor = ptr::null_mut();
    //                 HazardPointer::swap(&mut self.handle.curr_h, &mut self.handle.prev_h);
    //             } else {
    //                 return Ok(curr_node.key == *key);
    //             }
    //         } else {
    //             if self.anchor.is_null() {
    //                 self.anchor = self.prev;
    //                 self.anchor_next = self.curr;
    //                 HazardPointer::swap(&mut self.handle.anchor_h, &mut self.handle.prev_h);
    //             } else if self.anchor_next == self.prev {
    //                 HazardPointer::swap(&mut self.handle.anchor_next_h, &mut self.handle.prev_h);
    //             }
    //             self.prev = self.curr;
    //             self.curr = next_base;
    //             HazardPointer::swap(&mut self.handle.prev_h, &mut self.handle.curr_h);
    //         }
    //     }
    // }
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
}

pub struct HList<K, V> {
    inner: List<K, V>,
}

impl<K, V> HList<K, V>
where
    K: Ord + 'static,
{
    /// Pop the first element efficiently.
    /// This method is used for only the fine grained benchmark (src/bin/long_running).
    pub fn pop<'hp>(&self, handle: &'hp mut Handle<'_>) -> Option<(&'hp K, &'hp V)> {
        self.inner.pop(handle)
    }
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
    fn get<'hp>(&self, handle: &'hp mut Self::Handle<'_>, key: &K) -> Option<&'hp V> {
        self.inner.harris_get(key, handle)
    }
    #[inline(always)]
    fn insert(&self, handle: &mut Self::Handle<'_>, key: K, value: V) -> bool {
        self.inner.harris_insert(key, value, handle)
    }
    #[inline(always)]
    fn remove<'hp>(&self, handle: &'hp mut Self::Handle<'_>, key: &K) -> Option<&'hp V> {
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
    fn get<'hp>(&self, handle: &'hp mut Self::Handle<'_>, key: &K) -> Option<&'hp V> {
        self.inner.harris_michael_get(key, handle)
    }
    #[inline(always)]
    fn insert(&self, handle: &mut Self::Handle<'_>, key: K, value: V) -> bool {
        self.inner.harris_michael_insert(key, value, handle)
    }
    #[inline(always)]
    fn remove<'hp>(&self, handle: &'hp mut Self::Handle<'_>, key: &K) -> Option<&'hp V> {
        self.inner.harris_michael_remove(key, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::HMList;
    use crate::ds_impl::hp::concurrent_map;

    #[test]
    fn smoke_hm_list() {
        concurrent_map::tests::smoke::<HMList<i32, String>>();
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

    use super::HList;

    #[test]
    fn smoke_h_list() {
        concurrent_map::tests::smoke::<HList<i32, String>>();
    }

    #[test]
    fn litmus_h_pop() {
        use concurrent_map::ConcurrentMap;
        let map = HList::new();

        let handle = &mut HList::<i32, String>::handle();
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
