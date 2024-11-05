use super::concurrent_map::ConcurrentMap;

use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering;

use crystalline_l::{Atomic, Handle, HazardEra, Shared};

// `#[repr(C)]` is used to ensure the first field
// is also the first data in the memory alignment.
#[repr(C)]
pub struct Node<K, V> {
    /// Mark: tag(), Tag: not needed
    next: Atomic<Self>,
    key: K,
    value: V,
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
        let mut curr = self.head.load(Ordering::Relaxed);

        while !curr.is_null() {
            curr = unsafe { curr.into_owned() }.next.load(Ordering::Relaxed);
        }
    }
}

pub struct EraShields<'d, 'h, K, V> {
    prev_h: HazardEra<'d, 'h, Node<K, V>>,
    curr_h: HazardEra<'d, 'h, Node<K, V>>,
    next_h: HazardEra<'d, 'h, Node<K, V>>,
    // `anchor_h` and `anchor_next_h` are used for `find_harris`
    anchor_h: HazardEra<'d, 'h, Node<K, V>>,
    anchor_next_h: HazardEra<'d, 'h, Node<K, V>>,
}

impl<'d, 'h, K, V> EraShields<'d, 'h, K, V> {
    pub fn new(handle: &'h Handle<'d, Node<K, V>>) -> Self {
        Self {
            prev_h: HazardEra::new(handle),
            curr_h: HazardEra::new(handle),
            next_h: HazardEra::new(handle),
            anchor_h: HazardEra::new(handle),
            anchor_next_h: HazardEra::new(handle),
        }
    }

    // bypass E0499-E0503, etc that are supposed to be fixed by polonius
    #[inline]
    fn launder<'he2>(&mut self) -> &'he2 mut Self {
        unsafe { core::mem::transmute(self) }
    }
}

pub struct Cursor<K, V> {
    prev: Shared<Node<K, V>>,
    // For harris, this keeps the mark bit. Don't mix harris and harris-michael.
    curr: Shared<Node<K, V>>,
    // `anchor` is used for `find_harris`
    // anchor and anchor_next are non-null iff exist
    anchor: Shared<Node<K, V>>,
    anchor_next: Shared<Node<K, V>>,
}

impl<K, V> Cursor<K, V> {
    pub fn new<'d, 'h>(head: &Atomic<Node<K, V>>, shields: &mut EraShields<'d, 'h, K, V>) -> Self {
        Self {
            prev: Shared::from(head as *const _ as usize),
            curr: head.protect(&mut shields.curr_h),
            anchor: Shared::null(),
            anchor_next: Shared::null(),
        }
    }
}

impl<K, V> Cursor<K, V>
where
    K: Ord,
{
    /// Optimistically traverses while maintaining `anchor` and `anchor_next`.
    /// It is used for both Harris and Harris-Herlihy-Shavit traversals.
    #[inline]
    fn traverse_with_anchor<'d, 'h>(
        &mut self,
        key: &K,
        shields: &mut EraShields<'d, 'h, K, V>,
    ) -> Result<bool, ()> {
        // Invariants:
        // anchor, anchor_next: protected if they are not null.
        // prev: always protected with prev_sh
        // curr: not protected.
        // curr: also has tag value when it is obtained from prev.
        Ok(loop {
            let Some(curr_node) = (unsafe { self.curr.as_ref() }) else {
                break false;
            };

            // Validation depending on the state of `self.curr`.
            //
            // - If it is marked, validate on anchor.
            // - If it is not marked, it is already protected safely by the Crystalline.
            if self.curr.tag() != 0 {
                // Validate on anchor.

                debug_assert!(!self.anchor.is_null());
                debug_assert!(!self.anchor_next.is_null());
                let an_new = unsafe { self.anchor.deref() }.next.load(Ordering::Acquire);

                if an_new.tag() != 0 {
                    return Err(());
                } else if !an_new.ptr_eq(self.anchor_next) {
                    // Anchor is updated but clear, so can restart from anchor.

                    self.prev = self.anchor;
                    self.curr = an_new;
                    self.anchor = Shared::null();

                    // Set prev HP as anchor HP, since prev should always be protected.
                    HazardEra::swap(&mut shields.prev_h, &mut shields.anchor_h);
                    continue;
                }
            }

            let next = curr_node.next.protect(&mut shields.next_h);
            if next.tag() == 0 {
                if curr_node.key < *key {
                    self.prev = self.curr;
                    self.curr = next;
                    self.anchor = Shared::null();
                    HazardEra::swap(&mut shields.curr_h, &mut shields.prev_h);
                    HazardEra::swap(&mut shields.curr_h, &mut shields.next_h);
                } else {
                    break curr_node.key == *key;
                }
            } else {
                if self.anchor.is_null() {
                    self.anchor = self.prev;
                    self.anchor_next = self.curr;
                    HazardEra::swap(&mut shields.anchor_h, &mut shields.prev_h);
                } else if self.anchor_next.ptr_eq(self.prev) {
                    HazardEra::swap(&mut shields.anchor_next_h, &mut shields.prev_h);
                }
                self.prev = self.curr;
                self.curr = next;
                HazardEra::swap(&mut shields.curr_h, &mut shields.prev_h);
                HazardEra::swap(&mut shields.curr_h, &mut shields.next_h);
            }
        })
    }

    #[inline]
    fn find_harris<'d, 'h>(
        &mut self,
        key: &K,
        shields: &mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Result<bool, ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)

        let found = self.traverse_with_anchor(key, shields)?;

        scopeguard::defer! {
            shields.anchor_h.clear();
            shields.anchor_next_h.clear();
        }

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
            while !node.with_tag(0).ptr_eq(self.curr.with_tag(0)) {
                // NOTE: It may seem like this can be done with a NA load, but we do a `fetch_or` in remove, which always does an write.
                // This can be a NA load if the `fetch_or` in delete is changed to a CAS, but it is not clear if it is worth it.
                let next = unsafe { node.deref().next.load(Ordering::Relaxed) };
                debug_assert!(next.tag() != 0);
                unsafe { handle.retire(node) };
                node = next;
            }
            self.prev = self.anchor.with_tag(0);
            self.curr = self.curr.with_tag(0);
            Ok(found)
        }
    }

    #[inline]
    fn find_harris_michael<'d, 'h>(
        &mut self,
        key: &K,
        shields: &mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Result<bool, ()> {
        loop {
            debug_assert_eq!(self.curr.tag(), 0);
            let Some(curr_node) = (unsafe { self.curr.as_ref() }) else {
                return Ok(false);
            };
            let mut next = curr_node.next.protect(&mut shields.next_h);

            if next.tag() != 0 {
                next = next.with_tag(0);
                unsafe { self.prev.deref() }
                    .next
                    .compare_exchange(self.curr, next, Ordering::AcqRel, Ordering::Acquire)
                    .map_err(|_| ())?;
                unsafe { handle.retire(self.curr) };
                HazardEra::swap(&mut shields.curr_h, &mut shields.next_h);
                self.curr = next;
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    HazardEra::swap(&mut shields.prev_h, &mut shields.curr_h);
                    HazardEra::swap(&mut shields.curr_h, &mut shields.next_h);
                    self.prev = self.curr;
                    self.curr = next;
                }
                Equal => return Ok(true),
                Greater => return Ok(false),
            }
        }
    }

    #[inline]
    fn find_harris_herlihy_shavit<'d, 'h>(
        &mut self,
        key: &K,
        shields: &mut EraShields<'d, 'h, K, V>,
        _handle: &'h Handle<'d, Node<K, V>>,
    ) -> Result<bool, ()> {
        let found = self.traverse_with_anchor(key, shields)?;
        // Return only the found `curr` node.
        // Others are not necessary because we are not going to do insertion or deletion
        // with this Harris-Herlihy-Shavit traversal.
        self.curr = self.curr.with_tag(0);
        shields.anchor_h.clear();
        shields.anchor_next_h.clear();
        Ok(found)
    }
}

impl<K, V> List<K, V>
where
    K: Ord,
{
    /// Creates a new list.
    pub fn new() -> Self {
        List {
            head: Atomic::null(),
        }
    }

    #[inline]
    fn get<'d, 'h, 'he, F>(
        &self,
        key: &K,
        find: F,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Option<&'he V>
    where
        F: Fn(
            &mut Cursor<K, V>,
            &K,
            &mut EraShields<'d, 'h, K, V>,
            &'h Handle<'d, Node<K, V>>,
        ) -> Result<bool, ()>,
    {
        loop {
            let mut cursor = Cursor::new(&self.head, shields);
            match find(&mut cursor, key, shields, handle) {
                Ok(true) => return unsafe { Some(&(cursor.curr.deref().value)) },
                Ok(false) => return None,
                Err(_) => continue,
            }
        }
    }

    fn insert_inner<'d, 'h, 'he, F>(
        &self,
        node: Shared<Node<K, V>>,
        find: &F,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Result<bool, ()>
    where
        F: Fn(
            &mut Cursor<K, V>,
            &K,
            &mut EraShields<'d, 'h, K, V>,
            &'h Handle<'d, Node<K, V>>,
        ) -> Result<bool, ()>,
    {
        loop {
            let mut cursor = Cursor::new(&self.head, shields);
            let found = find(&mut cursor, unsafe { &node.deref().key }, shields, handle)?;
            if found {
                drop(unsafe { node.into_owned() });
                return Ok(false);
            }

            unsafe { node.deref() }
                .next
                .store(cursor.curr, Ordering::Relaxed);
            if unsafe { cursor.prev.deref() }
                .next
                .compare_exchange(cursor.curr, node, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(true);
            }
        }
    }

    #[inline]
    fn insert<'d, 'h, 'he, F>(
        &self,
        key: K,
        value: V,
        find: F,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> bool
    where
        F: Fn(
            &mut Cursor<K, V>,
            &K,
            &mut EraShields<'d, 'h, K, V>,
            &'h Handle<'d, Node<K, V>>,
        ) -> Result<bool, ()>,
    {
        let node = Shared::new(
            Node {
                key,
                value,
                next: Atomic::null(),
            },
            handle,
        );

        loop {
            match self.insert_inner(node, &find, shields, handle) {
                Ok(r) => return r,
                Err(()) => continue,
            }
        }
    }

    fn remove_inner<'d, 'h, 'he, F>(
        &self,
        key: &K,
        find: &F,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Result<Option<&'he V>, ()>
    where
        F: Fn(
            &mut Cursor<K, V>,
            &K,
            &mut EraShields<'d, 'h, K, V>,
            &'h Handle<'d, Node<K, V>>,
        ) -> Result<bool, ()>,
    {
        loop {
            let mut cursor = Cursor::new(&self.head, shields);
            let found = find(&mut cursor, key, shields, handle)?;
            if !found {
                return Ok(None);
            }

            let curr_node = unsafe { cursor.curr.deref() };
            let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
            if next.tag() == 1 {
                continue;
            }

            if unsafe { &cursor.prev.deref().next }
                .compare_exchange(cursor.curr, next, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                unsafe { handle.retire(cursor.curr) };
            }

            return Ok(Some(&curr_node.value));
        }
    }

    #[inline]
    fn remove<'d, 'h, 'he, F>(
        &self,
        key: &K,
        find: F,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Option<&'he V>
    where
        F: Fn(
            &mut Cursor<K, V>,
            &K,
            &mut EraShields<'d, 'h, K, V>,
            &'h Handle<'d, Node<K, V>>,
        ) -> Result<bool, ()>,
    {
        loop {
            match self.remove_inner(key, &find, shields.launder(), handle) {
                Ok(r) => return r,
                Err(_) => continue,
            }
        }
    }

    pub fn harris_get<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Option<&'he V> {
        self.get(key, Cursor::find_harris, shields, handle)
    }

    pub fn harris_insert<'d, 'h, 'he>(
        &self,
        key: K,
        value: V,
        shields: &mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> bool {
        self.insert(key, value, Cursor::find_harris, shields, handle)
    }

    pub fn harris_remove<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Option<&'he V> {
        self.remove(key, Cursor::find_harris, shields, handle)
    }

    pub fn harris_michael_get<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Option<&'he V> {
        self.get(key, Cursor::find_harris_michael, shields, handle)
    }

    pub fn harris_michael_insert<'d, 'h>(
        &self,
        key: K,
        value: V,
        shields: &mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> bool {
        self.insert(key, value, Cursor::find_harris_michael, shields, handle)
    }

    pub fn harris_michael_remove<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Option<&'he V> {
        self.remove(key, Cursor::find_harris_michael, shields, handle)
    }

    pub fn harris_herlihy_shavit_get<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &'h Handle<'d, Node<K, V>>,
    ) -> Option<&'he V> {
        self.get(key, Cursor::find_harris_herlihy_shavit, shields, handle)
    }
}

pub struct HList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HList<K, V>
where
    K: Ord + 'static,
    V: 'static,
{
    type Node = Node<K, V>;

    type Shields<'d, 'h>
        = EraShields<'d, 'h, K, V>
    where
        'd: 'h;

    fn shields<'d, 'h>(handle: &'h Handle<'d, Self::Node>) -> Self::Shields<'d, 'h> {
        EraShields::new(handle)
    }

    fn new<'d, 'h>(_: &'h Handle<'d, Self::Node>) -> Self {
        Self { inner: List::new() }
    }

    fn get<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V> {
        self.inner.harris_get(key, shields, handle)
    }

    fn insert<'d, 'h>(
        &self,
        key: K,
        value: V,
        shields: &mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> bool {
        self.inner.harris_insert(key, value, shields, handle)
    }

    fn remove<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V> {
        self.inner.harris_remove(key, shields, handle)
    }
}

pub struct HMList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HMList<K, V>
where
    K: Ord + 'static,
    V: 'static,
{
    type Node = Node<K, V>;

    type Shields<'d, 'h>
        = EraShields<'d, 'h, K, V>
    where
        'd: 'h;

    fn shields<'d, 'h>(handle: &'h Handle<'d, Self::Node>) -> Self::Shields<'d, 'h> {
        EraShields::new(handle)
    }

    fn new<'d, 'h>(_: &'h Handle<'d, Self::Node>) -> Self {
        Self { inner: List::new() }
    }

    fn get<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V> {
        self.inner.harris_michael_get(key, shields, handle)
    }

    fn insert<'d, 'h>(
        &self,
        key: K,
        value: V,
        shields: &mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> bool {
        self.inner
            .harris_michael_insert(key, value, shields, handle)
    }

    fn remove<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V> {
        self.inner.harris_michael_remove(key, shields, handle)
    }
}

pub struct HHSList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HHSList<K, V>
where
    K: Ord + 'static,
    V: 'static,
{
    type Node = Node<K, V>;

    type Shields<'d, 'h>
        = EraShields<'d, 'h, K, V>
    where
        'd: 'h;

    fn shields<'d, 'h>(handle: &'h Handle<'d, Self::Node>) -> Self::Shields<'d, 'h> {
        EraShields::new(handle)
    }

    fn new<'d, 'h>(_: &'h Handle<'d, Self::Node>) -> Self {
        Self { inner: List::new() }
    }

    fn get<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V> {
        self.inner.harris_herlihy_shavit_get(key, shields, handle)
    }

    fn insert<'d, 'h>(
        &self,
        key: K,
        value: V,
        shields: &mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> bool {
        self.inner.harris_insert(key, value, shields, handle)
    }

    fn remove<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V> {
        self.inner.harris_remove(key, shields, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::{HHSList, HList, HMList};
    use crate::ds_impl::crystalline_l::concurrent_map;

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
