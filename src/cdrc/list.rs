use super::concurrent_map::ConcurrentMap;
use cdrc_rs::{AcquireRetire, AtomicRcPtr, RcPtr, SnapshotPtr};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::mem;
use std::sync::atomic::Ordering;

struct Node<K, V, Guard>
where
    Guard: AcquireRetire,
{
    /// Mark: tag(), Tag: not needed
    next: AtomicRcPtr<Self, Guard>,
    key: K,
    value: V,
}

struct List<K, V, Guard>
where
    Guard: AcquireRetire,
{
    head: AtomicRcPtr<Node<K, V, Guard>, Guard>,
}

impl<K, V, Guard> Default for List<K, V, Guard>
where
    K: Ord + Default,
    V: Default,
    Guard: AcquireRetire,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, Guard> Drop for List<K, V, Guard>
where
    Guard: AcquireRetire,
{
    fn drop(&mut self) {
        // `Drop` for CDRC List is not necessary, but it effectively prevents a stack overflow.
        let guard = unsafe { &Guard::unprotected() };
        unsafe {
            let mut curr = self.head.load(guard);
            let mut next;

            while !curr.is_null() {
                let curr_ref = curr.deref_mut();
                next = curr_ref.next.load(guard);
                curr_ref.next.store_null(guard);
                curr = next;
            }
        }
    }
}

impl<K, V, Guard> Node<K, V, Guard>
where
    Guard: AcquireRetire,
    K: Default,
    V: Default,
{
    /// Creates a new node.
    fn new(key: K, value: V) -> Self {
        Self {
            next: AtomicRcPtr::null(),
            key,
            value,
        }
    }

    /// Creates a dummy head.
    /// We never deref key and value of this head node.
    fn head() -> Self {
        Self {
            next: AtomicRcPtr::null(),
            key: K::default(),
            value: V::default(),
        }
    }
}

struct Cursor<'g, K, V, Guard>
where
    Guard: AcquireRetire,
{
    // `SnapshotPtr`s are used only for traversing the list.
    prev: SnapshotPtr<'g, Node<K, V, Guard>, Guard>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: SnapshotPtr<'g, Node<K, V, Guard>, Guard>,
}

impl<'g, K, V, Guard> Cursor<'g, K, V, Guard>
where
    K: Ord,
    Guard: AcquireRetire,
{
    /// Creates a cursor.
    #[inline]
    fn new(head: &'g AtomicRcPtr<Node<K, V, Guard>, Guard>, guard: &'g Guard) -> Self {
        let prev = head.load_snapshot(guard);
        let curr = unsafe { prev.deref() }.next.load_snapshot(guard);
        Self { prev, curr }
    }
}

impl<K, V, Guard> List<K, V, Guard>
where
    K: Ord + Default,
    V: Default,
    Guard: AcquireRetire,
{
    /// Creates a new list.
    #[inline]
    pub fn new() -> Self {
        List {
            head: AtomicRcPtr::new(Node::head(), unsafe { &Guard::unprotected() }),
        }
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn find_harris<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard,
    ) -> Result<(bool, Cursor<'g, K, V, Guard>), ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
        let mut cursor = Cursor::new(&self.head, guard);
        let mut prev_next = cursor.curr.clone(guard);
        let found = loop {
            let curr_node = some_or!(unsafe { cursor.curr.as_ref() }, break false);
            let next = curr_node.next.load_snapshot(guard);

            // - finding stage is done if cursor.curr advancement stops
            // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
            // - stop cursor.curr if (not marked) && (cursor.curr >= key)
            // - advance cursor.prev if not marked

            if next.mark() != 0 {
                // We add a 0 tag here so that `self.curr`s tag is always 0.
                cursor.curr = next.with_mark(0);
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    mem::swap(&mut cursor.prev, &mut cursor.curr);
                    cursor.curr = next.clone(guard);
                    prev_next = next;
                }
                Equal => break true,
                Greater => break false,
            }
        };

        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == cursor.curr {
            return Ok((found, cursor));
        }

        // cleanup marked nodes between prev and curr
        unsafe { cursor.prev.deref() }
            .next
            .compare_exchange_ss_ss(&prev_next, &cursor.curr, guard)
            .map_err(|_| ())?;

        Ok((found, cursor))
    }

    /// Clean up a single logically removed node in each traversal.
    #[inline]
    fn find_harris_michael<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard,
    ) -> Result<(bool, Cursor<'g, K, V, Guard>), ()> {
        let mut cursor = Cursor::new(&self.head, guard);
        loop {
            debug_assert_eq!(cursor.curr.mark(), 0);

            let curr_node = some_or!(unsafe { cursor.curr.as_ref() }, return Ok((false, cursor)));
            let mut next = curr_node.next.load_snapshot(guard);

            // NOTE: original version aborts here if self.prev is tagged

            if next.mark() != 0 {
                next = next.with_mark(0);
                unsafe { cursor.prev.deref_mut() }
                    .next
                    .compare_exchange_ss_ss(&cursor.curr, &next, guard)
                    .map_err(|_| ())?;
                cursor.curr = next;
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    mem::swap(&mut cursor.prev, &mut cursor.curr);
                    cursor.curr = next;
                }
                Equal => return Ok((true, cursor)),
                Greater => return Ok((false, cursor)),
            }
        }
    }

    /// Gotta go fast. Doesn't fail.
    #[inline]
    fn find_harris_herlihy_shavit<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard,
    ) -> Result<(bool, Cursor<'g, K, V, Guard>), ()> {
        let mut cursor = Cursor::new(&self.head, guard);
        Ok(loop {
            let curr_node = some_or!(unsafe { cursor.curr.as_ref() }, break (false, cursor));
            match curr_node.key.cmp(key) {
                Less => {
                    mem::swap(&mut cursor.curr, &mut cursor.prev);
                    cursor.curr = curr_node.next.load_snapshot(guard);
                    continue;
                }
                Equal => break (curr_node.next.load_snapshot(guard).mark() == 0, cursor),
                Greater => break (false, cursor),
            }
        })
    }

    #[inline]
    fn get<'g, F>(&'g self, key: &K, find: F, guard: &'g Guard) -> Option<&'g V>
    where
        F: for<'h> Fn(&'h Self, &K, &'h Guard) -> Result<(bool, Cursor<'h, K, V, Guard>), ()>,
    {
        loop {
            let (found, cursor) = ok_or!(find(self, key, guard), continue);
            if found {
                return unsafe { cursor.curr.as_ref() }.map(|n| &n.value);
            }
            return None;
        }
    }

    #[inline]
    fn insert<'g, F>(&'g self, key: K, value: V, find: F, guard: &'g Guard) -> bool
    where
        F: for<'h> Fn(&'h Self, &K, &'h Guard) -> Result<(bool, Cursor<'h, K, V, Guard>), ()>,
    {
        let node = RcPtr::make_shared(Node::new(key, value), guard);
        loop {
            let (found, cursor) =
                ok_or!(find(self, unsafe { &node.deref().key }, guard), continue);
            if found {
                return false;
            }

            unsafe { node.deref() }.next.store_snapshot(
                cursor.curr.clone(guard),
                Ordering::Relaxed,
                guard,
            );
            if unsafe { cursor.prev.deref() }
                .next
                .compare_exchange_ss_rc(&cursor.curr, &node, guard)
                .is_ok()
            {
                return true;
            }
        }
    }

    #[inline]
    fn remove<'g, F>(&'g self, key: &K, find: F, guard: &'g Guard) -> Option<&'g V>
    where
        F: for<'h> Fn(&'h Self, &K, &'h Guard) -> Result<(bool, Cursor<'h, K, V, Guard>), ()>,
    {
        loop {
            let (found, cursor) = ok_or!(find(self, key, guard), continue);
            if !found {
                return None;
            }

            let curr_node = unsafe { cursor.curr.deref() };

            let next = curr_node.next.load_snapshot(guard);
            if next.mark() == 1 {
                continue;
            }
            if curr_node
                .next
                .compare_exchange_mark(&next, 1, guard)
                .is_err()
            {
                continue;
            }

            let _ = unsafe { cursor.prev.deref() }.next.compare_exchange_ss_ss(
                &cursor.curr,
                &next,
                guard,
            );

            return Some(&curr_node.value);
        }
    }

    #[inline]
    pub fn pop<'g>(&'g self, guard: &'g Guard) -> Option<(&'g K, &'g V)> {
        loop {
            let cursor = Cursor::new(&self.head, guard);
            if cursor.curr.is_null() {
                return None;
            }

            let curr_node = unsafe { cursor.curr.deref() };

            let next = curr_node.next.load_snapshot(guard);
            if next.mark() == 1
                || curr_node
                    .next
                    .compare_exchange_mark(&next, 1, guard)
                    .is_err()
            {
                continue;
            }

            let _ = unsafe { cursor.prev.deref() }.next.compare_exchange_ss_ss(
                &cursor.curr,
                &next,
                guard,
            );

            return Some((&curr_node.key, &curr_node.value));
        }
    }

    #[inline]
    pub fn harris_get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, Self::find_harris, guard)
    }

    #[inline]
    pub fn harris_insert<'g>(&'g self, key: K, value: V, guard: &'g Guard) -> bool {
        self.insert(key, value, Self::find_harris, guard)
    }

    #[inline]
    pub fn harris_remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.remove(key, Self::find_harris, guard)
    }

    #[inline]
    pub fn harris_michael_get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, Self::find_harris_michael, guard)
    }

    #[inline]
    pub fn harris_michael_insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.insert(key, value, Self::find_harris_michael, guard)
    }

    #[inline]
    pub fn harris_michael_remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.remove(key, Self::find_harris_michael, guard)
    }

    #[inline]
    pub fn harris_herlihy_shavit_get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, Self::find_harris_herlihy_shavit, guard)
    }
}

pub struct HList<K, V, Guard>
where
    Guard: AcquireRetire,
{
    inner: List<K, V, Guard>,
}

impl<K, V, Guard> ConcurrentMap<K, V, Guard> for HList<K, V, Guard>
where
    K: Ord + Default,
    V: Default,
    Guard: AcquireRetire,
{
    fn new() -> Self {
        HList { inner: List::new() }
    }

    #[inline(always)]
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_get(key, guard)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.inner.harris_insert(key, value, guard)
    }
    #[inline(always)]
    fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_remove(key, guard)
    }
}

pub struct HMList<K, V, Guard>
where
    Guard: AcquireRetire,
{
    inner: List<K, V, Guard>,
}

impl<K, V, Guard> HMList<K, V, Guard>
where
    K: Ord + Default,
    V: Default,
    Guard: AcquireRetire,
{
    /// For optimistic search on HashMap
    #[inline(always)]
    pub fn get_harris_herlihy_shavit<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_herlihy_shavit_get(key, guard)
    }
}

impl<K, V, Guard> ConcurrentMap<K, V, Guard> for HMList<K, V, Guard>
where
    K: Ord + Default,
    V: Default,
    Guard: AcquireRetire,
{
    fn new() -> Self {
        HMList { inner: List::new() }
    }

    #[inline(always)]
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_michael_get(key, guard)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.inner.harris_michael_insert(key, value, guard)
    }
    #[inline(always)]
    fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_michael_remove(key, guard)
    }
}

pub struct HHSList<K, V, Guard>
where
    Guard: AcquireRetire,
{
    inner: List<K, V, Guard>,
}

impl<K, V, Guard> HHSList<K, V, Guard>
where
    K: Ord + Default,
    V: Default,
    Guard: AcquireRetire,
{
    /// Pop the first element efficiently.
    /// This method is used for only the fine grained benchmark (src/bin/long_running).
    pub fn pop<'g>(&'g self, guard: &'g Guard) -> Option<(&'g K, &'g V)> {
        self.inner.pop(guard)
    }
}

impl<K, V, Guard> ConcurrentMap<K, V, Guard> for HHSList<K, V, Guard>
where
    K: Ord + Default,
    V: Default,
    Guard: AcquireRetire,
{
    fn new() -> Self {
        HHSList { inner: List::new() }
    }

    #[inline(always)]
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_herlihy_shavit_get(key, guard)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.inner.harris_insert(key, value, guard)
    }
    #[inline(always)]
    fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_remove(key, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::{HHSList, HList, HMList};
    use crate::cdrc::concurrent_map;
    use cdrc_rs::GuardEBR;

    #[test]
    fn smoke_ebr_h_list() {
        concurrent_map::tests::smoke::<GuardEBR, HList<i32, String, GuardEBR>>();
    }

    #[test]
    fn smoke_ebr_hm_list() {
        concurrent_map::tests::smoke::<GuardEBR, HMList<i32, String, GuardEBR>>();
    }

    #[test]
    fn smoke_ebr_hhs_list() {
        concurrent_map::tests::smoke::<GuardEBR, HHSList<i32, String, GuardEBR>>();
    }

    #[test]
    fn litmus_hhs_pop() {
        use cdrc_rs::AcquireRetire;
        use concurrent_map::ConcurrentMap;
        let map = HHSList::new();

        let guard = &GuardEBR::handle();
        map.insert(1, "1", guard);
        map.insert(2, "2", guard);
        map.insert(3, "3", guard);

        fn assert_eq(a: (&i32, &&str), b: (i32, &str)) {
            assert_eq!(*a.0, b.0);
            assert_eq!(*a.1, b.1);
        }

        assert_eq(map.pop(guard).unwrap(), (1, "1"));
        assert_eq(map.pop(guard).unwrap(), (2, "2"));
        assert_eq(map.pop(guard).unwrap(), (3, "3"));
        assert_eq!(map.pop(guard), None);
    }
}
