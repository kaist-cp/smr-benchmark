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
        let guard = &Guard::handle();
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
    fn new(head: &'g AtomicRcPtr<Node<K, V, Guard>, Guard>, guard: &'g Guard) -> Self {
        let prev = head.load_snapshot(guard);
        let curr = unsafe { prev.deref() }.next.load_snapshot(guard);
        Self { prev, curr }
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn find_harris(&mut self, key: &K, guard: &'g Guard) -> Result<bool, ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
        let mut prev_next = self.curr.clone(guard);
        let found = loop {
            let curr_node = some_or!(unsafe { self.curr.as_ref() }, break false);
            let next = curr_node.next.load_snapshot(guard);

            // - finding stage is done if cursor.curr advancement stops
            // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
            // - stop cursor.curr if (not marked) && (cursor.curr >= key)
            // - advance cursor.prev if not marked

            if next.mark() != 0 {
                // We add a 0 tag here so that `self.curr`s tag is always 0.
                self.curr = next.with_mark(0);
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    mem::swap(&mut self.prev, &mut self.curr);
                    self.curr = next.clone(guard);
                    prev_next = next;
                }
                Equal => break true,
                Greater => break false,
            }
        };

        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == self.curr {
            return Ok(found);
        }

        // cleanup marked nodes between prev and curr
        unsafe { self.prev.deref() }
            .next
            .compare_exchange_ss_ss(&prev_next, &self.curr, guard)
            .map_err(|_| ())?;

        Ok(found)
    }

    /// Clean up a single logically removed node in each traversal.
    #[inline]
    fn find_harris_michael(&mut self, key: &K, guard: &'g Guard) -> Result<bool, ()> {
        loop {
            debug_assert_eq!(self.curr.mark(), 0);

            let curr_node = some_or!(unsafe { self.curr.as_ref() }, return Ok(false));
            let mut next = curr_node.next.load_snapshot(guard);

            // NOTE: original version aborts here if self.prev is tagged

            if next.mark() != 0 {
                next = next.with_mark(0);
                unsafe { self.prev.deref_mut() }
                    .next
                    .compare_exchange_ss_ss(&self.curr, &next, guard)
                    .map_err(|_| ())?;
                self.curr = next;
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    mem::swap(&mut self.prev, &mut self.curr);
                    self.curr = next;
                }
                Equal => return Ok(true),
                Greater => return Ok(false),
            }
        }
    }

    /// Gotta go fast. Doesn't fail.
    #[inline]
    fn find_harris_herlihy_shavit(&mut self, key: &K, guard: &'g Guard) -> Result<bool, ()> {
        Ok(loop {
            let curr_node = some_or!(unsafe { self.curr.as_ref() }, break false);
            match curr_node.key.cmp(key) {
                Less => {
                    mem::swap(&mut self.curr, &mut self.prev);
                    self.curr = curr_node.next.load_snapshot(guard);
                    continue;
                }
                Equal => break curr_node.next.load_snapshot(guard).mark() == 0,
                Greater => break false,
            }
        })
    }

    /// gets the value.
    #[inline]
    pub fn get(&self) -> Option<&'g V> {
        unsafe { self.curr.as_ref() }.map(|n| &n.value)
    }

    /// Inserts a value.
    #[inline]
    pub fn insert(
        &mut self,
        node: RcPtr<'g, Node<K, V, Guard>, Guard>,
        guard: &'g Guard,
    ) -> Result<(), RcPtr<'g, Node<K, V, Guard>, Guard>> {
        unsafe { node.deref() }.next.store_snapshot(
            self.curr.clone(guard),
            Ordering::Relaxed,
            guard,
        );

        if unsafe { self.prev.deref() }
            .next
            .compare_exchange_ss_rc(&self.curr, &node, guard)
            .is_ok()
        {
            Ok(())
        } else {
            Err(node)
        }
    }

    /// removes the current node.
    #[inline]
    pub fn remove(self, guard: &'g Guard) -> Result<&'g V, ()> {
        let curr_node = unsafe { self.curr.deref() };

        let next = curr_node.next.fetch_or(1, guard);
        if next.mark() == 1 {
            return Err(());
        }

        let _ = unsafe { self.prev.deref() }
            .next
            .compare_exchange_ss_ss(&self.curr, &next, guard);

        Ok(&curr_node.value)
    }
}

impl<K, V, Guard> List<K, V, Guard>
where
    K: Ord + Default,
    V: Default,
    Guard: AcquireRetire,
{
    /// Creates a new list.
    pub fn new() -> Self {
        List {
            head: AtomicRcPtr::new(Node::head(), &Guard::handle()),
        }
    }

    /// Creates the head cursor.
    #[inline]
    pub fn head<'g>(&'g self, guard: &'g Guard) -> Cursor<'g, K, V, Guard> {
        Cursor::new(&self.head, guard)
    }

    /// Finds a key using the given find strategy.
    #[inline]
    fn find<'g, F>(&'g self, key: &K, find: &F, guard: &'g Guard) -> (bool, Cursor<'g, K, V, Guard>)
    where
        F: Fn(&mut Cursor<'g, K, V, Guard>, &K, &'g Guard) -> Result<bool, ()>,
    {
        loop {
            let mut cursor = self.head(guard);
            if let Ok(r) = find(&mut cursor, key, guard) {
                return (r, cursor);
            }
        }
    }

    #[inline]
    fn get<'g, F>(&'g self, key: &K, find: F, guard: &'g Guard) -> Option<&'g V>
    where
        F: Fn(&mut Cursor<'g, K, V, Guard>, &K, &'g Guard) -> Result<bool, ()>,
    {
        let (found, cursor) = self.find(key, &find, guard);
        if found {
            cursor.get()
        } else {
            None
        }
    }

    #[inline]
    fn insert<'g, F>(&'g self, key: K, value: V, find: F, guard: &'g Guard) -> bool
    where
        F: Fn(&mut Cursor<'g, K, V, Guard>, &K, &'g Guard) -> Result<bool, ()>,
    {
        let mut node = RcPtr::make_shared(Node::new(key, value), guard);
        loop {
            let (found, mut cursor) = self.find(&unsafe { node.deref() }.key, &find, guard);
            if found {
                return false;
            }

            match cursor.insert(node, guard) {
                Err(n) => node = n,
                Ok(()) => return true,
            }
        }
    }

    #[inline]
    fn remove<'g, F>(&'g self, key: &K, find: F, guard: &'g Guard) -> Option<&'g V>
    where
        F: Fn(&mut Cursor<'g, K, V, Guard>, &K, &'g Guard) -> Result<bool, ()>,
    {
        loop {
            let (found, cursor) = self.find(key, &find, guard);
            if !found {
                return None;
            }

            match cursor.remove(guard) {
                Err(()) => continue,
                Ok(value) => return Some(value),
            }
        }
    }

    /// Omitted
    pub fn harris_get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, Cursor::find_harris, guard)
    }

    /// Omitted
    pub fn harris_insert<'g>(&'g self, key: K, value: V, guard: &'g Guard) -> bool {
        self.insert(key, value, Cursor::find_harris, guard)
    }

    /// Omitted
    pub fn harris_remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.remove(key, Cursor::find_harris, guard)
    }

    /// Omitted
    pub fn harris_michael_get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, Cursor::find_harris_michael, guard)
    }

    /// Omitted
    pub fn harris_michael_insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.insert(key, value, Cursor::find_harris_michael, guard)
    }

    /// Omitted
    pub fn harris_michael_remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.remove(key, Cursor::find_harris_michael, guard)
    }

    /// Omitted
    pub fn harris_herlihy_shavit_get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, Cursor::find_harris_herlihy_shavit, guard)
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

    #[inline]
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_get(key, guard)
    }
    #[inline]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.inner.harris_insert(key, value, guard)
    }
    #[inline]
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
    #[inline]
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

    #[inline]
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_michael_get(key, guard)
    }
    #[inline]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.inner.harris_michael_insert(key, value, guard)
    }
    #[inline]
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

impl<K, V, Guard> ConcurrentMap<K, V, Guard> for HHSList<K, V, Guard>
where
    K: Ord + Default,
    V: Default,
    Guard: AcquireRetire,
{
    fn new() -> Self {
        HHSList { inner: List::new() }
    }

    #[inline]
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_herlihy_shavit_get(key, guard)
    }
    #[inline]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.inner.harris_insert(key, value, guard)
    }
    #[inline]
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
}
