use super::concurrent_map::ConcurrentMap;
use cdrc_rs::{AcquireRetire, AtomicRcPtr, RcPtr, SnapshotPtr};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::mem;

/// Some or executing the given expression.
macro_rules! some_or {
    ($e:expr, $err:expr) => {{
        match $e {
            Some(r) => r,
            None => $err,
        }
    }};
}

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
    prev_snap: SnapshotPtr<'g, Node<K, V, Guard>, Guard>,
    curr_snap: SnapshotPtr<'g, Node<K, V, Guard>, Guard>,

    prev: RcPtr<'g, Node<K, V, Guard>, Guard>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: RcPtr<'g, Node<K, V, Guard>, Guard>,
}

impl<'g, K, V, Guard> Cursor<'g, K, V, Guard>
where
    K: Ord,
    Guard: AcquireRetire,
{
    /// Creates a cursor.
    fn new(head: &'g AtomicRcPtr<Node<K, V, Guard>, Guard>, guard: &'g Guard) -> Self {
        let prev_snap = head.load_snapshot(guard);
        let curr_snap = unsafe { prev_snap.deref() }.next.load_snapshot(guard);
        Self {
            prev_snap,
            curr_snap,
            prev: RcPtr::default(),
            curr: RcPtr::default(),
        }
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn find_harris(&mut self, key: &K, guard: &'g Guard) -> Result<bool, ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
        let mut prev_next = self.curr_snap.clone(guard);
        let found = loop {
            let curr_node = some_or!(unsafe { self.curr_snap.as_ref() }, break false);
            let next = curr_node.next.load_snapshot(guard);

            // - finding stage is done if cursor.curr advancement stops
            // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
            // - stop cursor.curr if (not marked) && (cursor.curr >= key)
            // - advance cursor.prev if not marked

            if next.mark() != 0 {
                // We add a 0 tag here so that `self.curr`s tag is always 0.
                self.curr_snap = next.with_mark(0);
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    mem::swap(&mut self.prev_snap, &mut self.curr_snap);
                    self.curr_snap = next.clone(guard);
                    prev_next = next;
                }
                Equal => break true,
                Greater => break false,
            }
        };

        self.prev = RcPtr::from_snapshot(&self.prev_snap, guard);
        self.curr = RcPtr::from_snapshot(&self.curr_snap, guard);

        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == self.curr_snap {
            return Ok(found);
        }

        let prev_next = RcPtr::from_snapshot(&prev_next, guard);

        // cleanup marked nodes between prev and curr
        if !unsafe { self.prev.deref() }
            .next
            .compare_exchange(&prev_next, &self.curr, guard)
        {
            return Err(());
        }

        // defer_destroy from cursor.prev.load() to cursor.curr (exclusive)
        let mut node = prev_next;
        while !node.eq_without_tag(&self.curr) {
            let node_ref = unsafe { node.deref() };
            let next = node_ref.next.load(guard);
            node_ref.next.store_null(guard);
            node = next;
        }

        Ok(found)
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
        let curr = mem::take(&mut self.curr);
        unsafe { node.deref() }
            .next
            .store_relaxed(curr.clone(guard), guard);

        if unsafe { self.prev.deref() }
            .next
            .compare_exchange(&curr, &node, guard)
        {
            self.curr = node;
            Ok(())
        } else {
            Err(node)
        }
    }

    /// removes the current node.
    #[inline]
    pub fn remove(self, guard: &'g Guard) -> Result<&'g V, ()> {
        let curr_node = unsafe { self.curr.deref() };

        let next = curr_node.next.fetch_mark(1, guard);
        if next.mark() == 1 {
            return Err(());
        }

        unsafe { self.prev.deref() }
            .next
            .compare_exchange(&self.curr, &next, guard);

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

#[cfg(test)]
mod tests {
    use super::HList;
    use crate::cdrc::concurrent_map;
    use cdrc_rs::GuardEBR;

    #[test]
    fn smoke_ebr_h_list() {
        concurrent_map::tests::smoke::<GuardEBR, HList<i32, String, GuardEBR>>();
    }
}
