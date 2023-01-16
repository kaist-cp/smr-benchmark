use super::concurrent_map::ConcurrentMap;
use crossbeam_ebr::{unprotected, Atomic, Guard, Owned, Shared};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering;

#[derive(Debug)]
struct Node<K, V> {
    /// Mark: tag(), Tag: not needed
    next: Atomic<Node<K, V>>,
    key: K,
    value: V,
}

struct List<K, V> {
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
                let next = curr_ref.next.load(Ordering::Relaxed, unprotected());
                drop(curr.into_owned());
                curr = next;
            }
        }
    }
}

impl<K, V> Node<K, V> {
    /// Creates a new node.
    fn new(key: K, value: V) -> Self {
        Self {
            next: Atomic::null(),
            key,
            value,
        }
    }
}

struct Cursor<'g, K, V> {
    prev: &'g Atomic<Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: Shared<'g, Node<K, V>>,
}

impl<'g, K, V> Cursor<'g, K, V>
where
    K: Ord,
{
    /// Creates a cursor.
    fn new(prev: &'g Atomic<Node<K, V>>, curr: Shared<'g, Node<K, V>>) -> Self {
        Self {
            prev,
            curr: curr.with_tag(0),
        }
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn find_harris(&mut self, key: &K, guard: &'g Guard) -> Result<bool, ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
        let mut prev_next = self.curr;
        let found = loop {
            let curr_node = some_or!(unsafe { self.curr.as_ref() }, break false);
            let next = curr_node.next.load(Ordering::Acquire, guard);

            // - finding stage is done if cursor.curr advancement stops
            // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
            // - stop cursor.curr if (not marked) && (cursor.curr >= key)
            // - advance cursor.prev if not marked

            if next.tag() != 0 {
                // We add a 0 tag here so that `self.curr`s tag is always 0.
                self.curr = next.with_tag(0);
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    self.curr = next;
                    self.prev = &curr_node.next;
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
        self.prev
            .compare_exchange(
                prev_next,
                self.curr,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            )
            .map_err(|_| ())?;

        // defer_destroy from cursor.prev.load() to cursor.curr (exclusive)
        let mut node = prev_next;
        while node.with_tag(0) != self.curr {
            let next = unsafe { node.deref() }.next.load(Ordering::Acquire, guard);
            unsafe { guard.defer_destroy(node) };
            node = next;
        }

        Ok(found)
    }

    /// Clean up a single logically removed node in each traversal.
    #[inline]
    fn find_harris_michael(&mut self, key: &K, guard: &'g Guard) -> Result<bool, ()> {
        loop {
            debug_assert_eq!(self.curr.tag(), 0);

            let curr_node = some_or!(unsafe { self.curr.as_ref() }, return Ok(false));
            let mut next = curr_node.next.load(Ordering::Acquire, guard);

            // NOTE: original version aborts here if self.prev is tagged

            if next.tag() != 0 {
                next = next.with_tag(0);
                self.prev
                    .compare_exchange(self.curr, next, Ordering::Release, Ordering::Relaxed, guard)
                    .map_err(|_| ())?;
                unsafe { guard.defer_destroy(self.curr) };
                self.curr = next;
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    self.prev = &curr_node.next;
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
                    self.curr = curr_node.next.load(Ordering::Acquire, guard);
                    // NOTE: unnecessary (this function is expected to be used only for `get`)
                    self.prev = &curr_node.next;
                    continue;
                }
                Equal => break curr_node.next.load(Ordering::Relaxed, guard).tag() == 0,
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
        node: Owned<Node<K, V>>,
        guard: &'g Guard,
    ) -> Result<(), Owned<Node<K, V>>> {
        node.next.store(self.curr, Ordering::Relaxed);
        match self.prev.compare_exchange(
            self.curr,
            node,
            Ordering::Release,
            Ordering::Relaxed,
            guard,
        ) {
            Ok(node) => {
                self.curr = node;
                Ok(())
            }
            Err(e) => Err(e.new),
        }
    }

    /// removes the current node.
    #[inline]
    pub fn remove(self, guard: &'g Guard) -> Result<&'g V, ()> {
        let curr_node = unsafe { self.curr.deref() };

        let next = curr_node.next.fetch_or(1, Ordering::Acquire, guard);
        if next.tag() == 1 {
            return Err(());
        }

        if self
            .prev
            .compare_exchange(self.curr, next, Ordering::Release, Ordering::Relaxed, guard)
            .is_ok()
        {
            unsafe { guard.defer_destroy(self.curr) };
        }

        Ok(&curr_node.value)
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

    /// Creates the head cursor.
    #[inline]
    pub fn head<'g>(&'g self, guard: &'g Guard) -> Cursor<'g, K, V> {
        Cursor::new(&self.head, self.head.load(Ordering::Acquire, guard))
    }

    /// Finds a key using the given find strategy.
    #[inline]
    fn find<'g, F>(&'g self, key: &K, find: &F, guard: &'g Guard) -> (bool, Cursor<'g, K, V>)
    where
        F: Fn(&mut Cursor<'g, K, V>, &K, &'g Guard) -> Result<bool, ()>,
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
        F: Fn(&mut Cursor<'g, K, V>, &K, &'g Guard) -> Result<bool, ()>,
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
        F: Fn(&mut Cursor<'g, K, V>, &K, &'g Guard) -> Result<bool, ()>,
    {
        let mut node = Owned::new(Node::new(key, value));
        loop {
            let (found, mut cursor) = self.find(&node.key, &find, guard);
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
        F: Fn(&mut Cursor<'g, K, V>, &K, &'g Guard) -> Result<bool, ()>,
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

pub struct HList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HList<K, V>
where
    K: Ord,
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

pub struct HMList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HMList<K, V>
where
    K: Ord,
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

pub struct HHSList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HHSList<K, V>
where
    K: Ord,
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
    use crate::ebr::concurrent_map;

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
