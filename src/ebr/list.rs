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
    #[inline]
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
    /// Creates the head cursor.
    #[inline]
    pub fn head(head: &'g Atomic<Node<K, V>>, guard: &'g Guard) -> Cursor<'g, K, V> {
        Self {
            prev: head,
            curr: head.load(Ordering::Acquire, guard),
        }
    }
}

impl<K, V> List<K, V>
where
    K: Ord,
{
    /// Creates a new list.
    #[inline]
    pub fn new() -> Self {
        List {
            head: Atomic::null(),
        }
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn find_harris<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard,
    ) -> Result<(bool, Cursor<'g, K, V>), ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
        let mut cursor = Cursor::head(&self.head, guard);
        let mut prev_next = cursor.curr;
        let found = loop {
            let curr_node = some_or!(unsafe { cursor.curr.as_ref() }, break false);
            let next = curr_node.next.load(Ordering::Acquire, guard);

            // - finding stage is done if cursor.curr advancement stops
            // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
            // - stop cursor.curr if (not marked) && (cursor.curr >= key)
            // - advance cursor.prev if not marked

            if next.tag() != 0 {
                // We add a 0 tag here so that `self.curr`s tag is always 0.
                cursor.curr = next.with_tag(0);
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    cursor.curr = next;
                    cursor.prev = &curr_node.next;
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
        cursor
            .prev
            .compare_exchange(
                prev_next,
                cursor.curr,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            )
            .map_err(|_| ())?;

        // defer_destroy from cursor.prev.load() to cursor.curr (exclusive)
        let mut node = prev_next;
        while node.with_tag(0) != cursor.curr {
            let next = unsafe { node.deref() }.next.load(Ordering::Acquire, guard);
            unsafe { guard.defer_destroy(node) };
            node = next;
        }

        Ok((found, cursor))
    }

    /// Clean up a single logically removed node in each traversal.
    #[inline]
    fn find_harris_michael<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard,
    ) -> Result<(bool, Cursor<'g, K, V>), ()> {
        let mut cursor = Cursor::head(&self.head, guard);
        loop {
            debug_assert_eq!(cursor.curr.tag(), 0);

            let curr_node = some_or!(unsafe { cursor.curr.as_ref() }, return Ok((false, cursor)));
            let mut next = curr_node.next.load(Ordering::Acquire, guard);

            // NOTE: original version aborts here if self.prev is tagged

            if next.tag() != 0 {
                next = next.with_tag(0);
                cursor
                    .prev
                    .compare_exchange(
                        cursor.curr,
                        next,
                        Ordering::Release,
                        Ordering::Relaxed,
                        guard,
                    )
                    .map_err(|_| ())?;
                unsafe { guard.defer_destroy(cursor.curr) };
                cursor.curr = next;
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    cursor.prev = &curr_node.next;
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
    ) -> Result<(bool, Cursor<'g, K, V>), ()> {
        let mut cursor = Cursor::head(&self.head, guard);
        Ok(loop {
            let curr_node = some_or!(unsafe { cursor.curr.as_ref() }, break (false, cursor));
            match curr_node.key.cmp(key) {
                Less => {
                    cursor.curr = curr_node.next.load(Ordering::Acquire, guard);
                    // NOTE: unnecessary (this function is expected to be used only for `get`)
                    cursor.prev = &curr_node.next;
                    continue;
                }
                Equal => {
                    break (
                        curr_node.next.load(Ordering::Relaxed, guard).tag() == 0,
                        cursor,
                    )
                }
                Greater => break (false, cursor),
            }
        })
    }

    #[inline]
    fn get<'g, F>(&'g self, key: &K, find: F, guard: &'g Guard) -> Option<&'g V>
    where
        F: for<'h> Fn(&'h Self, &K, &'h Guard) -> Result<(bool, Cursor<'h, K, V>), ()>,
    {
        loop {
            let (found, cursor) = ok_or!(find(self, key, guard), continue);
            if found {
                return unsafe { cursor.curr.as_ref().map(|n| &n.value) };
            }
            return None;
        }
    }

    #[inline]
    fn insert<'g, F>(&'g self, key: K, value: V, find: F, guard: &'g Guard) -> bool
    where
        F: for<'h> Fn(&'h Self, &K, &'h Guard) -> Result<(bool, Cursor<'h, K, V>), ()>,
    {
        let mut node = Owned::new(Node::new(key, value));
        loop {
            let (found, mut cursor) = ok_or!(find(self, &node.key, guard), continue);
            if found {
                return false;
            }

            node.next.store(cursor.curr, Ordering::Relaxed);
            match cursor.prev.compare_exchange(
                cursor.curr,
                node,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                Ok(node) => {
                    cursor.curr = node;
                    return true;
                }
                Err(e) => node = e.new,
            }
        }
    }

    #[inline]
    fn remove<'g, F>(&'g self, key: &K, find: F, guard: &'g Guard) -> Option<&'g V>
    where
        F: for<'h> Fn(&'h Self, &K, &'h Guard) -> Result<(bool, Cursor<'h, K, V>), ()>,
    {
        loop {
            let (found, cursor) = ok_or!(find(self, key, guard), continue);
            if !found {
                return None;
            }

            let curr_node = unsafe { cursor.curr.deref() };

            let next = curr_node.next.fetch_or(1, Ordering::AcqRel, guard);
            if next.tag() == 1 {
                continue;
            }

            if cursor
                .prev
                .compare_exchange(
                    cursor.curr,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok()
            {
                unsafe { guard.defer_destroy(cursor.curr) };
            }

            return Some(&curr_node.value);
        }
    }

    #[inline]
    pub fn pop<'g>(&'g self, guard: &'g Guard) -> Option<(&'g K, &'g V)> {
        loop {
            let cursor = Cursor::head(&self.head, guard);
            if cursor.curr.is_null() {
                return None;
            }

            let curr_node = unsafe { cursor.curr.deref() };

            let next = curr_node.next.fetch_or(1, Ordering::AcqRel, guard);
            if next.tag() == 1 {
                continue;
            }

            if cursor
                .prev
                .compare_exchange(
                    cursor.curr,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok()
            {
                unsafe { guard.defer_destroy(cursor.curr) };
            }

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

    #[inline(never)]
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_get(key, guard)
    }
    #[inline(never)]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.inner.harris_insert(key, value, guard)
    }
    #[inline(never)]
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

    #[inline(never)]
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_michael_get(key, guard)
    }
    #[inline(never)]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.inner.harris_michael_insert(key, value, guard)
    }
    #[inline(never)]
    fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_michael_remove(key, guard)
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
    pub fn pop<'g>(&'g self, guard: &'g Guard) -> Option<(&'g K, &'g V)> {
        self.inner.pop(guard)
    }
}

impl<K, V> ConcurrentMap<K, V> for HHSList<K, V>
where
    K: Ord,
{
    fn new() -> Self {
        HHSList { inner: List::new() }
    }

    #[inline(never)]
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.inner.harris_herlihy_shavit_get(key, guard)
    }
    #[inline(never)]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.inner.harris_insert(key, value, guard)
    }
    #[inline(never)]
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

    #[test]
    fn litmus_hhs_pop() {
        use concurrent_map::ConcurrentMap;
        let map = HHSList::new();

        let guard = &crossbeam_ebr::pin();
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
