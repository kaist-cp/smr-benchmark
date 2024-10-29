use super::concurrent_map::{ConcurrentMap, OutputHolder};
use super::pointers::{Atomic, Shared};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering;

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
    K: Ord + 'static,
    V: 'static,
{
    fn default() -> Self {
        Self::new()
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

struct Cursor<K, V> {
    prev: *const Atomic<Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: Shared<Node<K, V>>,
}

impl<K, V> Cursor<K, V>
where
    K: Ord,
{
    /// Creates the head cursor.
    #[inline]
    pub fn head(head: &Atomic<Node<K, V>>) -> Cursor<K, V> {
        Self {
            prev: head,
            curr: head.load(Ordering::Acquire),
        }
    }
}

impl<K, V> List<K, V>
where
    K: Ord + 'static,
    V: 'static,
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
    fn find_harris(&self, key: &K) -> Result<(bool, Cursor<K, V>), ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
        let mut cursor = Cursor::head(&self.head);
        let mut prev_next = cursor.curr;
        let found = loop {
            let curr_node = some_or!(unsafe { cursor.curr.as_ref() }, break false);
            let next = curr_node.next.load(Ordering::Acquire);

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
        unsafe { &*cursor.prev }
            .compare_exchange(prev_next, cursor.curr, Ordering::Release, Ordering::Relaxed)
            .map_err(|_| ())?;

        Ok((found, cursor))
    }

    /// Clean up a single logically removed node in each traversal.
    #[inline]
    fn find_harris_michael(&self, key: &K) -> Result<(bool, Cursor<K, V>), ()> {
        let mut cursor = Cursor::head(&self.head);
        loop {
            debug_assert_eq!(cursor.curr.tag(), 0);

            let curr_node = some_or!(unsafe { cursor.curr.as_ref() }, return Ok((false, cursor)));
            let mut next = curr_node.next.load(Ordering::Acquire);

            // NOTE: original version aborts here if self.prev is tagged

            if next.tag() != 0 {
                next = next.with_tag(0);
                unsafe { &*cursor.prev }
                    .compare_exchange(cursor.curr, next, Ordering::Release, Ordering::Relaxed)
                    .map_err(|_| ())?;
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
    fn find_harris_herlihy_shavit(&self, key: &K) -> Result<(bool, Cursor<K, V>), ()> {
        let mut cursor = Cursor::head(&self.head);
        Ok(loop {
            let curr_node = some_or!(unsafe { cursor.curr.as_ref() }, break (false, cursor));
            let next = curr_node.next.load(Ordering::Acquire);
            match curr_node.key.cmp(key) {
                Less => {
                    cursor.curr = next.with_tag(0);
                    continue;
                }
                Equal => break (next.tag() == 0, cursor),
                Greater => break (false, cursor),
            }
        })
    }

    #[inline]
    fn get<F>(&self, key: &K, find: F) -> Option<&'static V>
    where
        F: Fn(&Self, &K) -> Result<(bool, Cursor<K, V>), ()>,
    {
        loop {
            let (found, cursor) = ok_or!(find(self, key), continue);
            if found {
                return unsafe { cursor.curr.as_ref().map(|n| &n.value) };
            }
            return None;
        }
    }

    #[inline]
    fn insert<F>(&self, key: K, value: V, find: F) -> bool
    where
        F: Fn(&Self, &K) -> Result<(bool, Cursor<K, V>), ()>,
    {
        let mut node = Box::new(Node::new(key, value));
        loop {
            let (found, mut cursor) = ok_or!(find(self, &node.key), continue);
            if found {
                return false;
            }

            node.next.store(cursor.curr, Ordering::Relaxed);
            match unsafe { &*cursor.prev }.compare_exchange(
                cursor.curr,
                node,
                Ordering::Release,
                Ordering::Relaxed,
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
    fn remove<F>(&self, key: &K, find: F) -> Option<&'static V>
    where
        F: Fn(&Self, &K) -> Result<(bool, Cursor<K, V>), ()>,
    {
        loop {
            let (found, cursor) = ok_or!(find(self, key), continue);
            if !found {
                return None;
            }

            let curr_node = unsafe { cursor.curr.deref() };

            let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
            if next.tag() == 1 {
                continue;
            }

            let _ = unsafe { &*cursor.prev }.compare_exchange(
                cursor.curr,
                next,
                Ordering::Release,
                Ordering::Relaxed,
            );

            return Some(&curr_node.value);
        }
    }

    #[inline]
    pub fn pop(&self) -> Option<(&K, &V)> {
        loop {
            let cursor = Cursor::head(&self.head);
            if cursor.curr.is_null() {
                return None;
            }

            let curr_node = unsafe { cursor.curr.deref() };

            let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
            if next.tag() == 1 {
                continue;
            }

            let _ = unsafe { &*cursor.prev }.compare_exchange(
                cursor.curr,
                next,
                Ordering::Release,
                Ordering::Relaxed,
            );

            return Some((&curr_node.key, &curr_node.value));
        }
    }

    #[inline]
    pub fn harris_get(&self, key: &K) -> Option<&'static V> {
        self.get(key, Self::find_harris)
    }

    #[inline]
    pub fn harris_insert(&self, key: K, value: V) -> bool {
        self.insert(key, value, Self::find_harris)
    }

    #[inline]
    pub fn harris_remove(&self, key: &K) -> Option<&'static V> {
        self.remove(key, Self::find_harris)
    }

    #[inline]
    pub fn harris_michael_get(&self, key: &K) -> Option<&'static V> {
        self.get(key, Self::find_harris_michael)
    }

    #[inline]
    pub fn harris_michael_insert(&self, key: K, value: V) -> bool {
        self.insert(key, value, Self::find_harris_michael)
    }

    #[inline]
    pub fn harris_michael_remove(&self, key: &K) -> Option<&'static V> {
        self.remove(key, Self::find_harris_michael)
    }

    #[inline]
    pub fn harris_herlihy_shavit_get(&self, key: &K) -> Option<&'static V> {
        self.get(key, Self::find_harris_herlihy_shavit)
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
    fn new() -> Self {
        HList { inner: List::new() }
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<impl OutputHolder<V>> {
        self.inner.harris_get(key)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V) -> bool {
        self.inner.harris_insert(key, value)
    }
    #[inline(always)]
    fn remove(&self, key: &K) -> Option<impl OutputHolder<V>> {
        self.inner.harris_remove(key)
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
    fn new() -> Self {
        HMList { inner: List::new() }
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<impl OutputHolder<V>> {
        self.inner.harris_michael_get(key)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V) -> bool {
        self.inner.harris_michael_insert(key, value)
    }
    #[inline(always)]
    fn remove(&self, key: &K) -> Option<impl OutputHolder<V>> {
        self.inner.harris_michael_remove(key)
    }
}

pub struct HHSList<K, V> {
    inner: List<K, V>,
}

impl<K, V> HHSList<K, V>
where
    K: Ord + 'static,
    V: 'static,
{
    /// Pop the first element efficiently.
    /// This method is used for only the fine grained benchmark (src/bin/long_running).
    pub fn pop(&self) -> Option<(&K, &V)> {
        self.inner.pop()
    }
}

impl<K, V> ConcurrentMap<K, V> for HHSList<K, V>
where
    K: Ord + 'static,
    V: 'static,
{
    fn new() -> Self {
        HHSList { inner: List::new() }
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<impl OutputHolder<V>> {
        self.inner.harris_herlihy_shavit_get(key)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V) -> bool {
        self.inner.harris_insert(key, value)
    }
    #[inline(always)]
    fn remove(&self, key: &K) -> Option<impl OutputHolder<V>> {
        self.inner.harris_remove(key)
    }
}

#[cfg(test)]
mod tests {
    use super::{HHSList, HList, HMList};
    use crate::ds_impl::nr::concurrent_map;

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
    fn litmus_hhs_pop() {
        use concurrent_map::ConcurrentMap;
        let map = HHSList::new();

        map.insert(1, "1");
        map.insert(2, "2");
        map.insert(3, "3");

        fn assert_eq(a: (&i32, &&str), b: (i32, &str)) {
            assert_eq!(*a.0, b.0);
            assert_eq!(*a.1, b.1);
        }

        assert_eq(map.pop().unwrap(), (1, "1"));
        assert_eq(map.pop().unwrap(), (2, "2"));
        assert_eq(map.pop().unwrap(), (3, "3"));
        assert_eq!(map.pop(), None);
    }
}
