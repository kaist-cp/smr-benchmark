use super::concurrent_map::ConcurrentMap;
use super::{tag, untagged};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::ptr::null_mut;
use std::sync::atomic::{compiler_fence, AtomicPtr, Ordering};

#[derive(Debug)]
struct Node<K, V> {
    /// Mark: tag(), Tag: not needed
    next: AtomicPtr<Node<K, V>>,
    key: K,
    value: V,
}

struct List<K, V> {
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

impl<K, V> Node<K, V> {
    /// Creates a new node.
    fn new(key: K, value: V) -> Self {
        Self {
            next: AtomicPtr::new(null_mut()),
            key,
            value,
        }
    }
}

struct Cursor<'g, K, V> {
    prev: &'g AtomicPtr<Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: *mut Node<K, V>,
}

impl<'g, K, V> Cursor<'g, K, V>
where
    K: Ord,
{
    /// Creates a cursor.
    #[inline]
    fn new(prev: &'g AtomicPtr<Node<K, V>>, curr: *mut Node<K, V>) -> Self {
        Self {
            prev,
            curr: untagged(curr),
        }
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn find_harris(&mut self, key: &K) -> Result<bool, ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
        let mut prev_next = self.curr;
        let found = loop {
            let curr_node = some_or!(unsafe { self.curr.as_ref() }, break false);
            let next = curr_node.next.load(Ordering::Acquire);

            // - finding stage is done if cursor.curr advancement stops
            // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
            // - stop cursor.curr if (not marked) && (cursor.curr >= key)
            // - advance cursor.prev if not marked

            if tag(next) != 0 {
                // We add a 0 tag here so that `self.curr`s tag is always 0.
                self.curr = untagged(next);
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

        // HACK: This compiler fence makes read-only throughput more reasonably.
        // (17377543 op/s, which is lower than EBR -> 22596132 op/s, which is higher than EBR)
        // It seems this let the compiler generate more optimized code for the traversal loop.
        compiler_fence(Ordering::SeqCst);

        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == self.curr {
            return Ok(found);
        }

        // cleanup marked nodes between prev and curr
        self.prev
            .compare_exchange(prev_next, self.curr, Ordering::Release, Ordering::Relaxed)
            .map_err(|_| ())?;

        Ok(found)
    }

    /// Clean up a single logically removed node in each traversal.
    #[inline]
    fn find_harris_michael(&mut self, key: &K) -> Result<bool, ()> {
        loop {
            debug_assert_eq!(tag(self.curr), 0);

            let curr_node = some_or!(unsafe { self.curr.as_ref() }, return Ok(false));
            let mut next = curr_node.next.load(Ordering::Acquire);

            // NOTE: original version aborts here if self.prev is tagged

            if tag(next) != 0 {
                next = untagged(next);
                self.prev
                    .compare_exchange(self.curr, next, Ordering::Release, Ordering::Relaxed)
                    .map_err(|_| ())?;
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
    fn find_harris_herlihy_shavit(&mut self, key: &K) -> Result<bool, ()> {
        Ok(loop {
            let curr_node = some_or!(unsafe { untagged(self.curr).as_ref() }, break false);
            match curr_node.key.cmp(key) {
                Less => {
                    self.curr = curr_node.next.load(Ordering::Acquire);
                    // NOTE: unnecessary (this function is expected to be used only for `get`)
                    self.prev = &curr_node.next;
                    continue;
                }
                Equal => break tag(curr_node.next.load(Ordering::Relaxed)) == 0,
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
    pub fn insert(&mut self, node: *mut Node<K, V>) -> bool {
        unsafe { &*node }.next.store(self.curr, Ordering::Relaxed);
        match self
            .prev
            .compare_exchange(self.curr, node, Ordering::Release, Ordering::Relaxed)
        {
            Ok(node) => {
                self.curr = node;
                true
            }
            Err(_) => false,
        }
    }

    /// removes the current node.
    #[inline]
    pub fn remove(self) -> Result<&'g V, ()> {
        let curr_node = unsafe { &*untagged(self.curr) };

        let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
        if tag(next) == 1 {
            return Err(());
        }

        let _ = self
            .prev
            .compare_exchange(self.curr, next, Ordering::Release, Ordering::Relaxed);

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
            head: AtomicPtr::new(null_mut()),
        }
    }

    /// Creates the head cursor.
    #[inline]
    pub fn head<'g>(&'g self) -> Cursor<'g, K, V> {
        Cursor::new(&self.head, self.head.load(Ordering::Acquire))
    }

    /// Finds a key using the given find strategy.
    #[inline(always)]
    fn find<'g, F>(&'g self, key: &K, find: &F) -> (bool, Cursor<'g, K, V>)
    where
        F: Fn(&mut Cursor<'g, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            let mut cursor = self.head();
            if let Ok(r) = find(&mut cursor, key) {
                return (r, cursor);
            }
        }
    }

    #[inline]
    fn get<'g, F>(&'g self, key: &K, find: F) -> Option<&'g V>
    where
        F: Fn(&mut Cursor<'g, K, V>, &K) -> Result<bool, ()>,
    {
        let (found, cursor) = self.find(key, &find);
        if found {
            cursor.get()
        } else {
            None
        }
    }

    #[inline]
    fn insert<'g, F>(&'g self, key: K, value: V, find: F) -> bool
    where
        F: Fn(&mut Cursor<'g, K, V>, &K) -> Result<bool, ()>,
    {
        let node = Box::into_raw(Box::new(Node::new(key, value)));
        loop {
            let (found, mut cursor) = self.find(unsafe { &((&*node).key) }, &find);
            if found {
                drop(unsafe { Box::from_raw(node) });
                return false;
            }

            if cursor.insert(node) {
                return true;
            }
        }
    }

    #[inline]
    fn remove<'g, F>(&'g self, key: &K, find: F) -> Option<&'g V>
    where
        F: Fn(&mut Cursor<'g, K, V>, &K) -> Result<bool, ()>,
    {
        loop {
            let (found, cursor) = self.find(key, &find);
            if !found {
                return None;
            }

            match cursor.remove() {
                Err(()) => continue,
                Ok(value) => return Some(value),
            }
        }
    }

    #[inline]
    pub fn pop<'g>(&'g self) -> Option<(&'g K, &'g V)> {
        loop {
            let cursor = self.head();
            if untagged(cursor.curr).is_null() {
                return None;
            }

            let curr_node = unsafe { &*untagged(cursor.curr) };

            let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
            if tag(next) == 1 {
                continue;
            }

            let _ = cursor.prev.compare_exchange(
                cursor.curr,
                next,
                Ordering::Release,
                Ordering::Relaxed,
            );

            return Some((&curr_node.key, &curr_node.value));
        }
    }

    #[inline]
    pub fn harris_get<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.get(key, Cursor::find_harris)
    }

    #[inline]
    pub fn harris_insert<'g>(&'g self, key: K, value: V) -> bool {
        self.insert(key, value, Cursor::find_harris)
    }

    #[inline]
    pub fn harris_remove<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.remove(key, Cursor::find_harris)
    }

    #[inline]
    pub fn harris_michael_get<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.get(key, Cursor::find_harris_michael)
    }

    #[inline]
    pub fn harris_michael_insert(&self, key: K, value: V) -> bool {
        self.insert(key, value, Cursor::find_harris_michael)
    }

    #[inline]
    pub fn harris_michael_remove<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.remove(key, Cursor::find_harris_michael)
    }

    #[inline]
    pub fn harris_herlihy_shavit_get<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.get(key, Cursor::find_harris_herlihy_shavit)
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
    fn get<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.inner.harris_get(key)
    }
    #[inline(never)]
    fn insert(&self, key: K, value: V) -> bool {
        self.inner.harris_insert(key, value)
    }
    #[inline(never)]
    fn remove<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.inner.harris_remove(key)
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
    #[inline]
    pub fn get_harris_herlihy_shavit<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.inner.harris_herlihy_shavit_get(key)
    }
}

impl<K, V> ConcurrentMap<K, V> for HMList<K, V>
where
    K: Ord,
{
    fn new() -> Self {
        HMList { inner: List::new() }
    }

    #[inline(never)]
    fn get<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.inner.harris_michael_get(key)
    }
    #[inline(never)]
    fn insert(&self, key: K, value: V) -> bool {
        self.inner.harris_michael_insert(key, value)
    }
    #[inline(never)]
    fn remove<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.inner.harris_michael_remove(key)
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
    pub fn pop<'g>(&'g self) -> Option<(&'g K, &'g V)> {
        self.inner.pop()
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
    fn get<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.inner.harris_get(key)
    }
    #[inline(never)]
    fn insert(&self, key: K, value: V) -> bool {
        self.inner.harris_insert(key, value)
    }
    #[inline(never)]
    fn remove<'g>(&'g self, key: &K) -> Option<&'g V> {
        self.inner.harris_remove(key)
    }
}

#[cfg(test)]
mod tests {
    use super::{HHSList, HList, HMList};
    use crate::nr::concurrent_map;

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
