use super::concurrent_map::{ConcurrentMap, OutputHolder};
use cdrc_rs::{AtomicRc, Cs, Pointer, Rc, Snapshot, StrongPtr, TaggedCnt};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering;

pub struct Node<K, V, C: Cs> {
    next: AtomicRc<Self, C>,
    key: K,
    value: V,
}

struct List<K, V, C: Cs> {
    head: AtomicRc<Node<K, V, C>, C>,
}

impl<K, V, C> Default for List<K, V, C>
where
    K: Ord + Default,
    V: Default,
    C: Cs,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, C> Node<K, V, C>
where
    K: Default,
    V: Default,
    C: Cs,
{
    /// Creates a new node.
    fn new(key: K, value: V) -> Self {
        Self {
            next: AtomicRc::null(),
            key,
            value,
        }
    }

    /// Creates a dummy head.
    /// We never deref key and value of this head node.
    fn head() -> Self {
        Self {
            next: AtomicRc::null(),
            key: K::default(),
            value: V::default(),
        }
    }
}

pub struct Cursor<K, V, C: Cs> {
    // `Snapshot`s are used only for traversing the list.
    prev: Snapshot<Node<K, V, C>, C>,
    // We don't have to protect the next pointer of `prev`.
    prev_next: TaggedCnt<Node<K, V, C>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // tagged pointer and cause cleanup to fail.
    curr: Snapshot<Node<K, V, C>, C>,
    next: Snapshot<Node<K, V, C>, C>,
}

impl<K, V, C: Cs> OutputHolder<V> for Cursor<K, V, C> {
    fn default() -> Self {
        Cursor::new()
    }

    fn output(&self) -> &V {
        &unsafe { self.curr.deref() }.value
    }
}

impl<K, V, C: Cs> Cursor<K, V, C> {
    fn new() -> Self {
        Self {
            prev: Snapshot::new(),
            prev_next: TaggedCnt::null(),
            curr: Snapshot::new(),
            next: Snapshot::new(),
        }
    }

    /// Initializes a cursor.
    fn initialize(&mut self, head: &AtomicRc<Node<K, V, C>, C>, cs: &C) {
        self.prev.load(head, cs);
        self.curr.load(&unsafe { self.prev.deref() }.next, cs);
        self.prev_next = self.curr.as_ptr();
    }
}

impl<K: Ord, V, C: Cs> Cursor<K, V, C> {
    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn find_harris(&mut self, key: &K, cs: &C) -> Result<bool, ()> {
        // Finding phase
        // - cursor.curr: first untagged node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous untagged node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
        let found = loop {
            let curr_node = some_or!(self.curr.as_ref(), break false);
            self.next.load(&curr_node.next, cs);

            // - finding stage is done if cursor.curr advancement stops
            // - advance cursor.curr if (.next is tagged) || (cursor.curr < key)
            // - stop cursor.curr if (not tagged) && (cursor.curr >= key)
            // - advance cursor.prev if not tagged

            if self.next.tag() != 0 {
                // We add a 0 tag here so that `self.curr`s tag is always 0.
                self.next.set_tag(0);
                Snapshot::swap(&mut self.next, &mut self.curr);
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    Snapshot::swap(&mut self.prev, &mut self.curr);
                    Snapshot::swap(&mut self.curr, &mut self.next);
                    self.prev_next = self.curr.as_ptr();
                }
                Equal => break true,
                Greater => break false,
            }
        };

        // If prev and curr WERE adjacent, no need to clean up
        if self.prev_next == self.curr.as_ptr() {
            return Ok(found);
        }

        // cleanup tagged nodes between prev and curr
        unsafe { self.prev.deref() }
            .next
            .compare_exchange(
                self.prev_next,
                &self.curr,
                Ordering::Release,
                Ordering::Relaxed,
                cs,
            )
            .map_err(|_| ())?;

        Ok(found)
    }

    /// Clean up a single logically removed node in each traversal.
    #[inline]
    fn find_harris_michael(&mut self, key: &K, cs: &C) -> Result<bool, ()> {
        loop {
            debug_assert_eq!(self.curr.tag(), 0);

            let curr_node = some_or!(self.curr.as_ref(), return Ok(false));
            self.next.load(&curr_node.next, cs);

            // NOTE: original version aborts here if self.prev is tagged

            if self.next.tag() != 0 {
                self.next.set_tag(0);
                unsafe { self.prev.deref() }
                    .next
                    .compare_exchange(
                        self.curr.as_ptr(),
                        &self.next,
                        Ordering::Release,
                        Ordering::Relaxed,
                        cs,
                    )
                    .map_err(|_| ())?;
                Snapshot::swap(&mut self.curr, &mut self.next);
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    Snapshot::swap(&mut self.prev, &mut self.curr);
                    Snapshot::swap(&mut self.curr, &mut self.next);
                }
                Equal => return Ok(true),
                Greater => return Ok(false),
            }
        }
    }

    /// Gotta go fast. Doesn't fail.
    #[inline]
    fn find_harris_herlihy_shavit(&mut self, key: &K, cs: &C) -> Result<bool, ()> {
        Ok(loop {
            let curr_node = some_or!(self.curr.as_ref(), break false);
            self.next.load(&curr_node.next, cs);
            match curr_node.key.cmp(key) {
                Less => Snapshot::swap(&mut self.curr, &mut self.next),
                Equal => break self.next.tag() == 0,
                Greater => break false,
            }
        })
    }

    /// Inserts a value.
    #[inline]
    pub fn insert(
        &mut self,
        node: Rc<Node<K, V, C>, C>,
        cs: &C,
    ) -> Result<(), Rc<Node<K, V, C>, C>> {
        unsafe { node.deref() }
            .next
            .store(&self.curr, Ordering::Relaxed, cs);

        match unsafe { self.prev.deref() }.next.compare_exchange(
            self.curr.as_ptr(),
            node,
            Ordering::Release,
            Ordering::Relaxed,
            cs,
        ) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.desired),
        }
    }

    /// removes the current node.
    #[inline]
    pub fn remove(&mut self, cs: &C) -> Result<(), ()> {
        let curr_node = unsafe { self.curr.deref() };

        self.next.load(&curr_node.next, cs);
        if curr_node
            .next
            .compare_exchange(
                self.next.with_tag(0).as_ptr(),
                self.next.with_tag(1),
                Ordering::AcqRel,
                Ordering::Relaxed,
                cs,
            )
            .is_err()
        {
            return Err(());
        }

        let _ = unsafe { self.prev.deref() }.next.compare_exchange(
            self.curr.as_ptr(),
            &self.next,
            Ordering::Release,
            Ordering::Relaxed,
            cs,
        );

        Ok(())
    }
}

impl<K, V, C> List<K, V, C>
where
    K: Ord + Default,
    V: Default,
    C: Cs,
{
    /// Creates a new list.
    pub fn new() -> Self {
        List {
            head: AtomicRc::new(Node::head()),
        }
    }

    #[inline]
    fn get<F>(&self, key: &K, find: F, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool
    where
        F: Fn(&mut Cursor<K, V, C>, &K, &C) -> Result<bool, ()>,
    {
        loop {
            cursor.initialize(&self.head, cs);
            if let Ok(r) = find(cursor, key, cs) {
                return r;
            }
        }
    }

    #[inline]
    fn insert<F>(&self, key: K, value: V, find: F, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool
    where
        F: Fn(&mut Cursor<K, V, C>, &K, &C) -> Result<bool, ()>,
    {
        let mut node = Rc::new(Node::new(key, value));
        loop {
            let found = self.get(&unsafe { node.deref() }.key, &find, cursor, cs);
            if found {
                drop(unsafe { node.into_inner() });
                return false;
            }

            match cursor.insert(node, cs) {
                Err(n) => node = n,
                Ok(()) => return true,
            }
        }
    }

    #[inline]
    fn remove<F>(&self, key: &K, find: F, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool
    where
        F: Fn(&mut Cursor<K, V, C>, &K, &C) -> Result<bool, ()>,
    {
        loop {
            let found = self.get(key, &find, cursor, cs);
            if !found {
                return false;
            }

            match cursor.remove(cs) {
                Err(()) => continue,
                Ok(_) => return true,
            }
        }
    }

    #[inline]
    fn pop(&self, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        loop {
            cursor.initialize(&self.head, cs);
            if cursor.curr.is_null() {
                return false;
            }

            match cursor.remove(cs) {
                Err(()) => continue,
                Ok(_) => return true,
            }
        }
    }

    /// Omitted
    pub fn harris_get(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        self.get(key, Cursor::find_harris, cursor, cs)
    }

    /// Omitted
    pub fn harris_insert(&self, key: K, value: V, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        self.insert(key, value, Cursor::find_harris, cursor, cs)
    }

    /// Omitted
    pub fn harris_remove(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        self.remove(key, Cursor::find_harris, cursor, cs)
    }

    /// Omitted
    pub fn harris_michael_get(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        self.get(key, Cursor::find_harris_michael, cursor, cs)
    }

    /// Omitted
    pub fn harris_michael_insert(
        &self,
        key: K,
        value: V,
        cursor: &mut Cursor<K, V, C>,
        cs: &C,
    ) -> bool {
        self.insert(key, value, Cursor::find_harris_michael, cursor, cs)
    }

    /// Omitted
    pub fn harris_michael_remove(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        self.remove(key, Cursor::find_harris_michael, cursor, cs)
    }

    /// Omitted
    pub fn harris_herlihy_shavit_get(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        self.get(key, Cursor::find_harris_herlihy_shavit, cursor, cs)
    }
}

pub struct HList<K, V, C: Cs> {
    inner: List<K, V, C>,
}

impl<K, V, C> ConcurrentMap<K, V, C> for HList<K, V, C>
where
    K: Ord + Default,
    V: Default,
    C: Cs,
{
    type Output = Cursor<K, V, C>;

    fn new() -> Self {
        HList { inner: List::new() }
    }

    #[inline(always)]
    fn get(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        self.inner.harris_get(key, output, cs)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V, output: &mut Self::Output, cs: &C) -> bool {
        self.inner.harris_insert(key, value, output, cs)
    }
    #[inline(always)]
    fn remove(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        self.inner.harris_remove(key, output, cs)
    }
}

pub struct HMList<K, V, C: Cs> {
    inner: List<K, V, C>,
}

impl<K, V, C> HMList<K, V, C>
where
    K: Ord + Default,
    V: Default,
    C: Cs,
{
    /// For optimistic search on HashMap
    #[inline(always)]
    pub fn get_harris_herlihy_shavit(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        self.inner.harris_herlihy_shavit_get(key, cursor, cs)
    }
}

impl<K, V, C> ConcurrentMap<K, V, C> for HMList<K, V, C>
where
    K: Ord + Default,
    V: Default,
    C: Cs,
{
    type Output = Cursor<K, V, C>;

    fn new() -> Self {
        HMList { inner: List::new() }
    }

    #[inline(always)]
    fn get(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        self.inner.harris_michael_get(key, output, cs)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V, output: &mut Self::Output, cs: &C) -> bool {
        self.inner.harris_michael_insert(key, value, output, cs)
    }
    #[inline(always)]
    fn remove(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        self.inner.harris_michael_remove(key, output, cs)
    }
}

pub struct HHSList<K, V, C: Cs> {
    inner: List<K, V, C>,
}

impl<K, V, C> HHSList<K, V, C>
where
    K: Ord + Default,
    V: Default,
    C: Cs,
{
    /// Pop the first element efficiently.
    /// This method is used for only the fine grained benchmark (src/bin/long_running).
    pub fn pop(&self, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        self.inner.pop(cursor, cs)
    }
}

impl<K, V, C> ConcurrentMap<K, V, C> for HHSList<K, V, C>
where
    K: Ord + Default,
    V: Default,
    C: Cs,
{
    type Output = Cursor<K, V, C>;

    fn new() -> Self {
        HHSList { inner: List::new() }
    }

    #[inline(always)]
    fn get(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        self.inner.harris_herlihy_shavit_get(key, output, cs)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V, output: &mut Self::Output, cs: &C) -> bool {
        self.inner.harris_insert(key, value, output, cs)
    }
    #[inline(always)]
    fn remove(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        self.inner.harris_remove(key, output, cs)
    }
}

#[cfg(test)]
mod tests {
    use super::{HHSList, HList, HMList};
    use crate::cdrc::concurrent_map;
    use cdrc_rs::{CsEBR, CsHP};

    #[test]
    fn smoke_ebr_h_list_ebr() {
        concurrent_map::tests::smoke::<CsEBR, HList<i32, String, CsEBR>>();
    }

    #[test]
    fn smoke_ebr_hm_list_ebr() {
        concurrent_map::tests::smoke::<CsEBR, HMList<i32, String, CsEBR>>();
    }

    #[test]
    fn smoke_ebr_hhs_list_ebr() {
        concurrent_map::tests::smoke::<CsEBR, HHSList<i32, String, CsEBR>>();
    }

    #[test]
    fn smoke_ebr_h_list_hp() {
        concurrent_map::tests::smoke::<CsHP, HList<i32, String, CsHP>>();
    }

    #[test]
    fn smoke_ebr_hm_list_hp() {
        concurrent_map::tests::smoke::<CsHP, HMList<i32, String, CsHP>>();
    }

    #[test]
    fn smoke_ebr_hhs_list_hp() {
        concurrent_map::tests::smoke::<CsHP, HHSList<i32, String, CsHP>>();
    }

    #[test]
    fn litmus_hhs_pop() {
        use cdrc_rs::Cs;
        use concurrent_map::ConcurrentMap;
        let map = HHSList::new();

        let output = &mut HHSList::empty_output();
        let cs = &CsEBR::new();
        map.insert(1, "1", output, cs);
        map.insert(2, "2", output, cs);
        map.insert(3, "3", output, cs);

        assert!(map.pop(output, cs));
        assert!(map.pop(output, cs));
        assert!(map.pop(output, cs));
        assert!(!map.pop(output, cs));
    }
}
