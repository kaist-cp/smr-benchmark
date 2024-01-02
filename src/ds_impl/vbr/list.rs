use super::concurrent_map::ConcurrentMap;
use vbr_rs::CompareExchangeError::Success;
use vbr_rs::{Entry, Global, Guard, ImmAtomic, Local, MutAtomic, Shared};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering;

pub struct Node<K, V>
where
    K: 'static + Copy,
    V: 'static + Copy,
{
    /// Mark: tag(), Tag: not needed
    next: MutAtomic<Node<K, V>>,
    key: ImmAtomic<K>,
    value: ImmAtomic<V>,
}

struct List<K, V>
where
    K: 'static + Copy,
    V: 'static + Copy,
{
    head: Entry<Node<K, V>>,
}

struct Cursor<'g, K, V>
where
    K: 'static + Copy,
    V: 'static + Copy,
{
    prev: Shared<'g, Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: Shared<'g, Node<K, V>>,
}

impl<'g, K, V> Cursor<'g, K, V>
where
    K: 'static + Ord + Copy,
    V: 'static + Copy,
{
    /// Creates the head cursor.
    #[inline]
    pub fn head(
        head: Shared<'g, Node<K, V>>,
        guard: &'g Guard<Node<K, V>>,
    ) -> Result<Cursor<'g, K, V>, ()> {
        Ok(Self {
            prev: head,
            curr: unsafe { head.deref() }
                .next
                .load(Ordering::Acquire, guard)?,
        })
    }
}

impl<K, V> List<K, V>
where
    K: 'static + Ord + Copy,
    V: 'static + Copy,
{
    /// Creates a new list.
    #[inline]
    pub fn new(local: &Local<Node<K, V>>) -> Self {
        loop {
            let guard = &local.guard();
            let node = ok_or!(
                guard.allocate(|node| unsafe {
                    node.deref().next.store(node, Shared::null());
                }),
                continue
            );
            return Self {
                head: Entry::new(node),
            };
        }
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn find_harris<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard<Node<K, V>>,
    ) -> Result<(bool, Cursor<'g, K, V>), ()> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
        let mut cursor = Cursor::head(self.head.load(guard)?, guard)?;
        let mut prev_next = cursor.curr;
        let found = loop {
            let curr_node = some_or!(cursor.curr.as_ref(), break false);
            let next = curr_node.next.load(Ordering::Acquire, guard)?;

            // - finding stage is done if cursor.curr advancement stops
            // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
            // - stop cursor.curr if (not marked) && (cursor.curr >= key)
            // - advance cursor.prev if not marked

            if next.tag() != 0 {
                // We add a 0 tag here so that `self.curr`s tag is always 0.
                cursor.curr = next.with_tag(0);
                continue;
            }

            match curr_node.key.get(guard)?.cmp(key) {
                Less => {
                    cursor.prev = cursor.curr;
                    cursor.curr = next;
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
            .compare_exchange(
                cursor.prev,
                prev_next,
                cursor.curr,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            )
            .success()?;

        // retire from cursor.prev.load() to cursor.curr (exclusive)
        let mut node = prev_next;
        while node.with_tag(0).as_raw() != cursor.curr.as_raw() {
            let next = unsafe { node.deref().next.load_unchecked(Ordering::Relaxed) };
            unsafe { guard.retire(node) };
            node = next;
        }

        Ok((found, cursor))
    }

    /// Clean up a single logically removed node in each traversal.
    #[inline]
    fn find_harris_michael<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard<Node<K, V>>,
    ) -> Result<(bool, Cursor<'g, K, V>), ()> {
        let mut cursor = Cursor::head(self.head.load(guard)?, guard)?;
        loop {
            debug_assert_eq!(cursor.curr.tag(), 0);

            let curr_node = some_or!(cursor.curr.as_ref(), return Ok((false, cursor)));
            let mut next = curr_node.next.load(Ordering::Acquire, guard)?;

            // NOTE: original version aborts here if self.prev is tagged

            if next.tag() != 0 {
                next = next.with_tag(0);
                let _ = unsafe { cursor.prev.deref() }
                    .next
                    .compare_exchange(
                        cursor.prev,
                        cursor.curr,
                        next,
                        Ordering::Release,
                        Ordering::Relaxed,
                        guard,
                    )
                    .success()?;
                unsafe { guard.retire(cursor.curr) };
                cursor.curr = next;
                continue;
            }

            match curr_node.key.get(guard)?.cmp(key) {
                Less => {
                    cursor.prev = cursor.curr;
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
        guard: &'g Guard<Node<K, V>>,
    ) -> Result<(bool, Cursor<'g, K, V>), ()> {
        let mut cursor = Cursor::head(self.head.load(guard)?, guard)?;
        Ok(loop {
            let curr_node = some_or!(cursor.curr.as_ref(), break (false, cursor));
            let next = curr_node.next.load(Ordering::Acquire, guard)?;
            match curr_node.key.get(guard)?.cmp(key) {
                Less => {
                    cursor.curr = next;
                    continue;
                }
                Equal => break (next.tag() == 0, cursor),
                Greater => break (false, cursor),
            }
        })
    }

    #[inline]
    fn get<F>(&self, key: &K, find: F, local: &Local<Node<K, V>>) -> Option<V>
    where
        F: for<'g> Fn(&'g Self, &K, &'g Guard<Node<K, V>>) -> Result<(bool, Cursor<'g, K, V>), ()>,
    {
        loop {
            let guard = &local.guard();
            let (found, cursor) = ok_or!(find(self, key, guard), continue);
            if found {
                if let Some(curr_ref) = cursor.curr.as_ref() {
                    let value = ok_or!(curr_ref.value.get(guard), continue);
                    return Some(value);
                } else {
                    return None;
                }
            }
            return None;
        }
    }

    #[inline]
    fn insert<F>(&self, key: K, value: V, find: F, local: &Local<Node<K, V>>) -> bool
    where
        F: for<'g> Fn(&'g Self, &K, &'g Guard<Node<K, V>>) -> Result<(bool, Cursor<'g, K, V>), ()>,
    {
        loop {
            let guard = &local.guard();
            let (found, cursor) = ok_or!(find(self, &key, guard), continue);
            if found {
                return false;
            }

            let node = ok_or!(
                guard.allocate(|node| unsafe {
                    node.deref().next.store(node, cursor.curr);
                    node.deref().key.set(key);
                    node.deref().value.set(value);
                }),
                continue
            );
            match unsafe { cursor.prev.deref() }.next.compare_exchange(
                cursor.prev,
                cursor.curr,
                node,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                Success(_) => return true,
                _ => unsafe { guard.retire(node) },
            }
        }
    }

    #[inline]
    fn remove<F>(&self, key: &K, find: F, local: &Local<Node<K, V>>) -> Option<V>
    where
        F: for<'g> Fn(&'g Self, &K, &'g Guard<Node<K, V>>) -> Result<(bool, Cursor<'g, K, V>), ()>,
    {
        loop {
            let guard = &mut local.guard();
            let (found, cursor) = ok_or!(find(self, key, guard), continue);
            if !found {
                return None;
            }

            let curr_node = unsafe { cursor.curr.deref() };
            let value = ok_or!(curr_node.value.get(guard), continue);

            let next = ok_or!(curr_node.next.load(Ordering::Acquire, guard), continue);
            if next.tag() == 1 {
                continue;
            }

            if curr_node
                .next
                .compare_exchange(
                    cursor.curr,
                    next,
                    next.with_tag(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    guard,
                )
                .success()
                .is_err()
            {
                continue;
            }

            if unsafe { cursor.prev.deref() }
                .next
                .compare_exchange(
                    cursor.prev,
                    cursor.curr,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .success()
                .is_ok()
            {
                unsafe { guard.retire(cursor.curr) };
            }

            return Some(value);
        }
    }

    #[inline]
    pub fn pop(&self, local: &Local<Node<K, V>>) -> Option<(K, V)> {
        loop {
            let guard = &local.guard();
            let head = ok_or!(self.head.load(guard), continue);
            let cursor = ok_or!(Cursor::head(head, guard), continue);
            if cursor.curr.is_null() {
                return None;
            }

            let curr_node = unsafe { cursor.curr.deref() };
            let key = ok_or!(curr_node.key.get(guard), continue);
            let value = ok_or!(curr_node.value.get(guard), continue);

            let next = ok_or!(curr_node.next.load(Ordering::Acquire, guard), continue);
            if next.tag() == 1 {
                continue;
            }
            if curr_node
                .next
                .compare_exchange(
                    cursor.curr,
                    next,
                    next.with_tag(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    guard,
                )
                .success()
                .is_err()
            {
                continue;
            }

            if unsafe { cursor.prev.deref() }
                .next
                .compare_exchange(
                    cursor.prev,
                    cursor.curr,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .success()
                .is_ok()
            {
                unsafe { guard.retire(cursor.curr) };
            }
            return Some((key, value));
        }
    }

    #[inline]
    pub fn harris_get<'g>(&'g self, key: &K, local: &Local<Node<K, V>>) -> Option<V> {
        self.get(key, Self::find_harris, local)
    }

    #[inline]
    pub fn harris_insert<'g>(&'g self, key: K, value: V, local: &Local<Node<K, V>>) -> bool {
        self.insert(key, value, Self::find_harris, local)
    }

    #[inline]
    pub fn harris_remove<'g>(&'g self, key: &K, local: &Local<Node<K, V>>) -> Option<V> {
        self.remove(key, Self::find_harris, local)
    }

    #[inline]
    pub fn harris_michael_get<'g>(&'g self, key: &K, local: &Local<Node<K, V>>) -> Option<V> {
        self.get(key, Self::find_harris_michael, local)
    }

    #[inline]
    pub fn harris_michael_insert(&self, key: K, value: V, local: &Local<Node<K, V>>) -> bool {
        self.insert(key, value, Self::find_harris_michael, local)
    }

    #[inline]
    pub fn harris_michael_remove<'g>(&'g self, key: &K, local: &Local<Node<K, V>>) -> Option<V> {
        self.remove(key, Self::find_harris_michael, local)
    }

    #[inline]
    pub fn harris_herlihy_shavit_get<'g>(
        &'g self,
        key: &K,
        local: &Local<Node<K, V>>,
    ) -> Option<V> {
        self.get(key, Self::find_harris_herlihy_shavit, local)
    }
}

pub struct HList<K, V>
where
    K: 'static + Ord + Copy,
    V: 'static + Copy,
{
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HList<K, V>
where
    K: 'static + Ord + Copy,
    V: 'static + Copy,
{
    type Global = Global<Node<K, V>>;
    type Local = Local<Node<K, V>>;

    fn global(key_range_hint: usize) -> Self::Global {
        Global::new(key_range_hint * 2)
    }

    fn local(global: &Self::Global) -> Self::Local {
        Local::new(global)
    }

    fn new(local: &Self::Local) -> Self {
        Self {
            inner: List::new(local),
        }
    }

    #[inline]
    fn get<'g>(&'g self, key: &K, local: &Self::Local) -> Option<V> {
        self.inner.harris_get(key, local)
    }
    #[inline]
    fn insert(&self, key: K, value: V, local: &Self::Local) -> bool {
        self.inner.harris_insert(key, value, local)
    }
    #[inline]
    fn remove<'g>(&'g self, key: &K, local: &Self::Local) -> Option<V> {
        self.inner.harris_remove(key, local)
    }
}

pub struct HMList<K, V>
where
    K: 'static + Ord + Copy,
    V: 'static + Copy,
{
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HMList<K, V>
where
    K: 'static + Ord + Copy,
    V: 'static + Copy,
{
    type Global = Global<Node<K, V>>;
    type Local = Local<Node<K, V>>;

    fn global(key_range_hint: usize) -> Self::Global {
        Global::new(key_range_hint * 2)
    }

    fn local(global: &Self::Global) -> Self::Local {
        Local::new(global)
    }

    fn new(local: &Self::Local) -> Self {
        Self {
            inner: List::new(local),
        }
    }

    #[inline]
    fn get<'g>(&'g self, key: &K, local: &Self::Local) -> Option<V> {
        self.inner.harris_michael_get(key, local)
    }
    #[inline]
    fn insert(&self, key: K, value: V, local: &Self::Local) -> bool {
        self.inner.harris_michael_insert(key, value, local)
    }
    #[inline]
    fn remove<'g>(&'g self, key: &K, local: &Self::Local) -> Option<V> {
        self.inner.harris_michael_remove(key, local)
    }
}

pub struct HHSList<K, V>
where
    K: 'static + Ord + Copy,
    V: 'static + Copy,
{
    inner: List<K, V>,
}

impl<K, V> HHSList<K, V>
where
    K: 'static + Ord + Copy,
    V: 'static + Copy,
{
    /// Pop the first element efficiently.
    /// This method is used for only the fine grained benchmark (src/bin/long_running).
    pub fn pop<'g>(&'g self, local: &Local<Node<K, V>>) -> Option<(K, V)> {
        self.inner.pop(local)
    }
}

impl<K, V> ConcurrentMap<K, V> for HHSList<K, V>
where
    K: 'static + Ord + Copy,
    V: 'static + Copy,
{
    type Global = Global<Node<K, V>>;
    type Local = Local<Node<K, V>>;

    fn global(key_range_hint: usize) -> Self::Global {
        Global::new(key_range_hint * 2)
    }

    fn local(global: &Self::Global) -> Self::Local {
        Local::new(global)
    }

    fn new(local: &Self::Local) -> Self {
        Self {
            inner: List::new(local),
        }
    }

    #[inline]
    fn get<'g>(&'g self, key: &K, local: &Self::Local) -> Option<V> {
        self.inner.harris_herlihy_shavit_get(key, local)
    }
    #[inline]
    fn insert(&self, key: K, value: V, local: &Self::Local) -> bool {
        self.inner.harris_insert(key, value, local)
    }
    #[inline]
    fn remove<'g>(&'g self, key: &K, local: &Self::Local) -> Option<V> {
        self.inner.harris_remove(key, local)
    }
}

#[cfg(test)]
mod tests {
    use super::{HHSList, HList, HMList};
    use crate::ds_impl::vbr::concurrent_map;

    #[test]
    fn smoke_h_list() {
        concurrent_map::tests::smoke::<HList<i32, i32>>();
    }

    #[test]
    fn smoke_hm_list() {
        concurrent_map::tests::smoke::<HMList<i32, i32>>();
    }

    #[test]
    fn smoke_hhs_list() {
        concurrent_map::tests::smoke::<HHSList<i32, i32>>();
    }

    #[test]
    fn litmus_hhs_pop() {
        use concurrent_map::ConcurrentMap;
        let global = &HHSList::global(1000);
        let local = &HHSList::local(global);
        let map = HHSList::new(local);

        map.insert(1, 1, local);
        map.insert(2, 2, local);
        map.insert(3, 3, local);

        assert_eq!(map.pop(local).unwrap(), (1, 1));
        assert_eq!(map.pop(local).unwrap(), (2, 2));
        assert_eq!(map.pop(local).unwrap(), (3, 3));
        assert_eq!(map.pop(local), None);
    }
}
