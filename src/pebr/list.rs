use super::concurrent_map::ConcurrentMap;
use crossbeam_pebr::{unprotected, Atomic, Guard, Owned, Pointer, Shared, Shield, ShieldError};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::mem::{self, ManuallyDrop};
use std::ptr;
use std::sync::atomic::Ordering;

enum FindError {
    Retry,
    ShieldError(ShieldError),
}

#[derive(Debug)]
struct Node<K, V> {
    /// Mark: tag(), Tag: not needed
    next: Atomic<Node<K, V>>,
    key: K,
    value: ManuallyDrop<V>,
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
        unsafe {
            let mut curr = self.head.load(Ordering::Relaxed, unprotected());

            while !curr.is_null() {
                let curr_ref = curr.deref_mut();
                let next = curr_ref.next.load(Ordering::Relaxed, unprotected());
                if next.tag() == 0 {
                    ManuallyDrop::drop(&mut curr_ref.value);
                }
                drop(curr.into_owned());
                curr = next;
            }
        }
    }
}

pub struct Cursor<K, V> {
    shields: [Shield<Node<K, V>>; 2],
    swap: usize,
}

impl<K, V> Cursor<K, V> {
    pub fn new(guard: &Guard) -> Self {
        Self {
            shields: [Shield::null(guard), Shield::null(guard)],
            swap: 0,
        }
    }

    #[inline]
    fn prev_mut(&mut self) -> &mut Shield<Node<K, V>> {
        unsafe { self.shields.get_unchecked_mut(self.swap & 1) }
    }

    #[inline]
    fn curr_mut(&mut self) -> &mut Shield<Node<K, V>> {
        unsafe {
            self.shields
                .get_unchecked_mut(self.swap.wrapping_add(1) & 1)
        }
    }

    #[inline]
    fn prev(&self) -> &Shield<Node<K, V>> {
        unsafe { self.shields.get_unchecked(self.swap & 1) }
    }

    #[inline]
    fn curr(&self) -> &Shield<Node<K, V>> {
        unsafe { self.shields.get_unchecked(self.swap.wrapping_add(1) & 1) }
    }

    #[inline]
    fn swap(&mut self) {
        self.swap = self.swap.wrapping_add(1);
    }

    #[inline]
    pub fn release(&mut self) {
        unsafe {
            self.shields.get_unchecked_mut(0).release();
            self.shields.get_unchecked_mut(1).release();
        }
    }

    fn init_find(&mut self, head: &Atomic<Node<K, V>>) {
        // HACK(@jeehoonkang): we're unsafely assuming the first 8 bytes of both `Node<K, V>`
        // and `List<K, V>` are `Atomic<Node<K, V>>`.
        unsafe {
            self.prev_mut()
                .defend_fake(Shared::from_usize(head as *const _ as usize));
        }
    }
}

/// Note `guard: &'g Guard`. The inner functions should fail if ejected. Repinning is the job of
/// the wrapper function.
///
/// Each find function expects `self.prev` to fake-defend
/// `Shared::from_usize(&list.head as *const Atomic<Node<K, V>> as usize)`.
impl<K, V> Cursor<K, V>
where
    K: Ord,
{
    #[inline]
    fn find_harris<'g>(&mut self, key: &K, guard: &'g Guard) -> Result<bool, FindError> {
        // Finding phase
        // - cursor.curr: first unmarked node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)

        let head = unsafe { &*(self.prev().shared().into_usize() as *const Atomic<Node<K, V>>) };
        let mut curr = head.load(Ordering::Acquire, guard);
        let mut prev_next = curr;

        let found = 'found: loop {
            for _ in 0..2 {
                if curr.is_null() {
                    unsafe { self.curr_mut().defend_fake(curr) };
                    break 'found false;
                }

                self.curr_mut()
                    .defend(curr, guard)
                    .map_err(FindError::ShieldError)?;
                let curr_node = unsafe { curr.deref() };

                let mut next = curr_node.next.load(Ordering::Acquire, guard);

                // - finding stage is done if cursor.curr advancement stops
                // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
                // - stop cursor.curr if (not marked) && (cursor.curr >= key)
                // - advance cursor.prev if not marked
                match (curr_node.key.cmp(key), next.tag()) {
                    (Less, tag) => {
                        curr = next.with_tag(0);
                        if tag == 0 {
                            self.swap();
                            prev_next = next;
                        }
                    }
                    (eq, 0) => {
                        next = curr_node.next.load(Ordering::Relaxed, guard);
                        if next.tag() == 0 {
                            break 'found eq == Equal;
                        } else {
                            return Err(FindError::Retry);
                        }
                    }
                    (_, _) => curr = next.with_tag(0),
                }
            }
        };

        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == curr {
            return Ok(found);
        }

        // cleanup marked nodes between prev and curr
        if unsafe { self.prev().deref() }
            .next
            .compare_and_set(prev_next, curr, Ordering::AcqRel, guard)
            .is_err()
        {
            return Err(FindError::Retry);
        }

        // defer_destroy from cursor.prev.load() to cursor.curr (exclusive)
        let mut node = prev_next;
        loop {
            if node.with_tag(0) == curr {
                return Ok(found);
            }
            let node_ref = unsafe { node.as_ref().unwrap() };
            let next = node_ref.next.load(Ordering::Relaxed, guard);
            unsafe {
                guard.defer_destroy(node);
            }
            node = next;
        }
    }

    #[inline]
    fn find_harris_michael<'g>(&mut self, key: &K, guard: &'g Guard) -> Result<bool, FindError> {
        let head = unsafe { &*(self.prev().shared().into_usize() as *const Atomic<Node<K, V>>) };
        let mut curr = head.load(Ordering::Acquire, guard);

        let result = 'result: loop {
            for _ in 0..2 {
                debug_assert_eq!(curr.tag(), 0);
                if curr.is_null() {
                    unsafe { self.curr_mut().defend_fake(curr) };
                    break 'result Ok(false);
                }

                self.curr_mut()
                    .defend(curr, guard)
                    .map_err(FindError::ShieldError)?;
                let curr_node = unsafe { curr.deref() };

                let mut next = curr_node.next.load(Ordering::Acquire, guard);

                if next.tag() == 0 {
                    match curr_node.key.cmp(key) {
                        Less => self.swap(),
                        Equal => break 'result Ok(true),
                        Greater => break 'result Ok(false),
                    }
                } else {
                    next = next.with_tag(0);
                    if unsafe { self.prev().deref() }
                        .next
                        .compare_and_set(curr, next, Ordering::AcqRel, guard)
                        .is_ok()
                    {
                        unsafe { guard.defer_destroy(curr) };
                    } else {
                        break 'result Err(FindError::Retry);
                    }
                }
                curr = next;
            }
        };

        result
    }

    #[inline]
    fn find_harris_herlihy_shavit<'g>(
        &mut self,
        key: &K,
        guard: &'g Guard,
    ) -> Result<bool, FindError> {
        let head = unsafe { &*(self.prev().shared().into_usize() as *const Atomic<Node<K, V>>) };
        let mut curr = head.load(Ordering::Acquire, guard);

        loop {
            for _ in 0..2 {
                if curr.is_null() {
                    unsafe { self.curr_mut().defend_fake(curr) };
                    return Ok(false);
                }

                self.curr_mut()
                    .defend(curr, guard)
                    .map_err(FindError::ShieldError)?;
                let curr_node = unsafe { curr.deref() };

                match curr_node.key.cmp(key) {
                    Less => {
                        curr = curr_node.next.load(Ordering::Acquire, guard);
                        self.swap(); // NOTE: not needed
                        continue;
                    }
                    _ => return Ok(curr_node.next.load(Ordering::Relaxed, guard).tag() == 0),
                }
            }
        }
    }
}

impl<K, V> List<K, V>
where
    K: Ord,
{
    pub fn new() -> Self {
        List {
            head: Atomic::null(),
        }
    }

    #[inline]
    fn find<'g, F>(
        &'g self,
        key: &K,
        find: &F,
        cursor: &mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> bool
    where
        F: Fn(&mut Cursor<K, V>, &K, &'g Guard) -> Result<bool, FindError>,
    {
        // TODO: we want to use `FindError::retry()`, but it requires higher-kinded things...
        loop {
            cursor.init_find(&self.head);
            match find(cursor, key, unsafe { &*(guard as *mut Guard) }) {
                Ok(r) => return r,
                Err(FindError::Retry) => continue,
                Err(FindError::ShieldError(ShieldError::Ejected)) => guard.repin(),
            }
        }
    }

    #[inline]
    fn get<'g, F>(
        &'g self,
        key: &K,
        find: F,
        cursor: &'g mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> Option<&'g V>
    where
        F: Fn(&mut Cursor<K, V>, &K, &'g Guard) -> Result<bool, FindError>,
    {
        let found = self.find(key, &find, cursor, guard);

        if found {
            Some(unsafe { &cursor.curr().deref().value })
        } else {
            None
        }
    }

    fn insert_inner<'g, F>(
        &'g self,
        mut node: Shared<'g, Node<K, V>>,
        find: &F,
        cursor: &mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> Result<bool, FindError>
    where
        F: Fn(&mut Cursor<K, V>, &K, &'g Guard) -> Result<bool, FindError>,
    {
        loop {
            cursor.init_find(&self.head);
            let found = find(cursor, unsafe { &node.deref().key }, guard)?;
            if found {
                unsafe {
                    ManuallyDrop::drop(&mut node.deref_mut().value);
                    drop(node.into_owned());
                }
                return Ok(false);
            }

            unsafe { node.deref() }
                .next
                .store(cursor.curr().shared(), Ordering::Relaxed);
            if unsafe { cursor.prev().deref() }
                .next
                .compare_and_set(cursor.curr().shared(), node, Ordering::AcqRel, guard)
                .is_ok()
            {
                return Ok(true);
            }
        }
    }

    #[inline]
    fn insert<'g, F>(
        &'g self,
        key: K,
        value: V,
        find: F,
        cursor: &mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> bool
    where
        F: Fn(&mut Cursor<K, V>, &K, &'g Guard) -> Result<bool, FindError>,
    {
        let node = Owned::new(Node {
            key: key,
            value: ManuallyDrop::new(value),
            next: Atomic::null(),
        })
        .into_shared(unsafe { unprotected() });

        loop {
            match self.insert_inner(node, &find, cursor, unsafe { &mut *(guard as *mut Guard) }) {
                Ok(r) => return r,
                Err(FindError::Retry) => continue,
                Err(FindError::ShieldError(ShieldError::Ejected)) => guard.repin(),
            }
        }
    }

    fn remove_inner<'g, F>(
        &'g self,
        key: &K,
        find: &F,
        cursor: &mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> Result<Option<V>, FindError>
    where
        F: Fn(&mut Cursor<K, V>, &K, &'g Guard) -> Result<bool, FindError>,
    {
        loop {
            cursor.init_find(&self.head);
            let found = find(cursor, key, guard)?;
            if !found {
                return Ok(None);
            }

            let curr_node = unsafe { cursor.curr().as_ref() }.unwrap();
            let next = curr_node.next.fetch_or(1, Ordering::AcqRel, guard);
            if next.tag() == 1 {
                continue;
            }

            let value = unsafe { ptr::read(&curr_node.value) };

            if unsafe { cursor.prev().deref() }
                .next
                .compare_and_set(cursor.curr().shared(), next, Ordering::AcqRel, guard)
                .is_ok()
            {
                unsafe { guard.defer_destroy(cursor.curr().shared()) };
            }

            return Ok(Some(ManuallyDrop::into_inner(value)));
        }
    }

    #[inline]
    fn remove<'g, F>(
        &'g self,
        key: &K,
        find: F,
        cursor: &mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> Option<V>
    where
        F: Fn(&mut Cursor<K, V>, &K, &'g Guard) -> Result<bool, FindError>,
    {
        loop {
            match self.remove_inner(key, &find, cursor, unsafe { &mut *(guard as *mut Guard) }) {
                Ok(r) => return r,
                Err(FindError::Retry) => continue,
                Err(FindError::ShieldError(ShieldError::Ejected)) => guard.repin(),
            }
        }
    }

    pub fn harris_get<'g>(
        &'g self,
        key: &K,
        cursor: &'g mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        self.get(key, Cursor::find_harris, cursor, guard)
    }

    pub fn harris_insert<'g>(
        &'g self,
        key: K,
        value: V,
        cursor: &mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> bool {
        self.insert(key, value, Cursor::find_harris, cursor, guard)
    }

    pub fn harris_remove<'g>(
        &'g self,
        key: &K,
        cursor: &mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> Option<V> {
        self.remove(key, Cursor::find_harris, cursor, guard)
    }

    pub fn harris_michael_get<'g>(
        &'g self,
        key: &K,
        cursor: &'g mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        self.get(key, Cursor::find_harris_michael, cursor, guard)
    }

    pub fn harris_michael_insert(
        &self,
        key: K,
        value: V,
        cursor: &mut Cursor<K, V>,
        guard: &mut Guard,
    ) -> bool {
        self.insert(key, value, Cursor::find_harris_michael, cursor, guard)
    }

    pub fn harris_michael_remove(
        &self,
        key: &K,
        cursor: &mut Cursor<K, V>,
        guard: &mut Guard,
    ) -> Option<V> {
        self.remove(key, Cursor::find_harris_michael, cursor, guard)
    }

    pub fn harris_herlihy_shavit_get<'g>(
        &'g self,
        key: &K,
        cursor: &'g mut Cursor<K, V>,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        self.get(key, Cursor::find_harris_herlihy_shavit, cursor, guard)
    }

    pub fn harris_herlihy_shavit_insert(
        &self,
        key: K,
        value: V,
        cursor: &mut Cursor<K, V>,
        guard: &mut Guard,
    ) -> bool {
        self.insert(key, value, Cursor::find_harris_michael, cursor, guard)
    }

    pub fn harris_herlihy_shavit_remove(
        &self,
        key: &K,
        cursor: &mut Cursor<K, V>,
        guard: &mut Guard,
    ) -> Option<V> {
        self.remove(key, Cursor::find_harris_michael, cursor, guard)
    }
}

pub struct HList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HList<K, V>
where
    K: Ord,
{
    type Handle = Cursor<K, V>;

    fn new() -> Self {
        HList { inner: List::new() }
    }

    fn handle(guard: &Guard) -> Self::Handle {
        Cursor::new(guard)
    }

    fn clear(handle: &mut Self::Handle) {
        handle.release();
    }

    #[inline]
    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &K,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        self.inner.harris_get(key, handle, guard)
    }
    #[inline]
    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool {
        self.inner.harris_insert(key, value, handle, guard)
    }
    #[inline]
    fn remove(&self, handle: &mut Self::Handle, key: &K, guard: &mut Guard) -> Option<V> {
        self.inner.harris_remove(key, handle, guard)
    }
}

pub struct HMList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HMList<K, V>
where
    K: Ord,
{
    type Handle = Cursor<K, V>;

    fn new() -> Self {
        HMList { inner: List::new() }
    }

    fn handle(guard: &Guard) -> Self::Handle {
        Cursor::new(guard)
    }

    fn clear(handle: &mut Self::Handle) {
        handle.release();
    }

    #[inline]
    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &K,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        self.inner.harris_michael_get(key, handle, guard)
    }
    #[inline]
    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool {
        self.inner.harris_michael_insert(key, value, handle, guard)
    }
    #[inline]
    fn remove(&self, handle: &mut Self::Handle, key: &K, guard: &mut Guard) -> Option<V> {
        self.inner.harris_michael_remove(key, handle, guard)
    }
}

pub struct HHSList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HHSList<K, V>
where
    K: Ord,
{
    type Handle = Cursor<K, V>;

    fn new() -> Self {
        HHSList { inner: List::new() }
    }

    fn handle(guard: &Guard) -> Self::Handle {
        Cursor::new(guard)
    }

    fn clear(handle: &mut Self::Handle) {
        handle.release();
    }

    #[inline]
    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &K,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        self.inner.harris_herlihy_shavit_get(key, handle, guard)
    }
    #[inline]
    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool {
        self.inner
            .harris_herlihy_shavit_insert(key, value, handle, guard)
    }
    #[inline]
    fn remove(&self, handle: &mut Self::Handle, key: &K, guard: &mut Guard) -> Option<V> {
        self.inner.harris_herlihy_shavit_remove(key, handle, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::{HHSList, HList, HMList};
    use crate::pebr::concurrent_map;

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
