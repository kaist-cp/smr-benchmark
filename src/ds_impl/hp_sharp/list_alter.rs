use super::concurrent_map::{ConcurrentMap, OutputHolder};

use hp_sharp::{
    Atomic, CsGuard, Owned, Protector, RollbackProof, Shared, Shield, Thread, Unprotected,
    Validatable,
};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering;

#[repr(C)]
struct Node<K, V> {
    /// Mark: tag(), Tag: not needed
    next: Atomic<Node<K, V>>,
    key: Option<K>,
    value: V,
}

struct List<K, V> {
    head: Atomic<Node<K, V>>,
}

impl<K, V> Default for List<K, V>
where
    K: Ord + Default,
    V: Default,
{
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for List<K, V> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let guard = Unprotected::new();
            let mut curr = self.head.load(Ordering::Relaxed, &guard);

            while let Some(curr_ref) = curr.as_ref() {
                let next = curr_ref.next.load(Ordering::Relaxed, &guard);
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
            key: Some(key),
            value,
        }
    }
}

impl<K, V: Default> Node<K, V> {
    /// Creates a new head node with a dummy key-value pair.
    #[inline]
    fn head() -> Self {
        Self {
            next: Atomic::null(),
            key: None,
            value: Default::default(),
        }
    }
}

struct SharedCursor<'g, K, V> {
    prev: Shared<'g, Node<K, V>>,
    prev_next: Shared<'g, Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: Shared<'g, Node<K, V>>,
}

impl<'g, K, V> SharedCursor<'g, K, V> {
    fn new(head: &Atomic<Node<K, V>>, guard: &'g CsGuard) -> Self {
        let curr = head.load(Ordering::Relaxed, guard);
        Self {
            prev: Shared::null(),
            curr,
            prev_next: curr,
        }
    }
}

impl<'g, K, V> Validatable for SharedCursor<'g, K, V> {
    fn empty() -> Self {
        Self {
            prev: Shared::null(),
            prev_next: Shared::null(),
            curr: Shared::null(),
        }
    }

    fn validate(&self, guard: &CsGuard) -> bool {
        unsafe { self.curr.as_ref() }
            .map(|node| node.next.load(Ordering::Acquire, guard).tag() == 0)
            .unwrap_or(true)
    }
}

impl<'g, K, V> Clone for SharedCursor<'g, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'g, K, V> Copy for SharedCursor<'g, K, V> {}

struct Cursor<K, V> {
    prev: Shield<Node<K, V>>,
    prev_next: Shield<Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: Shield<Node<K, V>>,
}

impl<K, V> Protector for Cursor<K, V> {
    type Target<'g> = SharedCursor<'g, K, V>;

    #[inline]
    fn new(thread: &mut Thread) -> Self {
        Self {
            prev: Shield::null(thread),
            prev_next: Shield::null(thread),
            curr: Shield::null(thread),
        }
    }

    #[inline]
    fn swap(a: &mut Self, b: &mut Self) {
        Shield::swap(&mut a.prev, &mut b.prev);
        Shield::swap(&mut a.prev_next, &mut b.prev_next);
        Shield::swap(&mut a.curr, &mut b.curr);
    }

    #[inline]
    fn protect(&mut self, ptrs: &Self::Target<'_>) {
        self.prev.protect(ptrs.prev);
        self.prev_next.protect(ptrs.prev_next);
        self.curr.protect(ptrs.curr);
    }
}

pub struct Output<K, V>(
    Cursor<K, V>,
    Cursor<K, V>,
    (Shield<Node<K, V>>, Shield<Node<K, V>>),
);

impl<K, V> OutputHolder<V> for Output<K, V> {
    #[inline]
    fn default(thread: &mut Thread) -> Self {
        Self(
            Cursor::new(thread),
            Cursor::new(thread),
            (Shield::null(thread), Shield::null(thread)),
        )
    }

    #[inline]
    fn output(&self) -> &V {
        self.0.curr.as_ref().map(|node| &node.value).unwrap()
    }
}

impl<K, V> List<K, V>
where
    K: Ord + Default,
    V: Default,
{
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Atomic::new(Node::head()),
        }
    }

    #[inline]
    fn harris_traverse(
        &self,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> Result<bool, ()> {
        let found = unsafe {
            thread.traverse(
                &mut output.0,
                &mut output.1,
                |guard| SharedCursor::new(&self.head, guard),
                |mut cursor, guard| {
                    let Some(curr_node) = cursor.curr.as_ref() else {
                        return (cursor, Some(false));
                    };
                    let next = curr_node.next.load(Ordering::Acquire, guard);

                    // - finding stage is done if cursor.curr advancement stops
                    // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
                    // - stop cursor.curr if (not marked) && (cursor.curr >= key)
                    // - advance cursor.prev if not marked

                    if next.tag() != 0 {
                        // We add a 0 tag here so that `self.curr`s tag is always 0.
                        cursor.curr = next.with_tag(0);
                        return (cursor, None);
                    }

                    match curr_node.key.as_ref().map(|k| k.cmp(key)).unwrap_or(Less) {
                        Less => {
                            cursor.prev = cursor.curr;
                            cursor.prev_next = next;
                            cursor.curr = next;
                            return (cursor, None);
                        }
                        Equal => return (cursor, Some(true)),
                        Greater => return (cursor, Some(false)),
                    }
                },
            )
        };
        let cursor = &output.0;

        // If prev and curr WERE adjacent, no need to clean up
        if cursor.prev_next.as_raw() == cursor.curr.as_raw() {
            return Ok(found);
        }

        // cleanup marked nodes between prev and curr
        if cursor
            .prev
            .as_ref()
            .unwrap()
            .next
            .compare_exchange(
                cursor.prev_next.shared(),
                cursor.curr.shared(),
                Ordering::Release,
                Ordering::Relaxed,
                thread,
            )
            .is_err()
        {
            return Err(());
        }

        // retire from cursor.prev.load() to cursor.curr (exclusive)
        unsafe {
            // Safety: As this thread is a winner of the physical CAS, it is the only one who
            // retires this chain.
            let guard = Unprotected::new();

            let mut node = cursor.prev_next.shared();
            while node.with_tag(0) != cursor.curr.shared() {
                let next = node.deref().next.load(Ordering::Relaxed, &guard);
                thread.retire(node);
                node = next;
            }
        }
        Ok(found)
    }

    #[inline]
    fn harris_michael_traverse(
        &self,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> Result<bool, ()> {
        unsafe {
            thread.traverse(
                &mut output.0,
                &mut output.1,
                |guard| SharedCursor::new(&self.head, guard),
                |mut cursor, guard| {
                    let Some(curr_node) = cursor.curr.as_ref() else {
                        return (cursor, Some(Ok(false)));
                    };
                    let mut next = curr_node.next.load(Ordering::Acquire, guard);

                    // NOTE: original version aborts here if self.prev is tagged

                    if next.tag() != 0 {
                        next = next.with_tag(0);
                        output.2 .0.protect(cursor.prev);
                        output.2 .1.protect(cursor.curr);

                        let success = guard
                            .mask(|guard| {
                                cursor
                                    .prev
                                    .deref()
                                    .next
                                    .compare_exchange(
                                        cursor.curr,
                                        next,
                                        Ordering::Release,
                                        Ordering::Relaxed,
                                        guard,
                                    )
                                    .map(|_| guard.retire(cursor.curr))
                                    .map_err(|_| ())
                            })
                            .is_ok();

                        if success {
                            return (cursor, None);
                        }
                        return (cursor, Some(Err(())));
                    }

                    match curr_node.key.as_ref().map(|k| k.cmp(key)).unwrap_or(Less) {
                        Less => {
                            cursor.prev = cursor.curr;
                            cursor.curr = next;
                            return (cursor, None);
                        }
                        Equal => return (cursor, Some(Ok(true))),
                        Greater => return (cursor, Some(Ok(false))),
                    }
                },
            )
        }
    }

    #[inline]
    fn harris_herlihy_shavit_traverse(
        &self,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> Result<bool, ()> {
        Ok(unsafe {
            thread.traverse(
                &mut output.0,
                &mut output.1,
                |guard| SharedCursor::new(&self.head, guard),
                |mut cursor, guard| {
                    let Some(curr_node) = cursor.curr.as_ref() else {
                        return (cursor, Some(false));
                    };
                    let next = curr_node.next.load(Ordering::Acquire, guard);

                    match curr_node.key.as_ref().map(|k| k.cmp(key)).unwrap_or(Less) {
                        Less => {
                            cursor.curr = next;
                            return (cursor, None);
                        }
                        Equal => return (cursor, Some(next.tag() == 0)),
                        Greater => return (cursor, Some(false)),
                    }
                },
            )
        })
    }

    #[inline]
    pub fn get<F>(&self, find: &F, key: &K, output: &mut Output<K, V>, thread: &mut Thread) -> bool
    where
        F: Fn(&List<K, V>, &K, &mut Output<K, V>, &mut Thread) -> Result<bool, ()>,
    {
        loop {
            if let Ok(found) = find(self, key, output, thread) {
                return found;
            }
        }
    }

    #[inline]
    pub fn insert<F>(
        &self,
        find: &F,
        key: K,
        value: V,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> bool
    where
        F: Fn(&List<K, V>, &K, &mut Output<K, V>, &mut Thread) -> Result<bool, ()>,
    {
        let mut new_node = Owned::new(Node::new(key, value));
        loop {
            if self.get(&find, new_node.key.as_ref().unwrap(), output, thread) {
                return false;
            }
            let cursor = &mut output.0;

            new_node
                .next
                .store(cursor.curr.shared(), Ordering::Relaxed, thread);

            match cursor.prev.as_ref().unwrap().next.compare_exchange(
                cursor.curr.shared(),
                new_node,
                Ordering::Release,
                Ordering::Relaxed,
                thread,
            ) {
                Ok(_) => return true,
                Err(e) => new_node = e.new,
            }
        }
    }

    #[inline]
    pub fn remove<F>(
        &self,
        find: &F,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> bool
    where
        F: Fn(&List<K, V>, &K, &mut Output<K, V>, &mut Thread) -> Result<bool, ()>,
    {
        loop {
            if !self.get(&find, &key, output, thread) {
                return true;
            }
            let cursor = &mut output.0;

            let curr_node = cursor.curr.as_ref().unwrap();
            let next = curr_node.next.fetch_or(1, Ordering::AcqRel, thread);

            if (next.tag() & 1) != 0 {
                continue;
            }

            if cursor
                .prev
                .as_ref()
                .unwrap()
                .next
                .compare_exchange(
                    cursor.curr.shared(),
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    thread,
                )
                .is_ok()
            {
                unsafe { thread.retire(cursor.curr.shared()) };
            }
            return true;
        }
    }

    #[inline]
    pub fn pop(&self, output: &mut Output<K, V>, thread: &mut Thread) -> bool {
        let cursor = &mut output.0;
        unsafe {
            loop {
                thread.critical_section(|guard| {
                    let prev = self.head.load(Ordering::Relaxed, guard);
                    let curr = prev.deref().next.load(Ordering::Acquire, guard);
                    cursor.prev.protect(prev);
                    cursor.curr.protect(curr);
                });
                let curr_node = match cursor.curr.as_ref() {
                    Some(node) => node,
                    None => return false,
                };

                let next = curr_node.next.fetch_or(1, Ordering::AcqRel, thread);

                if (next.tag() & 1) != 0 {
                    continue;
                }

                if cursor
                    .prev
                    .deref()
                    .next
                    .compare_exchange(
                        cursor.curr.shared(),
                        next,
                        Ordering::Release,
                        Ordering::Relaxed,
                        thread,
                    )
                    .is_ok()
                {
                    thread.retire(cursor.curr.shared());
                }
                return true;
            }
        }
    }
}

pub struct HList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HList<K, V>
where
    K: Ord + Default,
    V: Default,
{
    type Output = Output<K, V>;

    #[inline]
    fn new() -> Self {
        Self { inner: List::new() }
    }

    #[inline(always)]
    fn get(&self, key: &K, output: &mut Self::Output, thread: &mut hp_sharp::Thread) -> bool {
        self.inner.get(&List::harris_traverse, key, output, thread)
    }

    #[inline(always)]
    fn insert(
        &self,
        key: K,
        value: V,
        output: &mut Self::Output,
        thread: &mut hp_sharp::Thread,
    ) -> bool {
        self.inner
            .insert(&List::harris_traverse, key, value, output, thread)
    }

    #[inline(always)]
    fn remove<'domain, 'hp>(
        &self,
        key: &K,
        output: &mut Self::Output,
        thread: &mut hp_sharp::Thread,
    ) -> bool {
        self.inner
            .remove(&List::harris_traverse, key, output, thread)
    }
}

pub struct HMList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HMList<K, V>
where
    K: Ord + Default,
    V: Default,
{
    type Output = Output<K, V>;

    #[inline]
    fn new() -> Self {
        Self { inner: List::new() }
    }

    #[inline(always)]
    fn get(&self, key: &K, output: &mut Self::Output, thread: &mut hp_sharp::Thread) -> bool {
        self.inner
            .get(&List::harris_michael_traverse, key, output, thread)
    }

    #[inline(always)]
    fn insert(
        &self,
        key: K,
        value: V,
        output: &mut Self::Output,
        thread: &mut hp_sharp::Thread,
    ) -> bool {
        self.inner
            .insert(&List::harris_michael_traverse, key, value, output, thread)
    }

    #[inline(always)]
    fn remove<'domain, 'hp>(
        &self,
        key: &K,
        output: &mut Self::Output,
        thread: &mut hp_sharp::Thread,
    ) -> bool {
        self.inner
            .remove(&List::harris_michael_traverse, key, output, thread)
    }
}

pub struct HHSList<K, V> {
    inner: List<K, V>,
}

impl<K, V> HHSList<K, V>
where
    K: Ord + Default,
    V: Default,
{
    #[inline]
    pub fn pop(&self, output: &mut Output<K, V>, thread: &mut Thread) -> bool {
        self.inner.pop(output, thread)
    }
}

impl<K, V> ConcurrentMap<K, V> for HHSList<K, V>
where
    K: Ord + Default,
    V: Default,
{
    type Output = Output<K, V>;

    #[inline]
    fn new() -> Self {
        Self { inner: List::new() }
    }

    #[inline(always)]
    fn get(&self, key: &K, output: &mut Self::Output, thread: &mut hp_sharp::Thread) -> bool {
        self.inner
            .get(&List::harris_herlihy_shavit_traverse, key, output, thread)
    }

    #[inline(always)]
    fn insert(
        &self,
        key: K,
        value: V,
        output: &mut Self::Output,
        thread: &mut hp_sharp::Thread,
    ) -> bool {
        self.inner
            .insert(&List::harris_traverse, key, value, output, thread)
    }

    #[inline(always)]
    fn remove<'domain, 'hp>(
        &self,
        key: &K,
        output: &mut Self::Output,
        thread: &mut hp_sharp::Thread,
    ) -> bool {
        self.inner
            .remove(&List::harris_traverse, key, output, thread)
    }
}

#[test]
fn smoke_h_list() {
    super::concurrent_map::tests::smoke::<HList<i32, String>>();
}

#[test]
fn smoke_hm_list() {
    super::concurrent_map::tests::smoke::<HMList<i32, String>>();
}

#[test]
fn smoke_hhs_list() {
    super::concurrent_map::tests::smoke::<HHSList<i32, String>>();
}
