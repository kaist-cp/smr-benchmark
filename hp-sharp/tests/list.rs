use hp_sharp::{
    Atomic, CsGuard, Invalidate, Owned, RollbackProof, Shared, Shield, Thread, Unprotected,
};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering;

pub trait ConcurrentMap<K, V> {
    fn new() -> Self;
    fn get(&self, key: &K, output: &mut Output<K, V>, thread: &mut Thread) -> bool;
    fn insert(&self, key: K, value: V, output: &mut Output<K, V>, thread: &mut Thread) -> bool;
    fn remove<'domain, 'hp>(&self, key: &K, output: &mut Output<K, V>, thread: &mut Thread)
        -> bool;
}

struct Node<K, V> {
    /// Mark: tag(), Tag: not needed
    next: Atomic<Node<K, V>>,
    key: K,
    value: V,
}

impl<K, V> Invalidate for Node<K, V> {
    #[inline]
    fn is_invalidated(&self, guard: &CsGuard) -> bool {
        (self.next.load(Ordering::Acquire, guard).tag() & 1) != 0
    }
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
            key,
            value,
        }
    }
}

impl<K: Default, V: Default> Default for Node<K, V> {
    /// Creates a new head node with a dummy key-value pair.
    #[inline]
    fn default() -> Self {
        Self {
            next: Atomic::null(),
            key: Default::default(),
            value: Default::default(),
        }
    }
}

struct Cursor<K, V> {
    prev: Shield<Node<K, V>>,
    prev_next: Shield<Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: Shield<Node<K, V>>,
}

impl<K, V> Cursor<K, V> {
    #[inline]
    fn empty(thread: &mut Thread) -> Self {
        Self {
            prev: Shield::null(thread),
            prev_next: Shield::null(thread),
            curr: Shield::null(thread),
        }
    }
}

fn initialize<'g, K, V>(
    head: &Atomic<Node<K, V>>,
    guard: &'g CsGuard,
) -> (Shared<'g, Node<K, V>>, Shared<'g, Node<K, V>>) {
    let prev = head.load(Ordering::Relaxed, guard);
    let curr = unsafe { prev.deref() }.next.load(Ordering::Acquire, guard);
    (prev, curr)
}

pub struct Output<K, V>(Cursor<K, V>, Cursor<K, V>);

impl<K, V> Output<K, V> {
    #[inline]
    fn empty(thread: &mut Thread) -> Self {
        Self(Cursor::empty(thread), Cursor::empty(thread))
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
            head: Atomic::new(Node::default()),
        }
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn harris_traverse(
        &self,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> Result<bool, ()> {
        let cursor = &mut output.0;
        let found = unsafe {
            thread.critical_section(|guard| {
                let (mut prev, mut curr) = initialize(&self.head, guard);
                let mut prev_next = curr;
                // Finding phase
                // - cursor.curr: first unmarked node w/ key >= search key (4)
                // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
                // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
                let found = loop {
                    let Some(curr_node) = curr.as_ref() else {
                        break false;
                    };
                    let next = curr_node.next.load(Ordering::Acquire, guard);

                    // - finding stage is done if cursor.curr advancement stops
                    // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
                    // - stop cursor.curr if (not marked) && (cursor.curr >= key)
                    // - advance cursor.prev if not marked

                    if next.tag() != 0 {
                        // We add a 0 tag here so that `self.curr`s tag is always 0.
                        curr = next.with_tag(0);
                        continue;
                    }

                    match curr_node.key.cmp(key) {
                        Less => {
                            prev = curr;
                            prev_next = next;
                            curr = next;
                        }
                        Equal => break true,
                        Greater => break false,
                    }
                };

                cursor.prev.protect(prev);
                cursor.curr.protect(curr);
                cursor.prev_next.protect(prev_next);
                found
            })
        };

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
        let cursor = &mut output.0;
        unsafe {
            thread.critical_section(|guard| {
                let (mut prev, mut curr) = initialize(&self.head, guard);
                let found = loop {
                    let Some(curr_node) = curr.as_ref() else {
                        break false;
                    };
                    let mut next = curr_node.next.load(Ordering::Acquire, guard);

                    // NOTE: original version aborts here if self.prev is tagged

                    if next.tag() != 0 {
                        next = next.with_tag(0);
                        cursor.prev.protect(prev);
                        cursor.curr.protect(next);
                        guard.mask(|guard| {
                            prev.deref()
                                .next
                                .compare_exchange(
                                    curr,
                                    next,
                                    Ordering::Release,
                                    Ordering::Relaxed,
                                    guard,
                                )
                                .map(|_| guard.retire(curr))
                                .map_err(|_| ())
                        })?;
                        continue;
                    }

                    match curr_node.key.cmp(key) {
                        Less => {
                            prev = curr;
                            curr = next;
                        }
                        Equal => break true,
                        Greater => break false,
                    }
                };

                cursor.prev.protect(prev);
                cursor.curr.protect(curr);
                Ok(found)
            })
        }
    }

    #[inline]
    fn harris_herlihy_shavit_traverse(
        &self,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> Result<bool, ()> {
        let cursor = &mut output.0;
        unsafe {
            thread.critical_section(|guard| {
                let (_, mut curr) = initialize(&self.head, guard);
                let found = loop {
                    let Some(curr_node) = curr.as_ref() else {
                        break false;
                    };
                    let next = curr_node.next.load(Ordering::Acquire, guard);
                    match curr_node.key.cmp(key) {
                        Less => {
                            curr = next;
                            continue;
                        }
                        Equal => break next.tag() == 0,
                        Greater => break false,
                    }
                };
                cursor.curr.protect(curr);
                Ok(found)
            })
        }
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
            if self.get(&find, &new_node.key, output, thread) {
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
}

pub mod traverse {
    use super::{ConcurrentMap, List, Output};

    pub struct HList<K, V> {
        inner: List<K, V>,
    }

    impl<K, V> ConcurrentMap<K, V> for HList<K, V>
    where
        K: Ord + Default,
        V: Default,
    {
        #[inline]
        fn new() -> Self {
            Self { inner: List::new() }
        }

        #[inline(always)]
        fn get(&self, key: &K, output: &mut Output<K, V>, thread: &mut hp_sharp::Thread) -> bool {
            self.inner.get(&List::harris_traverse, key, output, thread)
        }

        #[inline(always)]
        fn insert(
            &self,
            key: K,
            value: V,
            output: &mut Output<K, V>,
            thread: &mut hp_sharp::Thread,
        ) -> bool {
            self.inner
                .insert(&List::harris_traverse, key, value, output, thread)
        }

        #[inline(always)]
        fn remove<'domain, 'hp>(
            &self,
            key: &K,
            output: &mut Output<K, V>,
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
        #[inline]
        fn new() -> Self {
            Self { inner: List::new() }
        }

        #[inline(always)]
        fn get(&self, key: &K, output: &mut Output<K, V>, thread: &mut hp_sharp::Thread) -> bool {
            self.inner
                .get(&List::harris_michael_traverse, key, output, thread)
        }

        #[inline(always)]
        fn insert(
            &self,
            key: K,
            value: V,
            output: &mut Output<K, V>,
            thread: &mut hp_sharp::Thread,
        ) -> bool {
            self.inner
                .insert(&List::harris_michael_traverse, key, value, output, thread)
        }

        #[inline(always)]
        fn remove<'domain, 'hp>(
            &self,
            key: &K,
            output: &mut Output<K, V>,
            thread: &mut hp_sharp::Thread,
        ) -> bool {
            self.inner
                .remove(&List::harris_michael_traverse, key, output, thread)
        }
    }

    pub struct HHSList<K, V> {
        inner: List<K, V>,
    }

    impl<K, V> ConcurrentMap<K, V> for HHSList<K, V>
    where
        K: Ord + Default,
        V: Default,
    {
        #[inline]
        fn new() -> Self {
            Self { inner: List::new() }
        }

        #[inline(always)]
        fn get(&self, key: &K, output: &mut Output<K, V>, thread: &mut hp_sharp::Thread) -> bool {
            self.inner
                .get(&List::harris_herlihy_shavit_traverse, key, output, thread)
        }

        #[inline(always)]
        fn insert(
            &self,
            key: K,
            value: V,
            output: &mut Output<K, V>,
            thread: &mut hp_sharp::Thread,
        ) -> bool {
            self.inner
                .insert(&List::harris_traverse, key, value, output, thread)
        }

        #[inline(always)]
        fn remove<'domain, 'hp>(
            &self,
            key: &K,
            output: &mut Output<K, V>,
            thread: &mut hp_sharp::Thread,
        ) -> bool {
            self.inner
                .remove(&List::harris_traverse, key, output, thread)
        }
    }

    #[test]
    fn smoke_h_list() {
        crate::smoke::<HList<i32, String>>();
    }

    #[test]
    fn smoke_hm_list() {
        crate::smoke::<HMList<i32, String>>();
    }

    #[test]
    fn smoke_hhs_list() {
        crate::smoke::<HHSList<i32, String>>();
    }
}

#[cfg(test)]
fn smoke<M: ConcurrentMap<i32, String> + Send + Sync>() {
    extern crate rand;
    use rand::prelude::SliceRandom;
    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    let map = &M::new();

    std::thread::scope(|s| {
        for t in 0..THREADS {
            s.spawn(move || {
                hp_sharp::THREAD.with(|thread| {
                    let thread = &mut **thread.borrow_mut();
                    let output = &mut Output::empty(thread);
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, i.to_string(), output, thread));
                    }
                });
            });
        }
    });

    std::thread::scope(|s| {
        for t in 0..(THREADS / 2) {
            s.spawn(move || {
                hp_sharp::THREAD.with(|thread| {
                    let thread = &mut **thread.borrow_mut();
                    let output = &mut Output::empty(thread);
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.remove(&i, output, thread));
                    }
                });
            });
        }
    });

    std::thread::scope(|s| {
        for t in (THREADS / 2)..THREADS {
            s.spawn(move || {
                hp_sharp::THREAD.with(|thread| {
                    let thread = &mut **thread.borrow_mut();
                    let output = &mut Output::empty(thread);
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.get(&i, output, thread));
                        assert_eq!(
                            i.to_string(),
                            output
                                .0
                                .curr
                                .as_ref()
                                .map(|node| node.value.clone())
                                .unwrap()
                        );
                    }
                });
            });
        }
    });
}
