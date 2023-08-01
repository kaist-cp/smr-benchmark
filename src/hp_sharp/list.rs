use super::concurrent_map::{ConcurrentMap, OutputHolder};
use hp_sharp::{
    Atomic, CsGuard, Invalidate, Owned, Pointer, Protector, Retire, Shared, Shield, Thread,
    TraverseStatus, WriteResult,
};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering;

struct Node<K, V> {
    /// Mark: tag(), Tag: not needed
    next: Atomic<Node<K, V>>,
    key: K,
    value: V,
}

// TODO(@jeonghyeon): automate
impl<K, V> Invalidate for Node<K, V> {
    #[inline]
    fn invalidate(&self) {
        // A logical deletion is equivalent to an invalidation.
    }

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
            let guard = CsGuard::unprotected();
            let mut curr = self.head.load(Ordering::Relaxed, &guard);

            while let Some(curr_ref) = curr.as_ref(&guard) {
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
    prev_next: usize,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: Shield<Node<K, V>>,
    found: bool,
}

// TODO(@jeonghyeon): automate
impl<K, V> Protector for Cursor<K, V> {
    type Target<'r> = SharedCursor<'r, K, V>;

    #[inline]
    fn empty(thread: &mut Thread) -> Self {
        Self {
            prev: Shield::null(thread),
            prev_next: 0,
            curr: Shield::null(thread),
            found: false,
        }
    }

    #[inline]
    fn protect_unchecked(&mut self, read: &Self::Target<'_>) {
        self.prev.protect_unchecked(&read.prev);
        self.prev_next = read.prev_next;
        self.curr.protect_unchecked(&read.curr);
        self.found = read.found;
    }

    #[inline]
    fn as_target<'r>(&self, guard: &'r CsGuard) -> Option<Self::Target<'r>> {
        Some(SharedCursor {
            prev: self.prev.as_target(guard)?,
            prev_next: self.prev_next,
            curr: self.curr.as_target(guard)?,
            found: self.found,
        })
    }

    #[inline]
    fn release(&mut self) {
        self.prev.release();
        self.prev_next = 0;
        self.curr.release();
        self.found = false;
    }
}

// TODO(@jeonghyeon): automate
struct SharedCursor<'r, K, V> {
    prev: Shared<'r, Node<K, V>>,
    prev_next: usize,
    curr: Shared<'r, Node<K, V>>,
    found: bool,
}

// TODO(@jeonghyeon): automate
impl<'r, K, V> Clone for SharedCursor<'r, K, V> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            prev_next: self.prev_next,
            curr: self.curr.clone(),
            found: self.found,
        }
    }
}

// TODO(@jeonghyeon): automate
impl<'r, K, V> Copy for SharedCursor<'r, K, V> {}

impl<'r, K, V> SharedCursor<'r, K, V>
where
    K: Ord,
{
    /// Creates a cursor.
    #[inline]
    fn new(head: &Atomic<Node<K, V>>, guard: &'r CsGuard) -> Self {
        let prev = head.load(Ordering::Relaxed, guard);
        let curr = prev
            .as_ref(guard)
            .unwrap()
            .next
            .load(Ordering::Acquire, guard);
        Self {
            prev,
            prev_next: curr.as_raw(),
            curr,
            found: false,
        }
    }
}

pub struct Output<K, V>(Cursor<K, V>, Cursor<K, V>, Shield<Node<K, V>>);

impl<K, V> OutputHolder<V> for Output<K, V> {
    #[inline]
    fn default(thread: &mut Thread) -> Self {
        Self(
            Cursor::empty(thread),
            Cursor::empty(thread),
            Shield::empty(thread),
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
            head: Atomic::new(Node::default()),
        }
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn harris_traverse(&self, key: &K, output: &mut Output<K, V>, thread: &mut Thread) -> bool {
        let cursor = &mut output.0;
        unsafe {
            cursor.traverse(thread, |guard| {
                let mut cursor = SharedCursor::new(&self.head, guard);
                // Finding phase
                // - cursor.curr: first unmarked node w/ key >= search key (4)
                // - cursor.prev: the ref of .next in previous unmarked node (1 -> 2)
                // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
                cursor.found = loop {
                    let curr_node = some_or!(cursor.curr.as_ref(guard), break false);
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
                            cursor.prev = cursor.curr;
                            cursor.prev_next = next.as_raw();
                            cursor.curr = next;
                        }
                        Equal => break true,
                        Greater => break false,
                    }
                };
                cursor
            });
        }

        // If prev and curr WERE adjacent, no need to clean up
        if cursor.prev_next == cursor.curr.as_raw() {
            return true;
        }

        // cleanup marked nodes between prev and curr
        if cursor
            .prev
            .as_ref()
            .unwrap()
            .next
            .compare_exchange(
                unsafe { Shared::from_usize(cursor.prev_next) },
                cursor.curr.shared(),
                Ordering::Release,
                Ordering::Relaxed,
                thread,
            )
            .is_err()
        {
            return false;
        }

        // retire from cursor.prev.load() to cursor.curr (exclusive)
        unsafe {
            // Safety: As this thread is a winner of the physical CAS, it is the only one who
            // retires this chain.
            let guard = CsGuard::unprotected();

            let mut node = Shared::from_usize(cursor.prev_next);
            while node.with_tag(0) != cursor.curr.shared() {
                let next = node.deref_unchecked().next.load(Ordering::Relaxed, &guard);
                thread.retire(node);
                node = next;
            }
        }
        return true;
    }

    #[inline]
    fn harris_michael_traverse(
        &self,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> bool {
        let aux = &mut output.2;
        let prot = &mut output.0;
        unsafe {
            prot.traverse(thread, |guard| {
                let mut cursor = SharedCursor::new(&self.head, guard);
                cursor.found = loop {
                    let curr_node = some_or!(cursor.curr.as_ref(guard), break false);
                    let next = curr_node.next.load(Ordering::Acquire, guard);

                    // NOTE: original version aborts here if self.prev is tagged

                    if next.tag() != 0 {
                        self.harris_michael_traverse_unlink(aux, next, &mut cursor, guard);
                        continue;
                    }

                    match curr_node.key.cmp(key) {
                        Less => {
                            cursor.prev = cursor.curr;
                            cursor.curr = next;
                        }
                        Equal => break true,
                        Greater => break false,
                    }
                };
                cursor
            });
        }
        // Always success to traverse.
        true
    }

    #[inline(never)]
    #[cold]
    unsafe fn harris_michael_traverse_unlink<'g>(
        &self,
        aux: &mut Shield<Node<K, V>>,
        mut next: Shared<'g, Node<K, V>>,
        cursor: &mut SharedCursor<'g, K, V>,
        guard: &'g CsGuard,
    ) {
        next = next.with_tag(0);
        aux.traverse_mask(guard, cursor.prev, |prev, guard| {
            if prev
                .deref_unchecked()
                .next
                .compare_exchange(
                    cursor.curr,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .is_err()
            {
                WriteResult::RepinEpoch
            } else {
                guard.retire(cursor.curr);
                WriteResult::Finished
            }
        });
        cursor.curr = next;
    }

    #[inline]
    fn harris_herlihy_shavit_traverse(
        &self,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> bool {
        let cursor = &mut output.0;
        unsafe {
            cursor.traverse(
                thread,
                |guard| {
                    let mut cursor = SharedCursor::new(&self.head, guard);
                    cursor.found = loop {
                        let curr_node = some_or!(cursor.curr.as_ref(guard), break false);
                        let next = curr_node.next.load(Ordering::Acquire, guard);
                        match curr_node.key.cmp(key) {
                            Less => {
                                cursor.prev = cursor.curr;
                                cursor.curr = next;
                                continue;
                            }
                            Equal => break next.tag() == 0,
                            Greater => break false,
                        }
                    };
                    cursor
                },
            );
        }
        true
    }

    #[inline]
    fn harris_herlihy_shavit_traverse_loop(
        &self,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> bool {
        let cursor = &mut output.0;
        let backup = &mut output.1;
        unsafe {
            cursor.traverse_loop(
                backup,
                thread,
                |guard| SharedCursor::new(&self.head, guard),
                |cursor, guard| {
                    let curr_node = match cursor.curr.as_ref(guard) {
                        Some(node) => node,
                        None => {
                            cursor.found = false;
                            return TraverseStatus::Finished;
                        }
                    };
                    let next = curr_node.next.load(Ordering::Acquire, guard);
                    match curr_node.key.cmp(key) {
                        Less => {
                            cursor.prev = cursor.curr;
                            cursor.curr = next;
                            return TraverseStatus::Continue;
                        }
                        Equal => cursor.found = next.tag() == 0,
                        Greater => cursor.found = false,
                    }
                    TraverseStatus::Finished
                },
            );
        }
        // Always success to traverse.
        true
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn harris_traverse_loop(
        &self,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> bool {
        let cursor = &mut output.0;
        let backup = &mut output.1;
        unsafe {
            cursor.traverse_loop(
                backup,
                thread,
                |guard| SharedCursor::new(&self.head, guard),
                |cursor, guard| {
                    let curr_node = match cursor.curr.as_ref(guard) {
                        Some(node) => node,
                        None => {
                            cursor.found = false;
                            return TraverseStatus::Finished;
                        }
                    };
                    let next = curr_node.next.load(Ordering::Acquire, guard);

                    // - finding stage is done if cursor.curr advancement stops
                    // - advance cursor.curr if (.next is marked) || (cursor.curr < key)
                    // - stop cursor.curr if (not marked) && (cursor.curr >= key)
                    // - advance cursor.prev if not marked

                    if next.tag() != 0 {
                        // We add a 0 tag here so that `self.curr`s tag is always 0.
                        cursor.curr = next.with_tag(0);
                        return TraverseStatus::Continue;
                    }

                    match curr_node.key.cmp(key) {
                        Less => {
                            cursor.prev = cursor.curr;
                            cursor.prev_next = next.as_raw();
                            cursor.curr = next;
                            return TraverseStatus::Continue;
                        }
                        Equal => cursor.found = true,
                        Greater => cursor.found = false,
                    }
                    TraverseStatus::Finished
                },
            );
        }

        // If prev and curr WERE adjacent, no need to clean up
        if cursor.prev_next == cursor.curr.as_raw() {
            return true;
        }

        // cleanup marked nodes between prev and curr
        if cursor
            .prev
            .as_ref()
            .unwrap()
            .next
            .compare_exchange(
                unsafe { Shared::from_usize(cursor.prev_next) },
                cursor.curr.shared(),
                Ordering::Release,
                Ordering::Relaxed,
                thread,
            )
            .is_err()
        {
            return false;
        }

        // retire from cursor.prev.load() to cursor.curr (exclusive)
        unsafe {
            // Safety: As this thread is a winner of the physical CAS, it is the only one who
            // retires this chain.
            let guard = CsGuard::unprotected();

            let mut node = Shared::from_usize(cursor.prev_next);
            while node.with_tag(0) != cursor.curr.shared() {
                let next = node.deref_unchecked().next.load(Ordering::Relaxed, &guard);
                thread.retire(node);
                node = next;
            }
        }
        return true;
    }

    #[inline]
    pub fn get<F>(&self, find: F, key: &K, output: &mut Output<K, V>, thread: &mut Thread) -> bool
    where
        F: Fn(&List<K, V>, &K, &mut Output<K, V>, &mut Thread) -> bool,
    {
        while !find(self, key, output, thread) {}
        output.0.found
    }

    #[inline]
    pub fn insert<F>(
        &self,
        find: F,
        key: K,
        value: V,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> bool
    where
        F: Fn(&List<K, V>, &K, &mut Output<K, V>, &mut Thread) -> bool,
    {
        let mut new_node = Owned::new(Node::new(key, value));
        loop {
            if !find(self, &new_node.key, output, thread) {
                continue;
            }
            let cursor = &mut output.0;
            if cursor.found {
                return false;
            }

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
        find: F,
        key: &K,
        output: &mut Output<K, V>,
        thread: &mut Thread,
    ) -> bool
    where
        F: Fn(&List<K, V>, &K, &mut Output<K, V>, &mut Thread) -> bool,
    {
        loop {
            if !find(self, key, output, thread) {
                continue;
            }
            let cursor = &mut output.0;
            if !cursor.found {
                return false;
            }

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
                cursor.traverse(thread, |guard| SharedCursor::new(&self.head, guard));
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
                    .deref_unchecked()
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
        type Output = Output<K, V>;

        #[inline]
        fn new() -> Self {
            Self { inner: List::new() }
        }

        #[inline(always)]
        fn get(&self, key: &K, output: &mut Self::Output, thread: &mut hp_sharp::Thread) -> bool {
            self.inner.get(List::harris_traverse, key, output, thread)
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
                .insert(List::harris_traverse, key, value, output, thread)
        }

        #[inline(always)]
        fn remove<'domain, 'hp>(
            &self,
            key: &K,
            output: &mut Self::Output,
            thread: &mut hp_sharp::Thread,
        ) -> bool {
            self.inner
                .remove(List::harris_traverse, key, output, thread)
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
                .get(List::harris_michael_traverse, key, output, thread)
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
                .insert(List::harris_michael_traverse, key, value, output, thread)
        }

        #[inline(always)]
        fn remove<'domain, 'hp>(
            &self,
            key: &K,
            output: &mut Self::Output,
            thread: &mut hp_sharp::Thread,
        ) -> bool {
            self.inner
                .remove(List::harris_michael_traverse, key, output, thread)
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
        type Output = Output<K, V>;

        #[inline]
        fn new() -> Self {
            Self { inner: List::new() }
        }

        #[inline(always)]
        fn get(&self, key: &K, output: &mut Self::Output, thread: &mut hp_sharp::Thread) -> bool {
            self.inner
                .get(List::harris_herlihy_shavit_traverse, key, output, thread)
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
                .insert(List::harris_traverse, key, value, output, thread)
        }

        #[inline(always)]
        fn remove<'domain, 'hp>(
            &self,
            key: &K,
            output: &mut Self::Output,
            thread: &mut hp_sharp::Thread,
        ) -> bool {
            self.inner
                .remove(List::harris_traverse, key, output, thread)
        }
    }

    #[test]
    fn smoke_h_list() {
        crate::hp_sharp::concurrent_map::tests::smoke::<HList<i32, String>>();
    }

    #[test]
    fn smoke_hm_list() {
        crate::hp_sharp::concurrent_map::tests::smoke::<HMList<i32, String>>();
    }

    #[test]
    fn smoke_hhs_list() {
        crate::hp_sharp::concurrent_map::tests::smoke::<HHSList<i32, String>>();
    }
}

pub mod traverse_loop {
    use super::{ConcurrentMap, List, Output};

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
            self.inner
                .get(List::harris_traverse_loop, key, output, thread)
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
                .insert(List::harris_traverse_loop, key, value, output, thread)
        }

        #[inline(always)]
        fn remove<'domain, 'hp>(
            &self,
            key: &K,
            output: &mut Self::Output,
            thread: &mut hp_sharp::Thread,
        ) -> bool {
            self.inner
                .remove(List::harris_traverse_loop, key, output, thread)
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
        pub fn pop(&self, output: &mut Output<K, V>, thread: &mut hp_sharp::Thread) -> bool {
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
            self.inner.get(
                List::harris_herlihy_shavit_traverse_loop,
                key,
                output,
                thread,
            )
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
                .insert(List::harris_traverse_loop, key, value, output, thread)
        }

        #[inline(always)]
        fn remove<'domain, 'hp>(
            &self,
            key: &K,
            output: &mut Self::Output,
            thread: &mut hp_sharp::Thread,
        ) -> bool {
            self.inner
                .remove(List::harris_traverse_loop, key, output, thread)
        }
    }

    #[test]
    fn smoke_h_list() {
        crate::hp_sharp::concurrent_map::tests::smoke::<HList<i32, String>>();
    }

    #[test]
    fn smoke_hhs_list() {
        crate::hp_sharp::concurrent_map::tests::smoke::<HHSList<i32, String>>();
    }

    #[test]
    fn litmus_hhs_pop() {
        use crate::hp_sharp::concurrent_map::OutputHolder;
        hp_sharp::THREAD.with(|thread| {
            let thread = &mut **thread.borrow_mut();
            let output = &mut HHSList::empty_output(thread);
            let map = HHSList::new();

            map.insert(1, "1", output, thread);
            map.insert(2, "2", output, thread);
            map.insert(3, "3", output, thread);

            fn validate_output(output: &Output<i32, &str>, target: &str) {
                assert!(output.output().eq(&target));
            }

            assert!(map.pop(output, thread));
            validate_output(output, "1");
            assert!(map.pop(output, thread));
            validate_output(output, "2");
            assert!(map.pop(output, thread));
            validate_output(output, "3");
            assert!(!map.pop(output, thread));
        });
    }
}
