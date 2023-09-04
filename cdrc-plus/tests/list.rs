use atomic::Ordering;
use cdrc_rs::{AtomicRc, Guard, Pointer, Rc, Snapshot, TaggedCnt};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::mem::swap;

/// Some or executing the given expression.
macro_rules! some_or {
    ($e:expr, $err:expr) => {{
        match $e {
            Some(r) => r,
            None => $err,
        }
    }};
}

struct Node<K, V, G: Guard> {
    next: AtomicRc<Self, G>,
    key: K,
    value: V,
}

struct List<K, V, G: Guard> {
    head: AtomicRc<Node<K, V, G>, G>,
}

impl<K, V, G> Default for List<K, V, G>
where
    K: Ord + Default,
    V: Default,
    G: Guard,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, G> Node<K, V, G>
where
    K: Default,
    V: Default,
    G: Guard,
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

struct Cursor<K, V, G: Guard> {
    // `Snapshot`s are used only for traversing the list.
    prev: Snapshot<Node<K, V, G>, G>,
    // We don't have to protect the next pointer of `prev`.
    prev_next: TaggedCnt<Node<K, V, G>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // tagged pointer and cause cleanup to fail.
    curr: Snapshot<Node<K, V, G>, G>,
    next: Snapshot<Node<K, V, G>, G>,
}

impl<K: Ord, V, G: Guard> Cursor<K, V, G> {
    fn new() -> Self {
        Self {
            prev: Snapshot::new(),
            prev_next: TaggedCnt::null(),
            curr: Snapshot::new(),
            next: Snapshot::new(),
        }
    }

    /// Initializes a cursor.
    fn initialize(&mut self, head: &AtomicRc<Node<K, V, G>, G>, guard: &G) {
        self.prev.load(head, guard);
        self.curr.load(&unsafe { self.prev.deref() }.next, guard);
        self.prev_next = self.curr.as_ptr();
    }

    /// Clean up a chain of logically removed nodes in each traversal.
    #[inline]
    fn find_harris(&mut self, key: &K, guard: &G) -> Result<bool, ()> {
        // Finding phase
        // - cursor.curr: first untagged node w/ key >= search key (4)
        // - cursor.prev: the ref of .next in previous untagged node (1 -> 2)
        // 1 -> 2 -x-> 3 -x-> 4 -> 5 -> ∅  (search key: 4)
        let found = loop {
            let curr_node = some_or!(unsafe { self.curr.as_ref() }, break false);
            self.next.load(&curr_node.next, guard);

            // - finding stage is done if cursor.curr advancement stops
            // - advance cursor.curr if (.next is tagged) || (cursor.curr < key)
            // - stop cursor.curr if (not tagged) && (cursor.curr >= key)
            // - advance cursor.prev if not tagged

            if self.next.tag() != 0 {
                // We add a 0 tag here so that `self.curr`s tag is always 0.
                self.next.set_tag(0);
                swap(&mut self.next, &mut self.curr);
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    swap(&mut self.prev, &mut self.curr);
                    swap(&mut self.curr, &mut self.next);
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
                guard,
            )
            .map_err(|_| ())?;

        Ok(found)
    }

    /// Inserts a value.
    #[inline]
    pub fn insert(
        &mut self,
        node: Rc<Node<K, V, G>, G>,
        guard: &G,
    ) -> Result<(), Rc<Node<K, V, G>, G>> {
        unsafe { node.deref() }.next.swap(
            Rc::from_snapshot(&self.curr, guard),
            Ordering::Relaxed,
            guard,
        );

        match unsafe { self.prev.deref() }.next.compare_exchange(
            self.curr.as_ptr(),
            node,
            Ordering::Release,
            Ordering::Relaxed,
            guard,
        ) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.desired),
        }
    }

    /// removes the current node.
    #[inline]
    pub fn remove(&mut self, guard: &G) -> Result<(), ()> {
        let curr_node = unsafe { self.curr.deref() };

        self.next.load(&curr_node.next, guard);
        if curr_node
            .next
            .compare_exchange(
                self.next.with_tag(0).as_ptr(),
                self.next.with_tag(1),
                Ordering::AcqRel,
                Ordering::Relaxed,
                guard,
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
            guard,
        );

        Ok(())
    }
}

impl<K, V, G> List<K, V, G>
where
    K: Ord + Default,
    V: Default,
    G: Guard,
{
    /// Creates a new list.
    pub fn new() -> Self {
        List {
            head: AtomicRc::new(Node::head(), &G::new()),
        }
    }

    #[inline]
    fn get<F>(&self, key: &K, find: F, cursor: &mut Cursor<K, V, G>, guard: &G) -> bool
    where
        F: Fn(&mut Cursor<K, V, G>, &K, &G) -> Result<bool, ()>,
    {
        loop {
            cursor.initialize(&self.head, guard);
            if let Ok(r) = find(cursor, key, guard) {
                return r;
            }
        }
    }

    #[inline]
    fn insert<F>(&self, key: K, value: V, find: F, cursor: &mut Cursor<K, V, G>, guard: &G) -> bool
    where
        F: Fn(&mut Cursor<K, V, G>, &K, &G) -> Result<bool, ()>,
    {
        let mut node = Rc::new(Node::new(key, value), guard);
        loop {
            let found = self.get(&unsafe { node.deref() }.key, &find, cursor, guard);
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
    fn remove<F>(&self, key: &K, find: F, cursor: &mut Cursor<K, V, G>, guard: &G) -> bool
    where
        F: Fn(&mut Cursor<K, V, G>, &K, &G) -> Result<bool, ()>,
    {
        loop {
            let found = self.get(key, &find, cursor, guard);
            if !found {
                return false;
            }

            match cursor.remove(guard) {
                Err(()) => continue,
                Ok(_) => return true,
            }
        }
    }

    /// Omitted
    pub fn harris_get(&self, key: &K, cursor: &mut Cursor<K, V, G>, guard: &G) -> bool {
        self.get(key, Cursor::find_harris, cursor, guard)
    }

    /// Omitted
    pub fn harris_insert(&self, key: K, value: V, cursor: &mut Cursor<K, V, G>, guard: &G) -> bool {
        self.insert(key, value, Cursor::find_harris, cursor, guard)
    }

    /// Omitted
    pub fn harris_remove(&self, key: &K, cursor: &mut Cursor<K, V, G>, guard: &G) -> bool {
        self.remove(key, Cursor::find_harris, cursor, guard)
    }
}

#[cfg(test)]
fn smoke<G: Guard>() {
    extern crate rand;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    let map = &List::new();

    thread::scope(|s| {
        for t in 0..THREADS {
            s.spawn(move |_| {
                let cursor = &mut Cursor::new();
                let rng = &mut rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(rng);
                for i in keys {
                    assert!(map.harris_insert(i, i.to_string(), cursor, &G::new()));
                }
            });
        }
    })
    .unwrap();

    thread::scope(|s| {
        for t in 0..(THREADS / 2) {
            s.spawn(move |_| {
                let cursor = &mut Cursor::new();
                let rng = &mut rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(rng);
                let guard = &mut G::new();
                for i in keys {
                    assert!(map.harris_remove(&i, cursor, guard));
                    assert_eq!(i.to_string(), unsafe { cursor.curr.deref() }.value);
                    guard.clear();
                }
            });
        }
    })
    .unwrap();

    thread::scope(|s| {
        for t in (THREADS / 2)..THREADS {
            s.spawn(move |_| {
                let cursor = &mut Cursor::new();
                let rng = &mut rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(rng);
                let guard = &mut G::new();
                for i in keys {
                    assert!(map.harris_get(&i, cursor, guard));
                    assert_eq!(i.to_string(), unsafe { cursor.curr.deref() }.value);
                    guard.clear();
                }
            });
        }
    })
    .unwrap();
}

#[test]
fn smoke_ebr() {
    smoke::<cdrc_rs::GuardEBR>();
}
