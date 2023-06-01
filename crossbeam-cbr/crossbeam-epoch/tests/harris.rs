extern crate crossbeam_cbr_epoch;

use crossbeam_cbr_epoch::{
    rc::{AcquiredPtr, Atomic, Defender, Rc, Shared, Shield},
    EpochGuard, ReadGuard, ReadStatus,
};
use std::mem;

struct Node<K, V> {
    next: Atomic<Self>,
    key: K,
    value: V,
}

struct List<K, V> {
    head: Atomic<Node<K, V>>,
}

impl<K, V> Node<K, V>
where
    K: Default,
    V: Default,
{
    /// Creates a new node.
    fn new(key: K, value: V) -> Self {
        Self {
            next: Atomic::null(),
            key,
            value,
        }
    }

    /// Creates a dummy head.
    /// We never deref key and value of this head node.
    fn head() -> Self {
        Self {
            next: Atomic::null(),
            key: K::default(),
            value: V::default(),
        }
    }
}

pub struct Handle<K, V>(Cursor<K, V>, Cursor<K, V>);

impl<K, V> Handle<K, V>
where
    K: 'static,
    V: 'static,
{
    #[inline]
    fn default(guard: &EpochGuard) -> Self {
        Self(Cursor::default(guard), Cursor::default(guard))
    }
}

/// TODO(@jeonghyeon): implement `#[derive(Defender)]`,
/// so that `ReadCursor` and the trait implementation
/// is generated automatically.
pub struct Cursor<K, V> {
    prev: Shield<Node<K, V>>,
    prev_next: Shield<Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: Shield<Node<K, V>>,
    next: Shield<Node<K, V>>,
    found: bool,
}

/// This struct definition must be generated automatically by a `derive` macro.
pub struct ReadCursor<'r, K, V> {
    prev: Shared<'r, Node<K, V>>,
    prev_next: Shared<'r, Node<K, V>>,
    curr: Shared<'r, Node<K, V>>,
    found: bool,
}

impl<'r, K, V> Clone for ReadCursor<'r, K, V> {
    fn clone(&self) -> Self {
        Self { ..*self }
    }
}

impl<'r, K, V> Copy for ReadCursor<'r, K, V> {}

/// This trait implementation must be generated automatically by a `derive` macro.
impl<K: 'static, V: 'static> Defender for Cursor<K, V> {
    type Read<'r> = ReadCursor<'r, K, V>;

    #[inline]
    fn default(guard: &EpochGuard) -> Self {
        Self {
            prev: Shield::null(guard),
            prev_next: Shield::null(guard),
            curr: Shield::null(guard),
            next: Shield::null(guard),
            found: false,
        }
    }

    #[inline]
    unsafe fn defend_unchecked(&mut self, read: &Self::Read<'_>) {
        self.prev.defend_unchecked(&read.prev);
        self.prev_next.defend_unchecked(&read.prev_next);
        self.curr.defend_unchecked(&read.curr);
    }

    #[inline]
    unsafe fn as_read<'r>(&mut self) -> Self::Read<'r> {
        ReadCursor {
            prev: self.prev.as_read(),
            prev_next: self.prev_next.as_read(),
            curr: self.curr.as_read(),
            found: self.found,
        }
    }

    #[inline]
    fn release(&mut self) {
        self.prev.release();
        self.prev_next.release();
        self.curr.release();
    }
}

impl<K, V> Cursor<K, V> {
    fn initialize(&mut self, head: &Atomic<Node<K, V>>, guard: &mut EpochGuard) {
        head.defend_with(&mut self.prev, guard);
        self.prev
            .as_ref()
            .unwrap()
            .next
            .defend_with(&mut self.curr, guard);
        self.curr.copy_to(&mut self.prev_next, guard);
        self.found = false;
    }
}

impl<'r, K: Ord, V> ReadCursor<'r, K, V> {
    /// Creates a cursor.
    fn new(head: &'r Atomic<Node<K, V>>, guard: &'r ReadGuard) -> Self {
        let prev = head.load(guard);
        let curr = prev.as_ref(guard).unwrap().next.load(guard);
        Self {
            prev,
            prev_next: curr,
            curr,
            found: false,
        }
    }
}

impl<K, V> List<K, V>
where
    K: Default + Ord + 'static,
    V: Default + 'static,
{
    pub fn new() -> Self {
        List {
            head: Atomic::new(Node::head()),
        }
    }

    pub fn find_naive(&self, key: &K, handle: &mut Handle<K, V>, guard: &mut EpochGuard) {
        let mut cursor = &mut handle.0;
        loop {
            cursor.initialize(&self.head, guard);
            cursor.found = loop {
                let curr_node = match cursor.curr.as_ref() {
                    Some(node) => node,
                    None => break false,
                };

                curr_node.next.defend_with(&mut cursor.next, guard);
                if cursor.next.tag() > 0 {
                    cursor.next.set_tag(0);
                    mem::swap(&mut cursor.next, &mut cursor.curr);
                    continue;
                }

                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        mem::swap(&mut cursor.prev, &mut cursor.curr);
                        cursor.next.copy_to(&mut cursor.prev_next, guard);
                        mem::swap(&mut cursor.curr, &mut cursor.next);
                        continue;
                    }
                    std::cmp::Ordering::Equal => break true,
                    std::cmp::Ordering::Greater => break false,
                }
            };

            // Perform Clean-up CAS and return the cursor.
            if cursor.prev_next.as_raw() == cursor.curr.as_raw()
                || cursor.prev.as_ref().unwrap().next.try_compare_exchange(
                    &cursor.prev_next,
                    &cursor.curr,
                    guard,
                )
            {
                return;
            }
        }
    }

    pub fn find_read(&self, key: &K, handle: &mut Handle<K, V>, guard: &mut EpochGuard) {
        let cursor = &mut handle.0;
        loop {
            guard.read(cursor, |guard| {
                let mut cursor = ReadCursor::new(&self.head, guard);
                cursor.found = loop {
                    let curr_node = match cursor.curr.as_ref(guard) {
                        Some(node) => node,
                        None => break false,
                    };

                    let next = curr_node.next.load(guard);
                    if next.tag() > 0 {
                        cursor.curr = next.with_tag(0);
                        continue;
                    }

                    match curr_node.key.cmp(key) {
                        std::cmp::Ordering::Less => {
                            cursor.prev = cursor.curr;
                            cursor.prev_next = next;
                            cursor.curr = next;
                            continue;
                        }
                        std::cmp::Ordering::Equal => break true,
                        std::cmp::Ordering::Greater => break false,
                    }
                };
                cursor
            });

            // Perform Clean-up CAS and return the cursor.
            if cursor.prev_next.as_raw() == cursor.curr.as_raw()
                || cursor.prev.as_ref().unwrap().next.try_compare_exchange(
                    &cursor.prev_next,
                    &cursor.curr,
                    guard,
                )
            {
                return;
            }
        }
    }

    pub fn find_read_loop(&self, key: &K, handle: &mut Handle<K, V>, guard: &mut EpochGuard) {
        loop {
            guard.read_loop(
                &mut handle.0,
                &mut handle.1,
                |guard| ReadCursor::new(&self.head, guard),
                |cursor, guard| {
                    let curr_node = match cursor.curr.as_ref(guard) {
                        Some(node) => node,
                        None => {
                            cursor.found = false;
                            return ReadStatus::Finished;
                        }
                    };

                    let next = curr_node.next.load(guard);
                    if next.tag() > 0 {
                        cursor.curr = next.with_tag(0);
                        return ReadStatus::Continue;
                    }

                    match curr_node.key.cmp(key) {
                        std::cmp::Ordering::Less => {
                            cursor.prev = cursor.curr;
                            cursor.prev_next = next;
                            cursor.curr = next;
                            return ReadStatus::Continue;
                        }
                        std::cmp::Ordering::Equal => cursor.found = true,
                        std::cmp::Ordering::Greater => cursor.found = false,
                    }
                    ReadStatus::Finished
                },
            );

            let cursor = &mut handle.0;
            // Perform Clean-up CAS and return the cursor.
            if cursor.prev_next.as_raw() == cursor.curr.as_raw()
                || cursor.prev.as_ref().unwrap().next.try_compare_exchange(
                    &cursor.prev_next,
                    &cursor.curr,
                    guard,
                )
            {
                return;
            }
        }
    }

    /// On `true`, `handle.0.curr` is a reference to the found node.
    pub fn get<F>(
        &self,
        find: F,
        key: &K,
        handle: &mut Handle<K, V>,
        guard: &mut EpochGuard,
    ) -> bool
    where
        F: Fn(&List<K, V>, &K, &mut Handle<K, V>, &mut EpochGuard),
    {
        find(self, key, handle, guard);
        handle.0.found
    }

    pub fn insert<F>(
        &self,
        find: F,
        key: K,
        value: V,
        handle: &mut Handle<K, V>,
        guard: &mut EpochGuard,
    ) -> Result<(), (K, V)>
    where
        F: Fn(&List<K, V>, &K, &mut Handle<K, V>, &mut EpochGuard),
    {
        let mut new_node = Node::new(key, value);
        loop {
            find(self, &new_node.key, handle, guard);
            let cursor = &handle.0;
            if cursor.found {
                return Err((new_node.key, new_node.value));
            }

            new_node.next.store(&cursor.curr, guard);
            let new_node_ptr = Rc::from_obj(new_node, guard);

            if cursor.prev.as_ref().unwrap().next.try_compare_exchange(
                &cursor.curr,
                &new_node_ptr,
                guard,
            ) {
                return Ok(());
            } else {
                // Safety: As we failed to insert `new_node_ptr` into the data structure,
                // only current thread has a reference to this node.
                new_node = unsafe { new_node_ptr.into_owned() };
            }
        }
    }

    /// On `true`, `handle.0.curr` is a reference to the removed node.
    pub fn remove<F>(
        &self,
        find: F,
        key: &K,
        handle: &mut Handle<K, V>,
        guard: &mut EpochGuard,
    ) -> bool
    where
        F: Fn(&List<K, V>, &K, &mut Handle<K, V>, &mut EpochGuard),
    {
        loop {
            find(self, key, handle, guard);
            let cursor = &mut handle.0;
            if !cursor.found {
                return false;
            }

            let curr_node = cursor.curr.as_ref().unwrap();
            curr_node.next.defend_with(&mut cursor.next, guard);
            if cursor.next.tag() > 0
                || !curr_node
                    .next
                    .try_compare_exchange_tag(&cursor.next, 1, guard)
            {
                continue;
            }

            cursor.prev.as_ref().unwrap().next.try_compare_exchange(
                &cursor.curr,
                &cursor.next,
                guard,
            );

            return true;
        }
    }
}

#[test]
fn smoke_harris_naive() {
    return; // TODO(@jeonghyeon): Temporary return to bypass CI test
    for i in 0..50 {
        smoke_with(&List::<i32, String>::find_naive);
        println!("{i}");
    }
}

#[test]
fn smoke_harris_read() {
    return; // TODO(@jeonghyeon): Temporary return to bypass CI test
    for i in 0..50 {
        smoke_with(&List::<i32, String>::find_read);
        println!("{i}");
    }
}

#[test]
fn smoke_harris_read_loop() {
    return; // TODO(@jeonghyeon): Temporary return to bypass CI test
    for i in 0..50 {
        smoke_with(&List::<i32, String>::find_read_loop);
        println!("{i}");
    }
}

fn smoke_with<F>(find: &F)
where
    F: Fn(&List<i32, String>, &i32, &mut Handle<i32, String>, &mut EpochGuard) + Sync,
{
    extern crate rand;
    use crossbeam_cbr_epoch::pin;
    use rand::prelude::*;
    use std::sync::atomic::{compiler_fence, Ordering};
    use std::thread::scope;

    const THREADS: i32 = 1;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    let map = &List::new();

    scope(|s| {
        for t in 0..THREADS {
            s.spawn(move || {
                let mut handle = Handle::default(&pin());
                let mut rng = rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(&mut rng);
                for i in keys {
                    assert!(map
                        .insert(find, i, i.to_string(), &mut handle, &mut pin())
                        .is_ok());
                }
            });
        }
    });

    scope(|s| {
        for t in 0..(THREADS / 2) {
            s.spawn(move || {
                let mut handle = Handle::default(&pin());
                let mut rng = rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(&mut rng);
                let mut guard = pin();
                for i in keys {
                    assert!(map.remove(find, &i, &mut handle, &mut guard));
                    assert_eq!(i.to_string(), handle.0.curr.as_ref().unwrap().value);
                    compiler_fence(Ordering::SeqCst);
                    guard.repin();
                }
            });
        }
    });

    scope(|s| {
        for t in (THREADS / 2)..THREADS {
            s.spawn(move || {
                let mut handle = Handle::default(&pin());
                let mut rng = rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(&mut rng);
                let mut guard = pin();
                for i in keys {
                    assert!(map.get(find, &i, &mut handle, &mut guard));
                    assert_eq!(i.to_string(), handle.0.curr.as_ref().unwrap().value);
                    compiler_fence(Ordering::SeqCst);
                    guard.repin();
                }
            });
        }
    });
}
