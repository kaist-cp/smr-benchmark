extern crate crossbeam_cbr_epoch;

use crossbeam_cbr_epoch::{
    AcquiredPtr, Atomic, Defender, EpochGuard, Rc, ReadGuard, ReadStatus, Shared, Shield,
    WriteResult,
};
use std::{mem, sync::atomic::Ordering};

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
        self.found = read.found;
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
        self.prev.defend(head, guard);
        self.curr.defend(&self.prev.as_ref().unwrap().next, guard);
        self.prev_next.copy_from(&self.curr, guard);
        self.found = false;
    }
}

impl<'r, K: Ord, V> ReadCursor<'r, K, V> {
    /// Creates a cursor.
    fn new(head: &'r Atomic<Node<K, V>>, guard: &'r ReadGuard) -> Self {
        let prev = head.load(Ordering::Relaxed, guard);
        let curr = prev
            .as_ref(guard)
            .unwrap()
            .next
            .load(Ordering::Acquire, guard);
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
        'find: loop {
            cursor.initialize(&self.head, guard);
            cursor.found = loop {
                let curr_node = match cursor.curr.as_ref() {
                    Some(node) => node,
                    None => break false,
                };
                cursor.next.defend(&curr_node.next, guard);

                if cursor.next.tag() != 0 {
                    cursor.next.set_tag(0);
                    if cursor
                        .prev
                        .as_ref()
                        .unwrap()
                        .next
                        .try_compare_exchange(
                            cursor.curr.shared(),
                            &cursor.next,
                            Ordering::Release,
                            Ordering::Relaxed,
                            guard,
                        )
                        .is_err()
                    {
                        continue 'find;
                    }
                    mem::swap(&mut cursor.curr, &mut cursor.next);
                    continue;
                }

                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        mem::swap(&mut cursor.prev, &mut cursor.curr);
                        mem::swap(&mut cursor.curr, &mut cursor.next);
                    }
                    std::cmp::Ordering::Equal => break true,
                    std::cmp::Ordering::Greater => break false,
                }
            };
            return;
        }
    }

    pub fn find_read_loop(&self, key: &K, handle: &mut Handle<K, V>, guard: &mut EpochGuard) {
        loop {
            let cursor = guard.read_loop(
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

                    let mut next = curr_node.next.load(Ordering::Acquire, guard);
                    if next.tag() != 0 {
                        next = next.with_tag(0);
                        guard.write::<_, [Shield<Node<K, V>>; 3]>(
                            [cursor.prev, cursor.curr, next],
                            |[prev, curr, next], guard| {
                                if prev
                                    .as_ref()
                                    .unwrap()
                                    .next
                                    .try_compare_exchange(
                                        curr.shared(),
                                        next,
                                        Ordering::Release,
                                        Ordering::Relaxed,
                                        guard,
                                    )
                                    .is_ok()
                                {
                                    return WriteResult::Finished;
                                } else {
                                    return WriteResult::RestartRead;
                                }
                            },
                        );
                        mem::swap(&mut cursor.curr, &mut next);
                        return ReadStatus::Continue;
                    }

                    match curr_node.key.cmp(key) {
                        std::cmp::Ordering::Less => {
                            cursor.prev = cursor.curr;
                            cursor.curr = next;
                            return ReadStatus::Continue;
                        }
                        std::cmp::Ordering::Equal => cursor.found = true,
                        std::cmp::Ordering::Greater => cursor.found = false,
                    }
                    ReadStatus::Finished
                },
            );
            return cursor;
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
        let new_node = Rc::new(Node::new(key, value), guard);
        loop {
            find(self, &new_node.as_ref().unwrap().key, handle, guard);
            let cursor = &mut handle.0;
            if cursor.found {
                let new_node = unsafe { new_node.into_owned() };
                return Err((new_node.key, new_node.value));
            }

            new_node.as_ref().unwrap().next.swap(
                cursor.curr.to_rc(guard),
                Ordering::Relaxed,
                guard,
            );
            loop {
                match unsafe { cursor.next.try_defend(new_node.shared(), guard) } {
                    Ok(_) => break,
                    Err(_) => guard.repin(),
                }
            }

            if cursor
                .prev
                .as_ref()
                .unwrap()
                .next
                .try_compare_exchange(
                    cursor.curr.shared(),
                    &cursor.next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok()
            {
                return Ok(());
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
            cursor.next.defend(&curr_node.next, guard);
            if cursor.next.tag() > 0 {
                continue;
            }

            cursor.next.set_tag(1);
            if curr_node
                .next
                .try_compare_exchange(
                    cursor.next.shared().with_tag(0),
                    &cursor.next,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    guard,
                )
                .is_err()
            {
                continue;
            }

            cursor.next.set_tag(0);
            let _ = cursor.prev.as_ref().unwrap().next.try_compare_exchange(
                cursor.curr.shared(),
                &cursor.next,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            );

            return true;
        }
    }
}

#[test]
fn smoke_harris_michael_naive() {
    for _ in 0..5 {
        smoke_with(&List::<i32, String>::find_naive);
    }
}

#[test]
fn smoke_harris_michael_read_loop() {
    for _ in 0..5 {
        smoke_with(&List::<i32, String>::find_read_loop);
    }
}

fn smoke_with<F>(find: &F)
where
    F: Fn(&List<i32, String>, &i32, &mut Handle<i32, String>, &mut EpochGuard) + Sync,
{
    extern crate rand;
    use crossbeam_cbr_epoch::pin;
    use rand::prelude::*;
    use std::sync::atomic::compiler_fence;
    use std::thread::scope;

    const THREADS: i32 = 30;
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
