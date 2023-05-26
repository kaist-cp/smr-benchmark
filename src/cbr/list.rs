use super::concurrent_map::ConcurrentMap;

use crossbeam_cbr::{
    rc::{AcquiredPtr, Atomic, Localizable, Rc, Shared, Shield},
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

/// TODO(@jeonghyeon): implement `#[derive(Localizable)]`,
/// so that `LocalizedCursor` and the trait implementation
/// is generated automatically.
struct Cursor<'r, K, V> {
    prev: Shared<'r, Node<K, V>>,
    prev_next: Shared<'r, Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: Shared<'r, Node<K, V>>,
    found: bool,
}

/// This trait implementation must be generated automatically by a `derive` macro.
impl<'r, K, V> Clone for Cursor<'r, K, V> {
    fn clone(&self) -> Self {
        Self { ..*self }
    }
}

/// This trait implementation must be generated automatically by a `derive` macro.
impl<'r, K, V> Copy for Cursor<'r, K, V> {}

/// This struct definition must be generated automatically by a `derive` macro.
pub struct LocalizedCursor<K, V> {
    prev: Shield<Node<K, V>>,
    prev_next: Shield<Node<K, V>>,
    curr: Shield<Node<K, V>>,
    found: bool,
}

/// This trait implementation must be generated automatically by a `derive` macro.
impl<'r, K, V> Localizable<'r> for Cursor<'r, K, V> {
    type Localized = LocalizedCursor<K, V>;

    fn protect_with(self, guard: &EpochGuard) -> Self::Localized {
        Self::Localized {
            prev: self.prev.protect_with(guard),
            prev_next: self.prev_next.protect_with(guard),
            curr: self.curr.protect_with(guard),
            found: self.found,
        }
    }
}

impl<'r, K: Ord, V> Cursor<'r, K, V> {
    /// Creates a cursor.
    fn new(head: &'r Atomic<Node<K, V>>, guard: &'r ReadGuard) -> Self {
        let prev = head.load(guard);
        let curr = prev.as_ref().unwrap().next.load(guard);
        Self {
            prev,
            prev_next: curr,
            curr,
            found: false,
        }
    }
}

impl<K, V> LocalizedCursor<K, V> {
    fn new(head: &Atomic<Node<K, V>>, guard: &mut EpochGuard) -> Self {
        let prev = head.defend(guard);
        let curr = prev.as_ref().unwrap().next.defend(guard);
        let mut prev_next = Shield::null(guard);
        curr.copy_to(&mut prev_next, guard);
        Self {
            prev,
            prev_next,
            curr,
            found: false,
        }
    }
}

impl<K, V> List<K, V>
where
    K: Default + Ord,
    V: Default,
{
    pub fn new() -> Self {
        List {
            head: Atomic::new(Node::head()),
        }
    }

    pub fn find_naive(&self, key: &K, guard: &mut EpochGuard) -> LocalizedCursor<K, V> {
        loop {
            let mut next = Shield::null(guard);
            let mut cursor = LocalizedCursor::new(&self.head, guard);
            cursor.found = loop {
                let curr_node = match cursor.curr.as_ref() {
                    Some(node) => node,
                    None => break false,
                };

                curr_node.next.defend_with(&mut next, guard);
                if next.tag() > 0 {
                    next = next.with_tag(0);
                    mem::swap(&mut next, &mut cursor.curr);
                    continue;
                }

                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        mem::swap(&mut cursor.prev, &mut cursor.curr);
                        next.copy_to(&mut cursor.prev_next, guard);
                        mem::swap(&mut cursor.curr, &mut next);
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
                return cursor;
            }
        }
    }

    pub fn find_read(&self, key: &K, guard: &mut EpochGuard) -> LocalizedCursor<K, V> {
        loop {
            let cursor = guard.read(|guard| {
                let mut cursor = Cursor::new(&self.head, guard);
                cursor.found = loop {
                    let curr_node = match cursor.curr.as_ref() {
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
                return cursor;
            }
        }
    }

    pub fn find_read_loop(&self, key: &K, guard: &mut EpochGuard) -> LocalizedCursor<K, V> {
        loop {
            let cursor = guard.read_loop(
                |guard| Cursor::new(&self.head, guard),
                |cursor, guard| {
                    let curr_node = match cursor.curr.as_ref() {
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

            // Perform Clean-up CAS and return the cursor.
            if cursor.prev_next.as_raw() == cursor.curr.as_raw()
                || cursor.prev.as_ref().unwrap().next.try_compare_exchange(
                    &cursor.prev_next,
                    &cursor.curr,
                    guard,
                )
            {
                return cursor;
            }
        }
    }

    pub fn get<F>(&self, find: F, key: &K, guard: &mut EpochGuard) -> Option<LocalizedCursor<K, V>>
    where
        F: Fn(&List<K, V>, &K, &mut EpochGuard) -> LocalizedCursor<K, V>,
    {
        let cursor = find(self, key, guard);
        if cursor.found {
            Some(cursor)
        } else {
            None
        }
    }

    pub fn insert<F>(&self, find: F, key: K, value: V, guard: &mut EpochGuard) -> Result<(), (K, V)>
    where
        F: Fn(&List<K, V>, &K, &mut EpochGuard) -> LocalizedCursor<K, V>,
    {
        let mut new_node = Node::new(key, value);
        loop {
            let cursor = find(self, &new_node.key, guard);
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

    pub fn remove<F>(
        &self,
        find: F,
        key: &K,
        guard: &mut EpochGuard,
    ) -> Option<LocalizedCursor<K, V>>
    where
        F: Fn(&List<K, V>, &K, &mut EpochGuard) -> LocalizedCursor<K, V>,
    {
        loop {
            let cursor = find(self, key, guard);
            if !cursor.found {
                return None;
            }

            let curr_node = cursor.curr.as_ref().unwrap();
            let mut next = Shield::null(guard);
            curr_node.next.defend_with(&mut next, guard);
            if next.tag() > 0 || !curr_node.next.try_compare_exchange_tag(&next, 1, guard) {
                continue;
            }

            cursor
                .prev
                .as_ref()
                .unwrap()
                .next
                .try_compare_exchange(&cursor.curr, &next, guard);

            return Some(cursor);
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
    type Localized = LocalizedCursor<K, V>;

    fn new() -> Self {
        HList { inner: List::new() }
    }

    #[inline]
    fn get<'g>(&self, key: &K, guard: &'g mut EpochGuard) -> Option<(&'g V, Self::Localized)> {
        match self.inner.get(List::find_read_loop, key, guard) {
            Some(found) => Some((unsafe { mem::transmute(found.curr.as_ref()) }, found)),
            None => None,
        }
    }

    #[inline]
    fn insert(&self, key: K, value: V, guard: &mut EpochGuard) -> bool {
        self.inner
            .insert(List::find_read_loop, key, value, guard)
            .is_ok()
    }

    #[inline]
    fn remove<'g>(&self, key: &K, guard: &'g mut EpochGuard) -> Option<(&'g V, Self::Localized)> {
        match self.inner.remove(List::find_read_loop, key, guard) {
            Some(found) => Some((unsafe { mem::transmute(found.curr.as_ref()) }, found)),
            None => None,
        }
    }
}
