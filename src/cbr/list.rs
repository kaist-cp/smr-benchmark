use super::concurrent_map::ConcurrentMap;

use crossbeam_cbr::{
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

/// TODO(@jeonghyeon): implement `#[derive(Defender)]`,
/// so that `ReadCursor` and the trait implementation
/// is generated automatically.
pub struct Cursor<K, V> {
    prev: Shield<Node<K, V>>,
    prev_next: Shield<Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: Shield<Node<K, V>>,
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

impl<'r, K: Ord, V> ReadCursor<'r, K, V> {
    /// Creates a cursor.
    fn new(head: &'r Atomic<Node<K, V>>, guard: &'r ReadGuard) -> Self {
        todo!()
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
}

pub struct HList<K, V> {
    inner: List<K, V>,
}

impl<K, V> ConcurrentMap<K, V> for HList<K, V>
where
    K: Ord + Default,
    V: Default,
{
    type Localized = ();

    fn new() -> Self {
        HList { inner: List::new() }
    }

    #[inline]
    fn get<'g>(&self, key: &K, guard: &'g mut EpochGuard) -> Option<(&'g V, Self::Localized)> {
        todo!()
    }

    #[inline]
    fn insert(&self, key: K, value: V, guard: &mut EpochGuard) -> bool {
        todo!()
    }

    #[inline]
    fn remove<'g>(&self, key: &K, guard: &'g mut EpochGuard) -> Option<(&'g V, Self::Localized)> {
        todo!()
    }
}
