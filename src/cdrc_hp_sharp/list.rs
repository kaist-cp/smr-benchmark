use hp_sharp::{AtomicRc, CsGuard, Invalidate, RaGuard, Rc};

use crate::hp_sharp::ConcurrentMap;

use std::cmp::Ordering::{Equal, Greater, Less};
use std::mem;
use std::sync::atomic::Ordering;

struct Node<K, V> {
    /// Mark: tag(), Tag: not needed
    next: AtomicRc<Self>,
    key: K,
    value: V,
}

// TODO(@jeonghyeon): automate
impl<K, V> Invalidate for Node<K, V> {
    #[inline]
    fn invalidate(&self) {
        self.next
            .fetch_or(1 | 2, Ordering::Release, &unsafe { RaGuard::unprotected() });
    }

    #[inline]
    fn is_invalidated(&self, guard: &CsGuard) -> bool {
        (self.next.load(Ordering::Acquire, guard).tag() & 2) != 0
    }
}

struct List<K, V> {
    head: AtomicRc<Node<K, V>>,
}

impl<K, V> Default for List<K, V>
where
    K: Ord + Default,
    V: Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for List<K, V> {
    fn drop(&mut self) {
        // `Drop` for CDRC List is not necessary, but it effectively prevents a stack overflow.
        unsafe {
            let cs = CsGuard::unprotected();
            let mut curr = self.head.load(Ordering::Relaxed, &cs);
            let mut next;

            while !curr.is_null() {
                let curr_ref = curr.deref_unchecked();
                next = curr_ref.next.load(Ordering::Relaxed, &cs);
                curr_ref
                    .next
                    .swap(Rc::null(), Ordering::Relaxed, &RaGuard::unprotected());
                curr = next;
            }
        }
    }
}

impl<K, V> Node<K, V>
where
    K: Default,
    V: Default,
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


