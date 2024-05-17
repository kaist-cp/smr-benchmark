//! Michael-Scott lock-free queue.
//!
//! Usable with any number of producers and consumers.
//!
//! Michael and Scott.  Simple, Fast, and Practical Non-Blocking and Blocking Concurrent Queue
//! Algorithms.  PODC 1996.  <http://dl.acm.org/citation.cfm?id=248106>
//!
//! Simon Doherty, Lindsay Groves, Victor Luchangco, and Mark Moir. 2004b. Formal Verification of a
//! Practical Lock-Free Queue Algorithm. <https://doi.org/10.1007/978-3-540-30232-2_7>

use core::mem::MaybeUninit;
use core::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use crossbeam_utils::{Backoff, CachePadded};

use super::super::{unprotected, Atomic, Guard, Owned, Shared};

// The representation here is a singly-linked list, with a sentinel node at the front. In general
// the `tail` pointer may lag behind the actual tail. Non-sentinel nodes are either all `Data` or
// all `Blocked` (requests for data from blocked threads).
#[derive(Debug)]
pub(crate) struct Queue<T> {
    head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
}

struct Node<T> {
    /// The slot in which a value of type `T` can be stored.
    ///
    /// The type of `data` is `MaybeUninit<T>` because a `Node<T>` doesn't always contain a `T`.
    /// For example, the sentinel node in a queue never contains a value: its slot is always empty.
    /// Other nodes start their life with a push operation and contain a value until it gets popped
    /// out. After that such empty nodes get added to the collector for destruction.
    data: MaybeUninit<T>,

    next: Atomic<Node<T>>,
}

// Any particular `T` should never be accessed concurrently, so no need for `Sync`.
unsafe impl<T: Send> Sync for Queue<T> {}
unsafe impl<T: Send> Send for Queue<T> {}

impl<T> Queue<T> {
    /// Create a new, empty queue.
    pub(crate) fn new() -> Queue<T> {
        let q = Queue {
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
        };
        let sentinel = Owned::new(Node {
            data: MaybeUninit::uninit(),
            next: Atomic::null(),
        });
        unsafe {
            let guard = unprotected();
            let sentinel = sentinel.into_shared(guard);
            q.head.store(sentinel, Relaxed);
            q.tail.store(sentinel, Relaxed);
            q
        }
    }

    /// Attempts to atomically place `n` into the `next` pointer of `onto`, and returns `true` on
    /// success. The queue's `tail` pointer may be updated.
    #[inline(always)]
    fn push_internal(
        &self,
        onto: Shared<'_, Node<T>>,
        new: Shared<'_, Node<T>>,
        guard: &Guard,
    ) -> bool {
        // is `onto` the actual tail?
        let o = unsafe { onto.deref() };
        let next = o.next.load(Acquire, guard);
        if unsafe { next.as_ref().is_some() } {
            // if not, try to "help" by moving the tail pointer forward
            let _ = self
                .tail
                .compare_exchange(onto, next, Release, Relaxed, guard);
            false
        } else {
            // looks like the actual tail; attempt to link in `n`
            let result = o
                .next
                .compare_exchange(Shared::null(), new, Release, Relaxed, guard)
                .is_ok();
            if result {
                // try to move the tail pointer forward
                let _ = self
                    .tail
                    .compare_exchange(onto, new, Release, Relaxed, guard);
            }
            result
        }
    }

    /// Adds `t` to the back of the queue, possibly waking up threads blocked on `pop`.
    pub(crate) fn push(&self, t: T, guard: &Guard) {
        let new = Owned::new(Node {
            data: MaybeUninit::new(t),
            next: Atomic::null(),
        });
        let new = Owned::into_shared(new, guard);

        loop {
            // We push onto the tail, so we'll start optimistically by looking there first.
            let tail = self.tail.load(Acquire, guard);

            // Attempt to push onto the `tail` snapshot; fails if `tail.next` has changed.
            if self.push_internal(tail, new, guard) {
                break;
            }
        }
    }

    /// Attempts to pop a data node. `Ok(None)` if queue is empty; `Err(())` if lost race to pop.
    #[inline(always)]
    fn pop_internal(&self, guard: &Guard) -> Result<Option<T>, ()> {
        let head = self.head.load(Acquire, guard);
        let h = unsafe { head.deref() };
        let next = h.next.load(Acquire, guard);
        match unsafe { next.as_ref() } {
            Some(n) => unsafe {
                self.head
                    .compare_exchange(head, next, Release, Relaxed, guard)
                    .map(|_| {
                        let tail = self.tail.load(Relaxed, guard);
                        // Advance the tail so that we don't retire a pointer to a reachable node.
                        if head == tail {
                            let _ = self
                                .tail
                                .compare_exchange(tail, next, Release, Relaxed, guard);
                        }
                        guard.defer_destroy(head);
                        // TODO: Replace with MaybeUninit::read when api is stable
                        Some(n.data.as_ptr().read())
                    })
                    .map_err(|_| ())
            },
            None => Ok(None),
        }
    }

    /// Attempts to pop a data node, if the data satisfies the given condition. `Ok(None)` if queue
    /// is empty or the data does not satisfy the condition; `Err(())` if lost race to pop.
    #[inline(always)]
    fn pop_if_internal<F>(&self, condition: F, guard: &Guard) -> Result<Option<T>, ()>
    where
        T: Sync,
        F: Fn(&T) -> bool,
    {
        let head = self.head.load(Acquire, guard);
        let h = unsafe { head.deref() };
        let next = h.next.load(Acquire, guard);
        match unsafe { next.as_ref() } {
            Some(n) if condition(unsafe { &*n.data.as_ptr() }) => unsafe {
                self.head
                    .compare_exchange(head, next, Release, Relaxed, guard)
                    .map(|_| {
                        let tail = self.tail.load(Relaxed, guard);
                        // Advance the tail so that we don't retire a pointer to a reachable node.
                        if head == tail {
                            let _ = self
                                .tail
                                .compare_exchange(tail, next, Release, Relaxed, guard);
                        }
                        guard.defer_destroy(head);
                        Some(n.data.as_ptr().read())
                    })
                    .map_err(|_| ())
            },
            None | Some(_) => Ok(None),
        }
    }

    /// Attempts to dequeue from the front.
    ///
    /// Returns `None` if the queue is observed to be empty.
    pub(crate) fn try_pop(&self, guard: &Guard) -> Option<T> {
        loop {
            if let Ok(head) = self.pop_internal(guard) {
                return head;
            }
        }
    }

    /// Attempts to dequeue from the front, if the item satisfies the given condition.
    ///
    /// Returns `None` if the queue is observed to be empty, or the head does not satisfy the given
    /// condition.
    pub(crate) fn try_pop_if<F>(&self, condition: F, guard: &Guard) -> Option<T>
    where
        T: Sync,
        F: Fn(&T) -> bool,
    {
        let backoff = Backoff::new();
        loop {
            if let Ok(head) = self.pop_if_internal(&condition, guard) {
                return head;
            }
            backoff.spin();
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.head.load(Acquire, unsafe { unprotected() })
            == self.tail.load(Acquire, unsafe { unprotected() })
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            let guard = unprotected();

            while self.try_pop(guard).is_some() {}

            // Destroy the remaining sentinel node.
            let sentinel = self.head.load(Relaxed, guard);
            drop(sentinel.into_owned());
        }
    }
}
