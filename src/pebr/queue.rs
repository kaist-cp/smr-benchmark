//! Michael-Scott lock-free queue.
//!
//! Usable with any number of producers and consumers.
//!
//! Michael and Scott.  Simple, Fast, and Practical Non-Blocking and Blocking Concurrent Queue
//! Algorithms.  PODC 1996.  http://dl.acm.org/citation.cfm?id=248106

use core::mem::{self, ManuallyDrop};
use core::ptr;
use core::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use crossbeam_utils::CachePadded;

use crossbeam_pebr::{unprotected, Atomic, Guard, Owned, Shared, Shield, ShieldError};

// The representation here is a singly-linked list, with a sentinel node at the front. In general
// the `tail` pointer may lag behind the actual tail. Non-sentinel nodes are either all `Data` or
// all `Blocked` (requests for data from blocked threads).
#[derive(Debug)]
pub struct Queue<T> {
    head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
}

#[derive(Debug)]
struct Node<T> {
    /// The slot in which a value of type `T` can be stored.
    ///
    /// The type of `data` is `ManuallyDrop<T>` because a `Node<T>` doesn't always contain a `T`.
    /// For example, the sentinel node in a queue never contains a value: its slot is always empty.
    /// Other nodes start their life with a push operation and contain a value until it gets popped
    /// out. After that such empty nodes get added to the collector for destruction.
    data: ManuallyDrop<T>,

    next: Atomic<Node<T>>,
}

pub struct Handle<T> {
    /// defend `head` or `tail`
    shield1: Shield<Node<T>>,
    /// defend `onto.next`
    shield2: Shield<Node<T>>,
}

enum PopError {
    Retry,
    ShieldError(ShieldError),
}

// Any particular `T` should never be accessed concurrently, so no need for `Sync`.
unsafe impl<T: Send> Sync for Queue<T> {}
unsafe impl<T: Send> Send for Queue<T> {}

impl<T> Queue<T> {
    /// Create a new, empty queue.
    pub fn new() -> Queue<T> {
        let q = Queue {
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
        };
        #[allow(deprecated)]
        let sentinel = Owned::new(Node {
            data: unsafe { mem::uninitialized() },
            next: Atomic::null(),
        });
        unsafe {
            let guard = &unprotected();
            let sentinel = sentinel.into_shared(guard);
            q.head.store(sentinel, Relaxed);
            q.tail.store(sentinel, Relaxed);
            q
        }
    }

    pub fn handle(guard: &Guard) -> Handle<T> {
        Handle {
            shield1: Shield::null(guard),
            shield2: Shield::null(guard),
        }
    }

    /// Attempts to atomically place `n` into the `next` pointer of `onto`, and returns `true` on
    /// success. The queue's `tail` pointer may be updated.
    #[inline(always)]
    fn push_internal(
        &self,
        onto: Shared<Node<T>>,
        new: Shared<Node<T>>,
        handle: &mut Handle<T>,
        guard: &Guard,
    ) -> Result<bool, ShieldError> {
        // is `onto` the actual tail?
        handle.shield1.defend(onto, guard)?;
        let o = unsafe { handle.shield1.deref() };
        let next = o.next.load(Acquire, guard);
        if !next.is_null() {
            // if not, try to "help" by moving the tail pointer forward
            let _ = self
                .tail
                .compare_and_set(handle.shield1.shared(), next, Release, guard);
            Ok(false)
        } else {
            // looks like the actual tail; attempt to link in `n`
            let result = o
                .next
                .compare_and_set(Shared::null(), new, Release, guard)
                .is_ok();
            if result {
                // try to move the tail pointer forward
                let _ = self
                    .tail
                    .compare_and_set(handle.shield1.shared(), new, Release, guard);
            }
            Ok(result)
        }
    }

    /// Adds `t` to the back of the queue, possibly waking up threads blocked on `pop`.
    pub fn push(&self, t: T, handle: &mut Handle<T>, guard: &mut Guard) {
        let new = Owned::new(Node {
            data: ManuallyDrop::new(t),
            next: Atomic::null(),
        });
        let new = Owned::into_shared(new, unsafe { unprotected() });

        loop {
            // We push onto the tail, so we'll start optimistically by looking there first.
            let tail = self.tail.load(Acquire, guard);

            // Attempt to push onto the `tail` snapshot; fails if `tail.next` has changed.
            match self.push_internal(tail, new, handle, guard) {
                Ok(true) => break,
                Ok(_) => continue,
                Err(ShieldError::Ejected) => guard.repin(),
            }
        }
    }

    /// Attempts to pop a data node. `Ok(None)` if queue is empty; `Err(())` if lost race to pop.
    #[inline(always)]
    fn pop_internal(&self, handle: &mut Handle<T>, guard: &Guard) -> Result<Option<T>, PopError> {
        let head = self.head.load(Acquire, guard);
        handle
            .shield1
            .defend(head, guard)
            .map_err(PopError::ShieldError)?;
        let h = unsafe { handle.shield1.deref() };
        let next = h.next.load(Acquire, guard);
        handle
            .shield2
            .defend(next, guard)
            .map_err(PopError::ShieldError)?;
        match unsafe { handle.shield2.as_ref() } {
            Some(n) => unsafe {
                self.head
                    .compare_and_set(
                        handle.shield1.shared(),
                        handle.shield2.shared(),
                        Release,
                        guard,
                    )
                    .map(|_| {
                        guard.defer_destroy(handle.shield1.shared());
                        Some(ManuallyDrop::into_inner(ptr::read(&n.data)))
                    })
                    .map_err(|_| PopError::Retry)
            },
            None => Ok(None),
        }
    }

    /// Attempts to dequeue from the front.
    ///
    /// Returns `None` if the queue is observed to be empty.
    pub fn try_pop(&self, handle: &mut Handle<T>, guard: &mut Guard) -> Option<T> {
        loop {
            match self.pop_internal(handle, guard) {
                Ok(head) => return head,
                Err(PopError::Retry) => continue,
                Err(PopError::ShieldError(ShieldError::Ejected)) => guard.repin(),
            }
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            let guard = &mut unprotected();
            let handle = &mut Self::handle(guard);

            while let Some(_) = self.try_pop(handle, guard) {}

            // Destroy the remaining sentinel node.
            let sentinel = self.head.load(Relaxed, guard);
            drop(sentinel.into_owned());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crossbeam_pebr::pin;
    use crossbeam_utils::thread;

    struct Queue<T> {
        queue: super::Queue<T>,
    }

    impl<T> Queue<T> {
        pub fn new() -> Queue<T> {
            Queue {
                queue: super::Queue::new(),
            }
        }

        pub fn handle() -> Handle<T> {
            super::Queue::handle(&pin())
        }

        pub fn push(&self, t: T, handle: &mut Handle<T>) {
            let guard = &mut pin();
            self.queue.push(t, handle, guard);
        }

        pub fn is_empty(&self, handle: &mut Handle<T>) -> bool {
            loop {
                let guard = &pin();
                let head = self.queue.head.load(Acquire, guard);
                if handle.shield1.defend(head, guard).is_err() {
                    continue;
                }
                let h = unsafe { handle.shield1.deref() };
                break h.next.load(Acquire, guard).is_null();
            }
        }

        pub fn try_pop(&self, handle: &mut Handle<T>) -> Option<T> {
            let guard = &mut pin();
            self.queue.try_pop(handle, guard)
        }
    }

    const CONC_COUNT: i64 = 1000000;
    const THREADS: i64 = 24;

    #[test]
    fn smoke_queue() {
        // push_try_pop_many_mpmc
        enum LR {
            Left(i64),
            Right(i64),
        }

        let q: Queue<LR> = Queue::new();
        let handle = &mut Queue::handle();
        assert!(q.is_empty(handle));

        thread::scope(|scope| {
            for _t in 0..THREADS / 3 {
                scope.spawn(|_| {
                    let handle = &mut Queue::handle();
                    for i in CONC_COUNT - 1..CONC_COUNT {
                        q.push(LR::Left(i), handle)
                    }
                });
                scope.spawn(|_| {
                    let handle = &mut Queue::handle();
                    for i in CONC_COUNT - 1..CONC_COUNT {
                        q.push(LR::Right(i), handle)
                    }
                });
                scope.spawn(|_| {
                    let mut vl = vec![];
                    let mut vr = vec![];
                    for _i in 0..CONC_COUNT {
                        let handle = &mut Queue::handle();
                        match q.try_pop(handle) {
                            Some(LR::Left(x)) => vl.push(x),
                            Some(LR::Right(x)) => vr.push(x),
                            _ => {}
                        }
                    }

                    let mut vl2 = vl.clone();
                    let mut vr2 = vr.clone();
                    vl2.sort();
                    vr2.sort();

                    assert_eq!(vl, vl2);
                    assert_eq!(vr, vr2);
                });
            }
        })
        .unwrap();
    }
}
