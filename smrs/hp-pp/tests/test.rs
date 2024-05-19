use core::sync::atomic::{AtomicPtr, Ordering::*};
use std::thread::sleep;
use std::time::Duration;

use hp_pp::*;
use queue::Queue;
use stack::Stack;
use std::thread::scope;

#[test]
fn counter() {
    const THREADS: usize = 4;
    const ITER: usize = 1024 * 16;

    let count = AtomicPtr::new(Box::leak(Box::new(0usize)));
    scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                for _ in 0..ITER {
                    let mut new = Box::new(0);
                    let mut hp = HazardPointer::default();
                    loop {
                        let cur_ptr = hp.protect(&count);
                        let value = unsafe { *cur_ptr };
                        *new = value + 1;
                        let new_ptr = Box::leak(new);
                        if count
                            .compare_exchange(cur_ptr, new_ptr, AcqRel, Acquire)
                            .is_ok()
                        {
                            unsafe { retire(cur_ptr) };
                            break;
                        } else {
                            new = unsafe { Box::from_raw(new_ptr) };
                        }
                    }
                }
            });
        }
    });
    let cur = count.load(Acquire);
    // exclusive access
    assert_eq!(unsafe { *cur }, THREADS * ITER);
    unsafe { retire(cur) };
}

// like `counter`, but trigger interesting interleaving using `sleep` and always call
// `do_reclamation`.
#[test]
fn counter_sleep() {
    const THREADS: usize = 4;
    const ITER: usize = 1024 * 16;

    let count = AtomicPtr::new(Box::leak(Box::new(0usize)));
    scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                for _ in 0..ITER {
                    let mut new = Box::new(0);
                    let mut hp = HazardPointer::default();
                    loop {
                        let cur_ptr = {
                            let mut cur = count.load(Relaxed);
                            loop {
                                match hp.try_protect(cur, &count) {
                                    Ok(_) => break cur,
                                    Err(new) => {
                                        sleep(Duration::from_micros(1));
                                        cur = new;
                                    }
                                }
                            }
                        };
                        sleep(Duration::from_micros(1));
                        let value = unsafe { *cur_ptr };
                        *new = value + 1;
                        let new_ptr = Box::leak(new);
                        if count
                            .compare_exchange(cur_ptr, new_ptr, AcqRel, Acquire)
                            .is_ok()
                        {
                            unsafe { retire(cur_ptr) };
                            do_reclamation();
                            break;
                        } else {
                            new = unsafe { Box::from_raw(new_ptr) };
                        }
                    }
                }
            });
        }
    });
    let cur = count.load(Acquire);
    // exclusive access
    assert_eq!(unsafe { *cur }, THREADS * ITER);
    unsafe { retire(cur) };
}

#[test]
fn stack() {
    const THREADS: usize = 8;
    const ITER: usize = 1024 * 16;

    let stack = Stack::default();
    scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                for i in 0..ITER {
                    stack.push(i);
                    assert!(stack.try_pop().is_some());
                    do_reclamation();
                }
            });
        }
    });
    assert!(stack.try_pop().is_none());
}

#[test]
fn queue() {
    const THREADS: usize = 8;
    const ITER: usize = 1024 * 32;

    let queue = Queue::default();
    scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                for i in 0..ITER {
                    queue.push(i);
                    assert!(queue.try_pop().is_some());
                    do_reclamation();
                }
            });
        }
    });
}

#[test]
fn stack_queue() {
    const THREADS: usize = 8;
    const ITER: usize = 1024 * 16;

    let stack = Stack::default();
    let queue = Queue::default();
    scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                for i in 0..ITER {
                    stack.push(i);
                    queue.push(i);
                    stack.try_pop();
                    queue.try_pop();
                    do_reclamation();
                }
            });
        }
    });
    assert!(stack.try_pop().is_none());
}

mod stack {
    use core::mem::ManuallyDrop;
    use core::ptr;
    use core::sync::atomic::{AtomicPtr, Ordering::*};

    use hp_pp::*;

    /// Treiber's lock-free stack.
    #[derive(Debug)]
    pub struct Stack<T> {
        head: AtomicPtr<Node<T>>,
    }

    #[derive(Debug)]
    struct Node<T> {
        data: ManuallyDrop<T>,
        next: *mut Node<T>,
    }

    unsafe impl<T: Send> Send for Node<T> {}
    unsafe impl<T: Sync> Sync for Node<T> {}

    impl<T> Default for Stack<T> {
        fn default() -> Self {
            Stack {
                head: AtomicPtr::new(ptr::null_mut()),
            }
        }
    }

    impl<T> Stack<T> {
        pub fn push(&self, t: T) {
            let new = Box::leak(Box::new(Node {
                data: ManuallyDrop::new(t),
                next: ptr::null_mut(),
            }));

            loop {
                let head = self.head.load(Relaxed);
                new.next = head;

                if self
                    .head
                    .compare_exchange(head, new, Release, Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }

        pub fn try_pop(&self) -> Option<T> {
            let mut hp = HazardPointer::default();
            loop {
                let head_ptr = hp.protect(&self.head);
                let head_ref = unsafe { head_ptr.as_ref() }?;

                if self
                    .head
                    .compare_exchange(head_ptr, head_ref.next, Relaxed, Relaxed)
                    .is_ok()
                {
                    let data = unsafe { ManuallyDrop::take(&mut (*head_ptr).data) };
                    unsafe { retire(head_ptr) };
                    return Some(data);
                }
            }
        }
    }

    impl<T> Drop for Stack<T> {
        fn drop(&mut self) {
            let mut curr = *self.head.get_mut();
            while !curr.is_null() {
                let curr_ref = unsafe { Box::from_raw(curr) };
                drop(ManuallyDrop::into_inner(curr_ref.data));
                curr = curr_ref.next;
            }
        }
    }
}

mod queue {
    use core::mem::MaybeUninit;
    use core::ptr;
    use core::sync::atomic::{AtomicPtr, Ordering::*};

    use hp_pp::*;

    /// Michael-Scott queue.
    #[derive(Debug)]
    pub struct Queue<T> {
        head: AtomicPtr<Node<T>>,
        tail: AtomicPtr<Node<T>>,
    }

    #[derive(Debug)]
    struct Node<T> {
        data: MaybeUninit<T>,
        next: AtomicPtr<Node<T>>,
    }

    unsafe impl<T: Send> Sync for Queue<T> {}
    unsafe impl<T: Send> Send for Queue<T> {}

    impl<T> Default for Queue<T> {
        fn default() -> Self {
            let q = Self {
                head: AtomicPtr::new(ptr::null_mut()),
                tail: AtomicPtr::new(ptr::null_mut()),
            };
            let sentinel = Box::leak(Box::new(Node {
                data: MaybeUninit::uninit(),
                next: AtomicPtr::new(ptr::null_mut()),
            }));
            q.head.store(sentinel, Relaxed);
            q.tail.store(sentinel, Relaxed);
            q
        }
    }

    impl<T> Queue<T> {
        pub fn push(&self, t: T) {
            let new = Box::leak(Box::new(Node {
                data: MaybeUninit::new(t),
                next: AtomicPtr::new(ptr::null_mut()),
            }));
            let mut hp = HazardPointer::default();

            loop {
                // We push onto the tail, so we'll start optimistically by looking there first.
                let tail = hp.protect(&self.tail);
                // SAFETY
                // 1. queue's `tail` is always valid as it will be CASed with valid nodes only.
                // 2. `tail` is protected & validated.
                let tail_ref = unsafe { tail.as_ref().unwrap() };

                let next = tail_ref.next.load(Acquire);
                if !next.is_null() {
                    let _ = self.tail.compare_exchange(tail, next, Release, Relaxed);
                    continue;
                }

                if tail_ref
                    .next
                    .compare_exchange(ptr::null_mut(), new, Release, Relaxed)
                    .is_ok()
                {
                    let _ = self.tail.compare_exchange(tail, new, Release, Relaxed);
                    break;
                }
            }
        }

        /// Attempts to dequeue from the front.
        ///
        /// Returns `None` if the queue is empty.
        pub fn try_pop(&self) -> Option<T> {
            let mut head_hp = HazardPointer::default();
            let mut next_hp = HazardPointer::default();
            let mut head = self.head.load(Acquire);
            loop {
                if let Err(new) = head_hp.try_protect(head, &self.head) {
                    head = new;
                    continue;
                }
                // SAFETY:
                // 1. queue's `head` is always valid as it will be CASed with valid nodes only.
                // 2. `head` is protected & validated.
                let head_ref = unsafe { &*head };

                let next = head_ref.next.load(Acquire);
                if next.is_null() {
                    return None;
                }
                next_hp.protect_raw(next);
                let next_ref = match HazardPointer::validate(head, &self.head) {
                    Ok(_) => {
                        // SAFETY:
                        // 1. If `next` was not null, then it must be a valid node that another
                        //    thread has `push()`ed.
                        // 2. Validation: If `head` is not retired, then `next` is not retired. So
                        //    re-validating `head` also validates `next.
                        unsafe { &*next }
                    }
                    Err(new) => {
                        next_hp.reset_protection();
                        head = new;
                        continue;
                    }
                };

                // Moves `tail` if it's stale. Relaxed load is enough because if tail == head, then
                // the messages for that node are already acquired.
                let tail = self.tail.load(Relaxed);
                if tail == head {
                    let _ = self.tail.compare_exchange(tail, next, Release, Relaxed);
                }

                if self
                    .head
                    .compare_exchange(head, next, Release, Relaxed)
                    .is_ok()
                {
                    let result = unsafe { next_ref.data.assume_init_read() };
                    unsafe { retire(head) };
                    return Some(result);
                }
            }
        }
    }

    impl<T> Drop for Queue<T> {
        fn drop(&mut self) {
            let sentinel = unsafe { Box::from_raw(*self.head.get_mut()) };
            let mut curr = sentinel.next.into_inner();
            while !curr.is_null() {
                let curr_ref = unsafe { Box::from_raw(curr) };
                drop(unsafe { curr_ref.data.assume_init() });
                curr = curr_ref.next.load(Relaxed);
            }
        }
    }
}
