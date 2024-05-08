use std::sync::atomic::Ordering;

use super::pointers::{Atomic, Shared};
use crossbeam_utils::CachePadded;

struct Node<T> {
    item: Option<T>,
    prev: Atomic<Node<T>>,
    next: CachePadded<Atomic<Node<T>>>,
}

impl<T> Node<T> {
    fn sentinel() -> Self {
        Self {
            item: None,
            prev: Atomic::null(),
            next: CachePadded::new(Atomic::null()),
        }
    }

    fn new(item: T) -> Self {
        Self {
            item: Some(item),
            prev: Atomic::null(),
            next: CachePadded::new(Atomic::null()),
        }
    }
}

unsafe impl<T: Sync> Sync for Node<T> {}
unsafe impl<T: Sync> Send for Node<T> {}

pub struct DoubleLink<T: Sync + Send> {
    head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
}

impl<T: Sync + Send> Default for DoubleLink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Sync + Send> DoubleLink<T> {
    #[inline]
    pub fn new() -> Self {
        let sentinel = Shared::from_owned(Node::sentinel());
        unsafe { sentinel.deref().prev.store(sentinel, Ordering::Relaxed) };
        Self {
            head: CachePadded::new(Atomic::from(sentinel)),
            tail: CachePadded::new(Atomic::from(sentinel)),
        }
    }

    #[inline]
    pub fn enqueue(&self, item: T) {
        let node = Shared::from_owned(Node::new(item));
        loop {
            let ltail = self.tail.load(Ordering::Acquire);
            let lprev = unsafe { ltail.deref().prev.load(Ordering::Relaxed).deref() };
            unsafe { node.deref() }.prev.store(ltail, Ordering::Relaxed);
            // Try to help the previous enqueue to complete.
            if lprev.next.load(Ordering::SeqCst).is_null() {
                lprev.next.store(ltail, Ordering::Relaxed);
            }
            if self
                .tail
                .compare_exchange(ltail, node, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                unsafe { ltail.deref() }.next.store(node, Ordering::Release);
                return;
            }
        }
    }

    #[inline]
    pub fn dequeue(&self) -> Option<&'static T> {
        loop {
            let lhead = self.head.load(Ordering::Acquire);
            let lnext = unsafe { lhead.deref().next.load(Ordering::Acquire) };
            // Check if this queue is empty.
            if lnext.is_null() {
                return None;
            }

            if self
                .head
                .compare_exchange(lhead, lnext, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let item = unsafe { lnext.deref().item.as_ref().unwrap() };
                return Some(item);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::DoubleLink;
    use crossbeam_utils::thread::scope;

    #[test]
    fn simple() {
        let queue = DoubleLink::new();
        assert!(queue.dequeue().is_none());
        queue.enqueue(1);
        queue.enqueue(2);
        queue.enqueue(3);
        assert_eq!(*queue.dequeue().unwrap(), 1);
        assert_eq!(*queue.dequeue().unwrap(), 2);
        assert_eq!(*queue.dequeue().unwrap(), 3);
        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn smoke() {
        const THREADS: usize = 100;
        const ELEMENTS_PER_THREAD: usize = 10000;

        let queue = DoubleLink::new();
        let mut found = Vec::new();
        found.resize_with(THREADS * ELEMENTS_PER_THREAD, || AtomicU32::new(0));

        scope(|s| {
            for t in 0..THREADS {
                let queue = &queue;
                s.spawn(move |_| {
                    for i in 0..ELEMENTS_PER_THREAD {
                        queue.enqueue((t * ELEMENTS_PER_THREAD + i).to_string());
                    }
                });
            }
        })
        .unwrap();

        scope(|s| {
            for _ in 0..THREADS {
                let queue = &queue;
                let found = &found;
                s.spawn(move |_| {
                    for _ in 0..ELEMENTS_PER_THREAD {
                        let res = queue.dequeue().unwrap();
                        assert_eq!(
                            found[res.parse::<usize>().unwrap()].fetch_add(1, Ordering::Relaxed),
                            0
                        );
                    }
                });
            }
        })
        .unwrap();

        assert!(
            found
                .iter()
                .filter(|v| v.load(Ordering::Relaxed) == 0)
                .count()
                == 0
        );
    }
}
