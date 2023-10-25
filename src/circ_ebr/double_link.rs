use std::sync::atomic::Ordering;

use circ::{AtomicRc, AtomicWeak, CsEBR, Pointer, Rc, Snapshot, StrongPtr, Weak};
use crossbeam_utils::CachePadded;

pub struct Output<T> {
    found: Snapshot<Node<T>, CsEBR>,
}

impl<T> Output<T> {
    pub fn output(&self) -> &T {
        self.found
            .as_ref()
            .map(|node| node.item.as_ref().unwrap())
            .unwrap()
    }
}

struct Node<T> {
    item: Option<T>,
    prev: AtomicWeak<Node<T>, CsEBR>,
    next: AtomicRc<Node<T>, CsEBR>,
}

impl<T> Node<T> {
    fn sentinel() -> Self {
        Self {
            item: None,
            prev: AtomicWeak::null(),
            next: AtomicRc::null(),
        }
    }

    fn new(item: T) -> Self {
        Self {
            item: Some(item),
            prev: AtomicWeak::null(),
            next: AtomicRc::null(),
        }
    }
}

unsafe impl<T: Sync> Sync for Node<T> {}
unsafe impl<T: Sync> Send for Node<T> {}

pub struct DoubleLink<T: Sync + Send> {
    head: CachePadded<AtomicRc<Node<T>, CsEBR>>,
    tail: CachePadded<AtomicRc<Node<T>, CsEBR>>,
}

impl<T: Sync + Send> DoubleLink<T> {
    pub fn new() -> Self {
        let sentinel = Rc::new(Node::sentinel());
        unsafe {
            sentinel
                .deref()
                .prev
                .store(Weak::from_strong(&sentinel), Ordering::Relaxed)
        };
        Self {
            head: CachePadded::new(AtomicRc::from(sentinel.clone())),
            tail: CachePadded::new(AtomicRc::from(sentinel)),
        }
    }

    pub fn enqueue(&self, item: T, cs: &CsEBR) {
        let mut node = Rc::new(Node::new(item));
        let mut node_ss = Snapshot::new();
        node_ss.protect(&node, cs);

        loop {
            let ltail = self.tail.load_ss(cs);
            let mut lprev = Snapshot::new();
            lprev.load_from_weak(unsafe { &ltail.deref().prev }, cs);
            if lprev.is_null() {
                continue;
            }
            unsafe { node.deref() }.prev.store(ltail, Ordering::Relaxed);
            // Try to help the previous enqueue to complete.
            let lprev = unsafe { lprev.deref() };
            if lprev.next.load(Ordering::SeqCst).is_null() {
                lprev.next.store(ltail, Ordering::Relaxed, cs);
            }
            match self.tail.compare_exchange(
                ltail.as_ptr(),
                node,
                Ordering::SeqCst,
                Ordering::SeqCst,
                cs,
            ) {
                Ok(_) => {
                    unsafe { ltail.deref() }
                        .next
                        .store(node_ss, Ordering::Release, cs);
                    return;
                }
                Err(e) => node = e.desired(),
            }
        }
    }

    pub fn dequeue(&self, cs: &CsEBR) -> Option<Output<T>> {
        loop {
            let lhead = self.head.load_ss(cs);
            let lnext = unsafe { lhead.deref().next.load_ss(cs) };
            // Check if this queue is empty.
            if lnext.is_null() {
                return None;
            }

            if self
                .head
                .compare_exchange(
                    lhead.as_ptr(),
                    lnext,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    cs,
                )
                .is_ok()
            {
                return Some(Output { found: lnext });
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::DoubleLink;
    use circ::{Cs, CsEBR};
    use crossbeam_utils::thread::scope;

    #[test]
    fn simple() {
        let queue = DoubleLink::new();
        let guard = &CsEBR::new();
        assert!(queue.dequeue(guard).is_none());
        queue.enqueue(1, guard);
        queue.enqueue(2, guard);
        queue.enqueue(3, guard);
        assert_eq!(*queue.dequeue(guard).unwrap().output(), 1);
        assert_eq!(*queue.dequeue(guard).unwrap().output(), 2);
        assert_eq!(*queue.dequeue(guard).unwrap().output(), 3);
        assert!(queue.dequeue(guard).is_none());
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
                        queue.enqueue((t * ELEMENTS_PER_THREAD + i).to_string(), &CsEBR::new());
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
                        let guard = CsEBR::new();
                        let output = queue.dequeue(&guard).unwrap();
                        let res = output.output();
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
