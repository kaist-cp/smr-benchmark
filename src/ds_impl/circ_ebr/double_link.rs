use std::sync::atomic::Ordering;

use circ::{AtomicRc, CsEBR, GraphNode, Pointer, Rc, Snapshot, StrongPtr, Weak};
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
    prev: Weak<Node<T>, CsEBR>,
    next: CachePadded<AtomicRc<Node<T>, CsEBR>>,
}

impl<T> GraphNode<CsEBR> for Node<T> {
    const UNIQUE_OUTDEGREE: bool = true;

    #[inline]
    fn pop_outgoings(&self, result: &mut Vec<Rc<Self, CsEBR>>)
    where
        Self: Sized,
    {
        result.push(self.next.swap(Rc::null(), Ordering::Relaxed));
    }

    #[inline]
    fn pop_unique(&self) -> Rc<Self, CsEBR>
    where
        Self: Sized,
    {
        self.next.swap(Rc::null(), Ordering::Relaxed)
    }
}

impl<T> Node<T> {
    fn sentinel() -> Self {
        Self {
            item: None,
            prev: Weak::null(),
            next: CachePadded::new(AtomicRc::null()),
        }
    }

    fn new(item: T) -> Self {
        Self {
            item: Some(item),
            prev: Weak::null(),
            next: CachePadded::new(AtomicRc::null()),
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
    #[inline]
    pub fn new() -> Self {
        let sentinel = Rc::new(Node::sentinel());
        // Note: In RC-based SMRs(CDRC, CIRC, ...), `sentinel.prev` MUST NOT be set to the self.
        // It will make a loop after the first enqueue, blocking the entire reclamation.
        Self {
            head: CachePadded::new(AtomicRc::from(sentinel.clone())),
            tail: CachePadded::new(AtomicRc::from(sentinel)),
        }
    }

    #[inline]
    pub fn enqueue(&self, item: T, cs: &CsEBR) {
        let [mut node, sub] = Rc::new_many(Node::new(item));

        loop {
            let ltail = self.tail.load_ss(cs);
            unsafe { node.deref_mut() }.prev = ltail.upgrade().downgrade();

            // Try to help the previous enqueue to complete.
            let mut lprev = Snapshot::new();
            lprev.protect_weak(unsafe { &ltail.deref().prev }, cs);
            if let Some(lprev) = lprev.as_ref() {
                if lprev.next.load(Ordering::SeqCst).is_null() {
                    lprev.next.store(ltail.upgrade(), Ordering::Relaxed, cs);
                }
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
                        .store(sub, Ordering::Release, cs);
                    return;
                }
                Err(e) => node = e.desired,
            }
        }
    }

    #[inline]
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
                    lnext.upgrade(),
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
