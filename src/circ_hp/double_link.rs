use std::sync::atomic::Ordering;

use circ::{AtomicRc, AtomicWeak, CsHP, Pointer, Rc, Snapshot, StrongPtr};
use crossbeam_utils::CachePadded;

pub struct Holder<T> {
    pri: Snapshot<Node<T>, CsHP>,
    sub: Snapshot<Node<T>, CsHP>,
    new: Snapshot<Node<T>, CsHP>,
}

impl<T> Holder<T> {
    pub fn new() -> Self {
        Self {
            pri: Snapshot::new(),
            sub: Snapshot::new(),
            new: Snapshot::new(),
        }
    }
}

struct Node<T> {
    item: Option<T>,
    prev: AtomicWeak<Node<T>, CsHP>,
    next: AtomicRc<Node<T>, CsHP>,
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
    head: CachePadded<AtomicRc<Node<T>, CsHP>>,
    tail: CachePadded<AtomicRc<Node<T>, CsHP>>,
}

impl<T: Sync + Send> DoubleLink<T> {
    pub fn new() -> Self {
        let sentinel = Rc::new(Node::sentinel());
        // Note: In RC-based SMRs(CDRC, CIRC, ...), `sentinel.prev` MUST NOT be set to the self.
        // It will make a loop after the first enqueue, blocking the entire reclamation.
        Self {
            head: CachePadded::new(AtomicRc::from(sentinel.clone())),
            tail: CachePadded::new(AtomicRc::from(sentinel)),
        }
    }

    pub fn enqueue(&self, item: T, holder: &mut Holder<T>, cs: &CsHP) {
        let new = &mut holder.new;
        let ltail = &mut holder.pri;
        let lprev = &mut holder.sub;

        let mut node = Rc::new(Node::new(item));
        new.protect(&node, cs);

        loop {
            ltail.load(&self.tail, cs);
            unsafe { node.deref() }
                .prev
                .store(&*ltail, Ordering::Relaxed);

            // Try to help the previous enqueue to complete.
            lprev.load_from_weak(unsafe { &ltail.deref().prev }, cs);
            if let Some(lprev) = lprev.as_ref() {
                if lprev.next.load(Ordering::SeqCst).is_null() {
                    lprev.next.store(&*ltail, Ordering::Relaxed, cs);
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
                        .store(&*new, Ordering::Release, cs);
                    return;
                }
                Err(e) => node = e.desired(),
            }
        }
    }

    pub fn dequeue<'h>(&self, holder: &'h mut Holder<T>, cs: &CsHP) -> Option<&'h T> {
        let lhead = &mut holder.pri;
        let lnext = &mut holder.sub;

        loop {
            lhead.load(&self.head, cs);
            lnext.load(unsafe { &lhead.deref().next }, cs);
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
                return Some(unsafe { lnext.deref().item.as_ref().unwrap() });
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::{DoubleLink, Holder};
    use circ::{Cs, CsHP};
    use crossbeam_utils::thread::scope;

    #[test]
    fn simple() {
        let queue = DoubleLink::new();
        let holder = &mut Holder::new();
        let cs = &CsHP::new();
        assert!(queue.dequeue(holder, cs).is_none());
        queue.enqueue(1, holder, cs);
        queue.enqueue(2, holder, cs);
        queue.enqueue(3, holder, cs);
        assert_eq!(*queue.dequeue(holder, cs).unwrap(), 1);
        assert_eq!(*queue.dequeue(holder, cs).unwrap(), 2);
        assert_eq!(*queue.dequeue(holder, cs).unwrap(), 3);
        assert!(queue.dequeue(holder, cs).is_none());
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
                    let holder = &mut Holder::new();
                    for i in 0..ELEMENTS_PER_THREAD {
                        queue.enqueue(
                            (t * ELEMENTS_PER_THREAD + i).to_string(),
                            holder,
                            &CsHP::new(),
                        );
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
                    let holder = &mut Holder::new();
                    for _ in 0..ELEMENTS_PER_THREAD {
                        let cs = CsHP::new();
                        let res = queue.dequeue(holder, &cs).unwrap();
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
