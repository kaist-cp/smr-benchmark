use std::sync::atomic::Ordering;

use cdrc_rs::{AtomicRc, AtomicWeak, Cs, Pointer, Rc, Snapshot, StrongPtr};
use crossbeam_utils::CachePadded;

pub struct Holder<T, C: Cs> {
    pri: Snapshot<Node<T, C>, C>,
    sub: Snapshot<Node<T, C>, C>,
    new: Snapshot<Node<T, C>, C>,
}

impl<T, C: Cs> Default for Holder<T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, C: Cs> Holder<T, C> {
    pub fn new() -> Self {
        Self {
            pri: Snapshot::new(),
            sub: Snapshot::new(),
            new: Snapshot::new(),
        }
    }
}

struct Node<T, C: Cs> {
    item: Option<T>,
    prev: AtomicWeak<Node<T, C>, C>,
    next: CachePadded<AtomicRc<Node<T, C>, C>>,
}

impl<T, C: Cs> Node<T, C> {
    fn sentinel() -> Self {
        Self {
            item: None,
            prev: AtomicWeak::null(),
            next: CachePadded::new(AtomicRc::null()),
        }
    }

    fn new(item: T) -> Self {
        Self {
            item: Some(item),
            prev: AtomicWeak::null(),
            next: CachePadded::new(AtomicRc::null()),
        }
    }
}

unsafe impl<T: Sync, C: Cs> Sync for Node<T, C> {}
unsafe impl<T: Sync, C: Cs> Send for Node<T, C> {}

pub struct DoubleLink<T: Sync + Send, C: Cs> {
    head: CachePadded<AtomicRc<Node<T, C>, C>>,
    tail: CachePadded<AtomicRc<Node<T, C>, C>>,
}

impl<T: Sync + Send, C: Cs> Default for DoubleLink<T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Sync + Send, C: Cs> DoubleLink<T, C> {
    #[inline]
    pub fn new() -> Self {
        let cs = &unsafe { Cs::unprotected() };
        let sentinel = Rc::new(Node::sentinel());
        // Note: In RC-based SMRs(CDRC, CIRC, ...), `sentinel.prev` MUST NOT be set to the self.
        // It will make a loop by after the first enqueue, blocking the entire reclamation.
        Self {
            head: CachePadded::new(AtomicRc::from(sentinel.clone(cs))),
            tail: CachePadded::new(AtomicRc::from(sentinel)),
        }
    }

    #[inline]
    pub fn enqueue(&self, item: T, holder: &mut Holder<T, C>, cs: &C) {
        let new = &mut holder.new;
        let ltail = &mut holder.pri;
        let lprev = &mut holder.sub;

        let mut node = Rc::new(Node::new(item));
        new.protect(&node, cs);

        loop {
            ltail.load(&self.tail, cs);
            unsafe { node.deref() }
                .prev
                .store(&*ltail, Ordering::Relaxed, cs);

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
                Err(e) => node = e.desired,
            }
        }
    }

    #[inline]
    pub fn dequeue<'h>(&self, holder: &'h mut Holder<T, C>, cs: &C) -> Option<&'h T> {
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
                    &*lnext,
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
    use cdrc_rs::{Cs, CsEBR, CsHP};
    use crossbeam_utils::thread::scope;

    fn simple<C: Cs>() {
        let queue = DoubleLink::new();
        let holder = &mut Holder::new();
        let cs = &C::new();
        assert!(queue.dequeue(holder, cs).is_none());
        queue.enqueue(1, holder, cs);
        queue.enqueue(2, holder, cs);
        queue.enqueue(3, holder, cs);
        assert_eq!(*queue.dequeue(holder, cs).unwrap(), 1);
        assert_eq!(*queue.dequeue(holder, cs).unwrap(), 2);
        assert_eq!(*queue.dequeue(holder, cs).unwrap(), 3);
        assert!(queue.dequeue(holder, cs).is_none());
    }

    fn smoke<C: Cs>() {
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
                        queue.enqueue((t * ELEMENTS_PER_THREAD + i).to_string(), holder, &C::new());
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
                        let cs = C::new();
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

    #[test]
    fn simple_all() {
        simple::<CsEBR>();
        simple::<CsHP>();
    }

    #[test]
    fn smoke_all() {
        smoke::<CsEBR>();
        smoke::<CsHP>();
    }
}
