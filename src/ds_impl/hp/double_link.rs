use std::{
    ptr::null_mut,
    sync::atomic::{AtomicPtr, Ordering},
};

use crossbeam_utils::CachePadded;
use hp_pp::{light_membarrier, HazardPointer, Thread, DEFAULT_DOMAIN};

struct Node<T> {
    item: Option<T>,
    prev: *mut Node<T>,
    next: CachePadded<AtomicPtr<Node<T>>>,
}

impl<T> Node<T> {
    fn sentinel() -> Self {
        Self {
            item: None,
            prev: null_mut(),
            next: CachePadded::new(AtomicPtr::new(null_mut())),
        }
    }

    fn new(item: T) -> Self {
        Self {
            item: Some(item),
            prev: null_mut(),
            next: CachePadded::new(AtomicPtr::new(null_mut())),
        }
    }
}

unsafe impl<T: Sync> Sync for Node<T> {}
unsafe impl<T: Sync> Send for Node<T> {}

pub struct DoubleLink<T: Sync + Send> {
    head: CachePadded<AtomicPtr<Node<T>>>,
    tail: CachePadded<AtomicPtr<Node<T>>>,
}

pub struct Handle<'domain> {
    pri: HazardPointer<'domain>,
    sub: HazardPointer<'domain>,
    thread: Thread<'domain>,
}

impl Default for Handle<'static> {
    fn default() -> Self {
        Self {
            pri: HazardPointer::default(),
            sub: HazardPointer::default(),
            thread: Thread::new(&DEFAULT_DOMAIN),
        }
    }
}

impl<T: Sync + Send> Default for DoubleLink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Sync + Send> DoubleLink<T> {
    #[inline]
    pub fn new() -> Self {
        let sentinel = Box::into_raw(Box::new(Node::sentinel()));
        unsafe { (*sentinel).prev = sentinel };
        Self {
            head: CachePadded::new(AtomicPtr::new(sentinel)),
            tail: CachePadded::new(AtomicPtr::new(sentinel)),
        }
    }

    #[inline]
    pub fn enqueue(&self, item: T, handle: &mut Handle) {
        let node = Box::into_raw(Box::new(Node::new(item)));
        let node_mut = unsafe { &mut *node };
        loop {
            let ltail = protect_link(&self.tail, &mut handle.pri);
            let lprev = unsafe { &*ltail }.prev;
            // The author's implementation remove this second protection by using customized HP.
            handle.sub.protect_raw(lprev);
            light_membarrier();
            if self.tail.load(Ordering::Acquire) != ltail {
                continue;
            }

            let lprev = unsafe { &*lprev };
            node_mut.prev = ltail;
            // Try to help the previous enqueue to complete.
            if lprev.next.load(Ordering::SeqCst).is_null() {
                lprev.next.store(ltail, Ordering::Relaxed);
            }
            if self
                .tail
                .compare_exchange(ltail, node, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                unsafe { &*ltail }.next.store(node, Ordering::Release);
                handle.pri.reset_protection();
                handle.sub.reset_protection();
                return;
            }
        }
    }

    #[inline]
    pub fn dequeue<'h>(&self, handle: &'h mut Handle) -> Option<&'h T> {
        loop {
            let lhead = protect_link(&self.head, &mut handle.pri);
            let lnext = unsafe { &*lhead }.next.load(Ordering::Acquire);
            // Check if this queue is empty.
            if lnext.is_null() {
                handle.pri.reset_protection();
                return None;
            }
            // The author's implementation remove this second protection by using customized HP.
            handle.sub.protect_raw(lnext);
            light_membarrier();
            if self.head.load(Ordering::Acquire) != lhead {
                continue;
            }

            if self
                .head
                .compare_exchange(lhead, lnext, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let item = unsafe { (*lnext).item.as_ref().unwrap() };
                unsafe { handle.thread.retire(lhead) };
                handle.pri.reset_protection();
                return Some(item);
            }
        }
    }
}

impl<T: Sync + Send> Drop for DoubleLink<T> {
    fn drop(&mut self) {
        let handle = &mut Handle::default();
        while self.dequeue(handle).is_some() {}
        unsafe { drop(Box::from_raw(self.head.load(Ordering::Relaxed))) };
    }
}

fn protect_link<T>(link: &AtomicPtr<Node<T>>, hazptr: &mut HazardPointer<'_>) -> *mut Node<T> {
    let mut ptr = link.load(Ordering::Relaxed);
    loop {
        hazptr.protect_raw(ptr);
        light_membarrier();
        let new_ptr = link.load(Ordering::Acquire);
        if ptr == new_ptr {
            return ptr;
        }
        ptr = new_ptr;
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::{DoubleLink, Handle};
    use crossbeam_utils::thread::scope;

    #[test]
    fn simple() {
        let queue = DoubleLink::new();
        let handle = &mut Handle::default();
        assert!(queue.dequeue(handle).is_none());
        queue.enqueue(1, handle);
        queue.enqueue(2, handle);
        queue.enqueue(3, handle);
        assert_eq!(*queue.dequeue(handle).unwrap(), 1);
        assert_eq!(*queue.dequeue(handle).unwrap(), 2);
        assert_eq!(*queue.dequeue(handle).unwrap(), 3);
        assert!(queue.dequeue(handle).is_none());
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
                    let mut handle = Handle::default();
                    for i in 0..ELEMENTS_PER_THREAD {
                        queue.enqueue((t * ELEMENTS_PER_THREAD + i).to_string(), &mut handle);
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
                    let mut handle = Handle::default();
                    for _ in 0..ELEMENTS_PER_THREAD {
                        let res = queue.dequeue(&mut handle).unwrap();
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
