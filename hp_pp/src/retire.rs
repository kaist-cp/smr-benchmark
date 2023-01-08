use core::ptr;
use core::sync::atomic::{AtomicPtr, Ordering};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Retired {
    pub(crate) ptr: *mut u8,
    pub(crate) deleter: unsafe fn(ptr: *mut u8),
}

// TODO: require <T: Send> in retire
unsafe impl Send for Retired {}

impl Retired {
    pub(crate) fn new<T>(ptr: *mut T) -> Self {
        unsafe fn free<T>(ptr: *mut u8) {
            drop(Box::from_raw(ptr as *mut T))
        }
        Self {
            ptr: ptr as *mut u8,
            deleter: free::<T>,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RetiredList {
    head: AtomicPtr<RetiredListNode>,
}

#[derive(Debug)]
struct RetiredListNode {
    retireds: Vec<Retired>,
    next: *const RetiredListNode,
}

impl RetiredList {
    pub(crate) const fn new() -> Self {
        Self {
            head: AtomicPtr::new(core::ptr::null_mut()),
        }
    }

    pub(crate) fn push(&self, retireds: Vec<Retired>) {
        let new = Box::leak(Box::new(RetiredListNode {
            retireds,
            next: ptr::null_mut(),
        }));

        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            new.next = head;
            match self
                .head
                .compare_exchange(head, new, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(head_new) => head = head_new,
            }
        }
    }

    pub(crate) fn pop_all(&self) -> Vec<Retired> {
        let mut cur = self.head.swap(core::ptr::null_mut(), Ordering::Acquire);
        let mut retireds = Vec::new();
        while !cur.is_null() {
            let mut cur_box = unsafe { Box::from_raw(cur) };
            retireds.append(&mut cur_box.retireds);
            cur = cur_box.next.cast_mut();
        }
        retireds
    }
}
