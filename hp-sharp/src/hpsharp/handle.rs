use core::cell::RefCell;
use core::ptr;
use core::sync::atomic::{fence, AtomicPtr, Ordering};
use std::mem::take;

use super::global::Global;
use super::hazard::ThreadRecord;
use crate::sync::Deferred;
use crate::{crcu, GLOBAL_GARBAGE_COUNT};

pub struct Handle {
    pub(crate) domain: *const Global,
    pub(crate) crcu_handle: RefCell<crcu::Handle>,
    pub(crate) hazards: *const ThreadRecord,
    /// Available slots of hazard array
    available_indices: Vec<usize>,
    local_deferred: Vec<Deferred>,
}

impl Handle {
    pub(crate) fn new(domain: &Global) -> Self {
        let (thread, available_indices) = domain.threads.acquire();
        let crcu_handle = RefCell::new(domain.crcu.register());
        Self {
            domain,
            crcu_handle,
            hazards: thread,
            available_indices,
            local_deferred: Vec::with_capacity(128),
        }
    }
}

impl Handle {
    #[inline]
    fn domain(&self) -> &Global {
        unsafe { &*self.domain }
    }

    #[inline]
    pub(crate) unsafe fn retire_inner(&mut self, mut deferred: Vec<Deferred>) {
        deferred.append(&mut self.local_deferred);
        let mut not_freed = self.do_reclamation(deferred);
        self.local_deferred.append(&mut not_freed);
    }

    pub(crate) fn do_reclamation(&mut self, deferred: Vec<Deferred>) -> Vec<Deferred> {
        let deferred_len = deferred.len();
        if deferred.is_empty() {
            return vec![];
        }

        fence(Ordering::SeqCst);

        let guarded_ptrs = unsafe { &*self.domain }.collect_guarded_ptrs(self);
        let not_freed: Vec<Deferred> = deferred
            .into_iter()
            .filter_map(|element| {
                if guarded_ptrs.contains(&element.data()) {
                    Some(element)
                } else {
                    unsafe { element.execute() };
                    None
                }
            })
            .collect();
        GLOBAL_GARBAGE_COUNT.fetch_sub(deferred_len - not_freed.len(), Ordering::AcqRel);
        not_freed
    }

    pub(crate) fn acquire(&mut self) -> usize {
        if let Some(idx) = self.available_indices.pop() {
            idx
        } else {
            self.grow_array();
            self.acquire()
        }
    }

    fn grow_array(&mut self) {
        let array_ptr = unsafe { (*self.hazards).hazptrs.load(Ordering::Relaxed) };
        let array = unsafe { &*array_ptr };
        let size = array.len();
        let new_size = size * 2;
        let mut new_array = Box::new(Vec::with_capacity(new_size));
        for i in 0..size {
            new_array.push(AtomicPtr::new(array[i].load(Ordering::Relaxed)));
        }
        for _ in size..new_size {
            new_array.push(AtomicPtr::new(ptr::null_mut()));
        }
        unsafe { &*self.hazards }
            .hazptrs
            .store(Box::into_raw(new_array), Ordering::Release);
        self.local_deferred
            .push(Deferred::new(array_ptr as _, free::<Vec<AtomicPtr<u8>>>));
        self.available_indices.extend(size..new_size)
    }

    /// release hazard slot
    pub(crate) fn release(&mut self, idx: usize) {
        self.available_indices.push(idx);
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        let mut global_deferred = self.domain().deferred.pop_all();
        if !self.local_deferred.is_empty() || !global_deferred.is_empty() {
            let mut deferred = take(&mut self.local_deferred);
            deferred.append(&mut global_deferred);
            let not_freed = self.do_reclamation(deferred);
            self.domain().deferred.append(not_freed.into_iter());
        }
        self.available_indices.clear();
        unsafe { (*self.domain).threads.release(&*self.hazards) };
    }
}

pub(crate) unsafe fn free<T>(ptr: *mut u8) {
    let ptr = ptr as *mut T;
    drop(Box::from_raw(ptr));
}
