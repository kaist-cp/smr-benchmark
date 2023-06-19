use core::ptr;
use core::sync::atomic::{AtomicPtr, Ordering};

use crate::crcu::LocalHandle;
use crate::domain::Domain;
use crate::hazard::ThreadRecord;

pub struct Thread {
    pub(crate) domain: *const Domain,
    pub(crate) hazards: *const ThreadRecord,
    pub(crate) rcu_handle: LocalHandle,
    /// available slots of hazard array
    pub(crate) available_indices: Vec<usize>,
}

impl Thread {
    pub fn new(domain: &Domain) -> Self {
        let (thread, available_indices) = domain.threads.acquire();
        let rcu_handle = domain.rcu.register();
        Self {
            domain,
            hazards: thread,
            rcu_handle,
            available_indices,
        }
    }
}

impl Thread {
    #[inline]
    pub unsafe fn retire<T>(&mut self, ptr: *mut T) {
        todo!()
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
        unsafe { self.retire(array_ptr) };
        self.available_indices.extend(size..new_size)
    }

    /// release hazard slot
    pub(crate) fn release(&mut self, idx: usize) {
        self.available_indices.push(idx);
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        self.available_indices.clear();
        unsafe { (*self.domain).threads.release(&*self.hazards) };
    }
}
