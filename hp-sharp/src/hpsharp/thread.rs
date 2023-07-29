use std::{ptr, sync::atomic::AtomicPtr};

use atomic::{fence, Ordering};

use crate::{Deferred, Thread};

use super::GlobalHPSharp;

impl Thread {
    #[inline]
    pub(crate) unsafe fn retire_inner(&mut self, mut deferred: Vec<Deferred>) {
        if let Some(local) = self.local.as_mut() {
            deferred.append(&mut local.local_deferred);
            let mut not_freed = self.do_reclamation(deferred);
            local.local_deferred.append(&mut not_freed);
        } else {
            for def in deferred {
                unsafe { def.execute() };
            }
        }
    }

    pub(crate) fn do_reclamation(&mut self, deferred: Vec<Deferred>) -> Vec<Deferred> {
        let deferred_len = deferred.len();
        if deferred.is_empty() {
            return vec![];
        }

        fence(Ordering::SeqCst);

        let global = unsafe { &*(*self.local).global };
        let guarded_ptrs = global.collect_guarded_ptrs(self);
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
        global
            .garbage_count
            .fetch_sub(deferred_len - not_freed.len(), Ordering::AcqRel);
        not_freed
    }

    pub(crate) fn acquire(&mut self) -> usize {
        if let Some(idx) = unsafe { &mut (*self.local).available_indices }.pop() {
            idx
        } else {
            self.grow_array();
            self.acquire()
        }
    }

    fn grow_array(&mut self) {
        let array_ptr = unsafe { (*self.local).hazptrs.load(Ordering::Relaxed) };
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
        unsafe { &(*self.local).hazptrs }.store(Box::into_raw(new_array), Ordering::Release);
        unsafe { &*(*self.local).global }
            .garbage_count
            .fetch_add(1, Ordering::AcqRel);
        unsafe { &mut (*self.local).local_deferred }
            .push(Deferred::new(array_ptr as _, free::<Vec<AtomicPtr<u8>>>));
        unsafe { &mut (*self.local).available_indices }.extend(size..new_size)
    }

    /// release hazard slot
    pub(crate) fn release(&mut self, idx: usize) {
        unsafe { &mut (*self.local).available_indices }.push(idx);
    }
}

pub(crate) unsafe fn free<T>(ptr: *mut u8) {
    let ptr = ptr as *mut T;
    drop(Box::from_raw(ptr));
}
