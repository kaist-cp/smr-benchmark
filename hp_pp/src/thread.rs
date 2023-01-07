use core::sync::atomic::{AtomicPtr, Ordering};
use core::{mem, ptr};

use crate::domain::Domain;
use crate::hazard::ThreadRecord;
use crate::retire::Retired;
use crate::HazardPointer;

pub struct Thread<'domain> {
    pub(crate) domain: &'domain Domain,
    pub(crate) hazards: &'domain ThreadRecord,
    /// available slots of hazard array
    pub(crate) available_indices: Vec<usize>,
    // Used for HP++
    pub(crate) hps: Vec<HazardPointer<'domain>>,
    pub(crate) retired: Vec<Retired>,
    pub(crate) collect_count: usize,
}

impl<'domain> Thread<'domain> {
    pub fn new(domain: &'domain Domain) -> Self {
        let (thread, available_indices) = domain.threads.acquire();
        Self {
            domain,
            hazards: thread,
            available_indices: available_indices,
            hps: Vec::new(),
            retired: Vec::new(),
            collect_count: 0,
        }
    }
}

// stuff related to reclamation
impl<'domain> Thread<'domain> {
    const COUNTS_BETWEEN_COLLECT: usize = 64;

    // NOTE: T: Send not required because we reclaim only locally.
    #[inline]
    pub unsafe fn retire<T>(&mut self, ptr: *mut T) {
        self.retired.push(Retired::new(ptr));

        let collect_count = self.collect_count.wrapping_add(1);
        self.collect_count = collect_count;
        if collect_count % Self::COUNTS_BETWEEN_COLLECT == 0 {
            self.do_reclamation();
        }
    }

    pub unsafe fn try_unlink<T, F1, F2>(
        &mut self,
        links: &[*mut T],
        to_be_unlinked: &[*mut T],
        do_unlink: F1,
        set_stop: F2,
    ) -> bool
    where
        F1: FnOnce() -> bool,
        F2: Fn(*mut T),
    {
        let mut hps: Vec<_> = links
            .iter()
            .map(|&ptr| {
                let mut hp = HazardPointer::new(self);
                hp.protect_raw(ptr);
                hp
            })
            .collect();

        let unlinked = do_unlink();
        if unlinked {
            for &ptr in to_be_unlinked {
                set_stop(ptr);
            }
            self.hps.append(&mut hps);
            for &ptr in to_be_unlinked {
                unsafe { self.retire(ptr) }
            }
        } else {
            drop(hps);
        }

        unlinked
    }

    #[inline]
    pub(crate) fn do_reclamation(&mut self) {
        membarrier::heavy();

        // only for hp++, but this doesn't introduce big cost for plain hp.
        drop(mem::take(&mut self.hps));

        let guarded_ptrs = self.domain.collect_guarded_ptrs(self);
        self.retired = self
            .retired
            .iter()
            .filter_map(|element| {
                if guarded_ptrs.contains(&element.ptr) {
                    Some(Retired {
                        ptr: element.ptr,
                        deleter: element.deleter,
                    })
                } else {
                    unsafe { (element.deleter)(element.ptr) };
                    None
                }
            })
            .collect();
    }
}

// stuff related to hazards
impl<'domain> Thread<'domain> {
    /// acquire hazard slot
    pub(crate) fn acquire(&mut self) -> usize {
        if let Some(idx) = self.available_indices.pop() {
            idx
        } else {
            self.grow_array();
            self.acquire()
        }
    }

    fn grow_array(&mut self) {
        let array_ptr = self.hazards.hazptrs.load(Ordering::Relaxed);
        let array = unsafe { &*array_ptr };
        let size = array.len();
        let new_size = size * 2;
        let mut new_array = Box::new(Vec::with_capacity(new_size));
        for i in 0..size {
            new_array.push(AtomicPtr::new(array[i].load(Ordering::Relaxed)));
        }
        unsafe { self.retire(array_ptr) };
        for _ in size..new_size {
            new_array.push(AtomicPtr::new(ptr::null_mut()));
        }
        self.hazards.hazptrs.store(Box::into_raw(new_array), Ordering::Release);
        self.available_indices.extend(size..new_size)
    }

    /// release hazard slot
    pub(crate) fn release(&mut self, idx: usize) {
        self.available_indices.push(idx);
    }
}

impl<'domain> Drop for Thread<'domain> {
    fn drop(&mut self) {
        self.domain.threads.release(self.hazards);
        while !self.retired.is_empty() {
            self.do_reclamation();
            core::hint::spin_loop();
        }
    }
}
