use core::sync::atomic::{AtomicPtr, Ordering};
use core::{mem, ptr};
use std::collections::VecDeque;

use crate::domain::Domain;
use crate::domain::EpochBarrier;
use crate::hazard::ThreadRecord;
use crate::retire::Retired;
use crate::HazardPointer;

pub struct Thread<'domain> {
    pub(crate) domain: &'domain Domain,
    pub(crate) hazards: &'domain ThreadRecord,
    /// available slots of hazard array
    pub(crate) available_indices: Vec<usize>,
    // Used for HP++
    // TODO: only 2 entries required
    pub(crate) hps: VecDeque<(usize, Vec<HazardPointer<'domain>>)>,
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
            hps: VecDeque::new(),
            retired: Vec::new(),
            collect_count: 0,
        }
    }
}

// stuff related to reclamation
impl<'domain> Thread<'domain> {
    const COUNTS_BETWEEN_PUSH: usize = 64;
    const COUNTS_BETWEEN_COLLECT: usize = 128;

    // NOTE: T: Send not required because we reclaim only locally.
    #[inline]
    pub unsafe fn retire<T>(&mut self, ptr: *mut T) {
        self.retired.push(Retired::new(ptr));
        if self.retired.len() > Self::COUNTS_BETWEEN_PUSH {
            self.domain.retireds.push(mem::take(&mut self.retired))
        }

        let collect_count = self.collect_count.wrapping_add(1);
        self.collect_count = collect_count;
        if collect_count % Self::COUNTS_BETWEEN_COLLECT == 0 {
            self.do_reclamation();
        }
    }

    pub unsafe fn try_unlink<T, F1, F2, F3>(
        &mut self,
        links: &[*mut T],
        collect_unlinked: F1,
        do_unlink: F2,
        set_stop: F3,
    ) -> bool
    where
        F1: FnOnce() -> Vec<*mut T>,
        F2: FnOnce() -> bool,
        F3: Fn(*mut T),
    {
        let hps: Vec<_> = links
            .iter()
            .map(|&ptr| {
                let mut hp = HazardPointer::new(self);
                hp.protect_raw(ptr);
                hp
            })
            .collect();

        let epoch = self.domain.barrier.read();
        while let Some(&(old_epoch, _)) = self.hps.front() {
            if EpochBarrier::check(old_epoch, epoch) {
                drop(self.hps.pop_front());
            } else {
                break;
            }
        }

        let unlinked = do_unlink();
        if unlinked {
            let unlinked_nodes = collect_unlinked();
            for &ptr in &unlinked_nodes {
                set_stop(ptr);
            }
            self.hps.push_back((epoch, hps));
            for &ptr in &unlinked_nodes {
                unsafe { self.retire(ptr) }
            }
        } else {
            drop(hps);
        }

        unlinked
    }

    #[inline]
    pub(crate) fn do_reclamation(&mut self) {
        let retireds = self.domain.retireds.pop_all();
        if retireds.is_empty() {
            return;
        }

        self.domain.barrier.barrier();

        // only for hp++, but this doesn't introduce big cost for plain hp.
        drop(mem::take(&mut self.hps));

        let guarded_ptrs = self.domain.collect_guarded_ptrs(self);
        let not_freed = retireds
            .into_iter()
            .filter_map(|element| {
                if guarded_ptrs.contains(&element.ptr) {
                    Some(element)
                } else {
                    unsafe { (element.deleter)(element.ptr) };
                    None
                }
            })
            .collect();
        self.domain.retireds.push(not_freed);
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
        self.hazards
            .hazptrs
            .store(Box::into_raw(new_array), Ordering::Release);
        self.available_indices.extend(size..new_size)
    }

    /// release hazard slot
    pub(crate) fn release(&mut self, idx: usize) {
        self.available_indices.push(idx);
    }
}

impl<'domain> Drop for Thread<'domain> {
    fn drop(&mut self) {
        // WARNING: Dropping HazardPointer touches available_indices. So available_indices MUST be
        // dropped after hps. For the same reason, Thread::drop MUST NOT acquire HazardPointer.
        drop(mem::take(&mut self.hps));
        drop(mem::take(&mut self.available_indices));
        self.domain.threads.release(self.hazards);
        self.domain.retireds.push(mem::take(&mut self.retired));
    }
}

impl core::fmt::Debug for Thread<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Thread")
            .field("domain", &(&self.domain as *const _))
            .field("hazards", &(&self.hazards as *const _))
            .field("available_indices", &self.available_indices.as_ptr())
            .field("hps", &self.hps)
            .field("retired", &format!("[...; {}]", self.retired.len()))
            .field("collect_count", &self.collect_count)
            .finish()
    }
}
