use core::sync::atomic::{AtomicPtr, Ordering};
use core::{mem, ptr};
use std::collections::VecDeque;

use crate::domain::Domain;
use crate::domain::EpochBarrier;
use crate::hazard::ThreadRecord;
use crate::retire::Retired;
use crate::HazardPointer;
use crate::{Invalidate, Unlink};

pub struct Thread<'domain> {
    pub(crate) domain: &'domain Domain,
    pub(crate) hazards: &'domain ThreadRecord,
    /// available slots of hazard array
    pub(crate) available_indices: Vec<usize>,
    // Used for HP++
    // TODO: only 2 entries required
    pub(crate) epoched_hps: VecDeque<(usize, Vec<HazardPointer<'domain>>)>,
    // Used for HP++. It's the thread-local `retireds` in the paper.
    // These should be invalidated and added to retireds.
    pub(crate) unlinkeds: Vec<(
        Vec<*mut u8>,
        unsafe fn(*mut u8),
        Vec<HazardPointer<'domain>>,
    )>,
    pub(crate) retired: Vec<Retired>,
    pub(crate) count: usize,
}

impl<'domain> Thread<'domain> {
    pub fn new(domain: &'domain Domain) -> Self {
        let (thread, available_indices) = domain.threads.acquire();
        Self {
            domain,
            hazards: thread,
            available_indices,
            epoched_hps: VecDeque::new(),
            unlinkeds: Vec::new(),
            retired: Vec::new(),
            count: 0,
        }
    }
}

// stuff related to reclamation
impl<'domain> Thread<'domain> {
    const COUNTS_BETWEEN_INVALIDATION: usize = 32;
    const COUNTS_BETWEEN_FLUSH: usize = 64;
    const COUNTS_BETWEEN_COLLECT: usize = 128;

    fn flush_retireds(&mut self) {
        self.domain.num_garbages.fetch_add(self.retired.len(), Ordering::AcqRel);
        self.domain.retireds.push(mem::take(&mut self.retired))
    }

    // NOTE: T: Send not required because we reclaim only locally.
    #[inline]
    pub unsafe fn retire<T>(&mut self, ptr: *mut T) {
        self.retired.push(Retired::new(ptr));
        let count = self.count.wrapping_add(1);
        self.count = count;
        if count % Self::COUNTS_BETWEEN_FLUSH == 0 {
            self.flush_retireds();
        }
        // TODO: collecting right after pushing is kinda weird
        if count % Self::COUNTS_BETWEEN_COLLECT == 0 {
            self.do_reclamation();
        }
    }

    pub unsafe fn try_unlink<T>(&mut self, unlink: impl Unlink<T>, links: &[*mut T]) -> bool
    where
        T: Invalidate,
    {
        let hps: Vec<_> = links
            .iter()
            .map(|&ptr| {
                let mut hp = HazardPointer::new(self);
                hp.protect_raw(ptr);
                hp
            })
            .collect();

        if let Ok(unlinkdes) = unlink.do_unlink() {
            unsafe fn invalidate<T: Invalidate>(ptr: *mut u8) {
                T::invalidate(&*(ptr as *mut T))
            }
            self.unlinkeds.push((
                mem::transmute::<Vec<_>, Vec<*mut u8>>(unlinkdes),
                invalidate::<T>,
                hps,
            ));

            let count = self.count.wrapping_add(1);
            self.count = count;
            if count % Self::COUNTS_BETWEEN_INVALIDATION == 0 {
                self.do_invalidation()
            }
            if count % Self::COUNTS_BETWEEN_FLUSH == 0 {
                self.flush_retireds();
            }
            if count % Self::COUNTS_BETWEEN_COLLECT == 0 {
                self.do_reclamation();
            }
            true
        } else {
            drop(hps);
            false
        }
    }

    pub(crate) fn do_invalidation(&mut self) {
        let unlinkeds = mem::take(&mut self.unlinkeds);
        let mut hps = Vec::with_capacity(2 * Self::COUNTS_BETWEEN_INVALIDATION);
        let mut invalidateds = Vec::with_capacity(2 * Self::COUNTS_BETWEEN_INVALIDATION);
        for (rs, invalidate, mut h) in unlinkeds {
            for r in rs {
                unsafe { invalidate(r) };
                invalidateds.push(r);
            }
            hps.append(&mut h);
        }

        let epoch = self.domain.barrier.read();
        while let Some(&(old_epoch, _)) = self.epoched_hps.front() {
            if EpochBarrier::check(old_epoch, epoch) {
                drop(self.epoched_hps.pop_front());
            } else {
                break;
            }
        }
        self.epoched_hps.push_back((epoch, hps));

        for r in invalidateds {
            self.retired.push(Retired::new(r));
        }
    }

    #[inline]
    pub(crate) fn do_reclamation(&mut self) {
        let retireds = self.domain.retireds.pop_all();
        let retireds_len = retireds.len();
        if retireds.is_empty() {
            return;
        }

        self.domain.barrier.barrier();

        // only for hp++, but this doesn't introduce big cost for plain hp.
        self.epoched_hps.clear();

        let guarded_ptrs = self.domain.collect_guarded_ptrs(self);
        let not_freed: Vec<Retired> = retireds
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
        self.domain.num_garbages.fetch_sub(retireds_len - not_freed.len(), Ordering::AcqRel);
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
        self.epoched_hps.clear();
        self.unlinkeds.clear();
        self.available_indices.clear();
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
            .field("epoched_hps", &self.epoched_hps)
            .field("retired", &format!("[...; {}]", self.retired.len()))
            .field("count", &self.count)
            .finish()
    }
}
