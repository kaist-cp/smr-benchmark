use core::sync::atomic::{AtomicPtr, Ordering};
use core::{mem, ptr};
use std::collections::VecDeque;

use crate::domain::Domain;
use crate::domain::EpochBarrier;
use crate::hazard::ThreadRecord;
use crate::retire::{Retired, Unlinked};
use crate::HazardPointer;
use crate::{Invalidate, Unlink};

pub static mut COUNTS_BETWEEN_FLUSH: usize = 64;

#[inline]
pub fn set_counts_between_flush(counts: usize) {
    assert!(
        counts >= 2 && counts % 2 == 0,
        "counts must be even and greater than 1."
    );
    unsafe { COUNTS_BETWEEN_FLUSH = counts };
}

#[inline]
pub fn counts_between_invalidation() -> usize {
    unsafe { COUNTS_BETWEEN_FLUSH / 2 }
}

#[inline]
pub fn counts_between_flush() -> usize {
    unsafe { COUNTS_BETWEEN_FLUSH }
}

#[inline]
pub fn counts_between_collect() -> usize {
    unsafe { COUNTS_BETWEEN_FLUSH * 2 }
}

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
    pub(crate) unlinkeds: Vec<Unlinked<'domain>>,
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
    fn flush_retireds(&mut self) {
        self.domain
            .num_garbages
            .fetch_add(self.retired.len(), Ordering::AcqRel);
        self.domain.retireds.push(mem::replace(
            &mut self.retired,
            Vec::with_capacity(counts_between_flush()),
        ))
    }

    // NOTE: T: Send not required because we reclaim only locally.
    #[inline]
    pub unsafe fn retire<T>(&mut self, ptr: *mut T) {
        self.retired.push(Retired::new(ptr));
        let count = self.count.wrapping_add(1);
        self.count = count;
        if count % counts_between_flush() == 0 {
            self.flush_retireds();
        }
        // TODO: collecting right after pushing is kinda weird
        if count % counts_between_collect() == 0 {
            self.do_reclamation();
        }
    }

    pub unsafe fn try_unlink<T>(&mut self, unlink: impl Unlink<T>, frontier: &[*mut T]) -> bool
    where
        T: Invalidate,
    {
        let hps = self.protect_frontier(frontier);
        if let Ok(unlinkeds) = unlink.do_unlink() {
            self.schedule_invalidation(hps, unlinkeds);
            true
        } else {
            drop(hps);
            false
        }
    }

    pub fn protect_frontier<T>(&mut self, frontier: &[*mut T]) -> Vec<HazardPointer<'domain>> {
        frontier
            .iter()
            .map(|&ptr| {
                let mut hp = HazardPointer::new(self);
                hp.protect_raw(ptr);
                hp
            })
            .collect()
    }

    pub fn schedule_invalidation<T: Invalidate>(
        &mut self,
        hps: Vec<HazardPointer<'domain>>,
        unlinkeds: Vec<*mut T>,
    ) {
        self.unlinkeds.push(Unlinked::new(unlinkeds, hps));

        let count = self.count.wrapping_add(1);
        self.count = count;
        if count % counts_between_invalidation() == 0 {
            self.do_invalidation()
        }
        if count % counts_between_flush() == 0 {
            self.flush_retireds();
        }
        if count % counts_between_collect() == 0 {
            self.do_reclamation();
        }
    }

    pub(crate) fn do_invalidation(&mut self) {
        let mut hps = Vec::with_capacity(2 * counts_between_invalidation());
        let mut invalidateds = Vec::with_capacity(2 * counts_between_invalidation());
        for unlinked in self.unlinkeds.drain(..) {
            let (mut ptrs, mut hs) = unlinked.do_invalidation();
            invalidateds.append(&mut ptrs);
            hps.append(&mut hs);
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

        self.retired.append(&mut invalidateds);
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
        self.domain
            .num_garbages
            .fetch_sub(retireds_len - not_freed.len(), Ordering::AcqRel);
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
        for _ in size..new_size {
            new_array.push(AtomicPtr::new(ptr::null_mut()));
        }
        self.hazards
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

impl<'domain> Drop for Thread<'domain> {
    fn drop(&mut self) {
        self.do_invalidation();
        self.flush_retireds();
        self.domain.barrier.barrier();
        self.epoched_hps.clear();
        assert!(self.unlinkeds.is_empty());
        assert!(self.retired.is_empty());
        assert!(self.epoched_hps.is_empty());
        // WARNING: Dropping HazardPointer touches available_indices. So available_indices MUST be
        // dropped after hps. For the same reason, Thread::drop MUST NOT acquire HazardPointer.
        self.available_indices.clear();
        self.domain.threads.release(self.hazards);
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
