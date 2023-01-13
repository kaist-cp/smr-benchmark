use core::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
use rustc_hash::FxHashSet;

use crate::hazard::ThreadRecords;
use crate::retire::RetiredList;
use crate::thread::Thread;

#[derive(Debug)]
pub struct Domain {
    pub(crate) threads: ThreadRecords,
    pub(crate) barrier: EpochBarrier,
    pub(crate) retireds: CachePadded<RetiredList>,
}

impl Domain {
    pub const fn new() -> Self {
        Self {
            threads: ThreadRecords::new(),
            barrier: EpochBarrier(AtomicUsize::new(0)),
            retireds: CachePadded::new(RetiredList::new()),
        }
    }

    pub fn collect_guarded_ptrs<'domain>(
        &self,
        reclaimer: &mut Thread<'domain>,
    ) -> FxHashSet<*mut u8> {
        self.threads
            .iter()
            .flat_map(|thread| thread.iter(reclaimer))
            .collect()
    }
}

impl Drop for Domain {
    fn drop(&mut self) {
        for t in self.threads.iter() {
            assert!(t.available.load(Ordering::Relaxed))
        }
        let mut retireds = self.retireds.pop_all();
        for r in retireds.drain(..) {
            unsafe { (r.deleter)(r.ptr) };
        }
    }
}

#[derive(Debug)]
pub(crate) struct EpochBarrier(AtomicUsize);

impl EpochBarrier {
    pub(crate) fn barrier(&self) -> usize {
        let epoch = self.0.load(Ordering::Acquire);
        membarrier::heavy();
        let new_epoch = epoch.wrapping_add(1);
        match self
            .0
            .compare_exchange(epoch, new_epoch, Ordering::Release, Ordering::Acquire)
        {
            Ok(_) => new_epoch,
            Err(new) => new,
        }
    }

    pub(crate) fn read(&self) -> usize {
        let mut epoch = self.0.load(Ordering::Acquire);
        loop {
            membarrier::light_membarrier();
            let new_epoch = self.0.load(Ordering::Acquire);
            if epoch == new_epoch {
                return epoch;
            }
            epoch = new_epoch
        }
    }

    pub(crate) fn check(old: usize, new: usize) -> bool {
        new.wrapping_sub(old) >= 2
    }
}
