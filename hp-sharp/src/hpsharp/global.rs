use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
use rustc_hash::FxHashSet;

use super::{handle::Handle, hazard::ThreadRecords};
use crate::crcu;
use crate::sync::{Deferred, Pile};

pub struct Global {
    pub(crate) crcu: crcu::Global,
    pub(crate) threads: CachePadded<ThreadRecords>,
    pub(crate) deferred: CachePadded<Pile<Deferred>>,
    pub(crate) num_garbages: CachePadded<AtomicUsize>,
}

impl Global {
    pub const fn new() -> Self {
        Self {
            crcu: crcu::Global::new(),
            threads: CachePadded::new(ThreadRecords::new()),
            deferred: CachePadded::new(Pile::new()),
            num_garbages: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    pub fn register(&self) -> Handle {
        Handle::new(self)
    }

    pub fn collect_guarded_ptrs(&self, reclaimer: &mut Handle) -> FxHashSet<*mut u8> {
        self.threads
            .iter()
            .flat_map(|thread| thread.iter(reclaimer))
            .collect()
    }

    pub fn num_garbages(&self) -> usize {
        self.num_garbages.load(Ordering::Relaxed)
    }
}

impl Drop for Global {
    fn drop(&mut self) {
        let mut deferred = self.deferred.pop_all();
        for r in deferred.drain(..) {
            unsafe { r.execute() };
        }
    }
}
