use crossbeam_utils::CachePadded;
use rustc_hash::FxHashSet;

use super::{handle::Handle, hazard::ThreadRecords};
use crate::crcu;
use crate::sync::{Deferred, Pile};

pub struct Global {
    pub(crate) crcu: crcu::Global,
    pub(crate) threads: CachePadded<ThreadRecords>,
    pub(crate) deferred: CachePadded<Pile<Deferred>>,
}

impl Global {
    pub const fn new() -> Self {
        Self {
            crcu: crcu::Global::new(),
            threads: CachePadded::new(ThreadRecords::new()),
            deferred: CachePadded::new(Pile::new()),
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
}

impl Drop for Global {
    fn drop(&mut self) {
        let mut deferred = self.deferred.pop_all();
        for r in deferred.drain(..) {
            unsafe { r.execute() };
        }
    }
}
