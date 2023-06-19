use atomic::Ordering;
use crossbeam_utils::CachePadded;
use rustc_hash::FxHashSet;

use crate::crcu::Global;
use crate::{hazard::ThreadRecords, thread::Thread};

pub struct Domain {
    pub(crate) rcu: Global,
    pub(crate) threads: CachePadded<ThreadRecords>,
}

impl Domain {
    pub const fn new() -> Self {
        Self {
            rcu: Global::new(),
            threads: CachePadded::new(ThreadRecords::new()),
        }
    }

    pub fn collect_guarded_ptrs(&self, reclaimer: &mut Thread) -> FxHashSet<*mut u8> {
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
    }
}
