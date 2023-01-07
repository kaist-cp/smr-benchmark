use std::collections::HashSet;

use crate::hazard::{ThreadRecord, ThreadRecords};

pub struct Domain {
    pub(crate) threads: ThreadRecords,
}

impl Domain {
    pub const fn new() -> Self {
        Self {
            threads: ThreadRecords::new(),
        }
    }
    pub fn acquire(&self) -> &ThreadRecord {
        self.threads.acquire()
    }

    pub fn collect_guarded_ptrs(&self) -> HashSet<*mut u8> {
        self.threads
            .iter()
            .flat_map(|thread| thread.hazptrs.iter())
            .collect()
    }
}
