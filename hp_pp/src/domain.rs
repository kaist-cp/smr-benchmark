use std::collections::HashSet;

use crate::{hazard::ThreadRecords, thread::Thread};

pub struct Domain {
    pub(crate) threads: ThreadRecords,
}

impl Domain {
    pub const fn new() -> Self {
        Self {
            threads: ThreadRecords::new(),
        }
    }

    pub fn collect_guarded_ptrs<'domain>(&self, reclaimer: &mut Thread<'domain>) -> HashSet<*mut u8> {
        self.threads
            .iter()
            .flat_map(|thread| thread.iter(reclaimer))
            .collect()
    }
}
