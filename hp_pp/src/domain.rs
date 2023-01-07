use rustc_hash::FxHashSet;

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

    pub fn collect_guarded_ptrs<'domain>(&self, reclaimer: &mut Thread<'domain>) -> FxHashSet<*mut u8> {
        self.threads
            .iter()
            .flat_map(|thread| thread.iter(reclaimer))
            .filter(|p| !p.is_null())
            .collect()
    }
}
