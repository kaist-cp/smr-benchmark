use std::sync::atomic::AtomicPtr;

use atomic::Ordering;
use rustc_hash::FxHashSet;

use crate::{rrcu::GlobalRRCU, Global, Local, Thread};

use super::HazardPointer;

pub trait GlobalHPSharp {
    fn register(&self) -> Thread;
    fn collect_guarded_ptrs(&self, reclaimer: &mut Thread) -> FxHashSet<*mut u8>;
}

impl GlobalHPSharp for Global {
    fn register(&self) -> Thread {
        let thread = GlobalRRCU::register(self);
        let hazptrs_len = unsafe { &*(*thread.local).hazptrs.load(Ordering::Relaxed) }.len();
        unsafe { &mut *thread.local }.available_indices = (0..hazptrs_len).collect();
        thread
    }

    fn collect_guarded_ptrs(&self, reclaimer: &mut Thread) -> FxHashSet<*mut u8> {
        self.locals
            .iter_using()
            .flat_map(|local| ThreadHazardArrayIter::new(local, reclaimer))
            .collect()
    }
}

struct ThreadHazardArrayIter {
    array: *const [AtomicPtr<u8>],
    idx: usize,
    _hp: HazardPointer,
}

impl ThreadHazardArrayIter {
    fn new(local: &Local, reader: &mut Thread) -> Self {
        let hp = HazardPointer::new(reader);
        let array = hp.protect(&local.hazptrs);
        ThreadHazardArrayIter {
            array: unsafe { &*array }.as_slice(),
            idx: 0,
            _hp: hp,
        }
    }
}

impl Iterator for ThreadHazardArrayIter {
    type Item = *mut u8;

    fn next(&mut self) -> Option<Self::Item> {
        let array = unsafe { &*self.array };
        for i in self.idx..array.len() {
            self.idx += 1;
            let slot = unsafe { array.get_unchecked(i) };
            let value = slot.load(Ordering::Acquire);
            if !value.is_null() {
                return Some(value);
            }
        }
        None
    }
}
