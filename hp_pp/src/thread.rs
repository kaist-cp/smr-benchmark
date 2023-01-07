use core::cell::RefCell;
use core::mem;

use crate::domain::Domain;
use crate::hazard::ThreadRecord;
use crate::retire::Retired;
use crate::HazardPointer;

pub struct Thread<'domain> {
    pub(crate) domain: &'domain Domain,
    pub(crate) hazards: &'domain ThreadRecord,
    pub(crate) reclaim: RefCell<Reclamation<'domain>>,
}

pub(crate) struct Reclamation<'domain> {
    // Used for HP++
    pub(crate) hps: Vec<HazardPointer<'domain>>,
    pub(crate) retired: Vec<Retired>,
    pub(crate) collect_count: usize,
}

impl<'domain> Thread<'domain> {
    const COUNTS_BETWEEN_COLLECT: usize = 128;

    pub fn new(domain: &'domain Domain) -> Self {
        let thread = domain.acquire();
        Self {
            domain,
            hazards: thread,
            reclaim: RefCell::new(Reclamation {
                hps: Vec::new(),
                retired: Vec::new(),
                collect_count: 0,
            }),
        }
    }

    pub unsafe fn retire<T>(&self, ptr: *mut T) {
        let mut reclaim = self.reclaim.borrow_mut();
        self.retire_inner(&mut reclaim, ptr);
    }

    // NOTE: T: Send not required because we reclaim only locally.
    #[inline]
    unsafe fn retire_inner<T>(&self, reclaim: &mut Reclamation, ptr: *mut T) {
        reclaim.retired.push(Retired::new(ptr));

        let collect_count = reclaim.collect_count.wrapping_add(1);
        reclaim.collect_count = collect_count;
        if collect_count % Self::COUNTS_BETWEEN_COLLECT == 0 {
            self.do_reclamation(reclaim);
        }
    }

    pub unsafe fn try_unlink<T, F1, F2>(
        &self,
        links: &[*mut T],
        to_be_unlinked: &[*mut T],
        do_unlink: F1,
        set_stop: F2,
    ) -> bool
    where
        F1: FnOnce() -> bool,
        F2: Fn(*mut T),
    {
        let mut reclaim = self.reclaim.borrow_mut();

        let mut hps: Vec<_> = links
            .iter()
            .map(|&ptr| {
                let mut hp = HazardPointer::new(self.hazards);
                hp.protect_raw(ptr);
                hp
            })
            .collect();

        let unlinked = do_unlink();
        if unlinked {
            for &ptr in to_be_unlinked {
                set_stop(ptr);
            }
            reclaim.hps.append(&mut hps);
            for &ptr in to_be_unlinked {
                unsafe { self.retire_inner(&mut reclaim, ptr) }
            }
        } else {
            drop(hps);
        }
        unlinked
    }

    #[inline]
    pub(crate) fn do_reclamation(&self, reclaim: &mut Reclamation) {
        membarrier::heavy();

        // only for hp++, but this doesn't introduce big cost for plain hp.
        drop(mem::take(&mut reclaim.hps));

        let guarded_ptrs = self.domain.collect_guarded_ptrs();
        reclaim.retired = reclaim
            .retired
            .iter()
            .filter_map(|element| {
                if guarded_ptrs.contains(&(element.ptr as *mut u8)) {
                    Some(Retired {
                        ptr: element.ptr,
                        deleter: element.deleter,
                    })
                } else {
                    unsafe { (element.deleter)(element.ptr) };
                    None
                }
            })
            .collect();
    }
}

impl<'domain> Drop for Thread<'domain> {
    fn drop(&mut self) {
        self.domain.threads.release(self.hazards);
        let mut reclaim = self.reclaim.borrow_mut();
        while !reclaim.retired.is_empty() {
            self.do_reclamation(&mut reclaim);
            core::hint::spin_loop();
        }
    }
}
