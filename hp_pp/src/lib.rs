mod domain;
mod hazard;
mod retire;
mod tag;
mod thread;

pub use hazard::HazardPointer;
pub use hazard::ProtectError;
pub use membarrier::light_membarrier;
pub use tag::*;

use core::cell::RefCell;
use std::thread_local;

use crate::domain::Domain;
use crate::thread::Thread;

static DEFAULT_DOMAIN: Domain = Domain::new();

// NOTE: MUST NOT take raw pointer to TLS. They randomly move???
thread_local! {
    static DEFAULT_THREAD: RefCell<Box<Thread<'static>>> = RefCell::new(Box::new(Thread::new(&DEFAULT_DOMAIN)));
}

/// Retire a pointer, in the thread-local retired pointer bag.
///
/// # Safety
/// TODO
#[inline]
pub unsafe fn retire<T>(ptr: *mut T) {
    DEFAULT_THREAD.with(|t| t.borrow_mut().retire(ptr))
}

/// Protects `links` and try unlinking by `do_unlink`. if successful, mark the returned nodes as stopped and
/// retire them.
/// 
/// `do_unlink` tries unlinking, and if successful, it returns raw pointers to unlinked nodes.
///
/// # Safety
/// * The memory blocks in `to_be_unlinked` are no longer modified.
/// * TODO
pub unsafe fn try_unlink<T, F1, F2>(
    links: &[*mut T],
    do_unlink: F1,
    set_stop: F2,
) -> bool
where
    F1: FnOnce() -> Result<Vec<*mut T>, ()>,
    F2: Fn(&T),
{
    DEFAULT_THREAD.with(|t| {
        t.borrow_mut()
            .try_unlink(links, do_unlink, set_stop)
    })
}

/// Trigger reclamation
pub fn do_reclamation() {
    DEFAULT_THREAD.with(|t| {
        t.borrow_mut().do_reclamation();
    })
}
