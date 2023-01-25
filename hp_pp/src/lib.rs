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

pub static DEFAULT_DOMAIN: Domain = Domain::new();

// NOTE: MUST NOT take raw pointer to TLS. They randomly move???
thread_local! {
    static DEFAULT_THREAD: RefCell<Box<Thread<'static>>> = RefCell::new(Box::new(Thread::new(&DEFAULT_DOMAIN)));
}

pub trait Unlink<T> {
    fn do_unlink(&self) ->Result<Vec<*mut T>, ()>;
}

pub trait Invalidate {
    fn invalidate(&self);
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
pub unsafe fn try_unlink<T>(
    unlink: impl Unlink<T>,
    frontier: &[*mut T],
) -> bool
where
    T: Invalidate,
{
    DEFAULT_THREAD.with(|t| {
        t.borrow_mut()
            .try_unlink(unlink, frontier)
    })
}

/// Trigger reclamation
pub fn do_reclamation() {
    DEFAULT_THREAD.with(|t| {
        t.borrow_mut().do_reclamation();
    })
}
