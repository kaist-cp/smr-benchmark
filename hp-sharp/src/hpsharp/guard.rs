use std::mem::ManuallyDrop;

use crate::{rrcu::Deferrable, CsGuard, Deferred, RaGuard, Thread};

use super::{free, Shared};

pub trait Invalidate {
    fn invalidate(&self);
    fn is_invalidated(&self, guard: &CsGuard) -> bool;
}

pub trait Retire {
    /// Retires a given shared pointer, so that it can be reclaimed when the current epoch is ended
    /// and there is no hazard pointer protecting the pointer.
    ///
    /// # Safety
    ///
    /// The given pointer must be a valid memory location which can be dereferenced.
    unsafe fn retire<'r, T: Invalidate>(&mut self, ptr: Shared<'r, T>);
}

impl Retire for Thread {
    #[inline]
    unsafe fn retire<'r, T: Invalidate>(&mut self, ptr: Shared<'r, T>) {
        // Invalidate immediately to prevent a slow thread to resume its traversal after a crash.
        ptr.deref_unchecked().invalidate();

        let collected = self.defer(Deferred::new(
            ptr.untagged().as_raw() as *const u8 as *mut u8,
            free::<T>,
        ));

        if let Some(collected) = collected {
            self.retire_inner(collected);
        }
    }
}

impl Retire for RaGuard {
    #[inline]
    unsafe fn retire<'r, T: Invalidate>(&mut self, ptr: Shared<'r, T>) {
        // Invalidate immediately to prevent a slow thread to resume its traversal after a crash.
        ptr.deref_unchecked().invalidate();

        let collected = self.defer(Deferred::new(
            ptr.untagged().as_raw() as *const u8 as *mut u8,
            free::<T>,
        ));

        if let Some(collected) = collected {
            ManuallyDrop::new(Thread { local: self.local }).retire_inner(collected);
        }
    }
}

/// A marker for all RAII guard types.
pub trait Guard {}

impl Guard for Thread {}
impl Guard for CsGuard {}
impl Guard for RaGuard {}
