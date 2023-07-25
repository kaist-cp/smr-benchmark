use std::{
    ptr::{null, null_mut, NonNull},
    sync::atomic::AtomicUsize,
};

use crate::{
    crcu::{self, Deferrable},
    hpsharp::{handle::free, Handle, Shared},
    sync::Deferred,
};

/// A high-level crashable critical section guard.
///
/// It is different with [`crate::crcu::EpochGuard`], as it contains a pointer to HP# [`Handle`]
/// on the top of [`crate::crcu::EpochGuard`]. This allow us to start a `mask` section with
/// protecting some desired pointers.
///
/// Note that defering is not allowed in this critical section because a crash may occur while
/// writing data on non-atomic storage. To conduct jobs with side-effects, we must open a
/// non-crashable section by `mask` method.
pub struct EpochGuard {
    pub(crate) inner: *mut crcu::EpochGuard,
    pub(crate) handle: *const Handle,
    pub(crate) backup_idx: Option<NonNull<AtomicUsize>>,
}

impl EpochGuard {
    pub(crate) fn new(
        inner: &mut crcu::EpochGuard,
        handle: &Handle,
        backup_idx: Option<&AtomicUsize>,
    ) -> Self {
        Self {
            inner,
            handle,
            backup_idx: backup_idx.map(|at| unsafe {
                NonNull::new_unchecked(at as *const AtomicUsize as *mut AtomicUsize)
            }),
        }
    }

    /// Creates an unprotected `EpochGuard`.
    pub unsafe fn unprotected() -> Self {
        Self {
            inner: null_mut(),
            handle: null(),
            backup_idx: None,
        }
    }
}

/// A high-level non-crashable write section guard.
///
/// It is different with [`crate::crcu::CrashGuard`], as it contains a pointer to HP# [`Handle`]
/// on the top of [`crate::crcu::CrashGuard`]. This allow us to retire pointers whose epochs are
/// expired.
///
/// Unlike a [`EpochGuard`], it may perform jobs with side-effects such as retiring, or physical
/// deletion for a data structure.
pub struct CrashGuard {
    inner: *mut crcu::CrashGuard,
    handle: *const Handle,
}

impl CrashGuard {
    pub(crate) fn new(inner: &mut crcu::CrashGuard, handle: *const Handle) -> Self {
        Self { inner, handle }
    }

    /// Creates an unprotected `CrashGuard`.
    pub unsafe fn unprotected() -> Self {
        Self {
            inner: null_mut(),
            handle: null(),
        }
    }
}

pub trait Invalidate {
    fn invalidate(&self);
    fn is_invalidated(&self, guard: &EpochGuard) -> bool;
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

impl Retire for Handle {
    #[inline]
    unsafe fn retire<'r, T: Invalidate>(&mut self, ptr: Shared<'r, T>) {
        // Invalidate immediately to prevent a slow thread to resume its traversal after a crash.
        ptr.deref_unchecked().invalidate();

        let collected = self.crcu_handle.borrow_mut().defer(Deferred::new(
            ptr.untagged().as_raw() as *const u8 as *mut u8,
            free::<T>,
        ));

        if let Some(collected) = collected {
            self.retire_inner(collected);
        }
    }
}

impl Retire for CrashGuard {
    #[inline]
    unsafe fn retire<'r, T: Invalidate>(&mut self, ptr: Shared<'r, T>) {
        // Invalidate immediately to prevent a slow thread to resume its traversal after a crash.
        ptr.deref_unchecked().invalidate();

        let collected = (*self.inner).defer(Deferred::new(
            ptr.untagged().as_raw() as *const u8 as *mut u8,
            free::<T>,
        ));

        if let Some(collected) = collected {
            (*self.handle.cast_mut()).retire_inner(collected);
        }
    }
}

/// A marker for all RAII guard types.
pub trait Guard {}

impl Guard for Handle {}
impl Guard for EpochGuard {}
impl Guard for CrashGuard {}
