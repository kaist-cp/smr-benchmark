use std::{
    ptr::{null, null_mut, NonNull},
    sync::atomic::{compiler_fence, fence, AtomicUsize, Ordering},
};

use crate::{
    crcu::{self, Deferrable},
    hpsharp::{handle::free, Handle, Protector, Shared, WriteResult},
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
    inner: *mut crcu::EpochGuard,
    handle: *const Handle,
    backup_idx: Option<NonNull<AtomicUsize>>,
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

    /// Starts a non-crashable section where we can conduct operations with global side-effects.
    ///
    /// In this section, we do not restart immediately when we receive signals from reclaimers.
    /// The whole critical section restarts after this `mask` section ends, if a reclaimer sent
    /// a signal, or we advanced our epoch to reclaim a full local garbage bag.
    pub fn mask<'r, F, D>(&'r self, to_deref: D::Target<'r>, body: F)
    where
        F: Fn(&D, &mut CrashGuard) -> WriteResult,
        D: Protector,
    {
        // Note that protecting must be conducted in a crash-free section.
        // Otherwise it may forget to drop acquired hazard slot on crashing.
        unsafe { &mut *self.inner }.mask(|guard| {
            let result = {
                // Allocate fresh hazard slots to protect pointers.
                let mut def = D::empty(unsafe { &mut *self.handle.cast_mut() });
                // Store pointers in hazard slots and issue a fence.
                def.protect_unchecked(&to_deref);
                fence(Ordering::SeqCst);

                // Restart if the thread is crashed while protecting.
                if guard.must_rollback() {
                    drop(def);
                    guard.repin();
                }

                body(&def, &mut CrashGuard::new(guard, self.handle))
            };

            compiler_fence(Ordering::SeqCst);
            if result == WriteResult::RepinEpoch {
                // Invalidate any saved checkpoints.
                if let Some(backup_idx) = self.backup_idx {
                    unsafe { backup_idx.as_ref() }.store(2, Ordering::Relaxed);
                }
                guard.repin();
            }
        });
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
