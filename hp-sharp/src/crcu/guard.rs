use std::sync::atomic::{compiler_fence, Ordering};

use crate::sync::Deferred;

use super::{local::Local, RecoveryGuard};

/// A crashable critical section guard.
///
/// Note that defering is not allowed in this critical section because a crash may occur while
/// writing data on non-atomic storage. To conduct jobs with side-effects, we must open a
/// non-crashable section by `mask` method.
pub struct EpochGuard {
    local: *mut Local,
    inner: RecoveryGuard,
}

impl EpochGuard {
    pub(crate) fn new(local: &mut Local, inner: RecoveryGuard) -> Self {
        Self { local, inner }
    }

    /// Starts a non-crashable section where we can conduct operations with global side-effects.
    ///
    /// In this section, we do not restart immediately when we receive signals from reclaimers.
    /// The whole critical section restarts after this `mask` section ends, if a reclaimer sent
    /// a signal, or we advanced our epoch to reclaim a full local garbage bag.
    ///
    /// The body may return an arbitrary value, and it will be returned without any modifications.
    /// However, it is required to return a *rollback-safe* variable from the body. For example,
    /// [`String`] or [`Box`] is dangerous to return as it will be leaked on a crash! On the other
    /// hand, [`Copy`] types is likely to be safe as they are totally defined by their bit-wise
    /// representations, and have no possibilities to be leaked after an unexpected crash.
    #[inline(always)]
    pub fn mask<F, R>(&mut self, body: F) -> R
    where
        F: Fn(&mut CrashGuard) -> R,
        R: Copy,
    {
        compiler_fence(Ordering::SeqCst);
        let result = self.inner.atomic(|guard| {
            let mut guard = CrashGuard::new(unsafe { &mut *self.local }, guard);
            let result = body(&mut guard);
            result
        });
        compiler_fence(Ordering::SeqCst);
        result
    }
}

/// A non-crashable write section guard.
///
/// Unlike a [`EpochGuard`], it may perform jobs with side-effects such as retiring, or physical
/// deletion for a data structure.
pub struct CrashGuard {
    local: *mut Local,
    inner: *const RecoveryGuard,
}

/// A non-crashable section guard.
impl CrashGuard {
    #[inline]
    pub(crate) fn new(local: &mut Local, inner: &RecoveryGuard) -> Self {
        Self { local, inner }
    }

    /// Repins its critical section if we are crashed(in other words, ejected).
    ///
    /// Developers must ensure that there is no possibilities of memory leaks across this.
    #[inline]
    pub fn repin(&self) -> ! {
        unsafe { (*self.inner).restart() }
    }

    #[inline]
    pub fn is_ejected(&self) -> bool {
        compiler_fence(Ordering::SeqCst);
        unsafe { !(*self.local).is_pinned() }
    }
}

/// A common trait for `Guard` types which allow defering tasks on a shared memory.
///
/// [`crate::crcu::Handle`] and [`CrashGuard`] implement this trait.
pub trait Deferrable {
    /// Defers a task which can be accessed after the current epoch ends.
    ///
    /// It returns a `Some(Vec<Deferred>)` if the global epoch is advanced and we have collected
    /// some expired deferred tasks.
    #[must_use]
    fn defer(&mut self, def: Deferred) -> Option<Vec<Deferred>>;
}

impl Deferrable for CrashGuard {
    #[inline]
    #[must_use]
    fn defer(&mut self, def: Deferred) -> Option<Vec<Deferred>> {
        unsafe { (*self.local).defer(def) }
    }
}
