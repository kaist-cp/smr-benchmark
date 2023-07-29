use std::sync::atomic::{compiler_fence, Ordering};

use crate::rrcu::local::LocalRRCU;
use crate::{CsGuard, Deferred, RaGuard, Thread};

pub trait ThreadRRCU {
    unsafe fn pin<F>(&mut self, body: F)
    where
        F: FnMut(&mut CsGuard);
}

pub trait CsGuardRRCU {
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
    fn mask<F, R>(&self, body: F) -> R
    where
        F: FnOnce(&mut RaGuard) -> R,
        R: Copy;
}

pub trait RaGuardRRCU {
    /// Repins its critical section if we are crashed(in other words, ejected).
    ///
    /// Developers must ensure that there is no possibilities of memory leaks across this.
    fn repin(&self) -> !;

    /// TODO(@jeonghyeon): replace it with remask
    fn must_rollback(&self) -> bool;
}

impl ThreadRRCU for Thread {
    /// Starts a crashable critical section where we cannot perform operations with side-effects,
    /// such as system calls, non-atomic write on a global variable, etc.
    ///
    /// # Safety
    ///
    /// In a section body, only *rollback-safe* operations are allowed. For example, non-atomic
    /// writes on a global variable and system-calls(File I/O and etc.) are dangerous, as they
    /// may cause an unexpected inconsistency on the whole system after a crash.
    #[inline(always)]
    unsafe fn pin<F>(&mut self, body: F)
    where
        F: FnMut(&mut CsGuard),
    {
        unsafe { (*self.local).pin(body) }
    }
}

impl CsGuardRRCU for CsGuard {
    #[inline(always)]
    fn mask<F, R>(&self, body: F) -> R
    where
        F: FnOnce(&mut RaGuard) -> R,
        R: Copy,
    {
        compiler_fence(Ordering::SeqCst);
        let result = self.rb.atomic(|rb| {
            let mut guard = RaGuard {
                local: self.local,
                rb,
            };
            let result = body(&mut guard);
            result
        });
        compiler_fence(Ordering::SeqCst);
        result
    }
}

impl RaGuardRRCU for RaGuard {
    /// Repins its critical section if we are crashed(in other words, ejected).
    ///
    /// Developers must ensure that there is no possibilities of memory leaks across this.
    #[inline]
    fn repin(&self) -> ! {
        unsafe { &*self.rb }.restart()
    }

    #[inline]
    fn must_rollback(&self) -> bool {
        unsafe { &*self.rb }.must_rollback()
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

impl Deferrable for Thread {
    #[inline]
    #[must_use]
    fn defer(&mut self, def: Deferred) -> Option<Vec<Deferred>> {
        if let Some(local) = unsafe { self.local.as_mut() } {
            local.defer(def)
        } else {
            Some(vec![def])
        }
    }
}

impl Deferrable for RaGuard {
    #[inline]
    #[must_use]
    fn defer(&mut self, def: Deferred) -> Option<Vec<Deferred>> {
        if let Some(local) = unsafe { self.local.as_mut() } {
            local.defer(def)
        } else {
            Some(vec![def])
        }
    }
}
