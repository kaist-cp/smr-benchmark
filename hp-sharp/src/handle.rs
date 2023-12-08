use std::mem::ManuallyDrop;
use std::ptr::{null, null_mut};
use std::sync::atomic::{compiler_fence, fence, Ordering};

use crate::deferred::Deferred;
use crate::internal::{free, Local};
use crate::pointers::Shared;
use crate::rollback::Rollbacker;

pub trait Invalidate {
    fn is_invalidated(&self, guard: &CsGuard) -> bool;
}

pub trait Handle {}

pub trait RollbackProof: Handle {
    /// Retires a given shared pointer, so that it can be reclaimed when the current epoch is ended
    /// and there is no hazard pointer protecting the pointer.
    ///
    /// # Safety
    ///
    /// The given pointer must be a valid memory location which can be dereferenced.
    unsafe fn retire<'r, T: Invalidate>(&mut self, ptr: Shared<'r, T>);
}

/// A thread-local handle managing local epoch and defering.
pub struct Thread {
    local: *mut Local,
}

impl Thread {
    /// Creates an unprotected `Thread`.
    pub unsafe fn unprotected() -> Self {
        Self { local: null_mut() }
    }

    pub(crate) fn from_raw(local: *mut Local) -> Self {
        Self { local }
    }

    pub(crate) unsafe fn local(&self) -> &Local {
        &*self.local
    }

    pub(crate) unsafe fn local_mut(&mut self) -> &mut Local {
        &mut *self.local
    }

    pub(crate) fn as_local_mut(&mut self) -> Option<&mut Local> {
        unsafe { self.local.as_mut() }
    }

    /// Starts a crashable critical section where we cannot perform operations with side-effects,
    /// such as system calls, non-atomic write on a global variable, etc.
    ///
    /// # Safety
    ///
    /// In a section body, only *rollback-safe* operations are allowed. For example, non-atomic
    /// writes on a global variable and system-calls(File I/O and etc.) are dangerous, as they
    /// may cause an unexpected inconsistency on the whole system after a crash.
    #[inline]
    pub unsafe fn pin<F, R>(&mut self, body: F) -> R
    where
        F: FnMut(&mut CsGuard) -> R,
        R: Copy,
    {
        unsafe { (*self.local).pin(body) }
    }

    /// Defers a task which can be accessed after the current epoch ends.
    ///
    /// It returns a `Some(Vec<Deferred>)` if the global epoch is advanced and we have collected
    /// some expired deferred tasks.
    #[inline]
    #[must_use]
    pub(crate) fn defer(&mut self, def: Deferred) -> Option<Vec<Deferred>> {
        if let Some(local) = unsafe { self.local.as_mut() } {
            local.defer(def)
        } else {
            Some(vec![def])
        }
    }

    #[inline]
    pub(crate) unsafe fn retire_inner(&mut self, mut deferred: Vec<Deferred>) {
        let Some(local) = self.local.as_mut() else {
            for def in deferred {
                unsafe { def.execute() };
            }
            return;
        };
        local.with_local_defs(move |mut defs| {
            deferred.append(&mut defs);
            self.do_reclamation(deferred)
        });
    }

    pub(crate) unsafe fn do_reclamation(&mut self, deferred: Vec<Deferred>) -> Vec<Deferred> {
        let deferred_len = deferred.len();
        if deferred.is_empty() {
            return vec![];
        }

        fence(Ordering::SeqCst);

        let guarded_ptrs = self.local_mut().collect_guarded_ptrs();
        let not_freed: Vec<Deferred> = deferred
            .into_iter()
            .filter_map(|element| {
                if guarded_ptrs.contains(&element.data()) {
                    Some(element)
                } else {
                    unsafe { element.execute() };
                    None
                }
            })
            .collect();
        self.local().decr_garb_stat(deferred_len - not_freed.len());
        not_freed
    }
}

impl Handle for Thread {}

impl RollbackProof for Thread {
    #[inline]
    unsafe fn retire<'r, T: Invalidate>(&mut self, ptr: Shared<'r, T>) {
        let collected = self.defer(Deferred::new(
            ptr.untagged().as_raw() as *const u8 as *mut u8,
            free::<T>,
        ));

        if let Some(collected) = collected {
            self.retire_inner(collected);
        }
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        unsafe {
            if let Some(local) = self.local.as_mut() {
                local.release();
            }
        }
    }
}

/// A crashable critical section guard.
///
/// Note that defering is not allowed in this critical section because a crash may occur while
/// writing data on non-atomic storage. To conduct jobs with side-effects, we must open a
/// non-crashable section by `mask` method.
pub struct CsGuard {
    local: *mut Local,
    rb: Rollbacker,
}

impl CsGuard {
    pub(crate) fn new(local: &mut Local, rb: Rollbacker) -> Self {
        Self { local, rb }
    }

    /// Creates an unprotected `CsGuard`.
    pub unsafe fn unprotected() -> Self {
        Self {
            local: null_mut(),
            rb: Rollbacker,
        }
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
    pub fn mask<F, R>(&self, body: F) -> R
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

impl Handle for CsGuard {}

/// A non-crashable write section guard.
///
/// Unlike a [`CsGuard`], it may perform jobs with side-effects such as retiring, or physical
/// deletion for a data structure.
pub struct RaGuard {
    local: *mut Local,
    rb: *const Rollbacker,
}

impl RaGuard {
    /// Creates an unprotected `RaGuard`.
    pub unsafe fn unprotected() -> Self {
        Self {
            local: null_mut(),
            rb: null(),
        }
    }

    /// Repins its critical section if we are crashed(in other words, ejected).
    ///
    /// Developers must ensure that there is no possibilities of memory leaks across this.
    #[inline]
    pub(crate) fn repin(&self) -> ! {
        unsafe { &*self.rb }.restart()
    }

    #[inline]
    pub(crate) fn must_rollback(&self) -> bool {
        unsafe { &*self.rb }.must_rollback()
    }
}

impl Handle for RaGuard {}

impl RollbackProof for RaGuard {
    unsafe fn retire<'r, T: Invalidate>(&mut self, ptr: Shared<'r, T>) {
        ManuallyDrop::new(Thread { local: self.local }).retire(ptr);
    }
}
