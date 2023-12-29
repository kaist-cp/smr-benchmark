use std::mem::{transmute, ManuallyDrop};
use std::sync::atomic::{compiler_fence, fence, AtomicUsize, Ordering};

use crate::deferred::Deferred;
use crate::internal::{free, Local};
use crate::pointers::Shared;
use crate::rollback::Rollbacker;
use crate::USE_ROLLBACK;

const BACKUP_PERIOD: usize = 512;

pub trait Validatable {
    fn empty() -> Self;
    fn validate(&self, guard: &CsGuard) -> bool;
}

pub trait Protector {
    type Target<'g>: Validatable + Copy;
    fn new(thread: &mut Thread) -> Self;
    fn protect(&mut self, ptrs: &Self::Target<'_>);
    fn swap(a: &mut Self, b: &mut Self);
}

pub trait Handle {}

pub trait RollbackProof: Handle {
    /// Retires a given shared pointer, so that it can be reclaimed when the current epoch is ended
    /// and there is no hazard pointer protecting the pointer.
    ///
    /// # Safety
    ///
    /// The given pointer must be a valid memory location which can be dereferenced.
    unsafe fn retire<T>(&mut self, ptr: Shared<T>);
}

/// A thread-local handle managing local epoch and defering.
pub struct Thread {
    local: *mut Local,
}

impl Thread {
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
    pub unsafe fn critical_section<F, R>(&mut self, body: F) -> R
    where
        F: FnMut(&CsGuard) -> R,
        R: Copy,
    {
        unsafe { (*self.local).pin(body) }
    }

    /// TODO
    ///
    /// # Safety
    ///
    /// In a section body, only *rollback-safe* operations are allowed. For example, non-atomic
    /// writes on a global variable and system-calls(File I/O and etc.) are dangerous, as they
    /// may cause an unexpected inconsistency on the whole system after a crash.
    #[inline]
    pub unsafe fn traverse<P, FI, FB, R>(
        &mut self,
        prot: &mut P,
        extra: &mut P,
        src_init: FI,
        mut step: FB,
    ) -> R
    where
        P: Protector,
        FI: for<'g> Fn(&'g CsGuard) -> P::Target<'g>,
        FB: for<'g> FnMut(P::Target<'g>, &'g CsGuard) -> (P::Target<'g>, Option<R>),
        R: Copy,
    {
        if unsafe { USE_ROLLBACK } {
            loop {
                if let Ok(r) = self.traverse_optim(prot, extra, &src_init, &mut step) {
                    return r;
                }
            }
        } else {
            loop {
                if let Ok(r) = self.traverse_naive(prot, extra, &src_init, &mut step) {
                    return r;
                }
            }
        }
    }

    #[inline]
    unsafe fn traverse_optim<P, FI, FB, R>(
        &mut self,
        prot: &mut P,
        extra: &mut P,
        src_init: &FI,
        step: &mut FB,
    ) -> Result<R, ()>
    where
        P: Protector,
        FI: for<'g> Fn(&'g CsGuard) -> P::Target<'g>,
        FB: for<'g> FnMut(P::Target<'g>, &'g CsGuard) -> (P::Target<'g>, Option<R>),
        R: Copy,
    {
        let src = self.critical_section(|guard| {
            let src = src_init(guard);
            extra.protect(&src);
            transmute::<P::Target<'_>, P::Target<'static>>(src)
        });
        let mut prots = [extra, prot];
        let mut ptrs = [src, P::Target::empty()];
        let backup_idx = AtomicUsize::new(0);

        let result = self.critical_section(|guard| {
            let mut bi = backup_idx.load(Ordering::Relaxed);
            if !ptrs[bi % 2].validate(guard) {
                return Err(());
            }

            // Safety: `curr` is validated successfully while `guard` is alive.
            let mut curr = transmute::<P::Target<'static>, P::Target<'_>>(ptrs[bi % 2]);

            for i in 1.. {
                let (next, done) = step(curr, guard);

                if done.is_some() || i % BACKUP_PERIOD == 0 {
                    prots[(bi + 1) % 2].protect(&next);
                    ptrs[(bi + 1) % 2] = transmute::<P::Target<'_>, P::Target<'static>>(next);
                    compiler_fence(Ordering::SeqCst);

                    bi += 1;
                    backup_idx.store(bi, Ordering::Relaxed);
                    compiler_fence(Ordering::SeqCst);
                }

                if let Some(r) = done {
                    return Ok(r);
                }
                curr = next;
            }
            unreachable!()
        });
        if backup_idx.load(Ordering::Relaxed) % 2 == 0 {
            let (p1, p2) = prots.split_at_mut(1);
            P::swap(p1[0], p2[0]);
        }
        result
    }

    #[inline]
    unsafe fn traverse_naive<P, FI, FB, R>(
        &mut self,
        prot: &mut P,
        extra: &mut P,
        src_init: &FI,
        step: &mut FB,
    ) -> Result<R, ()>
    where
        P: Protector,
        FI: for<'g> Fn(&'g CsGuard) -> P::Target<'g>,
        FB: for<'g> FnMut(P::Target<'g>, &'g CsGuard) -> (P::Target<'g>, Option<R>),
        R: Copy,
    {
        let src = self.critical_section(|guard| {
            let src = src_init(guard);
            extra.protect(&src);
            transmute::<P::Target<'_>, P::Target<'static>>(src)
        });
        let mut prots = [extra, prot];
        let mut ptrs = [src, P::Target::empty()];
        let mut backup_idx = 0;

        let result = loop {
            let result = self.critical_section(|guard| {
                if !ptrs[backup_idx % 2].validate(guard) {
                    return Err(false);
                }

                // Safety: `curr` is validated successfully while `guard` is alive.
                let mut curr = transmute::<P::Target<'static>, P::Target<'_>>(ptrs[backup_idx % 2]);
                let mut done = None;

                for i in 0.. {
                    (curr, done) = step(curr, guard);
                    if done.is_some() || (i >= BACKUP_PERIOD && curr.validate(guard)) {
                        break;
                    }
                }
                prots[(backup_idx + 1) % 2].protect(&curr);
                ptrs[(backup_idx + 1) % 2] = transmute::<P::Target<'_>, P::Target<'static>>(curr);
                if let Some(r) = done {
                    return Ok(r);
                }
                Err(true)
            });
            backup_idx += 1;

            match result {
                Ok(r) => break r,
                Err(false) => return Err(()),
                Err(true) => continue,
            }
        };

        if backup_idx % 2 == 0 {
            let (p1, p2) = prots.split_at_mut(1);
            P::swap(p1[0], p2[0]);
        }
        Ok(result)
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

    pub(crate) unsafe fn do_reclamation(&mut self, mut deferred: Vec<Deferred>) -> Vec<Deferred> {
        let deferred_len = deferred.len();
        if deferred_len < 256 {
            return deferred;
        }

        fence(Ordering::SeqCst);

        let mut guarded = vec![false; deferred_len];
        deferred.sort_unstable_by_key(|d| d.data());

        for g in self.local_mut().iter_guarded_ptrs() {
            if let Ok(idx) = deferred.binary_search_by_key(&g, |d| d.data()) {
                guarded[idx] = true;
            }
        }

        let not_freed: Vec<Deferred> = deferred
            .into_iter()
            .enumerate()
            .filter_map(|(i, element)| {
                if guarded[i] {
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
    unsafe fn retire<T>(&mut self, ptr: Shared<T>) {
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
        fence(Ordering::SeqCst);
        let result = self.rb.atomic(|_| body(&mut RaGuard { local: self.local }));
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
}

impl Handle for RaGuard {}

impl RollbackProof for RaGuard {
    unsafe fn retire<T>(&mut self, ptr: Shared<T>) {
        ManuallyDrop::new(Thread { local: self.local }).retire(ptr);
    }
}

/// A dummy guard that do not provide an epoch protection.
/// With this guard, one can perform any operations on atomic pointers.
/// Of course, it is `unsafe`. However, it is useful when it is guaranteed that
/// an exclusive ownership of a portion of data is acquired (e.g. `drop` of a
/// concurrent data structure).
pub struct Unprotected;

impl Unprotected {
    /// Creates a dummy guard that do not provide an epoch protection.
    ///
    /// # Safety
    ///
    /// Use this dummy guard only when you have an exclusive ownership of a data
    /// you are operating on.
    pub unsafe fn new() -> Self {
        Self
    }
}

impl Handle for Unprotected {}

impl RollbackProof for Unprotected {
    unsafe fn retire<T>(&mut self, ptr: Shared<T>) {
        free::<T>(ptr.untagged().as_raw() as *const u8 as *mut u8);
    }
}
