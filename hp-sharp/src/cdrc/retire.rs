use std::mem::ManuallyDrop;

use crate::{rrcu::Deferrable, Deferred, Thread, RaGuard, Invalidate, CsGuard};

use super::counted::{Counted, EjectAction};

pub enum RetireType {
    DecrementStrongCount,
    Dispose,
}

pub trait RetireRc {
    /// Defers decrementing the reference count of the given pointer.
    ///
    /// # Safety
    ///
    /// The given pointer must be a valid memory location which can be dereferenced.
    unsafe fn defer_decr<'r, T>(&mut self, counted: &Counted<T>, ret_type: RetireType);

    #[inline]
    fn dispose<T>(&mut self, counted: &Counted<T>) {
        debug_assert!(counted.use_count() == 0);
        unsafe { counted.dispose() };
        if counted.release_weak_refs(1) {
            self.destory(counted);
        }
    }

    #[inline]
    fn destory<T>(&mut self, counted: &Counted<T>) {
        debug_assert!(counted.use_count() == 0);
        drop(unsafe { Box::from_raw((counted as *const Counted<T>).cast_mut()) });
    }

    #[inline]
    fn eject<T>(&mut self, counted: &Counted<T>, ret_type: RetireType) {
        debug_assert!(!(counted as *const Counted<T>).is_null());

        match ret_type {
            RetireType::DecrementStrongCount => self.decrement_ref_cnt(counted),
            RetireType::Dispose => self.dispose(counted),
        }
    }

    #[inline]
    fn decrement_ref_cnt<T>(&mut self, counted: &Counted<T>) {
        debug_assert!(!(counted as *const Counted<T>).is_null());
        debug_assert!(counted.use_count() >= 1);

        match counted.release_refs(1) {
            EjectAction::Nothing => {}
            EjectAction::Delay => unsafe { self.defer_decr(counted, RetireType::Dispose) },
            EjectAction::Destroy => self.destory(counted),
        }
    }

    #[inline]
    fn delayed_decrement_ref_cnt<T>(&mut self, counted: &Counted<T>) {
        debug_assert!(counted.use_count() >= 1);
        unsafe { self.defer_decr(counted, RetireType::DecrementStrongCount) };
    }
}

impl RetireRc for Thread {
    unsafe fn defer_decr<'r, T>(&mut self, counted: &Counted<T>, ret_type: RetireType) {
        let collected = self.defer(Deferred::new(
            counted as *const _ as *mut u8,
            match ret_type {
                RetireType::DecrementStrongCount => decrement_ref_cnt::<T>,
                RetireType::Dispose => dispose::<T>,
            },
        ));

        if let Some(collected) = collected {
            self.retire_inner(collected);
        }
    }
}

impl RetireRc for RaGuard {
    unsafe fn defer_decr<'r, T>(&mut self, counted: &Counted<T>, ret_type: RetireType) {
        let collected = self.defer(Deferred::new(
            counted as *const _ as *mut u8,
            match ret_type {
                RetireType::DecrementStrongCount => decrement_ref_cnt::<T>,
                RetireType::Dispose => dispose::<T>,
            },
        ));

        if let Some(collected) = collected {
            ManuallyDrop::new(Thread { local: self.local }).retire_inner(collected);
        }
    }
}

impl<T> Invalidate for Counted<T> {
    fn invalidate(&self) {
        return;
    }

    fn is_invalidated(&self, _: &CsGuard) -> bool {
        false
    }
}

unsafe fn decrement_ref_cnt<T>(ptr: *mut u8) {
    let counted = &*(ptr as *mut Counted<T>);
    let mut guard = unsafe { Thread::unprotected() };
    guard.decrement_ref_cnt(counted);
}

unsafe fn dispose<T>(ptr: *mut u8) {
    let counted = &*(ptr as *mut Counted<T>);
    let mut guard = unsafe { Thread::unprotected() };
    guard.dispose(counted);
}
