use core::{cell::UnsafeCell, mem, sync::atomic::AtomicU32};
use std::{mem::ManuallyDrop, sync::atomic::compiler_fence, sync::atomic::Ordering};

use crate::{pin, Writable};

/// A wait-free atomic counter that supports increment and decrement,
/// such that attempting to increment the counter from zero fails and
/// does not perform the increment.
///
/// Useful for implementing reference counting, where the underlying
/// managed memory is freed when the counter hits zero, so that other
/// racing threads can not increment the counter back up from zero
///
/// Assumption: The counter should never go negative. That is, the
/// user should never decrement the counter by an amount greater
/// than its current value
///
/// Note: The counter steals the top two bits of the integer for book-
/// keeping purposes. Hence the maximum representable value in the
/// counter is 2^(8*32-2) - 1
#[derive(Debug)]
pub(crate) struct StickyCounter {
    x: AtomicU32,
}

impl StickyCounter {
    #[inline(always)]
    const fn zero_flag() -> u32 {
        1 << (mem::size_of::<u32>() * 8 - 1)
    }

    #[inline(always)]
    const fn zero_pending_flag() -> u32 {
        1 << (mem::size_of::<u32>() * 8 - 2)
    }

    #[inline(always)]
    pub fn new() -> Self {
        Self {
            x: AtomicU32::new(1),
        }
    }

    /// Increment the counter by the given amount if the counter is not zero.
    ///
    /// Returns true if the increment was successful, i.e., the counter
    /// was not stuck at zero. Returns false if the counter was zero
    #[inline(always)]
    pub fn increment(&self, add: u32, order: Ordering) -> bool {
        let val = self.x.fetch_add(add, order);
        (val & Self::zero_flag()) == 0
    }

    /// Decrement the counter by the given amount. The counter must initially be
    /// at least this amount, i.e., it is not permitted to decrement the counter
    /// to a negative number.
    ///
    /// Returns true if the counter was decremented to zero. Returns
    /// false if the counter was not decremented to zero
    #[inline(always)]
    pub fn decrement(&self, sub: u32, order: Ordering) -> bool {
        if self.x.fetch_sub(sub, order) == sub {
            match self
                .x
                .compare_exchange(0, Self::zero_flag(), Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return true,
                Err(actual) => {
                    return ((actual & Self::zero_pending_flag()) > 0)
                        && ((self.x.swap(Self::zero_flag(), Ordering::SeqCst)
                            & Self::zero_pending_flag())
                            > 0)
                }
            }
        }
        false
    }

    /// Loads the current value of the counter. If the current value is zero, it is guaranteed
    /// to remain zero until the counter is reset
    #[inline(always)]
    pub fn load(&self, order: Ordering) -> u32 {
        let val = self.x.load(order);
        if val != 0 {
            return if (val & Self::zero_flag()) > 0 {
                0
            } else {
                val
            };
        }

        match self.x.compare_exchange(
            val,
            Self::zero_flag() | Self::zero_pending_flag(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => 0,
            Err(actual) => {
                if (actual & Self::zero_flag()) > 0 {
                    0
                } else {
                    actual
                }
            }
        }
    }
}

impl From<u32> for StickyCounter {
    #[inline(always)]
    fn from(value: u32) -> Self {
        Self {
            x: AtomicU32::new(if value == 0 { Self::zero_flag() } else { value }),
        }
    }
}

#[derive(Debug)]
pub(crate) enum EjectAction {
    Nothing,
    Delay,
    Destroy,
}

/// An instance of an object of type T with an atomic reference count.
#[derive(Debug)]
pub struct Counted<T> {
    storage: UnsafeCell<ManuallyDrop<T>>,
    ref_cnt: StickyCounter,
    weak_cnt: StickyCounter,
}

impl<T> Counted<T> {
    #[inline(always)]
    pub(crate) fn new(val: T) -> Self {
        Self {
            storage: UnsafeCell::new(ManuallyDrop::new(val)),
            ref_cnt: StickyCounter::new(),
            weak_cnt: StickyCounter::new(),
        }
    }

    #[inline(always)]
    pub(crate) fn data(&self) -> &T {
        unsafe { &*self.storage.get() }
    }

    /// Destroy the managed object, but keep the control data intact
    #[inline(always)]
    pub(crate) unsafe fn dispose(&self) {
        ManuallyDrop::drop(&mut *self.storage.get());
    }

    #[inline(always)]
    pub(crate) fn use_count(&self) -> u32 {
        self.ref_cnt.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub(crate) fn weak_count(&self) -> u32 {
        self.weak_cnt.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub(crate) fn add_refs(&self, count: u32) -> bool {
        self.ref_cnt.increment(count, Ordering::SeqCst)
    }

    /// Release strong references to the object. If the strong reference count reaches zero,
    /// the managed object will be destroyed, and the weak reference count will be decremented
    /// by one. If this causes the weak reference count to hit zero, returns true, indicating
    /// that the caller should delete this object.
    #[inline(always)]
    pub(crate) fn release_refs<G: Writable>(&self, count: u32, _: &G) -> EjectAction {
        // A decrement-release + an acquire fence is recommended by Boost's documentation:
        // https://www.boost.org/doc/libs/1_57_0/doc/html/atomic/usage_examples.html
        // Alternatively, an acquire-release decrement would work, but might be less efficient since the
        // acquire is only relevant if the decrement zeros the counter.
        if self.ref_cnt.decrement(count, Ordering::Release) {
            compiler_fence(Ordering::Acquire);
            // If there are no live weak pointers, we can immediately destroy
            // everything. Otherwise, we have to defer the disposal of the
            // managed object since an atomic_weak_ptr might be about to
            // take a snapshot...
            if self.weak_cnt.load(Ordering::Relaxed) == 1 {
                // Immediately destroy the managed object and
                // collect the control data, since no more
                // live (strong or weak) references exist
                unsafe { self.dispose() };
                EjectAction::Destroy
            } else {
                // At least one weak reference exists, so we have to
                // delay the destruction of the managed object
                EjectAction::Delay
            }
        } else {
            EjectAction::Nothing
        }
    }

    // TODO(@jeonghyeon): Implement weak ptr variants and remove `#[allow(unused)`.
    #[inline(always)]
    #[allow(unused)]
    pub(crate) fn add_weak_refs(&self, count: u32) -> bool {
        self.weak_cnt.increment(count, Ordering::Relaxed)
    }

    // Release weak references to the object. If this causes the weak reference count
    // to hit zero, returns true, indicating that the caller should delete this object.
    #[inline(always)]
    pub(crate) fn release_weak_refs<G: Writable>(&self, count: u32, _: &G) -> bool {
        self.weak_cnt.decrement(count, Ordering::Release)
    }

    pub(crate) fn into_owned(self) -> T {
        ManuallyDrop::into_inner(self.storage.into_inner())
    }
}

#[inline]
pub(crate) unsafe fn dispose<T, G: Writable>(ptr: *const Counted<T>, guard: &G) {
    debug_assert!((*ptr).use_count() == 0);
    (*ptr).dispose();
    if (*ptr).release_weak_refs(1, guard) {
        destory(ptr);
    }
}

#[inline]
pub(crate) unsafe fn destory<T>(ptr: *const Counted<T>) {
    debug_assert!((*ptr).use_count() == 0);
    drop(unsafe { Box::from_raw(ptr.cast_mut()) });
}

#[inline]
pub(crate) unsafe fn eject<T, G: Writable>(
    ptr: *const Counted<T>,
    ret_type: RetireType,
    guard: &G,
) {
    debug_assert!(!ptr.is_null());

    match ret_type {
        RetireType::DecrementStrongCount => decrement_ref_cnt(ptr, guard),
        RetireType::DecrementWeakCount => decrement_weak_cnt(ptr, guard),
        RetireType::Dispose => dispose(ptr, guard),
    }
}

#[inline]
pub(crate) unsafe fn decrement_ref_cnt<T, G: Writable>(ptr: *const Counted<T>, guard: &G) {
    debug_assert!(!ptr.is_null());
    let counted = unsafe { &*ptr };
    debug_assert!(counted.use_count() >= 1);

    match counted.release_refs(1, guard) {
        EjectAction::Nothing => {}
        EjectAction::Delay => retire(ptr, RetireType::Dispose, guard),
        EjectAction::Destroy => destory(ptr),
    }
}

#[inline]
pub(crate) unsafe fn decrement_weak_cnt<T, G: Writable>(ptr: *const Counted<T>, guard: &G) {
    debug_assert!(!ptr.is_null());
    let counted = unsafe { &*ptr };
    debug_assert!(counted.weak_count() >= 1);

    if counted.release_weak_refs(1, guard) {
        destory(ptr);
    }
}

#[inline]
pub(crate) unsafe fn delayed_decrement_ref_cnt<T, G: Writable>(ptr: *const Counted<T>, guard: &G) {
    debug_assert!((*ptr).use_count() >= 1);
    retire(ptr, RetireType::DecrementStrongCount, guard);
}

#[inline]
#[allow(unused)] // TODO(@jeonghyeon): Remove this after implementing weak pointers.
pub(crate) unsafe fn delayed_decrement_weak_cnt<T, G: Writable>(ptr: *const Counted<T>, guard: &G) {
    debug_assert!((*ptr).weak_count() >= 1);
    retire(ptr, RetireType::DecrementWeakCount, guard);
}

#[inline]
pub(crate) unsafe fn retire<T, G: Writable>(
    ptr: *const Counted<T>,
    ret_type: RetireType,
    guard: &G,
) {
    guard.defer(ptr, move |ptr| {
        eject(ptr, ret_type, &pin());
    });
}

pub(crate) enum RetireType {
    DecrementStrongCount,
    DecrementWeakCount,
    Dispose,
}
