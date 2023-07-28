use std::{ptr::null_mut, sync::atomic::AtomicPtr};

use atomic::{fence, Ordering};

use crate::Thread;

/// A low-level owner of hazard pointer slot.
///
/// A `Shield` owns a `HazardPointer` as its field.
pub(crate) struct HazardPointer {
    thread: *const Thread,
    idx: usize,
}

impl HazardPointer {
    /// Creates a hazard pointer in the given thread.
    pub fn new(thread: &mut Thread) -> Self {
        let idx = thread.acquire();
        Self { thread, idx }
    }

    #[inline]
    fn slot(&self) -> &AtomicPtr<u8> {
        unsafe {
            let array = &*(*(*self.thread).local).hazptrs.load(Ordering::Relaxed);
            array.get_unchecked(self.idx)
        }
    }

    /// Protect the given address.
    #[inline]
    pub fn protect_raw<T>(&self, ptr: *mut T, order: Ordering) {
        self.slot().store(ptr as *mut u8, order);
    }

    /// Release the protection awarded by this hazard pointer, if any.
    #[inline]
    pub fn reset_protection(&self) {
        self.slot().store(null_mut(), Ordering::Release);
    }

    /// Check if `src` still points to `pointer`. If not, returns the current value.
    ///
    /// For a pointer `p`, if "`src` still pointing to `pointer`" implies that `p` is not retired,
    /// then `Ok(())` means that shields set to `p` are validated.
    #[inline]
    pub fn validate<T>(pointer: *mut T, src: &AtomicPtr<T>) -> Result<(), *mut T> {
        fence(Ordering::SeqCst);
        let new = src.load(Ordering::Acquire);
        if pointer == new {
            Ok(())
        } else {
            Err(new)
        }
    }

    /// Try protecting `pointer` obtained from `src`. If not, returns the current value.
    ///
    /// If "`src` still pointing to `pointer`" implies that `pointer` is not retired, then `Ok(())`
    /// means that this shield is validated.
    #[inline]
    pub fn try_protect<T>(&self, pointer: *mut T, src: &AtomicPtr<T>) -> Result<(), *mut T> {
        self.protect_raw(pointer, Ordering::Release);
        Self::validate(pointer, src)
    }

    /// Get a protected pointer from `src`.
    ///
    /// See `try_protect()`.
    #[inline]
    pub fn protect<T>(&self, src: &AtomicPtr<T>) -> *mut T {
        let mut pointer = src.load(Ordering::Relaxed);
        while let Err(new) = self.try_protect(pointer, src) {
            pointer = new;
        }
        pointer
    }
}

impl Drop for HazardPointer {
    fn drop(&mut self) {
        self.reset_protection();
        unsafe { (*self.thread.cast_mut()).release(self.idx) };
    }
}
