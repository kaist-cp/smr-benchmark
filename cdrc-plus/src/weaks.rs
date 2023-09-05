use std::{
    marker::PhantomData,
    mem::{self, forget, replace},
    sync::atomic::AtomicUsize,
};

use atomic::{Atomic, Ordering};
use static_assertions::const_assert;

use crate::{Guard, Pointer, Rc, Snapshot, StrongPtr, Tagged, TaggedCnt, TaggedSnapshot};

/// A result of unsuccessful `compare_exchange`.
///
/// It returns the ownership of [`Weak`] pointer which was given as a parameter.
pub struct CompareExchangeErrorWeak<T, P> {
    /// The `desired` pointer which was given as a parameter of `compare_exchange`.
    pub desired: P,
    /// The actual pointer value inside the atomic pointer.
    pub actual: TaggedCnt<T>,
}

pub struct AtomicWeak<T, G: Guard> {
    pub(crate) link: Atomic<TaggedCnt<T>>,
    _marker: PhantomData<G>,
}

unsafe impl<T, G: Guard> Send for AtomicWeak<T, G> {}
unsafe impl<T, G: Guard> Sync for AtomicWeak<T, G> {}

// Ensure that TaggedPtr<T> is 8-byte long,
// so that lock-free atomic operations are possible.
const_assert!(Atomic::<TaggedCnt<u8>>::is_lock_free());
const_assert!(mem::size_of::<TaggedCnt<u8>>() == mem::size_of::<usize>());
const_assert!(mem::size_of::<Atomic<TaggedCnt<u8>>>() == mem::size_of::<AtomicUsize>());

impl<T, G: Guard> AtomicWeak<T, G> {
    #[inline(always)]
    pub fn null() -> Self {
        Self {
            link: Atomic::new(Tagged::null()),
            _marker: PhantomData,
        }
    }

    /// Swap the currently stored shared pointer with the given shared pointer.
    /// This operation is thread-safe.
    /// (It is equivalent to `exchange` from the original implementation.)
    #[inline(always)]
    pub fn swap(&self, new: Weak<T, G>, order: Ordering, _: &G) -> Weak<T, G> {
        let new_ptr = new.into_raw();
        Weak::from_raw(self.link.swap(new_ptr, order))
    }

    /// Atomically compares the underlying pointer with expected, and if they refer to
    /// the same managed object, replaces the current pointer with a copy of desired
    /// (incrementing its reference count) and returns true. Otherwise, returns false.
    #[inline(always)]
    pub fn compare_exchange<'g, P>(
        &self,
        expected: TaggedCnt<T>,
        desired: P,
        success: Ordering,
        failure: Ordering,
        _: &'g G,
    ) -> Result<Weak<T, G>, CompareExchangeErrorWeak<T, P>>
    where
        P: WeakPtr<T, G> + Pointer<T>,
    {
        match self
            .link
            .compare_exchange(expected, desired.as_ptr(), success, failure)
        {
            Ok(_) => {
                let weak = Weak::from_raw(expected);
                // Here, `into_weak_count` increment the reference count of `desired` only if
                // `desired` is `Snapshot` or its variants.
                //
                // If `desired` is `Rc`, semantically the ownership of the reference count from
                // `desired` is moved to `self`. Because of this reason, we must skip decrementing
                // the reference count of `desired`.
                desired.into_weak_count();
                Ok(weak)
            }
            Err(e) => Err(CompareExchangeErrorWeak { desired, actual: e }),
        }
    }

    #[inline(always)]
    pub fn fetch_or<'g>(&self, tag: usize, order: Ordering, _: &'g G) -> TaggedCnt<T> {
        // HACK: The size and alignment of `Atomic<TaggedCnt<T>>` will be same with `AtomicUsize`.
        // The equality of the sizes is checked by `const_assert!`.
        let link = unsafe { &*(&self.link as *const _ as *const AtomicUsize) };
        let prev = link.fetch_or(tag, order);
        TaggedCnt::new(prev as *mut _)
    }
}

impl<T, G: Guard> From<Weak<T, G>> for AtomicWeak<T, G> {
    fn from(value: Weak<T, G>) -> Self {
        let init_ptr = value.into_raw();
        Self {
            link: Atomic::new(init_ptr),
            _marker: PhantomData,
        }
    }
}

impl<T, G: Guard> Drop for AtomicWeak<T, G> {
    #[inline(always)]
    fn drop(&mut self) {
        let ptr = self.link.load(Ordering::SeqCst);
        unsafe {
            if let Some(cnt) = ptr.untagged().as_mut() {
                let guard = G::new();
                guard.delayed_decrement_weak_cnt(cnt);
            }
        }
    }
}

impl<T, G: Guard> Default for AtomicWeak<T, G> {
    #[inline(always)]
    fn default() -> Self {
        Self::null()
    }
}

pub struct Weak<T, G: Guard> {
    ptr: TaggedCnt<T>,
    _marker: PhantomData<G>,
}

impl<T, G: Guard> Weak<T, G> {
    #[inline(always)]
    pub fn null() -> Self {
        Self::from_raw(TaggedCnt::null())
    }

    #[inline(always)]
    pub(crate) fn from_raw(ptr: TaggedCnt<T>) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn from_strong<'g, P>(ptr: &P, guard: &'g G) -> Self
    where
        P: StrongPtr<T, G> + Pointer<T>,
    {
        unsafe {
            if let Some(cnt) = ptr.as_ptr().untagged().as_ref() {
                if guard.increment_weak_cnt(cnt) {
                    return Self {
                        ptr: ptr.as_ptr(),
                        _marker: PhantomData,
                    };
                }
            }
        }
        Self::null()
    }

    #[inline(always)]
    pub fn clone(&self, guard: &G) -> Self {
        let weak = Self {
            ptr: self.ptr,
            _marker: PhantomData,
        };
        unsafe {
            if let Some(cnt) = weak.ptr.untagged().as_ref() {
                guard.increment_weak_cnt(cnt);
            }
        }
        weak
    }

    #[inline]
    pub fn finalize(self, guard: &G) {
        unsafe {
            if let Some(cnt) = self.ptr.untagged().as_mut() {
                guard.delayed_decrement_weak_cnt(cnt);
            }
        }
        // Prevent recursive finalizing.
        forget(self);
    }

    #[inline(always)]
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    #[inline]
    pub fn upgrade(&self) -> Rc<T, G> {
        todo!()
    }

    #[inline(always)]
    pub fn ref_count(&self) -> u32 {
        unsafe { self.ptr.deref().ref_count() }
    }

    #[inline(always)]
    pub fn weak_count(&self) -> u32 {
        unsafe { self.ptr.deref().weak_count() }
    }

    #[inline(always)]
    pub fn tag(&self) -> usize {
        self.ptr.tag()
    }

    #[inline(always)]
    pub fn untagged(mut self) -> Self {
        self.ptr = TaggedCnt::new(self.ptr.untagged());
        self
    }

    #[inline(always)]
    pub fn with_tag(mut self, tag: usize) -> Self {
        self.ptr.set_tag(tag);
        self
    }

    pub(crate) fn into_raw(self) -> TaggedCnt<T> {
        let new_ptr = self.as_ptr();
        // Skip decrementing the ref count.
        forget(self);
        new_ptr
    }
}

impl<T, G: Guard> Drop for Weak<T, G> {
    #[inline(always)]
    fn drop(&mut self) {
        if !self.is_null() {
            replace(self, Weak::null()).finalize(&G::new());
        }
    }
}

impl<T, G: Guard> PartialEq for Weak<T, G> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl<T, G: Guard> Pointer<T> for Weak<T, G> {
    fn as_ptr(&self) -> TaggedCnt<T> {
        self.ptr
    }
}

pub trait WeakPtr<T, G> {
    /// Consumes the aquired pointer, incrementing the reference count if we didn't increment
    /// it before.
    ///
    /// Semantically, it is equivalent to giving ownership of a reference count outside the
    /// environment.
    ///
    /// For example, we do nothing but forget its ownership if the pointer is [`Weak`],
    /// but increment the reference count if the pointer is [`Snapshot`].
    fn into_weak_count(self);
}

impl<T, G: Guard> WeakPtr<T, G> for Weak<T, G> {
    fn into_weak_count(self) {
        // As we have a reference count already, we don't have to do anything, but
        // prevent calling a destructor which decrements it.
        forget(self);
    }
}

impl<T, G: Guard> WeakPtr<T, G> for Snapshot<T, G> {
    fn into_weak_count(self) {
        if let Some(cnt) = unsafe { self.as_ptr().untagged().as_ref() } {
            cnt.add_ref();
        }
    }
}

impl<T, G: Guard> WeakPtr<T, G> for &Snapshot<T, G> {
    fn into_weak_count(self) {
        if let Some(cnt) = unsafe { self.as_ptr().untagged().as_ref() } {
            cnt.add_ref();
        }
    }
}

impl<'s, T, G: Guard> WeakPtr<T, G> for TaggedSnapshot<'s, T, G> {
    fn into_weak_count(self) {
        if let Some(cnt) = unsafe { self.as_ptr().untagged().as_ref() } {
            cnt.add_ref();
        }
    }
}
