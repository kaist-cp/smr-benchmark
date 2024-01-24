use std::{
    marker::PhantomData,
    mem::{align_of, forget, swap},
    ops::{Deref, DerefMut},
    sync::atomic::AtomicUsize,
};

use atomic::Ordering;

use crate::handle::{Handle, RollbackProof, Thread};
use crate::hazard::HazardPointer;

/// A result of unsuccessful `compare_exchange`.
pub struct CompareExchangeError<'g, T: ?Sized, P: Pointer> {
    /// The `new` pointer which was given as a parameter of `compare_exchange`.
    pub new: P,
    /// The actual pointer value inside the atomic pointer.
    pub actual: Shared<'g, T>,
}

pub struct Atomic<T> {
    link: AtomicUsize,
    _marker: PhantomData<*mut T>,
}

unsafe impl<T> Send for Atomic<T> {}
unsafe impl<T> Sync for Atomic<T> {}

impl<T> Atomic<T> {
    /// Allocates `init` on the heap and returns a new atomic pointer pointing to it.
    #[inline]
    pub fn new(init: T) -> Self {
        let ptr = Box::into_raw(Box::new(init));
        Self {
            link: AtomicUsize::new(ptr as _),
            _marker: PhantomData,
        }
    }

    /// Returns a new null atomic pointer.
    #[inline]
    pub const fn null() -> Self {
        Self {
            link: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    /// Stores a [`Shared`] pointer into the atomic pointer, returning the previous [`Shared`].
    #[inline]
    pub fn swap<G: RollbackProof>(&self, ptr: Shared<T>, order: Ordering, _: &G) -> Shared<T> {
        let prev = self.link.swap(ptr.inner, order);
        Shared::new(prev)
    }

    /// Stores a [`Shared`] pointer into the atomic pointer.
    #[inline]
    pub fn store<G: RollbackProof>(&self, ptr: Shared<T>, order: Ordering, _: &G) {
        self.link.store(ptr.inner, order);
    }

    /// Loads a [`Shared`] from the atomic pointer.
    #[inline]
    pub fn load<'r, G: Handle>(&self, order: Ordering, _: &G) -> Shared<'r, T> {
        let ptr = self.link.load(order);
        Shared::new(ptr)
    }

    /// Stores the pointer `new` into the atomic pointer if the current value is the
    /// same as `current`. The tag is also taken into account, so two pointers to the same object,
    /// but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success a
    /// [`Shared`] which is taken out from the atomic pointer is returned. On failure a
    /// [`CompareExchangeError`] which contains an actual value from the atomic pointer and
    /// the ownership of `new` pointer which was given as a parameter is returned.
    #[inline]
    pub fn compare_exchange<'g, P: Pointer, G: RollbackProof>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        _: &G,
    ) -> Result<Shared<T>, CompareExchangeError<'g, T, P>> {
        let current = current.inner;
        let new = new.into_usize();

        match self.link.compare_exchange(current, new, success, failure) {
            Ok(actual) => Ok(Shared::new(actual)),
            Err(actual) => Err(CompareExchangeError {
                new: unsafe { P::from_usize(new) },
                actual: Shared::new(actual),
            }),
        }
    }

    /// Bitwise "or" with the current tag.
    ///
    /// Performs a bitwise "or" operation on the current tag and the argument `tag`, and sets the
    /// new tag to the result. Returns the previous pointer.
    #[inline]
    pub fn fetch_or<'r, G: RollbackProof>(
        &self,
        tag: usize,
        order: Ordering,
        _: &'r G,
    ) -> Shared<'r, T> {
        Shared::new(self.link.fetch_or(decompose_data::<T>(tag).1, order))
    }

    /// Takes ownership of the pointee.
    ///
    /// This consumes the atomic and converts it into [`Owned`]. As [`Atomic`] doesn't have a
    /// destructor and doesn't drop the pointee while [`Owned`] does, this is suitable for
    /// destructors of data structures.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object.
    #[inline]
    pub unsafe fn into_owned(self) -> Owned<T> {
        Owned::from_usize(self.link.into_inner())
    }
}

impl<T> Default for Atomic<T> {
    fn default() -> Self {
        Self::null()
    }
}

/// A pointer to a shared object.
///
/// This pointer is valid for use only during the lifetime `'r`.
///
/// This is the most basic shared pointer type, which can be loaded directly from [`Atomic`].
/// Also it is worth noting that [`Shield`] can create a [`Shared`] which has a lifetime parameter
/// of the original pointer.
#[derive(Debug)]
pub struct Shared<'r, T: ?Sized> {
    inner: usize,
    _marker: PhantomData<(&'r (), *const T)>,
}

impl<'r, T> Default for Shared<'r, T> {
    fn default() -> Self {
        Self {
            inner: 0,
            _marker: PhantomData,
        }
    }
}

impl<'r, T> Clone for Shared<'r, T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'r, T> Copy for Shared<'r, T> {}

impl<'r, T> PartialEq for Shared<'r, T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl<'r, T> Eq for Shared<'r, T> {}

impl<'r, T> Shared<'r, T> {
    #[inline]
    pub(crate) fn new(ptr: usize) -> Self {
        Self {
            inner: ptr,
            _marker: PhantomData,
        }
    }

    /// Returns a new null shared pointer.
    #[inline]
    pub const fn null() -> Self {
        Self {
            inner: 0,
            _marker: PhantomData,
        }
    }

    /// Returns `true` if the pointer is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        decompose_data::<T>(self.inner).0 as usize == 0
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    ///
    /// It is possible to directly dereference a [`Shared`] if and only if the current context is
    /// in a critical section.
    ///
    /// # Safety
    ///
    /// The `self` must be a valid memory location.
    #[inline]
    pub unsafe fn as_ref(&self) -> Option<&'r T> {
        unsafe { decompose_data::<T>(self.inner).0.as_ref() }
    }

    /// Converts the pointer to a mutable reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    ///
    /// It is possible to directly dereference a [`Shared`] if and only if the current context is
    /// in a critical section.
    ///
    /// # Safety
    ///
    /// The `self` must be a valid memory location.
    #[inline]
    pub unsafe fn as_mut(&mut self) -> Option<&'r mut T> {
        unsafe { decompose_data::<T>(self.inner).0.as_mut() }
    }

    /// Converts the pointer to a reference, without guaranteeing any safety.
    ///
    /// # Safety
    ///
    /// The `self` must be a valid memory location.
    #[inline]
    pub unsafe fn deref(&self) -> &T {
        &*decompose_data::<T>(self.inner).0
    }

    /// Converts the pointer to a reference, without guaranteeing any safety.
    ///
    /// # Safety
    ///
    /// The `self` must be a valid memory location.
    #[inline]
    pub unsafe fn deref_mut(&mut self) -> &mut T {
        &mut *decompose_data::<T>(self.inner).0
    }

    /// Returns the tag stored within the pointer.
    #[inline]
    pub fn tag(&self) -> usize {
        decompose_data::<T>(self.inner).1
    }

    /// Returns the same pointer, but the tag bits are cleared.
    #[inline]
    pub fn untagged(&self) -> Self {
        Shared::new(decompose_data::<T>(self.inner).0 as usize)
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn with_tag(&self, tag: usize) -> Self {
        Shared::new(data_with_tag::<T>(self.inner, tag))
    }

    /// Returns the machine representation of the pointer, including tag bits.
    #[inline]
    pub fn as_raw(&self) -> usize {
        self.inner
    }

    /// Takes ownership of the pointee.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object.
    #[inline]
    pub unsafe fn into_owned(self) -> Owned<T> {
        Owned::from_usize(self.inner)
    }
}

/// An owned heap-allocated object.
///
/// This type is very similar to `Box<T>`.
pub struct Owned<T> {
    inner: usize,
    _marker: PhantomData<*const T>,
}

unsafe impl<T> Sync for Owned<T> {}
unsafe impl<T> Send for Owned<T> {}

impl<T> Deref for Owned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*decompose_data::<T>(self.inner).0 }
    }
}

impl<T> DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *decompose_data::<T>(self.inner).0 }
    }
}

impl<T> Owned<T> {
    /// Allocates `init` on the heap and returns a new owned pointer pointing to it.
    pub fn new(init: T) -> Self {
        Self {
            inner: Box::into_raw(Box::new(init)) as usize,
            _marker: PhantomData,
        }
    }

    /// Returns the machine representation of the pointer, including tag bits.
    pub fn as_raw(&self) -> usize {
        self.inner
    }

    /// Returns the tag stored within the pointer.
    #[inline]
    pub fn tag(&self) -> usize {
        decompose_data::<T>(self.inner).1
    }

    /// Returns the same pointer, but the tag bits are cleared.
    #[inline]
    pub fn untagged(self) -> Self {
        self.with_tag(0)
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn with_tag(self, tag: usize) -> Self {
        let result = Self {
            inner: data_with_tag::<T>(self.inner, tag),
            _marker: PhantomData,
        };
        forget(self);
        result
    }

    /// Converts the owned pointer into a [`Shared`].
    #[inline]
    pub fn into_shared<'r>(self) -> Shared<'r, T> {
        unsafe { Shared::from_usize(self.into_usize()) }
    }
}

impl<T> Drop for Owned<T> {
    fn drop(&mut self) {
        drop(unsafe { Box::from_raw(decompose_data::<T>(self.inner).0) });
    }
}

/// A pointer to a shared object, which is protected by a hazard pointer.
pub struct Shield<T> {
    hazptr: HazardPointer,
    inner: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T> Sync for Shield<T> {}

impl<T> Shield<T> {
    #[inline]
    pub fn null(thread: &mut Thread) -> Self {
        if let Some(local) = thread.as_local_mut() {
            Self {
                hazptr: HazardPointer::new(local),
                inner: 0,
                _marker: PhantomData,
            }
        } else {
            panic!("Cannot create a `Shield` from an unprotected `Thread`.")
        }
    }

    /// Stores the given pointer in a hazard slot without any validations.
    ///
    /// Just storing a pointer in a hazard slot doesn't guarantee that the pointer is
    /// truly protected, as the memory block may already be reclaimed. We must validate whether
    /// the memory block is reclaimed or not, by reloading the atomic pointer or checking the
    /// local RRCU epoch.
    #[inline]
    pub fn protect(&mut self, ptr: Shared<'_, T>) {
        let raw = ptr.untagged().as_raw();
        self.hazptr
            .protect_raw(raw as *const T as *mut T, Ordering::Release);
        self.inner = ptr.inner;
    }

    /// Stores the given pointer only in the inner allocation **without any protections**.
    /// It is usually faster than `protect` because it skips an indirection to the hazard slot.
    /// (Of course however, it is `unsafe`.)
    ///
    /// # Safety
    ///
    /// The given pointer must be either
    ///
    /// 1. a null pointer, or
    /// 2. a dereferencable location at least while this `Shield` is alive.
    #[inline]
    pub unsafe fn store_wo_prot(&mut self, ptr: Shared<'_, T>) {
        self.inner = ptr.inner;
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    #[inline]
    pub fn as_ref(&self) -> Option<&T> {
        unsafe { decompose_data::<T>(self.inner).0.as_ref() }
    }

    /// Converts the pointer to a reference.
    ///
    /// # Safety
    ///
    /// The given pointer must be a valid memory location.
    #[inline]
    pub unsafe fn deref(&self) -> &T {
        &*decompose_data::<T>(self.inner).0
    }

    /// Converts the pointer to a mutable reference.
    ///
    /// # Safety
    ///
    /// The given pointer must be a valid memory location.
    #[inline]
    pub unsafe fn deref_mut(&mut self) -> &mut T {
        &mut *decompose_data::<T>(self.inner).0
    }

    /// Converts the pointer to a mutable reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    #[inline]
    pub fn as_mut(&self) -> Option<&mut T> {
        unsafe { decompose_data::<T>(self.inner).0.as_mut() }
    }

    /// Returns `true` if the protected pointer is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        decompose_data::<T>(self.inner).0 as usize == 0
    }

    /// Returns the tag stored within the shield.
    #[inline]
    pub fn tag(&self) -> usize {
        decompose_data::<T>(self.inner).1
    }

    /// Changes the tag bits to `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn set_tag(&mut self, tag: usize) {
        self.inner = data_with_tag::<T>(self.inner, tag);
    }

    /// Releases the inner hazard pointer.
    #[inline]
    pub fn release(&mut self) {
        self.inner = 0;
        self.hazptr.reset_protection();
    }

    /// Returns the machine representation of the pointer, including tag bits.
    #[inline]
    pub fn as_raw(&self) -> usize {
        self.inner
    }

    /// Creates a `Shared` pointer whose lifetime is equal to `self`.
    #[inline]
    pub fn shared(&self) -> Shared<T> {
        Shared::new(self.inner)
    }

    #[inline]
    pub fn swap(a: &mut Self, b: &mut Self) {
        HazardPointer::swap(&mut a.hazptr, &mut b.hazptr);
        swap(&mut a.inner, &mut b.inner);
    }
}

/// A trait for either `Owned` or `Shared` pointers.
pub trait Pointer {
    /// Returns the machine representation of the pointer.
    fn into_usize(self) -> usize;

    /// Returns a new pointer pointing to the tagged pointer `data`.
    ///
    /// # Safety
    ///
    /// The given `data` should have been created by `Pointer::into_usize()`, and one `data` should
    /// not be converted back by `Pointer::from_usize()` multiple times.
    unsafe fn from_usize(data: usize) -> Self;
}

impl<'r, T> Pointer for Shared<'r, T> {
    #[inline]
    fn into_usize(self) -> usize {
        self.as_raw()
    }

    #[inline]
    unsafe fn from_usize(data: usize) -> Self {
        Self::new(data)
    }
}

impl<T> Pointer for Owned<T> {
    fn into_usize(self) -> usize {
        let inner = self.inner;
        forget(self);
        inner
    }

    unsafe fn from_usize(data: usize) -> Self {
        Self {
            inner: data,
            _marker: PhantomData,
        }
    }
}

/// Panics if the pointer is not properly unaligned.
#[inline]
pub fn ensure_aligned<T>(raw: *const T) {
    assert_eq!(raw as usize & low_bits::<T>(), 0, "unaligned pointer");
}

/// Returns a bitmask containing the unused least significant bits of an aligned pointer to `T`.
#[inline]
pub fn low_bits<T>() -> usize {
    (1 << align_of::<T>().trailing_zeros()) - 1
}

/// Given a tagged pointer `data`, returns the same pointer, but tagged with `tag`.
///
/// `tag` is truncated to fit into the unused bits of the pointer to `T`.
#[inline]
pub fn data_with_tag<T>(data: usize, tag: usize) -> usize {
    (data & !low_bits::<T>()) | (tag & low_bits::<T>())
}

/// Decomposes a tagged pointer `data` into the pointer and the tag.
#[inline]
pub fn decompose_data<T>(data: usize) -> (*mut T, usize) {
    let raw = (data & !low_bits::<T>()) as *mut T;
    let tag = data & low_bits::<T>();
    (raw, tag)
}

/// Returns a highest tag bit in a memory representation of `T`.
#[inline]
pub fn highest_tag<T>() -> usize {
    1 << (usize::BITS - low_bits::<T>().leading_zeros() - 1)
}
