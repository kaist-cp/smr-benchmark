use std::{
    marker::PhantomData,
    mem,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    crcu::{Guard, LocalHandle, Writable},
    hazard::HazardPointer,
};

/// A result of unsuccessful `compare_exchange`.
pub struct CompareExchangeError<'r, T> {
    /// The `desired` pointer which was given as a parameter of `compare_exchange`.
    pub desired: Shared<'r, T>,
    /// The actual pointer value inside the atomic pointer.
    pub actual: Shared<'r, T>,
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
    pub fn swap<G: Writable>(&self, ptr: Shared<T>, ord: Ordering, _: &G) -> Shared<T> {
        todo!()
    }

    /// Loads a [`Shared`] from the atomic pointer. This can be called only in a read phase.
    #[inline]
    pub fn load<'r>(&self, ord: Ordering, _: &'r Guard) -> Shared<'r, T> {
        todo!()
    }

    /// Stores the pointer `desired` into the atomic pointer if the current value is the
    /// same as `expected`. The tag is also taken into account, so two pointers to the same object,
    /// but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success a
    /// [`Shared`] which is taken out from the atomic pointer is returned. On failure a
    /// [`CompareExchangeError`] which contains an actual value from the atomic pointer and
    /// the ownership of `desired` pointer which was given as a parameter is returned.
    #[inline]
    pub fn compare_exchange<'p, 'r, G: Writable>(
        &self,
        expected: Shared<'p, T>,
        desired: Shared<'r, T>,
        success: Ordering,
        failure: Ordering,
        _: &'r G,
    ) -> Result<Shared<T>, CompareExchangeError<'r, T>> {
        todo!()
    }

    /// Bitwise "or" with the current tag.
    ///
    /// Performs a bitwise "or" operation on the current tag and the argument `tag`, and sets the
    /// new tag to the result. Returns the previous pointer.
    #[inline]
    pub fn fetch_or<'r, G: Writable>(&self, tag: usize, ord: Ordering, _: &'r G) -> Shared<'r, T> {
        todo!()
    }
}

impl<T> Drop for Atomic<T> {
    fn drop(&mut self) {
        todo!()
    }
}

/// A pointer to an shared object.
///
/// This pointer is valid for use only during the lifetime `'r`.
///
/// This is the most basic shared pointer type, which can be loaded directly from [`Atomic`].
/// Also it is worth noting that [`Shield`] can create a [`Shared`] which has a lifetime parameter
/// of the original pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shared<'r, T> {
    inner: usize,
    _marker: PhantomData<&'r T>,
}

impl<'r, T> Shared<'r, T> {
    #[inline]
    pub(crate) fn new(ptr: usize) -> Self {
        todo!()
    }

    /// Returns a new null shared pointer.
    #[inline]
    pub fn null() -> Self {
        todo!()
    }

    /// Returns `true` if the pointer is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        todo!()
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    ///
    /// It is possible to directly dereference a [`Shared`] if and only if the current context is
    /// in a read phase which can be started by `read` and `read_loop` method.
    #[inline]
    pub fn as_ref(&self, _: &Guard) -> Option<&'r T> {
        todo!()
    }

    /// Returns the tag stored within the pointer.
    #[inline]
    pub fn tag(&self) -> usize {
        todo!()
    }

    /// Returns the same pointer, but the tag bits are cleared.
    #[inline]
    pub fn untagged(&self) -> Self {
        todo!()
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn with_tag(&self, tag: usize) -> Self {
        todo!()
    }
}

/// A pointer to an shared object, which is protected by a hazard pointer.
///
/// It prevents the reference count decreasing.
pub struct Shield<T> {
    hazptr: HazardPointer,
    inner: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T> Sync for Shield<T> {}

impl<T> Shield<T> {
    /// Returns a new null [`Shield`].
    #[inline]
    pub fn null(guard: &Guard) -> Self {
        todo!()
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    #[inline]
    pub fn as_ref<'s>(&'s self) -> Option<&'s T> {
        todo!()
    }

    /// Converts the pointer to a mutable reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    #[inline]
    pub fn as_mut<'s>(&'s self) -> Option<&'s mut T> {
        todo!()
    }

    /// Returns `true` if the defended pointer is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        todo!()
    }

    /// Returns the tag stored within the shield.
    #[inline]
    pub fn tag(&self) -> usize {
        todo!()
    }

    /// Returns the same pointer, but wrapped with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn with_tag<'s>(&'s self, tag: usize) -> TaggedShield<'s, T> {
        todo!()
    }

    /// Changes the tag bits to `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn set_tag(&mut self, tag: usize) {
        todo!()
    }

    /// Releases the inner hazard pointer.
    #[inline]
    pub fn release(&mut self) {
        todo!()
    }
}

impl<T> Drop for Shield<T> {
    fn drop(&mut self) {
        todo!()
    }
}

/// A reference of a [`Shield`] with a overwriting tag value.
///
/// # Motivation
///
/// `with_tag` for [`Shared`] can be implemented by taking the ownership of the original
/// pointer and returning the same one but tagged with the given value. However, for [`Shield`],
/// taking ownership is not a good option because [`Shield`]s usually live as fields of [`Defender`]
/// and we cannot take partial ownership in the most cases.
///
/// Before proposing [`TaggedShield`], we just provided `set_tag` only for a [`Shield`], which changes
/// a tag value only. Unfortunately, it was not easy to use because there were many circumstances
/// where we just want to make a temporary tagged operand for atomic operators.
///
/// For this reason, a method to easily produce a tagged [`Shield`] pointer for a temporary use is
/// needed.
pub struct TaggedShield<'s, T> {
    inner: &'s Shield<T>,
    tag: usize,
}

/// A trait for `Shield` which can protect `Shared`.
pub trait Defender {
    /// A set of `Shared` pointers which is protected by an epoch.
    type Read<'r>: Clone + Copy;

    /// Returns a default `Defender` with empty hazard pointers.
    fn default(guard: &Guard) -> Self;

    /// Stores the given `Read` pointers in hazard slots.
    ///
    /// # Safety
    ///
    /// Just storing a pointer in a hazard slot doesn't guarantee that the pointer is
    /// truly protected, as the memory block may already be reclaimed. We must validate whether
    /// the memory block is reclaimed or not, by reloading the atomic pointer or checking the
    /// local CRCU epoch.
    unsafe fn defend_unchecked(&mut self, read: &Self::Read<'_>);

    /// Loads currently protected pointers and create a new `Read`.
    unsafe fn as_read<'r>(&mut self) -> Self::Read<'r>;

    /// Nullify all hazard slots, allowing the previous memory block to be reclaimed.
    fn release(&mut self);

    /// Starts a crashable critical section where we cannot perform operations with side-effects,
    /// such as system calls, non-atomic write on a global variable, etc.
    ///
    /// After finishing the section, it defends the returned `Read` pointers, so that they can be
    /// dereferenced outside of the phase.
    ///
    /// # Safety
    ///
    /// In a section body, only *rollback-safe* operations are allowed.
    unsafe fn read<F>(&mut self, handle: &LocalHandle, body: F)
    where
        F: for<'r> Fn(&'r mut Guard) -> Self::Read<'r>;
}

// An empty `Defender`.
impl Defender for () {
    type Read<'r> = ();

    fn default(_: &Guard) -> Self {
        ()
    }

    unsafe fn defend_unchecked(&mut self, _: &Self::Read<'_>) {}

    unsafe fn as_read<'r>(&mut self) -> Self::Read<'r> {
        ()
    }

    fn release(&mut self) {}

    unsafe fn read<F>(&mut self, handle: &LocalHandle, body: F)
    where
        F: for<'r> Fn(&'r mut Guard) -> Self::Read<'r>,
    {
        handle.read(|guard| {
            let _ = body(guard);
        });
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
    (1 << mem::align_of::<T>().trailing_zeros()) - 1
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
