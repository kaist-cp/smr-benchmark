use core::{
    marker::PhantomData,
    mem::{forget, transmute, zeroed, MaybeUninit},
    sync::atomic::{AtomicUsize, Ordering},
};

use super::utils::{decrement_ref_cnt, delayed_decrement_ref_cnt, Counted};
use crate::{
    pebr_backend::{
        guard::Readable,
        tag::{data_with_tag, decompose_data},
        Defender, EpochGuard, Pointer, ReadGuard, ShieldError, Writable,
    },
    pin,
};

/// An reference counting atomic pointer that can be safely shared between threads.
#[derive(Debug)]
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
        let counted = Counted::new(init);
        let ptr = Box::into_raw(Box::new(counted));
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

    /// Stores a `Rc` pointer into the atomic pointer, returning the previous `Rc`.
    #[inline]
    pub fn swap<G: Writable>(&self, ptr: Rc<T>, ord: Ordering, _: &G) -> Rc<T> {
        let old = self.link.swap(ptr.as_raw() as _, ord) as *const Counted<T>;
        // Skip decrementing the reference count.
        forget(ptr);
        Rc::from_atomic_rmw(old)
    }

    /// Loads a `Shared` from the atomic pointer. This can be called only in a read phase.
    #[inline]
    pub fn load<'r, G: Readable>(&self, ord: Ordering, _: &'r G) -> Shared<'r, T> {
        Shared::new(self.link.load(ord))
    }

    /// TODO(@jeonghyeon): Rewrite this document (return type is changed)
    ///
    /// Stores the pointer `desired` (either `Rc` or `Shield`) into the atomic pointer if the current
    /// value is the same as `expected`. The tag is also taken into account, so two pointers to the
    /// same object, but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success a `true`
    /// is returned. On failure the actual current value is protected with `err_shield` and a `false` is
    /// returned.
    ///
    /// **Note that this function is not wait-free, to protect and return a precise pointer on a failure.**
    /// If you don't need an actual pointer on a failure, use `try_compare_exchange` which is wait-free.
    #[inline]
    pub fn compare_exchange<'r, G: Writable>(
        &self,
        expected: Shared<'_, T>,
        desired: &Shield<T>,
        success: Ordering,
        failure: Ordering,
        _: &'r G,
    ) -> Result<Rc<T>, Shared<'r, T>> {
        match self.link.compare_exchange(
            expected.as_raw() as usize,
            desired.as_raw() as usize,
            success,
            failure,
        ) {
            Ok(_) => {
                let rc = Rc::from_atomic_rmw(expected.as_untagged_raw());
                if let Some(desired) = unsafe { desired.as_untagged_raw().as_ref() } {
                    desired.add_refs(1);
                }
                return Ok(rc);
            }
            Err(actual) => Err(Shared::new(actual)),
        }
    }

    /// Bitwise "or" with the current tag.
    ///
    /// Performs a bitwise "or" operation on the current tag and the argument `tag`, and sets the
    /// new tag to the result. Returns the previous pointer.
    #[inline]
    pub fn fetch_or<'r, G: Writable>(&self, tag: usize, ord: Ordering, _: &'r G) -> Shared<'r, T> {
        Shared::new(self.link.fetch_or(decompose_data::<Counted<T>>(tag).1, ord))
    }
}

impl<T> Drop for Atomic<T> {
    fn drop(&mut self) {
        let val = self.link.load(Ordering::Acquire);
        let ptr = decompose_data::<Counted<T>>(val).0.cast_const();
        unsafe {
            if !ptr.is_null() {
                let guard = &pin();
                delayed_decrement_ref_cnt(ptr, guard);
            }
        }
    }
}

/// A pointer to an shared object.
///
/// This pointer is valid for use only during the lifetime `'r`.
///
/// This is the most basic shared pointer type, which can be loaded directly from `Atomic`.
/// Also it is worth noting that any protected pointer types like `Shield` and `Rc` can
/// create a `Shared` which has a lifetime parameter of the original pointer.
#[derive(Debug)]
pub struct Shared<'r, T> {
    inner: crate::pebr_backend::Shared<'r, Counted<T>>,
}

impl<'r, T> Clone for Shared<'r, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<'r, T> Copy for Shared<'r, T> {}

impl<'r, T> Shared<'r, T> {
    #[inline]
    pub(crate) fn new(ptr: usize) -> Self {
        Self {
            inner: unsafe { crate::pebr_backend::Shared::from_usize(ptr) },
        }
    }

    /// Returns a new null shared pointer.
    #[inline]
    pub fn null() -> Self {
        Self {
            inner: crate::pebr_backend::Shared::null(),
        }
    }

    /// Returns `true` if the pointer is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        self.inner.is_null()
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    ///
    /// It is possible to directly dereference a `Shared` if and only if the current context is
    /// in a read phase which can be started by `EpochGuard::read` or `EpochGuard::read_loop`.
    #[inline]
    pub fn as_ref(&self, _: &ReadGuard) -> Option<&'r T> {
        unsafe { self.inner.as_ref().map(|cnt| cnt.data()) }
    }

    /// Returns the tag stored within the pointer.
    #[inline]
    pub fn tag(&self) -> usize {
        self.inner.tag()
    }

    /// Returns the same pointer, but the tag bits are cleared.
    #[inline]
    pub fn untagged(&self) -> Self {
        self.with_tag(0)
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn with_tag(&self, tag: usize) -> Self {
        Self::new(self.inner.with_tag(tag).into_usize())
    }
}

/// A pointer to an shared object, which is protected by a hazard pointer.
///
/// It prevents the reference count decreasing.
#[derive(Debug)]
pub struct Shield<T> {
    inner: crate::pebr_backend::Shield<Counted<T>>,
}

unsafe impl<T> Sync for Shield<T> {}

impl<T> Shield<T> {
    /// Returns a new null `Shield`.
    #[inline]
    pub fn null(guard: &EpochGuard) -> Self {
        Self {
            inner: crate::pebr_backend::Shield::null(guard),
        }
    }

    /// Constructs a new `Rc` from `Shield`.
    #[inline]
    pub fn to_rc<G: Writable>(&self, _: &G) -> Rc<T> {
        if let Some(counted) = unsafe { self.as_untagged_raw().as_ref() } {
            counted.add_refs(1);
        }
        Rc {
            ptr: self.as_raw() as _,
            delayed_decr: false,
            _marker: PhantomData,
        }
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    #[inline]
    pub fn as_ref<'s>(&'s self) -> Option<&'s T> {
        unsafe { self.inner.as_ref().map(|cnt| cnt.data()) }
    }

    /// Returns `true` if the defended pointer is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        self.inner.shared().is_null()
    }

    /// Returns the tag stored within the shield.
    #[inline]
    pub fn tag(&self) -> usize {
        self.inner.tag()
    }

    /// Changes the tag bits to `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn set_tag(&mut self, tag: usize) {
        let modified = self.inner.shared().with_tag(tag).into_usize();
        unsafe {
            self.inner
                .defend_fake(crate::pebr_backend::Shared::from_usize(modified))
        };
    }

    /// Releases the inner hazard pointer.
    #[inline]
    pub fn release(&mut self) {
        self.inner.release();
    }

    /// Defends a hazard pointer.
    ///
    /// This method registers a shared pointer as hazardous so that other threads will not destroy
    /// the pointer, and returns a `Shield` pointer as a handle for the registration.
    ///
    /// This may fail if the current epoch is ejected. In this case, defending must be retried
    /// after repinning the epoch.
    #[inline]
    pub fn try_defend<'r>(
        &mut self,
        ptr: Shared<'r, T>,
        guard: &'r mut EpochGuard,
    ) -> Result<(), ShieldError> {
        self.inner.defend(ptr.inner, guard)
    }

    /// Loads a pointer from the given pointer, and defends it. Tries again until it
    /// succeeds to defend the pointer.
    #[inline]
    pub fn defend<P: GeneralPtr<T>>(&mut self, ptr: &P, guard: &mut EpochGuard) {
        while self.inner.defend_usize(ptr.as_raw(), guard).is_err() {
            guard.repin();
        }
    }
}

impl<T> Drop for Shield<T> {
    fn drop(&mut self) {
        self.inner.release()
    }
}

/// A pointer to an shared object, which is protected by a reference count.
#[derive(Debug)]
pub struct Rc<T> {
    // Safety: `ptr` is protected by a reference counter.
    // That is, the lifetime of the object is equal to or longer than
    // the lifetime of this object.
    ptr: usize,
    // If the reference count originated from `Atomic`,
    // we need to decrement its reference count with a delayed manner.
    delayed_decr: bool,
    _marker: PhantomData<T>,
}

unsafe impl<T> Send for Rc<T> {}
unsafe impl<T> Sync for Rc<T> {}

impl<T> Rc<T> {
    /// Constructs a new `Rc` by allocating the given object on the heap.
    #[inline]
    pub fn new<G: Writable>(obj: T, _: &G) -> Self {
        Self {
            ptr: Box::into_raw(Box::new(Counted::new(obj))) as *const _ as _,
            delayed_decr: false,
            _marker: PhantomData,
        }
    }

    /// Constructs a `Rc` which delays decrementing its reference count on `drop`.
    ///
    /// If the reference count originated from `Atomic`,
    /// we need to decrement its reference count with a delayed manner.
    pub(crate) fn from_atomic_rmw(ptr: *const Counted<T>) -> Self {
        Self {
            ptr: ptr as _,
            delayed_decr: true,
            _marker: PhantomData,
        }
    }

    /// Returns a new null `Rc`.
    #[inline]
    pub fn null() -> Self {
        Self {
            ptr: 0,
            delayed_decr: false,
            _marker: PhantomData,
        }
    }

    /// Takes ownership of the pointee.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object.
    #[inline]
    pub unsafe fn into_owned(self) -> T {
        let result = Box::from_raw(self.as_untagged_raw().cast_mut()).into_owned();
        forget(self);
        result
    }

    /// Returns a copy of the pointer after incrementing its reference count.
    #[inline]
    pub fn clone<G: Writable>(&self, _: &G) -> Self {
        if let Some(counted) = unsafe { self.as_untagged_raw().as_ref() } {
            counted.add_refs(1);
        }
        Self { ..*self }
    }

    /// Returns `true` if the defended pointer is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        self.as_untagged_raw().is_null()
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    #[inline]
    pub fn as_ref<'s>(&'s self) -> Option<&'s T> {
        unsafe { self.as_untagged_raw().as_ref().map(|cnt| cnt.data()) }
    }

    /// Returns the tag stored within the pointer.
    #[inline]
    pub fn tag(&self) -> usize {
        decompose_data::<Counted<T>>(self.ptr as usize).1
    }

    /// Returns the same pointer, but the tag bits are cleared.
    #[inline]
    pub fn untagged(self) -> Self {
        self.with_tag(0)
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn with_tag(mut self, tag: usize) -> Self {
        self.ptr = data_with_tag::<Counted<T>>(self.ptr as usize, tag) as _;
        self
    }
}

impl<T> Drop for Rc<T> {
    fn drop(&mut self) {
        if !self.is_null() {
            unsafe {
                let guard = &pin();
                if self.delayed_decr {
                    delayed_decrement_ref_cnt(self.as_untagged_raw(), guard);
                } else {
                    decrement_ref_cnt(self.as_untagged_raw(), guard);
                }
            }
        }
    }
}

/// A general pointer trait.
///
/// The main four pointer types implements this trait: `Atomic`, `Shared`, `Shield` and `Rc`.
pub trait GeneralPtr<T> {
    /// Gets a `usize` representing a tagged pointer value.
    ///
    /// Note that we must not directly dereference the returned pointer
    /// by casting it to a raw pointer, as it may contain tag bits.
    fn as_raw(&self) -> usize;

    /// Gets a `usize` representing an untagged pointer value.
    #[inline]
    fn as_untagged_raw(&self) -> *const Counted<T> {
        decompose_data::<Counted<T>>(self.as_raw() as _)
            .0
            .cast_const()
    }
}

impl<T> GeneralPtr<T> for Atomic<T> {
    #[inline]
    fn as_raw(&self) -> usize {
        self.link.load(Ordering::Acquire)
    }
}

impl<'r, T> GeneralPtr<T> for Shared<'r, T> {
    #[inline]
    fn as_raw(&self) -> usize {
        self.inner.into_usize()
    }
}

impl<T> GeneralPtr<T> for Shield<T> {
    #[inline]
    fn as_raw(&self) -> usize {
        self.inner.data
    }
}

impl<T> GeneralPtr<T> for Rc<T> {
    #[inline]
    fn as_raw(&self) -> usize {
        self.ptr
    }
}

/// An aquired pointer trait.
///
/// This represents pointers which are protected by other than epoch.
///
/// The two pointer types implements this trait: `Shield` and `Rc`.
pub trait AcquiredPtr<T>: GeneralPtr<T> {
    /// Gets a `Shared` pointer to the same object.
    fn shared<'p>(&'p self) -> Shared<'p, T>;
}

impl<T> AcquiredPtr<T> for Shield<T> {
    #[inline]
    fn shared<'p>(&'p self) -> Shared<'p, T> {
        Shared {
            inner: self.inner.shared(),
        }
    }
}

impl<T> AcquiredPtr<T> for Rc<T> {
    #[inline]
    fn shared<'p>(&'p self) -> Shared<'p, T> {
        Shared::new(self.ptr)
    }
}

impl<T: 'static> Defender for Shield<T> {
    type Read<'r> = Shared<'r, T>;

    #[inline]
    fn default(guard: &EpochGuard) -> Self {
        Self::null(guard)
    }

    #[inline]
    unsafe fn defend_unchecked(&mut self, read: &Self::Read<'_>) {
        self.inner.defend_unchecked(read.inner);
    }

    #[inline]
    unsafe fn as_read<'r>(&mut self) -> Self::Read<'r> {
        Shared::new(self.inner.shared().into_usize())
    }

    #[inline]
    fn release(&mut self) {
        self.inner.release();
    }
}

macro_rules! impl_defender_for_array {(
    $($N:literal)*
) => (
    $(
        impl<T: 'static> Defender for [Shield<T>; $N] {
            type Read<'r> = [Shared<'r, T>; $N];

            #[inline]
            fn default(guard: &EpochGuard) -> Self {
                let mut result: [MaybeUninit<Shield<T>>; $N] = unsafe { zeroed() };
                for shield in &mut result {
                    shield.write(Shield::null(guard));
                }
                unsafe { transmute(result) }
            }

            #[inline]
            unsafe fn defend_unchecked(&mut self, read: &Self::Read<'_>) {
                for (shield, shared) in self.iter_mut().zip(read) {
                    shield.inner.defend_unchecked(shared.inner);
                }
            }

            #[inline]
            unsafe fn as_read<'r>(&mut self) -> Self::Read<'r> {
                let mut result: [MaybeUninit<Shared<'r, T>>; $N] = zeroed();
                for (shield, shared) in self.iter().zip(result.iter_mut()) {
                    shared.write(Shared::new(shield.inner.shared().into_usize()));
                }
                transmute(result)
            }

            #[inline]
            fn release(&mut self) {
                for shield in self {
                    shield.release();
                }
            }
        }
    )*
)}

impl_defender_for_array! {
    00
    01 02 03 04 05 06 07 08
    09 10 11 12 13 14 15 16
    17 18 19 20 21 22 23 24
    25 26 27 28 29 30 31 32
}
