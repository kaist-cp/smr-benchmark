use std::{
    marker::PhantomData,
    mem::{forget, transmute},
};

use atomic::Ordering;

use crate::{
    data_with_tag, decompose_data, Atomic, Retire, Shared, Shield, Thread, Guard, TaggedShield,
};

use super::{counted::Counted, retire::RetireRc};

/// An reference counting atomic pointer that can be safely shared between threads.
pub struct AtomicRc<T> {
    link: Atomic<Counted<T>>,
}

impl<T> Default for AtomicRc<T> {
    fn default() -> Self {
        Self::null()
    }
}

unsafe impl<T> Send for AtomicRc<T> {}
unsafe impl<T> Sync for AtomicRc<T> {}

impl<T> AtomicRc<T> {
    /// Allocates `init` on the heap and returns a new atomic pointer pointing to it.
    #[inline]
    pub fn new(init: T) -> Self {
        let counted = Counted::new(init);
        Self {
            link: Atomic::new(counted),
        }
    }

    /// Returns a new null atomic pointer.
    #[inline]
    pub const fn null() -> Self {
        Self {
            link: Atomic::null(),
        }
    }

    /// Stores a [`Rc`] pointer into the atomic pointer, returning the previous [`Rc`].
    #[inline]
    pub fn swap<G: Retire>(&self, ptr: Rc<T>, ord: Ordering, guard: &G) -> Rc<T> {
        let old = self.link.swap(ptr.shared(), ord, guard);
        // Skip decrementing the reference count.
        forget(ptr);
        Rc::from_atomic_rmw(old)
    }

    /// Loads a [`Shared`] from the atomic pointer. This can be called only in a read phase.
    #[inline]
    pub fn load<'r, G: Guard>(&self, ord: Ordering, guard: &'r G) -> Shared<'r, Counted<T>> {
        self.link.load(ord, guard)
    }

    /// Stores the pointer `desired` into the atomic pointer if the current value is the
    /// same as `expected`. The tag is also taken into account, so two pointers to the same object,
    /// but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success a
    /// [`Rc`] which is taken out from the atomic pointer is returned. On failure a
    /// [`CompareExchangeError`] which contains an actual value from the atomic pointer and
    /// the ownership of `desired` pointer which was given as a parameter is returned.
    #[inline]
    pub fn compare_exchange<'p, 'r, P, G>(
        &self,
        expected: Shared<'p, Counted<T>>,
        desired: P,
        success: Ordering,
        failure: Ordering,
        guard: &'r G,
    ) -> Result<Rc<T>, CompareExchangeErrorRc<'p, T, P>>
    where
        P: Acquired<T>,
        G: Retire,
    {
        match self.link.compare_exchange(
            expected,
            unsafe { transmute::<_, Shared<Counted<T>>>(desired.shared()) },
            success,
            failure,
            guard,
        ) {
            Ok(_) => {
                let rc = Rc::from_atomic_rmw(expected);
                // Here, `into_ref_count` increment the reference count of `desired` only if `desired`
                // is `DefendedPtr`.
                //
                // If `desired` is `Rc`, semantically the ownership of the reference count from
                // `desired` is moved to `self`. Because of this reason, we must skip decrementing
                // the reference count of `desired`.
                desired.into_ref_count();
                return Ok(rc);
            }
            Err(e) => Err(CompareExchangeErrorRc {
                desired,
                actual: e.actual,
            }),
        }
    }

    /// Bitwise "or" with the current tag.
    ///
    /// Performs a bitwise "or" operation on the current tag and the argument `tag`, and sets the
    /// new tag to the result. Returns the previous pointer.
    #[inline]
    pub fn fetch_or<'r, G: Retire>(
        &self,
        tag: usize,
        ord: Ordering,
        guard: &'r G,
    ) -> Shared<'r, Counted<T>> {
        self.link.fetch_or(tag, ord, guard)
    }
}

impl<T> Drop for AtomicRc<T> {
    fn drop(&mut self) {
        let mut thread = unsafe { Thread::internal() };
        let ptr = self.link.load(Ordering::Acquire, &*thread);
        if !ptr.is_null() {
            thread.delayed_decrement_ref_cnt(unsafe { ptr.deref_unchecked() });
        }
    }
}

/// A pointer to an shared object, which is protected by a reference count.
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
    /// Constructs a new [`Rc`] by allocating the given object on the heap.
    #[inline]
    pub fn new<G: Retire>(obj: T, _: &G) -> Self {
        Self {
            ptr: Box::into_raw(Box::new(Counted::new(obj))) as *const _ as _,
            delayed_decr: false,
            _marker: PhantomData,
        }
    }

    /// Constructs a [`Rc`] which delays decrementing its reference count on `drop`.
    ///
    /// If the reference count originated from [`Atomic`],
    /// we need to decrement its reference count with a delayed manner.
    pub(crate) fn from_atomic_rmw(ptr: Shared<Counted<T>>) -> Self {
        Self {
            ptr: ptr.as_raw(),
            delayed_decr: true,
            _marker: PhantomData,
        }
    }

    /// Returns a new null [`Rc`].
    #[inline]
    pub fn null() -> Self {
        Self {
            ptr: 0,
            delayed_decr: false,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn as_untagged_raw(&self) -> *const Counted<T> {
        decompose_data::<Counted<T>>(self.ptr).0
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
    pub fn clone<G: RetireRc>(&self, _: &G) -> Self {
        if let Some(counted) = unsafe { self.as_untagged_raw().as_ref() } {
            counted.add_refs(1);
        }
        Self { ..*self }
    }

    /// Constructs a new [`Rc`] from [`Shield`].
    #[inline]
    pub fn from_shield(shield: &Shield<Counted<T>>) -> Self {
        if let Some(counted) = shield.as_ref() {
            counted.add_refs(1);
        }
        Self {
            ptr: shield.as_raw(),
            delayed_decr: false,
            _marker: PhantomData,
        }
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
        unsafe { self.as_untagged_raw().as_ref().map(|cnt| cnt.deref()) }
    }

    /// Converts the pointer to a mutable reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    #[inline]
    pub fn as_mut<'s>(&'s self) -> Option<&'s mut T> {
        unsafe {
            self.as_untagged_raw()
                .cast_mut()
                .as_mut()
                .map(|cnt| cnt.deref_mut())
        }
    }

    /// Converts the pointer to a reference.
    ///
    /// # Safety
    /// 
    /// The `self` must be a valid memory location.
    #[inline]
    pub unsafe fn deref(&self) -> &T {
        unsafe { (*self.as_untagged_raw()).deref() }
    }

    /// Converts the pointer to a mutable reference.
    ///
    /// # Safety
    /// 
    /// The `self` must be a valid memory location.
    #[inline]
    pub unsafe fn deref_mut(&self) -> &mut T {
        unsafe { (*self.as_untagged_raw()).deref_mut() }
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
        self.ptr = data_with_tag::<Counted<T>>(self.ptr, tag);
        self
    }
}

impl<T> Drop for Rc<T> {
    fn drop(&mut self) {
        if let Some(counted) = unsafe { self.as_untagged_raw().as_ref() } {
            let mut thread = unsafe { Thread::internal() };
            if self.delayed_decr {
                thread.delayed_decrement_ref_cnt(counted);
            } else {
                thread.decrement_ref_cnt(counted);
            }
        }
    }
}


/// A result of unsuccessful `compare_exchange`.
///
/// It returns the ownership of [`Rc`] pointer which was given as a parameter.
pub struct CompareExchangeErrorRc<'p, T, P> {
    /// The `desired` pointer which was given as a parameter of `compare_exchange`.
    pub desired: P,
    /// The actual pointer value inside the atomic pointer.
    pub actual: Shared<'p, Counted<T>>,
}

/// An aquired pointer trait.
///
/// This represents a pointer which is protected by other than epoch.
pub trait Acquired<T> {
    /// Gets a [`Shared`] pointer to the same object.
    fn shared<'p>(&'p self) -> Shared<'p, Counted<T>>;

    /// Consumes the aquired pointer, incrementing the reference count if we didn't increment
    /// it before.
    ///
    /// Semantically, it is equivalent to giving ownership of a reference count outside the
    /// environment.
    ///
    /// For example, we do nothing but forget its ownership if the pointer is [`Rc`],
    /// but increment the reference count if the pointer is [`Shield`].
    fn into_ref_count(self);
}

impl<T> Acquired<T> for Shield<Counted<T>> {
    #[inline]
    fn shared<'p>(&'p self) -> Shared<'p, Counted<T>> {
        self.shared()
    }

    #[inline]
    fn into_ref_count(self) {
        if let Some(counted) = self.as_ref() {
            counted.add_refs(1);
        }
    }
}

impl<T> Acquired<T> for &Shield<Counted<T>> {
    #[inline]
    fn shared<'p>(&'p self) -> Shared<'p, Counted<T>> {
        Shield::shared(self)
    }

    #[inline]
    fn into_ref_count(self) {
        if let Some(counted) = self.as_ref() {
            counted.add_refs(1);
        }
    }
}

impl<'g, T> Acquired<T> for TaggedShield<'g, Counted<T>> {
    #[inline]
    fn shared<'p>(&'p self) -> Shared<'p, Counted<T>> {
        Shield::shared(self.inner).with_tag(self.tag)
    }

    #[inline]
    fn into_ref_count(self) {
        if let Some(counted) = self.inner.as_ref() {
            counted.add_refs(1);
        }
    }
}

impl<T> Acquired<T> for Rc<T> {
    #[inline]
    fn shared<'p>(&'p self) -> Shared<'p, Counted<T>> {
        Shared::new(self.ptr)
    }

    #[inline]
    fn into_ref_count(self) {
        // As we have a reference count already, we don't have to do anything, but
        // prevent calling a destructor which decrements it.
        forget(self);
    }
}
