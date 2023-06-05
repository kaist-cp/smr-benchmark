use core::{
    marker::PhantomData,
    mem::{forget, transmute, zeroed, MaybeUninit},
    sync::atomic::{AtomicUsize, Ordering},
};

use super::utils::{decrement_ref_cnt, delayed_decrement_ref_cnt, Counted};
use crate::{
    pebr_backend::{
        tag::{data_with_tag, decompose_data},
        Defender, EpochGuard, Pointer, ReadGuard, Writable,
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

    /// Stores a `Rc` or `Shield` pointer into the atomic pointer.
    #[inline]
    pub fn store<P, G>(&self, ptr: &P, guard: &G)
    where
        P: AcquiredPtr<T>,
        G: Writable,
    {
        let old = self.link.swap(ptr.as_raw() as _, Ordering::SeqCst) as *const Counted<T>;
        let old_ptr = decompose_data::<Counted<T>>(old as _).0.cast_const();
        let new_ptr = ptr.as_untagged_raw();
        if old_ptr != new_ptr {
            if let Some(counted) = unsafe { new_ptr.as_ref() } {
                counted.add_refs(1);
            }
            if !old_ptr.is_null() {
                unsafe { delayed_decrement_ref_cnt(old, guard) };
            }
        }
    }

    /// Consums a `Rc` pointer, and store its pointer value into the atomic pointer.
    ///
    /// This is more efficient than normal `store` function, as it doesn't have to
    /// modify a reference count of the new object.
    #[inline]
    pub fn consume<G: Writable>(&self, ptr: Rc<T>, guard: &G) {
        let new = ptr.release();
        let old = self.link.swap(new, Ordering::SeqCst) as *const Counted<T>;
        let old_ptr = decompose_data::<Counted<T>>(old as _).0.cast_const();
        if !old_ptr.is_null() {
            unsafe { delayed_decrement_ref_cnt(old_ptr, guard) };
        }
    }

    /// Loads a pointer from the atomic pointer, and defend it with a new `Shield`.
    ///
    /// Although this function is more convinient than `defend_with`, it is better to use
    /// `defend_with` to reuse a pre-allocated `Shield`.
    #[inline]
    pub fn defend(&self, guard: &mut EpochGuard) -> Shield<T> {
        let mut shield = Shield::null(guard);
        self.defend_with(&mut shield, guard);
        shield
    }

    /// Loads a pointer from the atomic pointer, and defend it with a provided `Shield`.
    #[inline]
    pub fn defend_with(&self, dst: &mut Shield<T>, guard: &mut EpochGuard) {
        loop {
            let ptr = self.link.load(Ordering::Acquire);
            if dst.hazptr.defend_usize(ptr, guard).is_ok() {
                return;
            }
            guard.repin();
        }
    }

    /// Loads a `Shared` from the atomic pointer. This can be called only in a read phase.
    #[inline]
    pub fn load<'r>(&self, _guard: &'r ReadGuard) -> Shared<'r, T> {
        Shared::new(unsafe {
            crate::pebr_backend::Shared::from_usize(self.link.load(Ordering::Acquire))
        })
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
    pub fn compare_exchange<'s, P1, P2, G>(
        &self,
        expected: &P1,
        desired: &P2,
        err_shield: &'s mut Shield<T>,
        guard: &mut EpochGuard,
    ) -> Result<Rc<T>, &'s mut Shield<T>>
    where
        P1: AcquiredPtr<T>,
        P2: AcquiredPtr<T>,
        G: Writable,
    {
        loop {
            self.defend_with(err_shield, guard);
            match self.link.compare_exchange(
                expected.as_raw() as usize,
                desired.as_raw() as usize,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    let rc = Rc::from_atomic_cas_succ(expected.as_untagged_raw());
                    if let Some(desired) = unsafe { desired.as_untagged_raw().as_ref() } {
                        desired.add_refs(1);
                    }
                    return Ok(rc);
                }
                Err(actual) => {
                    if actual == err_shield.as_raw() {
                        return Err(err_shield);
                    }
                }
            }
        }
    }

    /// TODO(@jeonghyeon): Rewrite this document (return type is changed)
    ///
    /// Stores the pointer `desired` (either `Rc` or `Shield`) into the atomic pointer if the current
    /// value is the same as `expected`. The tag is also taken into account, so two pointers to the
    /// same object, but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success a `true`
    /// is returned. On failure a `false` is returned.
    ///
    /// If you need an actual pointer on a failure, use `compare_exchange`.
    #[inline]
    pub fn try_compare_exchange<P1, P2, G>(
        &self,
        expected: &P1,
        desired: &P2,
        guard: &G,
    ) -> Result<Rc<T>, ()>
    where
        P1: AcquiredPtr<T>,
        P2: AcquiredPtr<T>,
        G: Writable,
    {
        if let Some(desired) = unsafe { desired.as_untagged_raw().as_ref() } {
            desired.add_refs(1);
        }

        match self.link.compare_exchange(
            expected.as_raw() as usize,
            desired.as_raw() as usize,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {
                let rc = Rc::from_atomic_cas_succ(expected.as_untagged_raw());
                Ok(rc)
            }
            Err(_actual) => {
                if !desired.as_untagged_raw().is_null() {
                    unsafe { decrement_ref_cnt(desired.as_untagged_raw(), guard) };
                }
                Err(())
            }
        }
    }

    /// Stores the pointer `expected` (either `Rc` or `Shield`) with a given tag, into the atomic
    /// pointer if the current value is the same as `expected`. The tag is also taken into account,
    /// so two pointers to the same object, but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success a `true`
    /// is returned. On failure the actual current value is protected with `err_shield` and a `false` is
    /// returned.
    ///
    /// **Note that this function is not wait-free, to protect and return a precise pointer on a failure.**
    /// If you don't need an actual pointer on a failure, use `try_compare_exchange_tag` which is wait-free.
    ///
    /// # Why do we need CAS-tag variants?
    ///
    /// Sometimes we just want to switch a tag of a atomic pointer. (for example when logically
    /// removing a node from Harris's List)
    ///
    /// To implement it with `compare_exchange`, we must clone the pointer (either `Rc` or `Shield`)
    /// and feed it as a `desired` argument. However, cloning causes an a non-negligible overhead
    /// for both `Rc` and `Shield`.
    ///
    /// For this reason, it is desirable to provide a tagging operation without an additional cloning.
    #[inline]
    pub fn compare_exchange_tag<P, G>(
        &self,
        expected: &P,
        tag: usize,
        err_shield: &mut Shield<T>,
        guard: &mut EpochGuard,
    ) -> bool
    where
        P: AcquiredPtr<T>,
        G: Writable,
    {
        loop {
            self.defend_with(err_shield, guard);
            let expected_tagged = expected.as_raw();
            let desired_tagged = data_with_tag::<Counted<T>>(expected_tagged, tag);

            match self.link.compare_exchange(
                expected_tagged,
                desired_tagged,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return true;
                }
                Err(actual) => {
                    if actual == err_shield.as_raw() {
                        return false;
                    }
                }
            }
        }
    }

    /// Stores the pointer `expected` (either `Rc` or `Shield`) with a given tag, into the atomic
    /// pointer if the current value is the same as `expected`. The tag is also taken into account,
    /// so two pointers to the same object, but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success a `true`
    /// is returned. On failure a `false` is returned.
    ///
    /// **Note that this function is not wait-free, to protect and return a precise pointer on a failure.**
    /// If you don't need an actual pointer on a failure, use `try_compare_exchange_tag` which is wait-free.
    ///
    /// # Why do we need CAS-tag variants?
    ///
    /// Sometimes we just want to switch a tag of a atomic pointer. (for example when logically
    /// removing a node from Harris's List)
    ///
    /// To implement it with `compare_exchange`, we must clone the pointer (either `Rc` or `Shield`)
    /// and feed it as a `desired` argument. However, cloning causes an a non-negligible overhead
    /// for both `Rc` and `Shield`.
    ///
    /// For this reason, it is desirable to provide a tagging operation without an additional cloning.
    #[inline]
    pub fn try_compare_exchange_tag<P, G>(&self, expected: &P, tag: usize, _guard: &G) -> bool
    where
        P: AcquiredPtr<T>,
        G: Writable,
    {
        let expected_tagged = expected.as_raw();
        let desired_tagged = data_with_tag::<Counted<T>>(expected_tagged, tag);

        self.link
            .compare_exchange(
                expected_tagged,
                desired_tagged,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
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

pub struct Shared<'r, T> {
    ptr: crate::pebr_backend::Shared<'r, Counted<T>>,
}

impl<'r, T> Clone for Shared<'r, T> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr.clone(),
        }
    }
}

impl<'r, T> Copy for Shared<'r, T> {}

impl<'r, T> Shared<'r, T> {
    #[inline]
    pub(crate) fn new(ptr: crate::pebr_backend::Shared<'r, Counted<T>>) -> Self {
        Self { ptr }
    }

    #[inline]
    pub fn null() -> Self {
        Self {
            ptr: crate::pebr_backend::Shared::null(),
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    #[inline]
    pub fn as_ref(&self, _guard: &ReadGuard) -> Option<&'r T> {
        unsafe { self.ptr.as_ref().map(|cnt| cnt.data()) }
    }

    #[inline]
    pub fn tag(&self) -> usize {
        self.ptr.tag()
    }

    #[inline]
    pub fn untagged(&self) -> Self {
        self.with_tag(0)
    }

    #[inline]
    pub fn with_tag(&self, tag: usize) -> Self {
        Self::new(self.ptr.with_tag(tag))
    }
}

pub struct Shield<T> {
    hazptr: crate::pebr_backend::Shield<Counted<T>>,
}

unsafe impl<T> Sync for Shield<T> {}

impl<T> Shield<T> {
    #[inline]
    pub fn null(guard: &EpochGuard) -> Self {
        Self {
            hazptr: crate::pebr_backend::Shield::null(guard),
        }
    }

    #[inline]
    pub fn copy_to(&self, dst: &mut Shield<T>, guard: &mut EpochGuard) {
        while dst.hazptr.defend_usize(self.hazptr.data, guard).is_err() {
            guard.repin();
        }
    }

    #[inline]
    pub fn as_ref<'s>(&'s self) -> Option<&'s T> {
        unsafe { self.hazptr.as_ref().map(|cnt| cnt.data()) }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.hazptr.shared().is_null()
    }

    #[inline]
    pub fn tag(&self) -> usize {
        self.hazptr.tag()
    }

    #[inline]
    pub fn set_tag(&mut self, tag: usize) {
        let modified = self.hazptr.shared().with_tag(tag).into_usize();
        unsafe {
            self.hazptr
                .defend_fake(crate::pebr_backend::Shared::from_usize(modified))
        };
    }

    #[inline]
    pub fn release(&mut self) {
        self.hazptr.release();
    }
}

impl<T> Drop for Shield<T> {
    fn drop(&mut self) {
        self.hazptr.release()
    }
}

pub struct Rc<T> {
    // Safety: `ptr` is protected by a reference counter.
    // That is, the lifetime of the object is equal to or longer than
    // the lifetime of this object.
    ptr: usize,
    // If this `Rc` is from `compare_exchange` of `Atomic`,
    // we need to decrement its reference count with a delayed manner.
    delayed_decr: bool,
    _marker: PhantomData<T>,
}

unsafe impl<T> Send for Rc<T> {}
unsafe impl<T> Sync for Rc<T> {}

impl<T> Rc<T> {
    #[inline]
    pub fn from_shield<G: Writable>(ptr: &Shield<T>, _: &G) -> Self {
        if let Some(counted) = unsafe { ptr.as_untagged_raw().as_ref() } {
            counted.add_refs(1);
        }
        Self {
            ptr: ptr.as_raw() as _,
            delayed_decr: false,
            _marker: PhantomData,
        }
    }

    pub(crate) fn from_atomic_cas_succ(ptr: *const Counted<T>) -> Self {
        Self {
            ptr: ptr as _,
            delayed_decr: true,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn null() -> Self {
        Self {
            ptr: 0,
            delayed_decr: false,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn from_obj<G: Writable>(obj: T, _: &G) -> Self {
        Self {
            ptr: Box::into_raw(Box::new(Counted::new(obj))) as *const _ as _,
            delayed_decr: false,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub unsafe fn into_owned(self) -> T {
        let result = Box::from_raw(self.as_untagged_raw().cast_mut()).into_owned();
        forget(self);
        result
    }

    #[inline]
    pub fn clone<G: Writable>(&self, _: &G) -> Self {
        if let Some(counted) = unsafe { self.as_untagged_raw().as_ref() } {
            counted.add_refs(1);
        }
        Self { ..*self }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.as_untagged_raw().is_null()
    }

    #[inline]
    pub fn as_ref<'s>(&'s self) -> Option<&'s T> {
        unsafe { self.as_untagged_raw().as_ref().map(|cnt| cnt.data()) }
    }

    #[inline]
    pub fn tag(&self) -> usize {
        decompose_data::<Counted<T>>(self.ptr as usize).1
    }

    #[inline]
    pub fn untagged(self) -> Self {
        self.with_tag(0)
    }

    #[inline]
    pub fn with_tag(mut self, tag: usize) -> Self {
        self.ptr = data_with_tag::<Counted<T>>(self.ptr as usize, tag) as _;
        self
    }

    #[inline]
    #[must_use]
    pub(crate) fn release(self) -> usize {
        let ptr = self.ptr;
        forget(self);
        ptr
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

pub trait AcquiredPtr<T> {
    /// Gets a `usize` representing a tagged pointer value.
    ///
    /// Note that we must not directly dereference the returned pointer
    /// by casting it to a raw pointer, as it may contain tag bits.
    fn as_raw(&self) -> usize;
    fn has_ref_count(&self) -> bool;

    fn as_untagged_raw(&self) -> *const Counted<T> {
        decompose_data::<Counted<T>>(self.as_raw() as _)
            .0
            .cast_const()
    }
}

impl<T> AcquiredPtr<T> for Shield<T> {
    #[inline]
    fn as_raw(&self) -> usize {
        self.hazptr.data
    }

    #[inline]
    fn has_ref_count(&self) -> bool {
        false
    }
}

impl<T> AcquiredPtr<T> for Rc<T> {
    #[inline]
    fn as_raw(&self) -> usize {
        self.ptr
    }

    #[inline]
    fn has_ref_count(&self) -> bool {
        true
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
        self.hazptr.defend_unchecked(read.ptr);
    }

    #[inline]
    unsafe fn as_read<'r>(&mut self) -> Self::Read<'r> {
        Shared {
            ptr: crate::pebr_backend::Shared::from_usize(self.hazptr.shared().as_raw() as _),
        }
    }

    #[inline]
    fn release(&mut self) {
        self.hazptr.release();
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
                    shield.hazptr.defend_unchecked(shared.ptr);
                }
            }

            #[inline]
            unsafe fn as_read<'r>(&mut self) -> Self::Read<'r> {
                let mut result: [MaybeUninit<Shared<'r, T>>; $N] = zeroed();
                for (shield, shared) in self.iter().zip(result.iter_mut()) {
                    shared.write(Shared {
                        ptr: crate::pebr_backend::Shared::from_usize(shield.hazptr.shared().as_raw() as _)
                    });
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
