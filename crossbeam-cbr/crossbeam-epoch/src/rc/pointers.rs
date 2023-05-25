use core::{
    marker::PhantomData,
    mem::{forget, transmute, zeroed, MaybeUninit},
    ptr::drop_in_place,
    sync::atomic::{AtomicUsize, Ordering},
};

use super::utils::{decrement_ref_cnt, delayed_decrement_ref_cnt, Counted};
use crate::{
    pin,
    tag::{data_with_tag, decompose_data},
    EpochGuard, Pointer, ReadGuard, Writable,
};

pub struct Atomic<T> {
    link: AtomicUsize,
    _marker: PhantomData<*mut T>,
}

unsafe impl<T> Send for Atomic<T> {}
unsafe impl<T> Sync for Atomic<T> {}

impl<T> Atomic<T> {
    #[inline]
    pub fn new(init: T) -> Self {
        let counted = Counted::new(init);
        let ptr = Box::into_raw(Box::new(counted));
        Self {
            link: AtomicUsize::new(ptr as _),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn null() -> Self {
        Self {
            link: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

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

    #[inline]
    pub fn consume<G: Writable>(&self, ptr: Rc<T>, guard: &G) {
        let new = ptr.release();
        let old = self.link.swap(new, Ordering::SeqCst) as *const Counted<T>;
        let old_ptr = decompose_data::<Counted<T>>(old as _).0.cast_const();
        if !old_ptr.is_null() {
            unsafe { delayed_decrement_ref_cnt(old_ptr, guard) };
        }
    }

    #[inline]
    pub fn defend<G: Writable>(&self, guard: &mut G) -> Shield<T> {
        let mut shield = Shield::null(unsafe { guard.as_epoch_guard() });
        self.defend_with(&mut shield, guard);
        shield
    }

    #[inline]
    pub fn defend_with<G: Writable>(&self, dst: &mut Shield<T>, guard: &mut G) {
        guard.defend(&self.link, &mut dst.shield);
    }

    #[inline]
    pub fn load<'r>(&self, guard: &'r ReadGuard) -> Shared<'r, T> {
        Shared::new(unsafe { crate::Shared::from_usize(self.link.load(Ordering::Acquire)) })
    }

    #[inline]
    pub fn compare_exchange<P1, P2, G>(
        &self,
        expected: &P1,
        desired: &P2,
        err_shield: &mut Shield<T>,
        guard: &mut G,
    ) -> bool
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
                    if !expected.as_untagged_raw().is_null() {
                        unsafe { delayed_decrement_ref_cnt(expected.as_untagged_raw(), guard) };
                    }
                    if let Some(desired) = unsafe { desired.as_untagged_raw().as_ref() } {
                        desired.add_refs(1);
                    }
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

    #[inline]
    pub fn try_compare_exchange<P1, P2, G>(&self, expected: &P1, desired: &P2, guard: &G) -> bool
    where
        P1: AcquiredPtr<T>,
        P2: AcquiredPtr<T>,
        G: Writable,
    {
        match self.link.compare_exchange(
            expected.as_raw() as usize,
            desired.as_raw() as usize,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {
                if !expected.as_untagged_raw().is_null() {
                    unsafe { delayed_decrement_ref_cnt(expected.as_untagged_raw(), guard) };
                }
                if let Some(desired) = unsafe { desired.as_untagged_raw().as_ref() } {
                    desired.add_refs(1);
                }
                true
            }
            Err(actual) => false,
        }
    }

    #[inline]
    pub fn compare_exchange_tag<P, G>(
        &self,
        expected: &P,
        tag: usize,
        err_shield: &mut Shield<T>,
        guard: &mut G,
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

    #[inline]
    pub fn try_compare_exchange_tag<P, G>(&self, expected: &P, tag: usize, guard: &G) -> bool
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
                let guard = pin();
                delayed_decrement_ref_cnt(ptr, &guard);
            }
        }
    }
}

pub struct Shared<'r, T> {
    ptr: crate::Shared<'r, Counted<T>>,
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
    pub(crate) fn new(ptr: crate::Shared<'r, Counted<T>>) -> Self {
        Self { ptr }
    }

    #[inline]
    pub fn null() -> Self {
        Self {
            ptr: crate::Shared::null(),
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    #[inline]
    pub fn as_ref(&self) -> Option<&'r T> {
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
    shield: crate::Shield<Counted<T>>,
}

unsafe impl<T> Sync for Shield<T> {}

impl<T> Shield<T> {
    #[inline]
    pub(crate) fn new(shield: crate::Shield<Counted<T>>) -> Self {
        Self { shield }
    }

    #[inline]
    pub fn null(guard: &EpochGuard) -> Self {
        Self {
            shield: crate::Shield::null(guard),
        }
    }

    #[inline]
    pub fn copy_to<G: Writable>(&self, dst: &mut Shield<T>, guard: &mut G) {
        guard.copy(&self.shield, &mut dst.shield)
    }

    #[inline]
    pub fn as_ref<'s>(&'s self) -> Option<&'s T> {
        unsafe { self.shield.as_ref().map(|cnt| cnt.data()) }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.shield.shared().is_null()
    }

    #[inline]
    pub fn tag(&self) -> usize {
        self.shield.tag()
    }

    #[inline]
    pub fn untagged(mut self) -> Self {
        self.with_tag(0)
    }

    #[inline]
    pub fn with_tag(mut self, tag: usize) -> Self {
        let modified = self.shield.shared().with_tag(tag).into_usize();
        unsafe { self.shield.defend_fake(crate::Shared::from_usize(modified)) };
        self
    }
}

impl<T> Drop for Shield<T> {
    fn drop(&mut self) {
        self.shield.release()
    }
}

pub struct Rc<T> {
    // Safety: `ptr` is protected by a reference counter.
    // That is, the lifetime of the object is equal to or longer than
    // the lifetime of this object.
    ptr: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T> Send for Rc<T> {}
unsafe impl<T> Sync for Rc<T> {}

impl<T> Rc<T> {
    #[inline]
    pub fn from_local<G: Writable>(ptr: &Shield<T>, _: &G) -> Self {
        if let Some(counted) = unsafe { ptr.as_untagged_raw().as_ref() } {
            counted.add_refs(1);
        }
        Self {
            ptr: ptr.as_raw() as _,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn from_obj<G: Writable>(obj: T, _: &G) -> Self {
        Self {
            ptr: Box::into_raw(Box::new(Counted::new(obj))) as *const _ as _,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub unsafe fn into_owned(mut self) -> T {
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
    pub fn untagged(mut self) -> Self {
        self.with_tag(0)
    }

    #[inline]
    pub fn with_tag(mut self, tag: usize) -> Self {
        self.ptr = data_with_tag::<Counted<T>>(self.ptr as usize, tag) as _;
        self
    }

    #[inline]
    #[must_use]
    pub(crate) fn release(mut self) -> usize {
        let ptr = self.ptr;
        forget(self);
        ptr
    }
}

impl<T> Drop for Rc<T> {
    fn drop(&mut self) {
        if !self.is_null() {
            let guard = pin();
            unsafe { decrement_ref_cnt(self.as_untagged_raw(), &guard) };
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
        self.shield.data
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

pub trait Localizable<'r> {
    type Localized;
    fn protect_with(self, guard: &EpochGuard) -> Self::Localized;
}

impl<'r, T> Localizable<'r> for Shared<'r, T> {
    type Localized = Shield<T>;

    #[inline]
    fn protect_with(self, guard: &EpochGuard) -> Self::Localized {
        let mut localized = Shield::null(guard);
        unsafe { localized.shield.defend_unchecked(self.ptr) };
        localized
    }
}

macro_rules! impl_localizable_for_array {(
    $($N:literal)*
) => (
    $(
        impl<'r, T> Localizable<'r> for [Shared<'r, T>; $N] {
            type Localized = [Shield<T>; $N];

            #[inline]
            fn protect_with(self, guard: &EpochGuard) -> Self::Localized {
                let mut shields: [MaybeUninit<Shield<T>>; $N] = unsafe { zeroed() };
                for (shield, shared) in shields.iter_mut().zip(self) {
                    let mut localized = Shield::null(guard);
                    unsafe { localized.shield.defend_unchecked(shared.ptr) };
                    shield.write(localized);
                }
                unsafe { transmute(shields) }
            }
        }
    )*
)}

impl_localizable_for_array! {
    00
    01 02 03 04 05 06 07 08
    09 10 11 12 13 14 15 16
    17 18 19 20 21 22 23 24
    25 26 27 28 29 30 31 32
}
