use core::marker::PhantomData;

use super::utils::Counted;
use crate::{EpochGuard, Pointer, ReadGuard, Writable};

pub struct Atomic<T> {
    link: crate::Atomic<Counted<T>>,
}

unsafe impl<T> Send for Atomic<T> {}
unsafe impl<T> Sync for Atomic<T> {}

impl<T> Atomic<T> {
    #[inline]
    pub fn new(init: T) -> Self {
        todo!()
    }

    #[inline]
    pub fn null() -> Self {
        todo!()
    }

    #[inline]
    pub fn store<P, G>(&self, ptr: &P, guard: &G)
    where
        P: AcquiredPtr<T>,
        G: Writable,
    {
        todo!()
    }

    #[inline]
    pub fn consume<P, G>(&self, ptr: P, guard: &G)
    where
        P: AcquiredPtr<T>,
        G: Writable,
    {
        todo!()
    }

    #[inline]
    pub fn load_rc<G: Writable>(&self, guard: &G) -> Rc<T> {
        todo!()
    }

    #[inline]
    pub fn load_local<G: Writable>(&self, guard: &G) -> Shield<T> {
        todo!()
    }

    #[inline]
    pub fn load_local_with<G: Writable>(&self, dst: &mut Shield<T>, guard: &G) {
        todo!()
    }

    #[inline]
    pub fn load_read<'r>(&self, guard: &'r ReadGuard) -> Shared<'r, T> {
        todo!()
    }

    #[inline]
    pub fn compare_exchange<P1, P2, G>(
        &self,
        expected: &P1,
        desired: &P2,
        guard: &G,
    ) -> Result<(), Shield<T>>
    where
        P1: AcquiredPtr<T>,
        P2: AcquiredPtr<T>,
        G: Writable,
    {
        todo!()
    }

    #[inline]
    pub fn try_compare_exchange<P1, P2, G>(&self, expected: &P1, desired: &P2, guard: &G) -> bool
    where
        P1: AcquiredPtr<T>,
        P2: AcquiredPtr<T>,
        G: Writable,
    {
        todo!()
    }

    #[inline]
    pub fn compare_exchange_tag<P, G>(
        &self,
        expected: &P,
        tag: usize,
        guard: &G,
    ) -> Result<(), Shield<T>>
    where
        P: AcquiredPtr<T>,
        G: Writable,
    {
        todo!()
    }

    #[inline]
    pub fn try_compare_exchange_tag<P, G>(&self, expected: &P, tag: usize, guard: &G) -> bool
    where
        P: AcquiredPtr<T>,
        G: Writable,
    {
        todo!()
    }
}

impl<T> Drop for Atomic<T> {
    fn drop(&mut self) {
        todo!()
    }
}

pub struct Shared<'r, T> {
    ptr: crate::Shared<'r, T>,
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
    pub(crate) fn new(ptr: crate::Shared<'r, T>) -> Self {
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
        todo!()
    }

    #[inline]
    pub fn tag(&self) -> usize {
        self.ptr.tag()
    }

    #[inline]
    pub fn untagged(&self) -> Self {
        todo!()
    }

    #[inline]
    pub fn with_tag(&self, tag: usize) -> Self {
        todo!()
    }

    #[inline]
    pub fn as_raw(&self) -> *const Counted<T> {
        todo!()
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
    pub fn clone(&self, guard: &EpochGuard) -> Self {
        todo!()
    }

    #[inline]
    pub fn as_ref<'s>(&'s self) -> Option<&'s T> {
        todo!()
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
        todo!()
    }
}

pub struct Rc<T> {
    // Safety: `ptr` is protected by a reference counter.
    // That is, the lifetime of the object is equal to or longer than
    // the lifetime of this object.
    ptr: *mut T,
    _marker: PhantomData<T>,
}

unsafe impl<T> Send for Rc<T> {}
unsafe impl<T> Sync for Rc<T> {}

impl<T> Rc<T> {
    #[inline]
    pub fn from_shared<'g>(ptr: crate::Shared<'g, T>, guard: &'g EpochGuard) -> Self {
        todo!()
    }

    #[inline]
    pub fn from_local(ptr: &Shield<T>) -> Self {
        todo!()
    }

    #[inline]
    pub fn from_obj(obj: T) -> Self {
        todo!()
    }

    pub unsafe fn into_owned(self) -> T {
        todo!()
    }

    #[inline]
    pub fn clone(&self) -> Self {
        todo!()
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        todo!()
    }

    #[inline]
    pub fn as_ref<'s>(&self) -> Option<&'s T> {
        todo!()
    }

    #[inline]
    pub fn tag(&self) -> usize {
        todo!()
    }

    #[inline]
    pub fn untagged(mut self) -> Self {
        todo!()
    }

    #[inline]
    pub fn with_tag(mut self) -> Self {
        todo!()
    }
}

pub trait AcquiredPtr<T> {
    fn as_raw(&self) -> *const Counted<T>;
    fn has_ref_count(&self) -> bool;
}

impl<T> AcquiredPtr<T> for Shield<T> {
    fn as_raw(&self) -> *const Counted<T> {
        todo!()
    }

    fn has_ref_count(&self) -> bool {
        todo!()
    }
}

impl<T> AcquiredPtr<T> for Rc<T> {
    fn as_raw(&self) -> *const Counted<T> {
        todo!()
    }

    fn has_ref_count(&self) -> bool {
        todo!()
    }
}

pub trait Localizable<'r> {
    type Localized;
    fn protect_with(self, guard: &EpochGuard) -> Self::Localized;
}

impl<'r, T> Localizable<'r> for Shared<'r, T> {
    type Localized = Shield<T>;

    fn protect_with(self, guard: &EpochGuard) -> Self::Localized {
        todo!()
    }
}
