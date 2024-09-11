use core::mem;
use std::{
    ptr::null_mut,
    sync::atomic::{AtomicPtr, Ordering},
};

pub struct CompareExchangeError<T, P: Pointer<T>> {
    pub new: P,
    pub current: Shared<T>,
}

pub struct Atomic<T> {
    link: AtomicPtr<T>,
}

unsafe impl<T> Sync for Atomic<T> {}
unsafe impl<T> Send for Atomic<T> {}

impl<T> Atomic<T> {
    #[inline]
    pub fn new(init: T) -> Self {
        let link = AtomicPtr::new(Box::into_raw(Box::new(init)));
        Self { link }
    }

    #[inline]
    pub fn null() -> Self {
        let link = AtomicPtr::new(null_mut());
        Self { link }
    }

    #[inline]
    pub fn load(&self, order: Ordering) -> Shared<T> {
        let ptr = self.link.load(order);
        Shared { ptr }
    }

    #[inline]
    pub fn store(&self, ptr: Shared<T>, order: Ordering) {
        self.link.store(ptr.into_raw(), order)
    }

    #[inline]
    pub fn fetch_or(&self, val: usize, order: Ordering) -> Shared<T> {
        let ptr = self.link.fetch_or(val, order);
        Shared { ptr }
    }

    #[inline]
    pub fn compare_exchange<P: Pointer<T>>(
        &self,
        current: Shared<T>,
        new: P,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Shared<T>, CompareExchangeError<T, P>> {
        let current = current.into_raw();
        let new = new.into_raw();

        match self.link.compare_exchange(current, new, success, failure) {
            Ok(current) => Ok(Shared { ptr: current }),
            Err(current) => {
                let new = unsafe { P::from_raw(new) };
                Err(CompareExchangeError {
                    new,
                    current: Shared { ptr: current },
                })
            }
        }
    }

    #[inline]
    pub unsafe fn try_into_owned(self) -> Option<Box<T>> {
        let ptr = base_ptr(self.link.into_inner());
        if ptr.is_null() {
            return None;
        } else {
            Some(unsafe { Box::from_raw(ptr) })
        }
    }

    #[inline]
    pub unsafe fn into_owned(self) -> Box<T> {
        unsafe { Box::from_raw(self.link.into_inner()) }
    }

    #[inline]
    // TODO: best API? might be better to just wrap as_ptr, without the deref.
    pub unsafe fn as_shared(&self) -> Shared<T> {
        Shared {
            ptr: unsafe { *self.link.as_ptr() },
        }
    }
}

impl<T> Default for Atomic<T> {
    #[inline]
    fn default() -> Self {
        Self {
            link: AtomicPtr::default(),
        }
    }
}

impl<T> From<Shared<T>> for Atomic<T> {
    #[inline]
    fn from(value: Shared<T>) -> Self {
        let link = AtomicPtr::new(value.into_raw());
        Self { link }
    }
}

pub struct Shared<T> {
    ptr: *mut T,
}

impl<T> Shared<T> {
    #[inline]
    pub fn from_owned(init: T) -> Shared<T> {
        let ptr = Box::into_raw(Box::new(init));
        Self { ptr }
    }

    #[inline]
    pub unsafe fn into_owned(self) -> T {
        unsafe { *Box::from_raw(base_ptr(self.ptr)) }
    }

    #[inline]
    pub fn null() -> Self {
        Self { ptr: null_mut() }
    }

    #[inline]
    pub fn tag(&self) -> usize {
        tag(self.ptr)
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        base_ptr(self.ptr).is_null()
    }

    #[inline]
    pub fn with_tag(&self, tag: usize) -> Self {
        let ptr = compose_tag(self.ptr, tag);
        Self { ptr }
    }

    #[inline]
    pub unsafe fn as_ref<'g>(&self) -> Option<&'g T> {
        base_ptr(self.ptr).as_ref()
    }

    #[inline]
    pub unsafe fn as_mut<'g>(&self) -> Option<&'g mut T> {
        base_ptr(self.ptr).as_mut()
    }

    #[inline]
    pub unsafe fn deref<'g>(&self) -> &'g T {
        &*base_ptr(self.ptr)
    }

    #[inline]
    pub unsafe fn deref_mut<'g>(&mut self) -> &'g mut T {
        &mut *base_ptr(self.ptr)
    }
}

impl<T> From<usize> for Shared<T> {
    #[inline]
    fn from(val: usize) -> Self {
        Self {
            ptr: val as *const T as *mut T,
        }
    }
}

impl<T> Clone for Shared<T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Shared<T> {}

impl<T> PartialEq for Shared<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl<T> Eq for Shared<T> {}

pub trait Pointer<T> {
    fn into_raw(self) -> *mut T;
    unsafe fn from_raw(val: *mut T) -> Self;
}

impl<T> Pointer<T> for Shared<T> {
    #[inline]
    fn into_raw(self) -> *mut T {
        self.ptr
    }

    #[inline]
    unsafe fn from_raw(val: *mut T) -> Self {
        Shared::from(val as usize)
    }
}

impl<T> Pointer<T> for Box<T> {
    #[inline]
    fn into_raw(self) -> *mut T {
        Box::into_raw(self)
    }

    #[inline]
    unsafe fn from_raw(val: *mut T) -> Self {
        Box::from_raw(val)
    }
}

/// Returns a bitmask containing the unused least significant bits of an aligned pointer to `T`.
#[inline]
fn low_bits<T: Sized>() -> usize {
    (1 << mem::align_of::<T>().trailing_zeros()) - 1
}

/// Given a tagged pointer `data`, returns the same pointer, but tagged with `tag`.
///
/// `tag` is truncated to fit into the unused bits of the pointer to `T`.
#[inline]
pub(crate) fn compose_tag<T: Sized>(ptr: *mut T, tag: usize) -> *mut T {
    ((ptr as usize & !low_bits::<T>()) | (tag & low_bits::<T>())) as _
}

#[inline]
pub(crate) fn base_ptr<T: Sized>(ptr: *mut T) -> *mut T {
    (ptr as usize & !low_bits::<T>()) as _
}

#[inline]
pub(crate) fn tag<T: Sized>(ptr: *mut T) -> usize {
    ptr as usize & low_bits::<T>()
}
