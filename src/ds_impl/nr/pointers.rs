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
    pub fn new(init: T) -> Self {
        let link = AtomicPtr::new(Box::into_raw(Box::new(init)));
        Self { link }
    }

    pub fn null() -> Self {
        let link = AtomicPtr::new(null_mut());
        Self { link }
    }

    pub fn load(&self, order: Ordering) -> Shared<T> {
        let ptr = self.link.load(order);
        Shared { ptr }
    }

    pub fn store(&self, ptr: Shared<T>, order: Ordering) {
        self.link.store(ptr.into_raw(), order)
    }

    pub fn fetch_or(&self, val: usize, order: Ordering) -> Shared<T> {
        let ptr = self.link.fetch_or(val, order);
        Shared { ptr }
    }

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

    pub unsafe fn into_owned(self) -> Box<T> {
        Box::from_raw(self.link.into_inner())
    }
}

impl<T> Default for Atomic<T> {
    fn default() -> Self {
        Self {
            link: AtomicPtr::default(),
        }
    }
}

impl<T> From<Shared<T>> for Atomic<T> {
    fn from(value: Shared<T>) -> Self {
        let link = AtomicPtr::new(value.into_raw());
        Self { link }
    }
}

impl<T> Clone for Atomic<T> {
    fn clone(&self) -> Self {
        Self {
            link: AtomicPtr::new(self.link.load(Ordering::Relaxed)),
        }
    }
}

pub struct Shared<T> {
    ptr: *mut T,
}

impl<T> Shared<T> {
    pub fn from_owned(init: T) -> Shared<T> {
        let ptr = Box::into_raw(Box::new(init));
        Self { ptr }
    }

    pub unsafe fn into_owned(self) -> T {
        *Box::from_raw(decompose_tag(self.ptr).0)
    }

    pub fn null() -> Self {
        Self { ptr: null_mut() }
    }

    pub fn tag(&self) -> usize {
        decompose_tag(self.ptr).1
    }

    pub fn is_null(&self) -> bool {
        decompose_tag(self.ptr).0.is_null()
    }

    pub fn with_tag(&self, tag: usize) -> Self {
        let ptr = compose_tag(self.ptr, tag);
        Self { ptr }
    }

    pub unsafe fn as_ref<'g>(&self) -> Option<&'g T> {
        decompose_tag(self.ptr).0.as_ref()
    }

    pub unsafe fn as_mut<'g>(&self) -> Option<&'g mut T> {
        decompose_tag(self.ptr).0.as_mut()
    }

    pub unsafe fn deref<'g>(&self) -> &'g T {
        &*decompose_tag(self.ptr).0
    }

    pub unsafe fn deref_mut<'g>(&mut self) -> &'g mut T {
        &mut *decompose_tag(self.ptr).0
    }
}

impl<T> From<usize> for Shared<T> {
    fn from(val: usize) -> Self {
        Self {
            ptr: val as *const T as *mut T,
        }
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Shared<T> {}

impl<T> PartialEq for Shared<T> {
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
    fn into_raw(self) -> *mut T {
        self.ptr
    }

    unsafe fn from_raw(val: *mut T) -> Self {
        Shared::from(val as usize)
    }
}

impl<T> Pointer<T> for Box<T> {
    fn into_raw(self) -> *mut T {
        Box::into_raw(self)
    }

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
    int_to_ptr_with_provenance(
        (ptr as usize & !low_bits::<T>()) | (tag & low_bits::<T>()),
        ptr,
    )
}

/// Decomposes a tagged pointer `data` into the pointer and the tag.
#[inline]
pub(crate) fn decompose_tag<T: Sized>(ptr: *mut T) -> (*mut T, usize) {
    (
        int_to_ptr_with_provenance(ptr as usize & !low_bits::<T>(), ptr),
        ptr as usize & low_bits::<T>(),
    )
}

// HACK: https://github.com/rust-lang/miri/issues/1866#issuecomment-985802751
#[inline]
fn int_to_ptr_with_provenance<T>(addr: usize, prov: *mut T) -> *mut T {
    let ptr = prov.cast::<u8>();
    ptr.wrapping_add(addr.wrapping_sub(ptr as usize)).cast()
}
