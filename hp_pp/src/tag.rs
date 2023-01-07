use core::mem;

/// Returns a bitmask containing the unused least significant bits of an aligned pointer to `T`.
#[inline]
const fn low_bits<T>() -> usize {
    (1 << mem::align_of::<T>().trailing_zeros()) - 1
}

/// Returns the pointer with the given tag
#[inline]
pub fn tagged<T>(ptr: *mut T, tag: usize) -> *mut T {
    ((ptr as usize & !low_bits::<T>()) | (tag & low_bits::<T>())) as *mut T
}

/// Decomposes a tagged pointer `data` into the pointer and the tag.
#[inline]
pub fn decompose_ptr<T>(ptr: *mut T) -> (*mut T, usize) {
    let ptr = ptr as usize;
    let raw = (ptr & !low_bits::<T>()) as *mut T;
    let tag = ptr & low_bits::<T>();
    (raw, tag)
}

/// Extract the actual address out of a tagged pointer
#[inline]
pub fn untagged<T>(ptr: *mut T) -> *mut T {
    let ptr = ptr as usize;
    (ptr & !low_bits::<T>()) as *mut T
}

/// Extracts the tag out of a tagged pointer
#[inline]
pub fn tag<T>(ptr: *mut T) -> usize {
    let ptr = ptr as usize;
    ptr & low_bits::<T>()
}
