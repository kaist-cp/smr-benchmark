use core::mem;

#[inline]
pub fn low_bits<T>() -> usize {
    (1 << mem::align_of::<T>().trailing_zeros()) - 1
}

#[inline]
pub fn ptr_with_tag<T>(ptr: *mut T, tag: usize) -> *mut T {
    ((ptr as usize & !low_bits::<T>()) | (tag & low_bits::<T>())) as *mut T
}

#[inline]
pub fn decompose_ptr<T>(ptr: *mut T) -> (*mut T, usize) {
    let ptr = ptr as usize;
    let raw = (ptr & !low_bits::<T>()) as *mut T;
    let tag = ptr & low_bits::<T>();
    (raw, tag)
}

#[inline]
// Extract an actual address out of a tagged pointer
pub fn remove_tag<T>(ptr: *mut T) -> *mut T {
    decompose_ptr(ptr).0
}

#[inline]
pub fn get_tag<T>(ptr: *mut T) -> usize {
    decompose_ptr(ptr).1
}
