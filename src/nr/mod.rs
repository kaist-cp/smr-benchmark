pub mod concurrent_map;

// pub mod bonsai_tree;
// pub mod ellen_tree;
pub mod list;
pub mod michael_hash_map;
// pub mod natarajan_mittal_tree;
// pub mod skip_list;

pub use self::concurrent_map::ConcurrentMap;

// pub use self::bonsai_tree::BonsaiTreeMap;
// pub use self::ellen_tree::EFRBTree;
pub use self::list::{HHSList, HList, HMList};
pub use self::michael_hash_map::HashMap;
// pub use self::natarajan_mittal_tree::NMTreeMap;
// pub use self::skip_list::SkipList;

use core::mem;

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

#[inline]
pub(crate) fn untagged<T: Sized>(ptr: *mut T) -> *mut T {
    decompose_tag(ptr).0
}

#[inline]
pub(crate) fn tag<T: Sized>(ptr: *mut T) -> usize {
    decompose_tag(ptr).1
}

// HACK: https://github.com/rust-lang/miri/issues/1866#issuecomment-985802751
#[inline]
fn int_to_ptr_with_provenance<T>(addr: usize, prov: *mut T) -> *mut T {
    let ptr = prov.cast::<u8>();
    ptr.wrapping_add(addr.wrapping_sub(ptr as usize)).cast()
}