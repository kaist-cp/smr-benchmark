use atomic::Atomic;

use crate::internal::utils::{RcInner, TaggedCnt};

/// A SMR-specific acquired pointer trait.
///
/// In most cases such as EBR, IBR and Hyaline, Acquired is equivalent to a simple tagged
/// pointer pointing a Counted<T>.
///
/// However, for some pointer-based SMR, `Acquired` should contain other information like an
/// index of a hazard slot. For this reason, a type for acquired pointer must be SMR-dependent,
/// and every SMR must provide some reasonable interfaces to access and manage this pointer.
pub trait Acquired<T> {
    fn clear(&mut self);
    fn as_ptr(&self) -> TaggedCnt<T>;
    fn set_tag(&mut self, tag: usize);
    fn null() -> Self;
    fn is_null(&self) -> bool;
    fn swap(p1: &mut Self, p2: &mut Self);
    fn eq(&self, other: &Self) -> bool;
}

/// A SMR-specific critical section manager trait.
///
/// We construct this `Cs` right before starting an operation,
/// and drop(or `clear`) it after the operation.
pub trait Cs {
    /// A SMR-specific acquired pointer trait
    ///
    /// For more information, read a comment on `Acquired<T>`.
    type RawShield<T>: Acquired<T>;

    fn new() -> Self;
    unsafe fn unprotected() -> Self;
    fn create_object<T>(obj: T) -> *mut RcInner<T>;
    fn delete_object<T>(ptr: *mut RcInner<T>);
    /// Creates a shield for the given pointer, assuming that `ptr` is already protected by a
    /// reference count.
    fn reserve<T>(&self, ptr: TaggedCnt<T>, shield: &mut Self::RawShield<T>);
    fn protect_from_strong<T>(&self, link: &Atomic<TaggedCnt<T>>, shield: &mut Self::RawShield<T>);
    unsafe fn defer<T, F>(&self, ptr: *mut RcInner<T>, f: F)
    where
        F: FnOnce(&mut RcInner<T>);
    fn clear(&mut self);
}