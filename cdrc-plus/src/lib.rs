#![feature(associated_type_bounds)]
mod internal;
mod strongs;
mod weaks;

pub use internal::*;
pub use strongs::*;
pub use weaks::*;

/// AtomicRc using EBR
pub type AtomicRcEBR<T> = AtomicRc<T, CsEBR>;
/// Rc using EBR
pub type RcEBR<T> = Rc<T, CsEBR>;
/// Snapshot using EBR
pub type SnapshotEBR<T> = Snapshot<T, CsEBR>;
