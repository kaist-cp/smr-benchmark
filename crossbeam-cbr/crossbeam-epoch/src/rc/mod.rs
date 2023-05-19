//! Reference-counting primitives.

mod pointers;
mod utils;

pub use pointers::{AcquiredPtr, AtomicRcPtr, LocalPtr, Localizable, RcPtr, ReadPtr};
