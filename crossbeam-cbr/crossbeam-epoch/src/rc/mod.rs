//! Reference-counting primitives.

mod pointers;
mod utils;

pub use pointers::{AcquiredPtr, Atomic, Localizable, Rc, Shared, Shield};
