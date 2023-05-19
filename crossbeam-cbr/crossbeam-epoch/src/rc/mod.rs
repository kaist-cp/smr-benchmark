//! Reference-counting primitives.

mod pointers;
mod utils;

pub use pointers::{AcquiredPtr, Atomic, Shield, Localizable, Rc, Shared};
