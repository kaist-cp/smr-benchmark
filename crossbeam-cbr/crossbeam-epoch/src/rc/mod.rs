//! Reference-counting primitives.

mod pointers;
mod utils;

pub use pointers::{AcquiredPtr, Atomic, Defender, Rc, Shared, Shield};
