mod deferred;
mod pile;

use std::sync::atomic::AtomicUsize;

pub use deferred::{Bag, Deferred};
pub use pile::Pile;
pub static GLOBAL_GARBAGE_COUNT: AtomicUsize = AtomicUsize::new(0);
