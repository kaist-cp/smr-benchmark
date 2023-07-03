pub mod crcu;
pub mod hpsharp;
mod sync;
pub use hpsharp::*;

use std::cell::RefCell;

pub static GLOBAL: Global = Global::new();

thread_local! {
    pub static HANDLE: RefCell<Box<Handle>> = RefCell::new(Box::new(Handle::new(&GLOBAL)));
}

pub use sync::GLOBAL_GARBAGE_COUNT;
