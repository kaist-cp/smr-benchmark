pub mod crcu;
pub mod hpsharp;
mod sync;
pub use hpsharp::*;

use std::cell::RefCell;

pub const DOMAIN: Global = Global::new();

thread_local! {
    pub static THREAD: RefCell<Box<Handle>> = RefCell::new(Box::new(Handle::new(&DOMAIN)));
}
