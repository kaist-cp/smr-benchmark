use std::cell::RefCell;

use domain::Global;
use thread::Handle;

pub mod crcu;
mod domain;
mod guard;
mod hazard;
mod pointers;
pub(crate) mod sync;
mod thread;

const DOMAIN: Global = Global::new();

thread_local! {
    static THREAD: RefCell<Box<Handle>> = RefCell::new(Box::new(Handle::new(&DOMAIN)));
}

pub use guard::*;
pub use pointers::*;
