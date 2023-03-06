// TODO
// #![warn(missing_docs)]
// #![warn(missing_debug_implementations)]

// Enabled unsable feature to use
// unstable functions of AtomicPtr (AtomicPtr::fetch_or)
#![feature(strict_provenance_atomic_ptr, strict_provenance)]

#[macro_use]
extern crate cfg_if;

cfg_if! {
    if #[cfg(all(not(feature = "sanitize"), target_os = "linux"))] {
        extern crate jemallocator;
        #[global_allocator]
        static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
    }
}

extern crate crossbeam_ebr;
extern crate crossbeam_pebr;
extern crate crossbeam_utils;
#[macro_use]
extern crate bitflags;
extern crate typenum;
#[macro_use]
extern crate scopeguard;

#[macro_use]
mod utils;

pub mod ebr;
pub mod hp;
pub mod hp_pp;
pub mod pebr;

use core::cell::Cell;

thread_local! {
    static TRAVERSE_COUNT: Cell<u64> = Cell::new(0);
    static RESTART_COUNT: Cell<u64> = Cell::new(0);
}
