// TODO
// #![warn(missing_docs)]
// #![warn(missing_debug_implementations)]
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

pub mod ebr;
pub mod pebr;
