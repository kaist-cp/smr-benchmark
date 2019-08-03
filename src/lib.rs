// TODO
// #![warn(missing_docs)]
// #![warn(missing_debug_implementations)]

extern crate jemallocator;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate crossbeam_ebr;
extern crate crossbeam_pebr;
extern crate crossbeam_utils;
#[macro_use]
extern crate bitflags;
extern crate typenum;

pub mod ebr;
pub mod pebr;
