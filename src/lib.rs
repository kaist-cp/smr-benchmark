// TODO
// #![warn(missing_docs)]
// #![warn(missing_debug_implementations)]

extern crate jemallocator;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate crossbeam_ebr;
extern crate crossbeam_utils;
#[macro_use]
extern crate bitflags;

pub mod concurrent_map;

pub mod bonsai_tree;
pub mod harris_michael_list;
pub mod michael_hash_map;
pub mod natarajan_mittal_tree;
