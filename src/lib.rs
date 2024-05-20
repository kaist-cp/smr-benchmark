#![doc = include_str!("../README.md")]
#![feature(strict_provenance_atomic_ptr, strict_provenance)]
#![feature(cfg_sanitize)]

#[macro_use]
extern crate cfg_if;

cfg_if! {
    if #[cfg(all(not(feature = "sanitize"), target_os = "linux"))] {
        extern crate tikv_jemallocator;
        #[global_allocator]
        static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

        extern crate tikv_jemalloc_ctl;
        pub struct MemSampler {
            epoch_mib: tikv_jemalloc_ctl::epoch_mib,
            allocated_mib: tikv_jemalloc_ctl::stats::allocated_mib,
        }

        impl Default for MemSampler {
            fn default() -> Self {
                Self::new()
            }
        }

        impl MemSampler {
            pub fn new() -> Self {
                MemSampler {
                    epoch_mib: tikv_jemalloc_ctl::epoch::mib().unwrap(),
                    allocated_mib: tikv_jemalloc_ctl::stats::allocated::mib().unwrap(),
                }
            }
            pub fn sample(&self) -> usize {
                self.epoch_mib.advance().unwrap();
                self.allocated_mib.read().unwrap()
            }
        }
    } else {
        pub struct MemSampler {}

        impl Default for MemSampler {
            fn default() -> Self {
                Self::new()
            }
        }

        impl MemSampler {
            pub fn new() -> Self {
                println!("NOTE: Memory usage benchmark is supported only for linux.");
                MemSampler {}
            }
            pub fn sample(&self) -> usize {
                0
            }
        }
    }
}

extern crate crossbeam_ebr;
extern crate crossbeam_utils;
#[macro_use]
extern crate bitflags;
extern crate clap;
extern crate typenum;

#[macro_use]
mod utils;
pub mod config;
pub mod ds_impl;
