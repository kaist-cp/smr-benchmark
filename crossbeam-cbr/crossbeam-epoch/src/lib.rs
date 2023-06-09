//! Reference-counting primitives with *PEBR* backend and signaling.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(feature = "nightly", feature(const_fn))]
#![cfg_attr(feature = "nightly", feature(cfg_target_has_atomic))]

#[macro_use]
extern crate cfg_if;
#[cfg(feature = "std")]
extern crate core;

cfg_if! {
    if #[cfg(feature = "alloc")] {
        extern crate alloc;
    } else if #[cfg(feature = "std")] {
        extern crate std as alloc;
    }
}

#[cfg_attr(
    feature = "nightly",
    cfg(all(target_has_atomic = "cas", target_has_atomic = "ptr"))
)]
cfg_if! {
    if #[cfg(any(feature = "alloc", feature = "std"))] {
        extern crate arrayvec;
        extern crate crossbeam_utils;
        #[macro_use]
        extern crate memoffset;
        #[macro_use]
        extern crate scopeguard;
        #[macro_use]
        extern crate static_assertions;
        extern crate murmur3;
        #[macro_use]
        extern crate bitflags;
        extern crate membarrier;
        extern crate nix;
        extern crate setjmp;

        pub mod pebr_backend;
        pub(crate) mod pointers;
        pub(crate) mod utils;

        pub use self::pebr_backend::{
            collector::{Collector, LocalHandle},
            guard::{
                unprotected, EpochGuard, ReadGuard, WriteGuard, Readable,
                Writable, ReadStatus, WriteResult, Defender,
            },
            recovery::{ejection_signal, set_ejection_signal},
        };
        pub use self::pointers::*;
        pub use self::utils::*;
    }
}

cfg_if! {
    if #[cfg(feature = "std")] {
        #[macro_use]
        extern crate lazy_static;

        pub use self::pebr_backend::default::{default_collector, is_pinned, pin};
    }
}

pub use self::pebr_backend::internal::GLOBAL_GARBAGE_COUNT;
