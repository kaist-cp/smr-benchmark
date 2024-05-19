#![feature(cfg_sanitize)]
mod smr;
mod smr_common;
mod strong;
mod utils;
mod weak;

pub use smr::*;
pub use smr_common::*;
pub use strong::*;
pub use utils::*;
pub use weak::*;

#[inline]
pub fn set_counts_between_flush_ebr(counts: usize) {
    smr::ebr_impl::set_bag_capacity(counts);
}

#[inline]
pub fn set_counts_between_flush_hp(counts: usize) {
    smr::hp_impl::set_counts_between_flush(counts);
}
