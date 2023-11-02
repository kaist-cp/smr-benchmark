#![feature(associated_type_bounds)]
mod internal;
mod strongs;
mod weaks;

pub use internal::*;
pub use strongs::*;
pub use weaks::*;

#[inline]
pub fn set_counts_between_flush_ebr(counts: usize) {
    crossbeam::epoch::set_bag_capacity(counts);
}

#[inline]
pub fn set_counts_between_flush_hp(counts: usize) {
    internal::hp_impl::set_counts_between_flush(counts);
}
