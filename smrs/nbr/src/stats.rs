use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) static GLOBAL_GARBAGE_COUNT: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn incr_garb(count: usize) {
    GLOBAL_GARBAGE_COUNT.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn decr_garb(count: usize) {
    GLOBAL_GARBAGE_COUNT.fetch_sub(count, Ordering::Relaxed);
}

/// Get current count of unreclaimed pointers.
pub fn count_garbages() -> usize {
    GLOBAL_GARBAGE_COUNT.load(Ordering::Relaxed)
}
