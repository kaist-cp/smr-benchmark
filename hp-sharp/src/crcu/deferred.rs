/// A deferred task consisted of data and a callable function.
pub struct Deferred {
    data: *mut u8,
    task: unsafe fn(*mut u8),
}

unsafe impl Sync for Deferred {}
unsafe impl Send for Deferred {}
