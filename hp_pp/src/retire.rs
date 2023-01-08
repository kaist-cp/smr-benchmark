pub(crate) struct Retired {
    pub(crate) ptr: *mut u8,
    pub(crate) deleter: unsafe fn(ptr: *mut u8),
}

// TODO: require <T: Send> in retire
unsafe impl Send for Retired {}

impl Retired {
    pub(crate) fn new<T>(ptr: *mut T) -> Self {
        unsafe fn free<T>(ptr: *mut u8) {
            drop(Box::from_raw(ptr as *mut T))
        }
        Self {
            ptr: ptr as *mut u8,
            deleter: free::<T>,
        }
    }
}
