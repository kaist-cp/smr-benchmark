/// Epoch-based garbage collector.
use core::fmt;
use core::sync::atomic::Ordering;

use super::guard::Guard;
use super::internal::{Global, Local};
use super::Epoch;
use std::sync::Arc;

/// An epoch-based garbage collector.
pub struct Collector {
    pub(crate) global: Arc<Global>,
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

impl Default for Collector {
    fn default() -> Self {
        Self {
            global: Arc::new(Global::new()),
        }
    }
}

impl Collector {
    /// Creates a new collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a new handle for the collector.
    pub fn register(&self) -> LocalHandle {
        Local::register(self)
    }

    /// Reads the global epoch, without issueing a fence.
    #[inline]
    pub fn global_epoch(&self) -> Epoch {
        self.global.epoch.load(Ordering::Relaxed)
    }

    /// Checks if the global queue is empty.
    pub fn is_global_queue_empty(&self) -> bool {
        self.global.is_global_queue_empty()
    }
}

impl Clone for Collector {
    /// Creates another reference to the same garbage collector.
    fn clone(&self) -> Self {
        Collector {
            global: self.global.clone(),
        }
    }
}

impl fmt::Debug for Collector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Collector { .. }")
    }
}

impl PartialEq for Collector {
    /// Checks if both handles point to the same collector.
    fn eq(&self, rhs: &Collector) -> bool {
        Arc::ptr_eq(&self.global, &rhs.global)
    }
}
impl Eq for Collector {}

/// A handle to a garbage collector.
pub struct LocalHandle {
    pub(crate) local: *const Local,
}

impl LocalHandle {
    /// Pins the handle.
    #[inline]
    pub fn pin(&self) -> Guard {
        unsafe { (*self.local).pin() }
    }

    /// Returns `true` if the handle is pinned.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        unsafe { (*self.local).is_pinned() }
    }

    /// Returns the `Collector` associated with this handle.
    #[inline]
    pub fn collector(&self) -> &Collector {
        unsafe { (*self.local).collector() }
    }
}

impl Drop for LocalHandle {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            Local::release_handle(&*self.local);
        }
    }
}

impl fmt::Debug for LocalHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("LocalHandle { .. }")
    }
}
