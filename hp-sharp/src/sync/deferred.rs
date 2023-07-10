use std::mem::forget;

use arrayvec::{ArrayVec, IntoIter};

/// Maximum number of objects a bag can contain.
#[cfg(not(sanitize = "address"))]
const MAX_OBJECTS: usize = 128;
#[cfg(sanitize = "address")]
const MAX_OBJECTS: usize = 4;

/// A deferred task consisted of data and a callable function.
///
/// Note that a [`Deferred`] must be finalized by `execute` function, and `drop`ping this object
/// will trigger a panic!
///
/// Also, [`Deferred`] is `Send` because it may be executed by an arbitrary thread.
#[derive(Debug)]
pub struct Deferred {
    data: *mut u8,
    task: unsafe fn(*mut u8),
}

impl Deferred {
    #[inline]
    #[must_use]
    pub fn new(data: *mut u8, task: unsafe fn(*mut u8)) -> Self {
        Self { data, task }
    }

    /// Executes and finalizes this deferred task.
    #[inline]
    pub unsafe fn execute(self) {
        (self.task)(self.data);
        // Prevent calling the `drop` for this object.
        forget(self);
    }

    /// Returns a copy of inner `data`.
    #[inline]
    pub fn data(&self) -> *mut u8 {
        self.data
    }
}

impl Drop for Deferred {
    fn drop(&mut self) {
        // Note that a `Deferred` must be finalized by `execute` function.
        // In other words, we must make sure that all deferred tasks are executed consequently!
        panic!("`Deferred` task must be finalized by `execute`!");
    }
}

/// [`Deferred`] can be collected by arbitrary threads.
unsafe impl Send for Deferred {}

/// A bag of deferred functions.
#[derive(Default)]
pub struct Bag {
    /// Stashed garbages.
    defs: ArrayVec<Deferred, MAX_OBJECTS>,
}

/// `Bag::try_push()` requires that it is safe for another thread to execute the given functions.
unsafe impl Send for Bag {}

impl Bag {
    /// Returns a new, empty bag.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempts to insert a deferred function into the bag.
    ///
    /// Returns `Ok(())` if successful, and `Err(deferred)` for the given `deferred` if the bag is
    /// full.
    #[inline]
    pub fn try_push(&mut self, def: Deferred) -> Result<(), Deferred> {
        self.defs.try_push(def).map_err(|e| e.element())
    }

    /// Creates an iterator of [`Deferred`] from a [`Bag`].
    #[must_use]
    #[inline]
    pub fn into_iter(self) -> IntoIter<Deferred, MAX_OBJECTS> {
        self.defs.into_iter()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.defs.len()
    }
}
