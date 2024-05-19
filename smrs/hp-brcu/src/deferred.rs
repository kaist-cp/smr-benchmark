use std::mem::forget;

use crate::epoch::Epoch;

/// Maximum number of objects a bag can contain.
#[cfg(not(sanitize = "address"))]
pub(crate) const MAX_OBJECTS: usize = 128;
#[cfg(sanitize = "address")]
pub(crate) const MAX_OBJECTS: usize = 4;

/// A deferred task consisted of data and a callable function.
///
/// Note that a [`Deferred`] must be finalized by `execute` function, and `drop`ping this object
/// will trigger a panic!
///
/// Also, [`Deferred`] is `Send` because it may be executed by an arbitrary thread.
#[derive(Debug)]
pub(crate) struct Deferred {
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
pub(crate) struct Bag {
    /// Stashed garbages.
    defs: Vec<Deferred>,
}

/// `Bag::try_push()` requires that it is safe for another thread to execute the given functions.
unsafe impl Send for Bag {}

impl Bag {
    /// Returns a new, empty bag.
    #[inline]
    pub fn new() -> Self {
        Self {
            defs: Vec::with_capacity(MAX_OBJECTS),
        }
    }

    /// Attempts to insert a deferred function into the bag.
    ///
    /// Returns `Ok(())` if successful, and `Err(deferred)` for the given `deferred` if the bag is
    /// full.
    #[inline]
    pub fn try_push(&mut self, def: Deferred) -> Result<(), Deferred> {
        if self.len() == MAX_OBJECTS {
            return Err(def);
        }
        self.defs.push(def);
        Ok(())
    }

    /// Creates an iterator of [`Deferred`] from a [`Bag`].
    #[inline]
    pub fn into_iter(self) -> impl Iterator<Item = Deferred> {
        self.defs.into_iter()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.defs.len()
    }
}

impl Default for Bag {
    fn default() -> Self {
        Self::new()
    }
}

/// A pair of an epoch and a bag.
pub(crate) struct SealedBag {
    epoch: Epoch,
    inner: Bag,
}

/// It is safe to share `SealedBag` because `is_expired` only inspects the epoch.
unsafe impl Sync for SealedBag {}

impl SealedBag {
    #[inline]
    pub(crate) fn new(epoch: Epoch, inner: Bag) -> Self {
        Self { epoch, inner }
    }

    /// Checks if it is safe to drop the bag w.r.t. the given global epoch.
    #[inline]
    pub(crate) fn is_expired(&self, global_epoch: Epoch) -> bool {
        global_epoch.value() - self.epoch.value() >= 2
    }

    #[inline]
    pub(crate) fn into_inner(self) -> Bag {
        self.inner
    }
}
