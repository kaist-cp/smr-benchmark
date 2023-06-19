use arrayvec::{ArrayVec, IntoIter};

/// Maximum number of objects a bag can contain.
#[cfg(not(feature = "sanitize"))]
const MAX_OBJECTS: usize = 64;
#[cfg(feature = "sanitize")]
const MAX_OBJECTS: usize = 4;

/// A deferred task consisted of data and a callable function.
pub struct Deferred {
    pub(crate) data: *mut u8,
    task: unsafe fn(*mut u8),
}

impl Deferred {
    /// Executes and finalizes this deferred task.
    #[inline]
    pub fn execute(self) {
        unsafe { (self.task)(self.data) };
    }
}

unsafe impl Sync for Deferred {}
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

    /// Returns `true` if the bag is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.defs.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.defs.len()
    }

    /// Attempts to insert a deferred function into the bag.
    ///
    /// Returns `Ok(())` if successful, and `Err(deferred)` for the given `deferred` if the bag is
    /// full.
    pub fn try_push(&mut self, def: Deferred) -> Result<(), Deferred> {
        self.defs.try_push(def).map_err(|e| e.element())
    }

    /// Creates an iterator from a `Bag`.
    #[must_use]
    pub fn into_iter(self) -> IntoIter<Deferred, MAX_OBJECTS> {
        self.defs.into_iter()
    }
}
