use arrayvec::ArrayVec;

use crate::pebr_backend::{
    bloom_filter::BloomFilter,
    deferred::{Deferred, DeferredWithHazard},
};

/// Maximum number of objects a bag can contain.
#[cfg(not(feature = "sanitize"))]
const MAX_OBJECTS: usize = 64;
#[cfg(feature = "sanitize")]
const MAX_OBJECTS: usize = 4;

/// A garbage to be collected.
// TODO(@jeehoonkang): I hope the layout of `Garbage` be optimized, as each case has a nonzero
// component inside it: `inner.call` for `Deferred` and `dtor` for `Destroy`.  See
// https://github.com/rust-lang/rust/issues/46213 for more information on enum layout optimizations.
#[derive(Debug)]
pub enum Garbage {
    Deferred { inner: Deferred },
    DeferredWithHazard { inner: DeferredWithHazard },
    Destroy { data: usize, dtor: unsafe fn(usize) },
}

impl Garbage {
    /// Returns if the garbage is hazardous.
    #[inline]
    pub fn is_hazardous(&self, hazards: Option<&BloomFilter>) -> bool {
        match self {
            Garbage::Deferred { .. } => false,
            Garbage::DeferredWithHazard { inner } => {
                hazards.map(|h| h.query(inner.hazard)).unwrap_or(false)
            }
            Garbage::Destroy { data, .. } => hazards.map(|h| h.query(*data)).unwrap_or(false),
        }
    }

    /// Disposes the garbage.
    #[inline]
    pub fn dispose(self) {
        match self {
            Garbage::Deferred { inner } => inner.call(),
            Garbage::DeferredWithHazard { inner } => inner.call(),
            Garbage::Destroy { data, dtor } => unsafe { dtor(data) },
        }
    }
}

/// A bag of deferred functions.
#[derive(Default, Debug)]
pub struct Bag {
    /// Stashed garbages.
    garbages: ArrayVec<[Garbage; MAX_OBJECTS]>,
}

/// `Bag::try_push()` requires that it is safe for another thread to execute the given functions.
unsafe impl Send for Bag {}

impl Bag {
    /// Returns a new, empty bag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the bag is empty.
    pub fn is_empty(&self) -> bool {
        self.garbages.is_empty()
    }

    pub fn len(&self) -> usize {
        self.garbages.len()
    }

    /// Attempts to insert a deferred function into the bag.
    ///
    /// Returns `Ok(())` if successful, and `Err(deferred)` for the given `deferred` if the bag is
    /// full.
    ///
    /// # Safety
    ///
    /// It should be safe for another thread to execute the given function.
    pub unsafe fn try_push(&mut self, garbage: Garbage) -> Result<(), Garbage> {
        self.garbages.try_push(garbage).map_err(|e| e.element())
    }

    /// Disposes the bag except for hazard pointers.
    pub fn dispose(&mut self, hazards: Option<&BloomFilter>) {
        let hazards = self
            .garbages
            .drain(..)
            .filter_map(|g| {
                if g.is_hazardous(hazards) {
                    Some(g)
                } else {
                    g.dispose();
                    None
                }
            })
            .collect::<ArrayVec<_>>();
        self.garbages = hazards;
    }
}

impl Drop for Bag {
    fn drop(&mut self) {
        // Collect all garbages.
        for garbage in self.garbages.drain(..) {
            garbage.dispose();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::pebr_backend::deferred::Deferred;

    #[test]
    fn check_bag() {
        static FLAG: AtomicUsize = AtomicUsize::new(0);
        fn incr() {
            FLAG.fetch_add(1, Ordering::Relaxed);
        }

        let mut bag = Bag::new();
        assert!(bag.is_empty());

        for _ in 0..MAX_OBJECTS {
            assert!(unsafe {
                bag.try_push(Garbage::Deferred {
                    inner: Deferred::new(incr),
                })
                .is_ok()
            });
            assert!(!bag.is_empty());
            assert_eq!(FLAG.load(Ordering::Relaxed), 0);
        }

        let result = unsafe {
            bag.try_push(Garbage::Deferred {
                inner: Deferred::new(incr),
            })
        };
        assert!(result.is_err());
        assert!(!bag.is_empty());
        assert_eq!(FLAG.load(Ordering::Relaxed), 0);

        drop(bag);
        assert_eq!(FLAG.load(Ordering::Relaxed), MAX_OBJECTS);
    }
}
