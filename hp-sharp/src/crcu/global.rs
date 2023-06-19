//! A *Crash-Optimized RCU*.

use atomic::fence;
use crossbeam_utils::CachePadded;
use nix::{errno::Errno, sys::pthread::pthread_self};

use crate::Bag;

use super::{
    epoch::{AtomicEpoch, Epoch},
    local::{LocalHandle, LocalList},
    pile::Pile,
    recovery,
};
use core::sync::atomic::Ordering;
use std::mem;

/// The width of the number of bags.
const BAGS_WIDTH: u32 = 3;

/// A pair of an epoch and a bag.
struct SealedBag {
    epoch: Epoch,
    inner: Bag,
}

/// It is safe to share `SealedBag` because `is_expired` only inspects the epoch.
unsafe impl Sync for SealedBag {}

impl SealedBag {
    /// Checks if it is safe to drop the bag w.r.t. the given global epoch.
    fn is_expired(&self, global_epoch: Epoch) -> bool {
        global_epoch.value() - self.epoch.value() >= 3
    }
}

/// The global data for a garbage collector.
pub struct Global {
    /// The intrusive linked list of `Local`s.
    locals: LocalList,

    /// The global pool of bags of deferred functions.
    bags: [CachePadded<Pile<SealedBag>>; 1 << BAGS_WIDTH],

    /// The global epoch.
    pub(crate) epoch: CachePadded<AtomicEpoch>,
}

impl Global {
    /// Creates a new global data for garbage collection.
    ///
    /// ```
    /// use hp_sharp::crcu::Global;
    ///
    /// let global = Global::new();
    /// ```
    #[inline]
    pub const fn new() -> Self {
        Self {
            locals: LocalList::new(),
            bags: [
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
            ],
            epoch: CachePadded::new(AtomicEpoch::new(Epoch::starting())),
        }
    }

    /// Registers a current thread as a participant associated with this [`Global`] epoch
    /// manager.
    ///
    /// ```
    /// use hp_sharp::crcu::Global;
    ///
    /// let global = Global::new();
    ///
    /// let handle = global.register();
    /// ```
    #[inline]
    pub fn register(&self) -> LocalHandle {
        // Install a signal handler to handle manual crash triggered by a reclaimer.
        unsafe { recovery::install() };
        let tid = pthread_self();
        self.locals.acquire(tid, self)
    }

    pub(crate) fn push_bag(&self, bag: &mut Bag) {
        let bag = mem::take(bag);

        fence(Ordering::SeqCst);

        let epoch = self.epoch.load(Ordering::Relaxed);
        let slot = &self.bags[epoch.value() as usize % (1 << BAGS_WIDTH)];
        slot.push(SealedBag { epoch, inner: bag });
    }

    #[must_use]
    pub(crate) fn collect(&self, global_epoch: Epoch) -> Vec<Bag> {
        let index = (global_epoch.value() - 3) as usize % (1 << BAGS_WIDTH);
        let bags = unsafe { self.bags.get_unchecked(index) };

        let deferred = bags.pop_all();
        let (collected, deferred): (Vec<_>, Vec<_>) = deferred
            .into_iter()
            .partition(|bag| bag.is_expired(global_epoch));

        bags.append(deferred.into_iter());
        collected.into_iter().map(|bag| bag.inner).collect()
    }

    /// Attempts to advance the global epoch.
    ///
    /// The global epoch can advance if all currently pinned participants have been pinned in
    /// the current epoch.
    ///
    /// Returns the current global epoch.
    ///
    /// `try_advance()` is annotated `#[cold]` because it is rarely called.
    ///
    /// # Example
    ///
    /// ```
    /// use hp_sharp::crcu::Global;
    ///
    /// let global = &Global::new();
    ///
    /// // If there's no working thread, `try_advance` would trivially succeed.
    /// assert!(global.try_advance().is_ok());
    /// ```
    #[cold]
    pub fn try_advance(&self) -> Result<Epoch, Epoch> {
        let global_epoch = self.epoch.load(Ordering::Relaxed);
        fence(Ordering::SeqCst);

        for local in self.locals.iter() {
            let local_epoch = local.epoch.load(Ordering::Relaxed);

            // If the participant was pinned in a different epoch, we cannot advance the
            // global epoch just yet.
            if local_epoch.is_pinned() && local_epoch.unpinned() != global_epoch {
                return Err(global_epoch);
            }
        }
        fence(Ordering::Acquire);

        // All pinned participants were pinned in the current global epoch.
        // Now let's advance the global epoch...
        //
        // Note that if another thread already advanced it before us, this store will simply
        // overwrite the global epoch with the same value. This is true because `try_advance` was
        // called from a thread that was pinned in `global_epoch`, and the global epoch cannot be
        // advanced two steps ahead of it.
        let new_epoch = global_epoch.successor();
        self.epoch.store(new_epoch, Ordering::Release);
        Ok(new_epoch)
    }

    /// Force advancing the global epoch.
    ///
    /// if currently there are pinned participants have been pinned in the other epoch than
    /// the current global epoch, sends signals to restart the slow participants.
    ///
    /// Returns the advanced global epoch.
    ///
    /// `advance()` is annotated `#[cold]` because it is rarely called.
    ///
    /// # Example
    ///
    /// ```
    /// use hp_sharp::crcu::Global;
    ///
    /// let global = &Global::new();
    ///
    /// // `advance` always succeeds, and it will restart any slow threads.
    /// global.advance();
    /// ```
    #[cold]
    pub fn advance(&self) -> Epoch {
        let global_epoch = self.epoch.load(Ordering::Relaxed);
        fence(Ordering::SeqCst);

        for local in self.locals.iter() {
            let local_epoch = local.epoch.load(Ordering::Relaxed);

            // If the participant was pinned in a different epoch, we eject its epoch.
            if local_epoch.is_pinned() && local_epoch.unpinned() != global_epoch {
                match unsafe { recovery::send_signal(local.owner.load(Ordering::Relaxed)) } {
                    // `ESRCH` indicates that the given pthread is already exited.
                    Ok(_) | Err(Errno::ESRCH) => {}
                    Err(err) => panic!("Failed to restart the thread: {}", err),
                }
                let _ = local.epoch.compare_exchange(
                    local_epoch,
                    Epoch::starting(),
                    Ordering::Release,
                    Ordering::Relaxed,
                );
            }
        }
        fence(Ordering::Acquire);

        // All pinned participants were pinned in the current global epoch.
        // Now let's advance the global epoch...
        //
        // Note that if another thread already advanced it before us, this store will simply
        // overwrite the global epoch with the same value. This is true because `try_advance` was
        // called from a thread that was pinned in `global_epoch`, and the global epoch cannot be
        // advanced two steps ahead of it.
        let new_epoch = global_epoch.successor();
        self.epoch.store(new_epoch, Ordering::Release);
        new_epoch
    }
}

#[cfg(test)]
mod test {
    use super::Global;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::scope;

    #[test]
    fn test_try_advance() {
        let global = &Global::new();
        let sync_clock = &AtomicUsize::new(0);

        // If there's no working thread, `try_advance` would trivially succeed.
        assert!(global.try_advance().is_ok());

        // Let's simulate a pinned slow thread.
        scope(|s| {
            s.spawn(|| {
                let handle = global.register();
                unsafe {
                    handle.read(|_| {
                        // Intentionally avoided using `Barrier` for synchronization.
                        // Although there will be no signaling in this example, it is worth
                        // noting that `Barrier` is not safe to use in a crashable section,
                        // because `Barrier` uses `Mutex` which involves a system call.
                        //
                        // Also, usually performing writing in `read` is not desirable. In this
                        // example, we used `fetch_add` just to demostrate the effect of
                        // `try_advance`.
                        sync_clock.fetch_add(1, Ordering::SeqCst);
                        while sync_clock.load(Ordering::SeqCst) == 1 {}
                    });
                }
            });

            while sync_clock.load(Ordering::SeqCst) == 0 {}

            // The first advancing must succeed because the pinned participant
            // is on the global epoch.
            assert!(global.try_advance().is_ok());
            // However, the next advancing will fail.
            assert!(global.try_advance().is_err());

            sync_clock.fetch_add(1, Ordering::SeqCst);
        });
    }

    #[test]
    fn test_advance() {
        let global = &Global::new();
        let sync_clock = &AtomicUsize::new(0);

        // Let's simulate a pinned slow thread.
        scope(|s| {
            s.spawn(|| {
                let handle = global.register();
                unsafe {
                    handle.read(|_| {
                        // Intentionally avoided using `Barrier` for synchronization.
                        // It is worth noting that `Barrier` is not safe to use in a crashable
                        // section, because `Barrier` uses `Mutex` which involves a system call.
                        //
                        // Also, usually performing writing in `read` is not desirable. In this
                        // example, we used `fetch_add` just to demostrate the effect of
                        // `try_advance` and `advance`.
                        sync_clock.fetch_add(1, Ordering::SeqCst);
                        while sync_clock.load(Ordering::SeqCst) == 1 {}
                    });
                }
            });

            while sync_clock.load(Ordering::SeqCst) == 0 {}

            // The first advancing must succeed because the pinned participant
            // is on the global epoch.
            assert!(global.try_advance().is_ok());
            // However, the next advancing will fail.
            assert!(global.try_advance().is_err());

            // `advance` always succeeds, and it will restart any slow threads.
            global.advance();
        });
    }
}
