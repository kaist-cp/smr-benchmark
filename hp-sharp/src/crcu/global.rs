//! A *Crash-Optimized RCU*.

use atomic::fence;
use crossbeam_utils::CachePadded;
use nix::{errno::Errno, sys::pthread::pthread_self};

use crate::{
    sync::{Bag, Pile},
    GLOBAL_GARBAGE_COUNT,
};

use super::{
    epoch::{AtomicEpoch, Epoch},
    local::{Handle, LocalList},
    recovery,
};
use core::sync::atomic::Ordering;
use std::{mem, sync::atomic::compiler_fence};

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
        global_epoch.value() - self.epoch.value() >= 2
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
    pub fn register(&self) -> Handle {
        // Install a signal handler to handle manual crash triggered by a reclaimer.
        unsafe { recovery::install() };
        let tid = pthread_self();
        self.locals.acquire(tid, self)
    }

    pub(crate) fn push_bag(&self, bag: &mut Bag) {
        GLOBAL_GARBAGE_COUNT.fetch_add(bag.len(), Ordering::AcqRel);
        let bag = mem::take(bag);

        fence(Ordering::SeqCst);

        let epoch = self.epoch.load(Ordering::Relaxed);
        let slot = &self.bags[epoch.value() as usize % (1 << BAGS_WIDTH)];
        slot.push(SealedBag { epoch, inner: bag });
    }

    #[must_use]
    pub(crate) fn collect(&self, global_epoch: Epoch) -> Vec<Bag> {
        let index = (global_epoch.value() - 2) as usize % (1 << BAGS_WIDTH);
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

        for local in self.locals.iter_using() {
            let local_epoch = local.epoch.load(Ordering::Relaxed);

            // Someone has advanced the global epoch already.
            if local_epoch.value() > global_epoch.value() {
                return Ok(global_epoch.successor());
            }

            // If the participant was pinned in a different epoch, we cannot advance the
            // global epoch just yet.
            if local_epoch.is_pinned() && local_epoch.unpinned().value() < global_epoch.value() {
                return Err(global_epoch);
            }
        }
        fence(Ordering::Acquire);

        // All pinned participants were pinned in the current global epoch.
        // Now let's advance the global epoch...
        //
        // Note that advancing here may fail if other thread already have advanced the epoch.
        let new_epoch = global_epoch.successor();
        let _ = self.epoch.compare_exchange(
            global_epoch,
            new_epoch,
            Ordering::Release,
            Ordering::Relaxed,
        );
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

        for local in self.locals.iter_using() {
            let local_epoch = local.epoch.load(Ordering::Relaxed);

            // Someone has advanced the global epoch already.
            if local_epoch.value() > global_epoch.value() {
                return global_epoch.successor();
            }

            // If the participant was pinned in a different epoch, we eject its epoch.
            if local_epoch.is_pinned() && local_epoch.unpinned().value() < global_epoch.value() {
                match unsafe { recovery::send_signal(local.owner.load(Ordering::Relaxed)) } {
                    // `ESRCH` indicates that the given pthread is already exited.
                    Ok(_) | Err(Errno::ESRCH) => {}
                    Err(err) => panic!("Failed to restart the thread: {}", err),
                }
                compiler_fence(Ordering::SeqCst);

                // As the slow thread will be interrupted by a signal, it is safe to eject its
                // epoch and reset it into a starting value. After being interrupted by the kernel
                // and performing `longjmp`, the slow thread will repin its epoch with the latest
                // global epoch. If repining happened before `compare_exchange` here, the operation
                // would fail because the global epoch which the restarted thread read cannot be
                // equal to `local_epoch` which the reclaimer read.
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
        // Note that advancing here may fail if other thread already have advanced the epoch.
        let new_epoch = global_epoch.successor();
        let _ = self.epoch.compare_exchange(
            global_epoch,
            new_epoch,
            Ordering::Release,
            Ordering::Relaxed,
        );
        new_epoch
    }
}

impl Drop for Global {
    fn drop(&mut self) {
        self.locals
            .iter()
            .flat_map(|local| mem::take(unsafe { &mut *local.bag.get() }).into_iter())
            .chain(self.bags.iter_mut().flat_map(|pile| {
                pile.pop_all()
                    .into_iter()
                    .flat_map(|bag| bag.inner.into_iter())
            }))
            .for_each(|def| unsafe { def.execute() });
    }
}

#[cfg(test)]
mod test {
    use crate::crcu::Deferrable;
    use crate::sync::Deferred;

    use super::Global;
    use std::hint::black_box;
    use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
    use std::thread::scope;

    #[test]
    fn try_advance() {
        let global = &Global::new();
        let sync_clock = &AtomicUsize::new(0);

        // If there's no working thread, `try_advance` would trivially succeed.
        assert!(global.try_advance().is_ok());

        // Let's simulate a pinned slow thread.
        scope(|s| {
            s.spawn(|| {
                let mut handle = global.register();
                unsafe {
                    handle.pin(|_| {
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
    fn advance() {
        let global = &Global::new();
        let sync_clock = &AtomicUsize::new(0);

        // Let's simulate a pinned slow thread.
        scope(|s| {
            s.spawn(|| {
                let mut handle = global.register();
                unsafe {
                    handle.pin(|_| {
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

    #[test]
    fn defer_incrs() {
        const THREADS: usize = 30;
        const COUNT_PER_THREAD: usize = 4096;

        let sum = &AtomicUsize::new(0);
        {
            unsafe fn increment(ptr: *mut u8) {
                let ptr = ptr as *mut AtomicUsize;
                (*ptr).fetch_add(1, Ordering::SeqCst);
            }
            let global = &Global::new();
            scope(|s| {
                for _ in 0..THREADS {
                    s.spawn(|| {
                        let mut handle = global.register();
                        for _ in 0..COUNT_PER_THREAD {
                            if let Some(collected) =
                                handle.defer(Deferred::new(sum as *const _ as *mut _, increment))
                            {
                                for def in collected {
                                    unsafe { def.execute() };
                                }
                            }
                        }
                    });
                }
            });
        }
        assert_eq!(sum.load(Ordering::SeqCst), THREADS * COUNT_PER_THREAD);
    }

    #[test]
    fn single_node() {
        const THREADS: usize = 30;
        const COUNT_PER_THREAD: usize = 1 << 20;

        let head = AtomicPtr::new(Box::into_raw(Box::new(0i32)));
        unsafe {
            unsafe fn free<T>(ptr: *mut u8) {
                let ptr = ptr as *mut T;
                drop(Box::from_raw(ptr));
            }
            let global = &Global::new();
            scope(|s| {
                for _ in 0..THREADS {
                    s.spawn(|| {
                        let mut handle = global.register();
                        for _ in 0..COUNT_PER_THREAD {
                            handle.pin(|guard| {
                                let ptr = head.load(Ordering::Acquire);
                                guard.mask(|guard| {
                                    let new = Box::into_raw(Box::new(0i32));
                                    if head
                                        .compare_exchange(
                                            ptr,
                                            new,
                                            Ordering::AcqRel,
                                            Ordering::Acquire,
                                        )
                                        .is_ok()
                                    {
                                        *ptr += 1;
                                        if let Some(collected) =
                                            guard.defer(Deferred::new(ptr as *mut _, free::<i32>))
                                        {
                                            for def in collected {
                                                def.execute();
                                            }
                                        }
                                    } else {
                                        drop(Box::from_raw(new));
                                    }
                                });

                                // This read must be safe.
                                let read = black_box(*ptr + 1);
                                black_box(read);
                            })
                        }
                    });
                }
            });
            drop(Box::from_raw(head.load(Ordering::Acquire)));
        }
    }
}
