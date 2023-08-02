//! A *Crash-Optimized RCU*.

use atomic::{fence, Ordering};
use nix::errno::Errno;
use std::mem;

use crate::{set_data, Bag, Epoch, Global, SealedBag, Thread, BAGS_WIDTH, Local};

use super::rollback;

pub trait GlobalRRCU {
    /// Registers a current thread as a participant associated with this [`GlobalRRCU`] epoch
    /// manager.
    fn register(&self) -> Thread;
    /// Attempts to advance the global epoch.
    ///
    /// The global epoch can advance if all currently pinned participants have been pinned in
    /// the current epoch.
    ///
    /// Returns the current global epoch.
    ///
    /// `try_advance()` is annotated `#[cold]` because it is rarely called.
    fn try_advance(&self) -> Result<Epoch, Epoch>;
    /// Force advancing the global epoch.
    ///
    /// if currently there are pinned participants have been pinned in the other epoch than
    /// the current global epoch, sends signals to restart the slow participants.
    ///
    /// Returns the advanced global epoch.
    ///
    /// `advance()` is annotated `#[cold]` because it is rarely called.
    fn advance(&self, advancer: &mut Local) -> Epoch;
}

impl Global {
    #[inline]
    pub(crate) fn push_bag(&self, bag: &mut Bag) {
        self.garbage_count.fetch_add(bag.len(), Ordering::SeqCst);
        let bag = mem::take(bag);

        fence(Ordering::SeqCst);

        let epoch = self.epoch.load(Ordering::Relaxed);
        let slot = &self.epoch_bags[epoch.value() as usize % (1 << BAGS_WIDTH)];
        slot.push(SealedBag { epoch, inner: bag });
    }

    #[must_use]
    pub(crate) fn collect(&self, global_epoch: Epoch) -> Vec<Bag> {
        let index = (global_epoch.value() - 2) as usize % (1 << BAGS_WIDTH);
        let bags = unsafe { self.epoch_bags.get_unchecked(index) };

        let deferred = bags.pop_all();
        let (collected, deferred): (Vec<_>, Vec<_>) = deferred
            .into_iter()
            .partition(|bag| bag.is_expired(global_epoch));

        bags.append(deferred.into_iter());
        collected.into_iter().map(|bag| bag.into_inner()).collect()
    }
}

impl GlobalRRCU for Global {
    #[inline]
    fn register(&self) -> Thread {
        // Install a signal handler to handle manual crash triggered by a reclaimer.
        unsafe { rollback::install() };
        let local = self.acquire();
        set_data(local);
        Thread { local }
    }

    #[cold]
    fn try_advance(&self) -> Result<Epoch, Epoch> {
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

    #[cold]
    fn advance(&self, advancer: &mut Local) -> Epoch {
        let advancer_owner = advancer.owner.load(Ordering::Relaxed);
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
                let owner = local.owner.load(Ordering::Relaxed);
                if advancer_owner == owner {
                    advancer.unpin_inner();
                }
                match unsafe { rollback::send_signal(owner) } {
                    // `ESRCH` indicates that the given pthread is already exited.
                    Ok(_) | Err(Errno::ESRCH) => {}
                    Err(err) => panic!("Failed to restart the thread: {}", err),
                }
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

#[cfg(test)]
mod test {
    use crate::rrcu::{CsGuardRRCU, Deferrable, GlobalRRCU, ThreadRRCU};
    use crate::Deferred;

    use super::Global;
    use std::hint::black_box;
    use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
    use std::thread::scope;

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
                        let mut thread = global.register();
                        for _ in 0..COUNT_PER_THREAD {
                            thread.pin(|guard| {
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
