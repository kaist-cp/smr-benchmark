use std::{
    cell::{Cell, UnsafeCell},
    marker::PhantomData,
    ptr::null_mut,
    sync::atomic::{compiler_fence, fence, AtomicBool, AtomicPtr, Ordering},
};

use atomic::Atomic;
use crossbeam_utils::CachePadded;
use nix::sys::{
    pthread::Pthread,
    signal::{pthread_sigmask, SigmaskHow},
    signalfd::SigSet,
};
use static_assertions::const_assert;

use crate::sync::{Bag, Deferred};

use super::{
    epoch::{AtomicEpoch, Epoch},
    global::Global,
    guard::EpochGuard,
    recovery, Deferrable,
};

const_assert!(Atomic::<Pthread>::is_lock_free());

pub(crate) struct Local {
    pub(crate) epoch: CachePadded<AtomicEpoch>,
    pub(crate) owner: Atomic<Pthread>,
    pub(crate) bag: UnsafeCell<Bag>,
    next: AtomicPtr<Local>,
    using: AtomicBool,
    global: *const Global,
    push_count: Cell<usize>,
}

impl Local {
    const COUNTS_BETWEEN_FORCE_ADVANCE: usize = 4;

    #[must_use]
    fn new(using: bool, global: &Global) -> Self {
        Self {
            epoch: CachePadded::new(AtomicEpoch::new(Epoch::starting())),
            owner: unsafe { core::mem::zeroed() },
            bag: UnsafeCell::new(Bag::new()),
            next: AtomicPtr::new(null_mut()),
            using: AtomicBool::new(using),
            global,
            push_count: Cell::new(0),
        }
    }

    /// Unpins and then pins the [`Local`].
    #[inline]
    pub(crate) fn repin(&mut self) {
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            // HACK: On x86 architectures we optimize out `fence(SeqCst)` with `compare_exchange`.
            // In this case, manually unpinning my epoch must be proceeded.
            // Check out comments on `pin_inner` for more information.
            self.unpin_inner();
        }
        self.pin_inner();
    }

    /// Pins the [`Local`].
    #[inline]
    pub(crate) fn pin_inner(&mut self) {
        let global_epoch = self.global().epoch.load(Ordering::Relaxed);
        let new_epoch = global_epoch.pinned();

        // Now we must store `new_epoch` into `self.epoch` and execute a `SeqCst` fence.
        // The fence makes sure that any future loads from `Atomic`s will not happen before
        // this store.
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            // HACK(stjepang): On x86 architectures there are two different ways of executing
            // a `SeqCst` fence.
            //
            // 1. `atomic::fence(SeqCst)`, which compiles into a `mfence` instruction.
            // 2. `_.compare_exchange(_, _, SeqCst, SeqCst)`, which compiles into a `lock cmpxchg`
            //    instruction.
            //
            // Both instructions have the effect of a full barrier, but benchmarks have shown
            // that the second one makes pinning faster in this particular case.  It is not
            // clear that this is permitted by the C++ memory model (SC fences work very
            // differently from SC accesses), but experimental evidence suggests that this
            // works fine.  Using inline assembly would be a viable (and correct) alternative,
            // but alas, that is not possible on stable Rust.
            let current = Epoch::starting();
            let res =
                self.epoch
                    .compare_exchange(current, new_epoch, Ordering::SeqCst, Ordering::SeqCst);
            debug_assert!(res.is_ok(), "participant was expected to be unpinned");
            // We add a compiler fence to make it less likely for LLVM to do something wrong
            // here.  Formally, this is not enough to get rid of data races; practically,
            // it should go a long way.
            compiler_fence(Ordering::SeqCst);
        } else {
            self.epoch.store(new_epoch, Ordering::Relaxed);
            fence(Ordering::SeqCst);
        }
    }

    /// Unpins the [`Local`].
    #[inline]
    pub(crate) fn unpin_inner(&mut self) {
        self.epoch.store(Epoch::starting(), Ordering::Release);
    }

    /// `#[inline(never)]` is used to reduce the chances for misoptimizations.
    ///
    /// In Rust, there is no concept of functions that return multiple times, like `setjmp`, so
    /// it's easy to imagine that Rust might generate incorrect code around such a function.
    ///
    /// Rust uses LLVM during compilation, which needs to be made aware of functions that return
    /// multiple times by using the `returns_twice` attribute; but Rust has no way to propagate
    /// that attribute to LLVM.
    ///
    /// Reference:
    /// * https://github.com/rust-lang/rfcs/issues/2625#issuecomment-460849462
    /// * https://github.com/jeff-davis/setjmp.rs#problems
    #[cfg(not(sanitize = "address"))]
    #[inline(never)]
    unsafe fn pin<F>(&mut self, mut body: F)
    where
        F: FnMut(&mut EpochGuard),
    {
        {
            // Makes a checkpoint and create a `RecoveryGuard`.
            let guard = recovery::guard!();

            // Repin the current epoch.
            // Acquiring an epoch must be proceeded after starting the crashable section,
            // not before. This is because if we acquire it before allowing a crash,
            // it is possible to be ejected before allowing. Although an ejection is occured,
            // the critical section would continues, as we would not `longjmp` from
            // the signal handler.
            self.repin();
            compiler_fence(Ordering::SeqCst);

            // Execute the body of this section.
            let mut guard = EpochGuard::new(self, guard);
            body(&mut guard);

            // Finaly, close this critical section by dropping `guard`.
        }

        // We are now out of the critical(crashable) section.
        // Unpin the local epoch to help reclaimers to freely collect bags.
        self.unpin_inner();
    }

    #[cfg(sanitize = "address")]
    #[inline(never)]
    unsafe fn pin<F>(&mut self, mut body: F)
    where
        F: FnMut(&mut EpochGuard),
    {
        // A dummy loop to bypass a false stack overflow from AdressSanitizer.
        //
        // # HACK: A dummy loop and `blackbox`
        //
        // It is not needed in normal builds, but when address-sanitizing,
        // the sanitizer often gives a false positive by recognizing `longjmp` as
        // stack buffer overflow (or stack corruption).
        //
        // However, awkwardly, if it wrapped by a loop block,
        // it seems that the sanitizer recognizes `longjmp` as
        // normal `continue` operation and totally satisfies with it.
        //
        // So, they are added to avoid false positives from the sanitizer.
        loop {
            {
                // Makes a checkpoint and create a `RecoveryGuard`.
                let guard = recovery::guard!();

                // Repin the current epoch.
                // Acquiring an epoch must be proceeded after starting the crashable section,
                // not before. This is because if we acquire it before allowing a crash,
                // it is possible to be ejected before allowing. Although an ejection is occured,
                // the critical section would continues, as we would not `longjmp` from
                // the signal handler.
                self.repin();
                compiler_fence(Ordering::SeqCst);

                // Execute the body of this section.
                let mut guard = EpochGuard::new(self, guard);
                body(&mut guard);

                // Finaly, close this critical section by dropping `guard`.
            }

            // We are now out of the critical(crashable) section.
            // Unpin the local epoch to help reclaimers to freely collect bags.
            self.unpin_inner();

            // # HACK: A dummy loop and `blackbox`
            // (See comments on the loop for more information.)
            if core::hint::black_box(true) {
                break;
            }
        }
    }

    #[inline]
    pub(crate) fn is_pinned(&self) -> bool {
        // Use `Acquire` to synchronize with `Release` from `advance`
        self.epoch.load(Ordering::Acquire).is_pinned()
    }

    #[inline]
    fn global(&self) -> &Global {
        unsafe { &*self.global }
    }

    /// Adds `deferred` to the thread-local bag.
    ///
    /// It returns a `Some(Vec<Deferred>)` if the global epoch is advanced and we have collected
    /// some expired deferred tasks.
    #[inline]
    pub(crate) fn defer(&mut self, def: Deferred) -> Option<Vec<Deferred>> {
        let bag = unsafe { &mut *self.bag.get() };

        if let Err(d) = bag.try_push(def) {
            self.global().push_bag(bag);
            bag.try_push(d).unwrap();

            let push_count = self.push_count.get() + 1;
            self.push_count.set(push_count);

            let collected = if push_count >= Self::COUNTS_BETWEEN_FORCE_ADVANCE {
                self.push_count.set(0);
                Some(self.global().collect(self.global().advance()))
            } else {
                let epoch = self.global().try_advance().ok()?;
                self.push_count.set(0);
                Some(self.global().collect(epoch))
            };

            return collected
                .map(|bags| bags.into_iter().flat_map(|bag| bag.into_iter()).collect());
        }
        None
    }
}

/// A grow-only linked list for [`Local`] registration.
pub(crate) struct LocalList {
    head: AtomicPtr<Local>,
}

impl LocalList {
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(null_mut()),
        }
    }

    /// Acquire an empty slot for a new participant.
    ///
    /// If there is an available slot, it returns a reference to that slot.
    /// Otherwise, it tries to append a new slot at the end of the list,
    /// and if it succeeds, returns the allocated slot.
    pub fn acquire<'c>(&'c self, tid: Pthread, global: &Global) -> Handle {
        let mut prev_link = &self.head;
        let local = loop {
            match unsafe { prev_link.load(Ordering::Acquire).as_mut() } {
                Some(curr) => {
                    if !curr.using.load(Ordering::Acquire)
                        && curr
                            .using
                            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
                            .is_ok()
                    {
                        break curr;
                    }
                    prev_link = &curr.next;
                }
                None => {
                    let new_local = Box::into_raw(Box::new(Local::new(true, global)));
                    if prev_link
                        .compare_exchange(
                            null_mut(),
                            new_local,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break unsafe { &mut *new_local };
                    } else {
                        unsafe { drop(Box::from_raw(new_local)) };
                    }
                }
            }
        };
        local.owner.store(tid, Ordering::Release);
        Handle { local }
    }

    /// Returns an iterator over all using `Local`s.
    pub(crate) fn iter_using<'g>(&'g self) -> impl Iterator<Item = &'g Local> {
        LocalIter {
            curr: self.head.load(Ordering::Acquire),
            predicate: |local| local.using.load(Ordering::Acquire),
            _marker: PhantomData,
        }
    }

    /// Returns an iterator over all `Local`s.
    pub(crate) fn iter<'g>(&'g self) -> impl Iterator<Item = &'g Local> {
        LocalIter {
            curr: self.head.load(Ordering::Acquire),
            predicate: |_| true,
            _marker: PhantomData,
        }
    }
}

impl Drop for LocalList {
    fn drop(&mut self) {
        let mut curr = self.head.load(Ordering::Acquire);
        while let Some(curr_ref) = unsafe { curr.as_ref() } {
            let next = curr_ref.next.load(Ordering::Acquire);
            drop(unsafe { Box::from_raw(curr) });
            curr = next;
        }
    }
}

pub(crate) struct LocalIter<'g, F>
where
    F: Fn(&Local) -> bool,
{
    curr: *const Local,
    predicate: F,
    _marker: PhantomData<&'g ()>,
}

impl<'g, F> Iterator for LocalIter<'g, F>
where
    F: Fn(&Local) -> bool,
{
    type Item = &'g Local;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(curr_ref) = unsafe { self.curr.as_ref() } {
            self.curr = curr_ref.next.load(Ordering::Acquire);
            if (self.predicate)(curr_ref) {
                return Some(curr_ref);
            }
        }
        None
    }
}

/// A thread-local handle managing local epoch and defering.
pub struct Handle {
    local: *mut Local,
}

impl Handle {
    /// Starts a crashable critical section where we cannot perform operations with side-effects,
    /// such as system calls, non-atomic write on a global variable, etc.
    ///
    /// # Safety
    ///
    /// In a section body, only *rollback-safe* operations are allowed. For example, non-atomic
    /// writes on a global variable and system-calls(File I/O and etc.) are dangerous, as they
    /// may cause an unexpected inconsistency on the whole system after a crash.
    #[inline]
    pub unsafe fn pin<F>(&mut self, body: F)
    where
        F: FnMut(&mut EpochGuard),
    {
        unsafe { (*self.local).pin(body) }
    }
}

impl Deferrable for Handle {
    #[inline]
    #[must_use]
    fn defer(&mut self, def: Deferred) -> Option<Vec<Deferred>> {
        unsafe { (*self.local).defer(def) }
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        let local = unsafe { &*self.local };
        local.epoch.store(Epoch::starting(), Ordering::Release);
        local.using.store(false, Ordering::Release);
    }
}
