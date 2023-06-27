use std::{
    cell::{Cell, UnsafeCell},
    hint::black_box,
    marker::PhantomData,
    ptr::null_mut,
    sync::atomic::{compiler_fence, AtomicBool, AtomicPtr, Ordering},
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
    next: AtomicPtr<Local>,
    using: AtomicBool,
    global: *const Global,
    bag: UnsafeCell<Bag>,
    handle_count: Cell<usize>,
    defer_count: Cell<usize>,
}

impl Local {
    const COUNTS_BETWEEN_TRY_ADVANCE: usize = 64;
    const COUNTS_BETWEEN_FORCE_ADVANCE: usize = 4 * 64;

    #[must_use]
    fn new(using: bool, global: &Global) -> Self {
        Self {
            epoch: CachePadded::new(AtomicEpoch::new(Epoch::starting())),
            owner: unsafe { core::mem::zeroed() },
            next: AtomicPtr::new(null_mut()),
            using: AtomicBool::new(using),
            global,
            bag: UnsafeCell::new(Bag::new()),
            handle_count: Cell::new(0),
            defer_count: Cell::new(0),
        }
    }

    /// Unpins and then pins the [`Local`].
    #[inline]
    pub(crate) fn repin(&mut self) {
        let epoch = self.epoch.load(Ordering::Relaxed);
        let global_epoch = self.global().epoch.load(Ordering::Relaxed).pinned();

        // Update the local epoch only if the global epoch is greater than the local epoch.
        if epoch != global_epoch {
            // We store the new epoch with `Release` because we need to ensure any memory
            // accesses from the previous epoch do not leak into the new one.
            self.epoch.store(global_epoch, Ordering::Release);

            // However, we don't need a following `SeqCst` fence, because it is safe for memory
            // accesses from the new epoch to be executed before updating the local epoch. At
            // worse, other threads will see the new epoch late and delay GC slightly.
        }
    }

    unsafe fn pin<F>(&self, body: F)
    where
        F: Fn(&mut EpochGuard),
    {
        let buf = recovery::jmp_buf();

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
            // Make a checkpoint with `sigsetjmp` for recovering in this critical section.
            if unsafe { setjmp::sigsetjmp(buf, 0) } == 1 {
                compiler_fence(Ordering::SeqCst);

                // Unblock the signal before restarting the section.
                let mut oldset = SigSet::empty();
                oldset.add(recovery::EJECTION_SIGNAL);
                pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&oldset), None)
                    .expect("Failed to unblock signal");
            }
            compiler_fence(Ordering::SeqCst);

            // Get ready to open the section by setting atomic indicators.
            debug_assert!(
                !recovery::is_restartable(),
                "restartable value should be false before starting a critical section"
            );
            recovery::set_restartable(true);
            compiler_fence(Ordering::SeqCst);

            // Repin the current epoch.
            // Acquiring an epoch must be proceeded after starting the crashable section,
            // not before. This is because if we acquire it before allowing a crash,
            // it is possible to be ejected before allowing. Although an ejection is occured,
            // the critical section would continues, as we would not `longjmp` from
            // the signal handler.
            self.repin();

            // Execute the body of this section.
            body(&mut EpochGuard::new(self));
            compiler_fence(Ordering::SeqCst);

            // Finaly, close this critical section by unsetting the `RESTARTABLE`.
            recovery::set_restartable(false);
            compiler_fence(Ordering::SeqCst);

            // # HACK: A dummy loop and `blackbox`
            // (See comments on the loop for more information.)
            if black_box(true) {
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

    #[inline]
    fn acquire_handle(&self) -> Handle {
        let count = self.handle_count.get();
        self.handle_count.set(count + 1);
        Handle { local: self }
    }

    #[inline]
    fn release_handle(&self) {
        let count = self.handle_count.get();
        self.handle_count.set(count - 1);
        if count == 1 {
            self.epoch.store(Epoch::starting(), Ordering::Release);
            self.using.store(false, Ordering::Release);
        }
    }

    /// Adds `deferred` to the thread-local bag.
    ///
    /// It returns a `Some(Vec<Deferred>)` if the global epoch is advanced and we have collected
    /// some expired deferred tasks.
    #[inline]
    pub(crate) fn defer(&self, mut def: Deferred) -> Option<Vec<Deferred>> {
        let bag = unsafe { &mut *self.bag.get() };

        while let Err(d) = bag.try_push(def) {
            self.global().push_bag(bag);
            def = d;
        }

        let defer_count = self.defer_count.get() + 1;
        self.defer_count.set(defer_count);

        if defer_count >= Self::COUNTS_BETWEEN_FORCE_ADVANCE {
            Some(self.global().collect(self.global().advance()))
        } else if defer_count % Self::COUNTS_BETWEEN_TRY_ADVANCE == 0 {
            Some(self.global().collect(self.global().try_advance().ok()?))
        } else {
            None
        }
        .map(|collected| {
            collected
                .into_iter()
                .flat_map(|bag| bag.into_iter())
                .collect()
        })
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
            match unsafe { prev_link.load(Ordering::Acquire).as_ref() } {
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
                        break unsafe { &*new_local };
                    } else {
                        unsafe { drop(Box::from_raw(new_local)) };
                    }
                }
            }
        };
        local.owner.store(tid, Ordering::Release);
        local.handle_count.set(1);
        Handle { local }
    }

    /// Returns an iterator over all objects.
    pub(crate) fn iter<'g>(&'g self) -> LocalIter<'g> {
        LocalIter {
            curr: self.head.load(Ordering::Acquire),
            _marker: PhantomData,
        }
    }
}

pub(crate) struct LocalIter<'g> {
    curr: *const Local,
    _marker: PhantomData<&'g ()>,
}

impl<'g> Iterator for LocalIter<'g> {
    type Item = &'g Local;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(curr_ref) = unsafe { self.curr.as_ref() } {
            self.curr = curr_ref.next.load(Ordering::Acquire);
            if curr_ref.using.load(Ordering::Acquire) {
                return Some(curr_ref);
            }
        }
        None
    }
}

/// A thread-local handle managing local epoch and defering.
pub struct Handle {
    local: *const Local,
}

impl Handle {
    #[inline]
    pub fn is_pinned(&self) -> bool {
        unsafe { (*self.local).is_pinned() }
    }

    /// Starts a crashable critical section where we cannot perform operations with side-effects,
    /// such as system calls, non-atomic write on a global variable, etc.
    ///
    /// # Safety
    ///
    /// In a section body, only *rollback-safe* operations are allowed. For example, non-atomic
    /// writes on a global variable and system-calls(File I/O and etc.) are dangerous, as they
    /// may cause an unexpected inconsistency on the whole system after a crash.
    #[inline]
    pub unsafe fn pin<F>(&self, body: F)
    where
        F: Fn(&mut EpochGuard),
    {
        unsafe { (*self.local).pin(body) }
    }
}

impl Deferrable for Handle {
    #[inline]
    #[must_use]
    fn defer(&self, def: Deferred) -> Option<Vec<Deferred>> {
        unsafe { (*self.local).defer(def) }
    }
}

impl Clone for Handle {
    fn clone(&self) -> Self {
        unsafe { &*self.local }.acquire_handle()
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        unsafe { &*self.local }.release_handle()
    }
}
