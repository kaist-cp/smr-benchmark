use std::{
    cell::Cell,
    hint::black_box,
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

use super::{
    epoch::{AtomicEpoch, Epoch},
    global::Global,
    guard::Guard,
    pointers::Shared,
    recovery,
};

const_assert!(Atomic::<Pthread>::is_lock_free());

pub(crate) struct Local {
    pub(crate) epoch: CachePadded<AtomicEpoch>,
    pub(crate) owner: Atomic<Pthread>,
    next: AtomicPtr<Local>,
    using: AtomicBool,
    global: *const Global,
    guard_exists: Cell<bool>,
}

impl Local {
    fn new(using: bool, global: &Global) -> Self {
        Self {
            epoch: CachePadded::new(AtomicEpoch::new(Epoch::starting())),
            owner: unsafe { core::mem::zeroed() },
            next: AtomicPtr::new(null_mut()),
            using: AtomicBool::new(using),
            global,
            guard_exists: Cell::new(false),
        }
    }

    /// Pins the `Local`.
    pub(crate) fn pin(&self) -> Guard {
        debug_assert!(
            !self.guard_exists.get(),
            "Creating multiple guards is not allowed."
        );
        self.guard_exists.set(true);

        let guard = Guard::new(self);

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

        guard
    }

    /// Unpins the [`Local`].
    #[inline]
    pub(crate) fn unpin(&self) {
        self.guard_exists.set(false);
        self.epoch.store(Epoch::starting(), Ordering::Release);
    }

    /// Unpins and then pins the [`Local`].
    #[inline]
    pub(crate) fn repin(&self) {
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

    fn read<F>(&self, body: F)
    where
        F: Fn(&mut Guard),
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
                fence(Ordering::SeqCst);

                // Unblock the signal before restarting the section.
                let mut oldset = SigSet::empty();
                oldset.add(unsafe { recovery::ejection_signal() });
                if pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&oldset), None).is_err() {
                    panic!("Failed to unblock signal");
                }
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
            let mut guard = Guard::new(self);

            // Execute the body of this section.
            body(&mut guard);
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

    fn is_pinned(&self) -> bool {
        self.epoch.load(Ordering::Relaxed).is_pinned()
    }

    fn global(&self) -> &Global {
        unsafe { &*self.global }
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
    pub fn acquire<'c>(&'c self, tid: Pthread, global: &Global) -> &'c Local {
        let mut prev_link = &self.head;
        let thread = loop {
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
                    let new_thread = Box::into_raw(Box::new(Local::new(true, global)));
                    if prev_link
                        .compare_exchange(
                            null_mut(),
                            new_thread,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break unsafe { &*new_thread };
                    } else {
                        unsafe { drop(Box::from_raw(new_thread)) };
                    }
                }
            }
        };
        thread.owner.store(tid, Ordering::Release);
        thread
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
pub struct LocalHandle {
    local: *const Local,
}

impl LocalHandle {
    #[inline]
    pub(crate) fn new(local: &Local) -> Self {
        Self { local }
    }

    #[inline]
    pub(crate) fn global(&self) -> &Global {
        unsafe { (*self.local).global() }
    }

    #[inline]
    pub fn is_pinned(&self) -> bool {
        unsafe { (*self.local).is_pinned() }
    }

    #[inline]
    pub fn read<F>(&self, body: F)
    where
        F: Fn(&mut Guard),
    {
        unsafe { (*self.local).read(body) }
    }

    /// Retires a detached pointer to reclaim after the current epoch ends.
    #[inline]
    pub fn retire<'r, T>(&self, ptr: Shared<'r, T>) {
        todo!()
    }
}

impl Clone for LocalHandle {
    fn clone(&self) -> Self {
        todo!()
    }
}
