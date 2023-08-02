use std::sync::atomic::compiler_fence;

use atomic::{fence, Ordering};
use nix::sys::{signal::pthread_sigmask, signal::SigmaskHow, signalfd::SigSet};

use super::rollback;
use crate::rrcu::GlobalRRCU;
use crate::{CsGuard, Deferred, Epoch, Global, Local};

pub trait LocalRRCU {
    unsafe fn pin<F>(&mut self, body: F)
    where
        F: FnMut(&mut CsGuard);
    fn defer(&mut self, def: Deferred) -> Option<Vec<Deferred>>;
}

impl Local {
    const COUNTS_BETWEEN_FORCE_ADVANCE: usize = 2;

    #[inline]
    fn global(&self) -> &Global {
        unsafe { &*self.global }
    }

    /// Unpins and then pins the [`Local`].
    #[inline]
    fn repin(&mut self) {
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
    fn pin_inner(&mut self) {
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
}

impl LocalRRCU for Local {
    /// `#[inline(never)]` is used to reduce the chances for misoptimizations.
    ///
    /// In Rust, there is no concept of functions that return multiple times, like `setjmp`, so
    /// it's easy to generate incorrect code around such a function in Rust.
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
        F: FnMut(&mut CsGuard),
    {
        // Makes a checkpoint and create a `Rollbacker`.
        let rb = rollback::checkpoint!(&self.status, &mut self.chkpt);
        compiler_fence(Ordering::SeqCst);

        // Repin the current epoch.
        // Acquiring an epoch must be proceeded after starting the crashable section,
        // not before. This is because if we acquire it before allowing a crash,
        // it is possible to be ejected before allowing. Although an ejection is occured,
        // the critical section would continues, as we would not `longjmp` from
        // the signal handler.
        self.repin();
        compiler_fence(Ordering::SeqCst);

        // Execute the body of this section.
        let mut guard = CsGuard::new(self, rb);
        body(&mut guard);
        compiler_fence(Ordering::SeqCst);

        // We are now out of the critical(crashable) section.
        // Unpin the local epoch to help reclaimers to freely collect bags.
        self.unpin_inner();

        // Finaly, close this critical section by dropping `rb`.
    }

    #[cfg(sanitize = "address")]
    #[inline(never)]
    unsafe fn pin<F>(&mut self, mut body: F)
    where
        F: FnMut(&mut CsGuard),
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
            // Makes a checkpoint and create a `Rollbacker`.
            let rb = rollback::checkpoint!(&self.status, &mut self.chkpt);
            compiler_fence(Ordering::SeqCst);

            // Repin the current epoch.
            // Acquiring an epoch must be proceeded after starting the crashable section,
            // not before. This is because if we acquire it before allowing a crash,
            // it is possible to be ejected before allowing. Although an ejection is occured,
            // the critical section would continues, as we would not `longjmp` from
            // the signal handler.
            self.repin();
            compiler_fence(Ordering::SeqCst);

            // Execute the body of this section.
            let mut guard = CsGuard::new(self, rb);
            body(&mut guard);
            compiler_fence(Ordering::SeqCst);

            // We are now out of the critical(crashable) section.
            // Unpin the local epoch to help reclaimers to freely collect bags.
            self.unpin_inner();

            // Finaly, close this critical section by dropping `rb`.

            // # HACK: A dummy loop and `blackbox`
            // (See comments on the loop for more information.)
            if core::hint::black_box(true) {
                break;
            }
        }
    }

    /// Adds `deferred` to the thread-local bag.
    ///
    /// It returns a `Some(Vec<Deferred>)` if the global epoch is advanced and we have collected
    /// some expired deferred tasks.
    #[inline]
    fn defer(&mut self, def: Deferred) -> Option<Vec<Deferred>> {
        let bag = unsafe { &mut *self.bag.get() };

        if let Err(d) = bag.try_push(def) {
            self.global().push_bag(bag);
            bag.try_push(d).unwrap();

            let push_count = self.push_count.get() + 1;
            self.push_count.set(push_count);

            let collected = if push_count >= Self::COUNTS_BETWEEN_FORCE_ADVANCE {
                self.push_count.set(0);
                let epoch = unsafe { &*self.global }.advance(self);
                Some(self.global().collect(epoch))
            } else {
                let epoch = match self.global().try_advance() {
                    Ok(epoch) => {
                        self.push_count.set(0);
                        epoch
                    }
                    Err(epoch) => epoch,
                };
                Some(self.global().collect(epoch))
            };

            return collected
                .map(|bags| bags.into_iter().flat_map(|bag| bag.into_iter()).collect());
        }
        None
    }
}
