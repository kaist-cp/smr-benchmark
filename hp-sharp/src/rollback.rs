/// A thread-local recovery manager with signal handling
use bitflags::bitflags;
use nix::libc::{c_void, siginfo_t};
use nix::sys::pthread::{pthread_kill, Pthread};
use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler, SigSet, Signal};
use setjmp::{sigjmp_buf, siglongjmp};
use std::mem::MaybeUninit;
use std::sync::atomic::{compiler_fence, AtomicU8, Ordering};

/// Represents a thread-local status.
///
/// A thread entering a critical section sets its `STATUS`.
///
/// Note that `STATUS` must be a type which can be read and written in tear-free manner.
/// For this reason, using non-atomic type such as `u8` is not safe, as any writes on this
/// variable may be splitted into multiple instructions and the thread may read inconsistent
/// value in its signal handler.
///
/// According to ISO/IEC TS 17961 C Secure Coding Rules, accessing values of objects that are
/// neither lock-free atomic objects nor of type `volatile sig_atomic_t` in a signal handler
/// results in undefined behavior.
///
/// `AtomicU8` is likely to be safe, because any accesses to it will be compiled into a
/// single ISA instruction. On top of that, by preventing reordering instructions across this
/// variable by issuing `compiler_fence`, we can have the same restrictions which
/// `volatile sig_atomic_t` has.
#[thread_local]
pub(crate) static STATUS: AtomicU8 = AtomicU8::new(0);
#[thread_local]
pub(crate) static mut CHKPT: MaybeUninit<sigjmp_buf> = MaybeUninit::zeroed();

/// A CRCU crash signal which is used to restart a slow thread.
///
/// We use `SIGUSR1` for a crash signal.
pub const CRASH_SIGNAL: Signal = Signal::SIGUSR1;

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub(crate) struct Status: u8 {
        const InCs = 0b001;
        const InCa = 0b010;
        const InCaRb = 0b100;
    }
}

/// Installs a signal handler.
///
/// Note that if a signal handler is installed for the parent thread before spawning childs, we
/// don't have to call `sigaction` for every child thread.
///
/// By default, SIGUSR1 is used as an ejection signal.
#[inline]
pub(crate) unsafe fn install() {
    sigaction(
        CRASH_SIGNAL,
        &SigAction::new(
            SigHandler::SigAction(handle_signal),
            // Restart any interrupted sys calls instead of silently failing
            SaFlags::SA_RESTART | SaFlags::SA_SIGINFO,
            // Block signals during handler
            SigSet::all(),
        ),
    )
    .expect("Failed to install signal handler.");
}

/// Sends a signal to a specific thread.
#[inline]
pub(crate) unsafe fn send_signal(pthread: Pthread) -> nix::Result<()> {
    pthread_kill(pthread, CRASH_SIGNAL)
}

pub(crate) struct Rollbacker;

impl Rollbacker {
    /// Performs a given closure crash-atomically.
    ///
    /// It takes a mutable reference, so calling `atomic` recursively is not possible.
    #[inline]
    pub fn atomic<F, R>(&self, body: F) -> R
    where
        F: FnOnce(&Self) -> R,
    {
        STATUS.store(Status::InCa.bits(), Ordering::Relaxed);

        compiler_fence(Ordering::SeqCst);
        let result = body(self);
        compiler_fence(Ordering::SeqCst);

        if STATUS
            .compare_exchange(
                Status::InCa.bits(),
                Status::InCs.bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
        {
            self.restart();
        }
        return result;
    }

    /// Returns to the checkpoint manually.
    #[inline]
    pub fn restart(&self) -> ! {
        compiler_fence(Ordering::SeqCst);
        unsafe { siglongjmp(CHKPT.as_mut_ptr(), 1) }
    }

    #[inline]
    pub fn must_rollback(&self) -> bool {
        compiler_fence(Ordering::SeqCst);
        Status::from_bits_truncate(STATUS.load(Ordering::Relaxed)).contains(Status::InCaRb)
    }
}

impl Drop for Rollbacker {
    #[inline]
    fn drop(&mut self) {
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            STATUS
                .compare_exchange(Status::InCs.bits(), 0, Ordering::SeqCst, Ordering::SeqCst)
                .expect("`Rollbacker` is dropped outside of a critical section");
        } else {
            STATUS.store(0, Ordering::Relaxed);
        }
    }
}

/// Makes a checkpoint and create a `Rollbacker`.
macro_rules! checkpoint {
    () => {{
        use crate::rollback::{Rollbacker, Status, CHKPT, STATUS};
        use nix::sys::signal::{pthread_sigmask, SigSet, SigmaskHow};
        use std::sync::atomic::{compiler_fence, Ordering};

        compiler_fence(Ordering::SeqCst);

        // Make a checkpoint with `sigsetjmp` for recovering in this critical section.
        if unsafe { setjmp::sigsetjmp(CHKPT.as_mut_ptr(), 0) } == 1 {
            compiler_fence(Ordering::SeqCst);

            // Unblock the signal before restarting the section.
            pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&SigSet::all()), None)
                .expect("Failed to unblock signal");
        }
        compiler_fence(Ordering::SeqCst);

        STATUS.store(Status::InCs.bits(), Ordering::Relaxed);
        Rollbacker
    }};
}

pub(crate) use checkpoint;

extern "C" fn handle_signal(_: i32, _: *mut siginfo_t, _: *mut c_void) {
    let current = Status::from_bits_truncate(STATUS.load(Ordering::Relaxed));

    // if we have made a checkpoint and are not in crash-atomic section, it is good to `longjmp`.
    if current == Status::InCs {
        unsafe { siglongjmp(CHKPT.as_mut_ptr(), 1) }
    }

    // If we are in crash-atomic section by `Rollbacker::atomic`, turn `InCaRb` on.
    if current == Status::InCa {
        STATUS.store((Status::InCa | Status::InCaRb).bits(), Ordering::Relaxed);
    }
}
