use bitflags::bitflags;
/// A thread-local recovery manager with signal handling
use nix::libc::{c_void, siginfo_t};
use nix::sys::pthread::{pthread_kill, Pthread};
use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler, SigSet, Signal};
use setjmp::{sigjmp_buf, siglongjmp};
use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{compiler_fence, AtomicU8, Ordering};

/// A CRCU crash signal which is used to restart a slow thread.
///
/// We use `SIGUSR1` for a crash signal.
pub const CRASH_SIGNAL: Signal = Signal::SIGUSR1;

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct Status: u8 {
        const InCs = 0b001;
        const InCa = 0b010;
        const InCaRb = 0b100;
    }
}

thread_local! {
    static JMP_BUF: RefCell<MaybeUninit<sigjmp_buf>> = RefCell::new(MaybeUninit::uninit());
    /// Represents a thread-local status.
    ///
    /// A thread entering a crashable section sets its `STATUS` by calling `guard!` macro.
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
    static STATUS: AtomicU8 = AtomicU8::new(0);
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

/// Gets a mutable thread-local pointer to `sigjmp_buf`, which is used for `sigsetjmp` at the
/// entrance of read phase.
#[inline]
pub(crate) fn jmp_buf() -> *mut sigjmp_buf {
    JMP_BUF.with(|buf| buf.borrow_mut().as_mut_ptr())
}

pub(crate) struct RecoveryGuard {}

impl RecoveryGuard {
    /// Creates a new `RecoveryGuard`.
    ///
    /// A new `RecoveryGuard` must always be created by calling `checkpoint` macro, not calling
    /// this `new` method directly.
    pub unsafe fn new() -> Self {
        debug_assert!(
            status().is_empty(),
            "a status word must be empty before starting a critical section"
        );
        activate(Status::InCs);

        RecoveryGuard {}
    }

    /// Performs a given closure crash-atomically.
    ///
    /// It takes a mutable reference, so calling `atomic` recursively is not possible.
    #[inline]
    pub fn atomic<F, R>(&mut self, body: F) -> R
    where
        F: Fn(&Self) -> R,
    {
        activate(Status::InCa);
        let result = body(self);
        deactivate(Status::InCa);

        if status().contains(Status::InCaRb) {
            clear_status();
            unsafe { perform_longjmp() };
        }
        return result;
    }

    /// Returns to the checkpoint manually.
    ///
    /// It takes immutable reference. It is able to be called in both a critical section and an
    /// atomic section.
    #[inline]
    pub fn restart(&self) -> ! {
        compiler_fence(Ordering::SeqCst);
        clear_status();
        unsafe { perform_longjmp() };
    }
}

impl Drop for RecoveryGuard {
    #[inline]
    fn drop(&mut self) {
        deactivate(Status::InCs);
    }
}

/// Makes a checkpoint and create a `RecoveryGuard`.
macro_rules! guard {
    () => {{
        let buf = recovery::jmp_buf();
        compiler_fence(Ordering::SeqCst);

        // Make a checkpoint with `sigsetjmp` for recovering in this critical section.
        if unsafe { setjmp::sigsetjmp(buf, 0) } == 1 {
            compiler_fence(Ordering::SeqCst);

            // Unblock the signal before restarting the section.
            let mut oldset = SigSet::empty();
            oldset.add(recovery::CRASH_SIGNAL);
            pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&oldset), None)
                .expect("Failed to unblock signal");
        }
        compiler_fence(Ordering::SeqCst);

        crate::crcu::RecoveryGuard::new()
    }};
}

pub(crate) use guard;

#[inline]
fn status() -> Status {
    compiler_fence(Ordering::SeqCst);
    let status = Status::from_bits_truncate(STATUS.with(|status| status.load(Ordering::Relaxed)));
    compiler_fence(Ordering::SeqCst);
    status
}

#[inline]
fn activate(flag: Status) {
    let current = status();
    debug_assert!(!current.intersects(flag));
    STATUS.with(|status| status.store(current.union(flag).bits(), Ordering::Relaxed));
    compiler_fence(Ordering::SeqCst);
}

#[inline]
fn deactivate(flag: Status) {
    let current = status();
    debug_assert!(current.contains(flag));
    STATUS.with(|status| status.store(current.difference(flag).bits(), Ordering::Relaxed));
    compiler_fence(Ordering::SeqCst);
}

#[inline]
fn clear_status() {
    compiler_fence(Ordering::SeqCst);
    STATUS.with(|status| status.store(0, Ordering::Relaxed));
    compiler_fence(Ordering::SeqCst);
}

extern "C" fn handle_signal(_: i32, _: *mut siginfo_t, _: *mut c_void) {
    // Load a current state bits.
    let current = status();

    // If we didn't make a checkpoint at all, just return.
    if !current.contains(Status::InCs) {
        return;
    }

    // If we are in crash-atomic section by `RecoveryGuard::atomic`, turn `InCaRb` on.
    if current.contains(Status::InCa) {
        STATUS.with(|status| status.store(current.union(Status::InCaRb).bits(), Ordering::Relaxed));
        return;
    }

    // if we have made a checkpoint and are not in crash-atomic section, it is good to `longjmp`.
    clear_status();
    unsafe { perform_longjmp() };
}

/// Perform `siglongjmp` without changing any phase-related variables like `InCs`.
///
/// It assume that the `jmp_buf` is properly initialized by calling `siglongjmp`.
#[inline]
unsafe fn perform_longjmp() -> ! {
    let buf = jmp_buf();
    compiler_fence(Ordering::SeqCst);

    siglongjmp(buf, 1)
}
