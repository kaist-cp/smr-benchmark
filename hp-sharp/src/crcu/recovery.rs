/// A thread-local recovery manager with signal handling
use bitflags::bitflags;
use nix::libc::{c_void, siginfo_t};
use nix::sys::pthread::{pthread_kill, Pthread};
use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler, SigSet, Signal};
use setjmp::{sigjmp_buf, siglongjmp};
use std::cell::UnsafeCell;
use std::ptr::{null, null_mut, NonNull};
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
    static CHKPT: UnsafeCell<*mut sigjmp_buf> = UnsafeCell::new(null_mut());
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
    static STATUS: UnsafeCell<*const AtomicU8> = UnsafeCell::new(null());
}

pub(crate) fn set_data(status: &AtomicU8, chkpt: &mut sigjmp_buf) {
    unsafe {
        CHKPT.with(|c| *c.get() = chkpt);
        STATUS.with(|s| *s.get() = status);
    }
}

pub(crate) fn clear_data() {
    unsafe {
        CHKPT.with(|c| *c.get() = null_mut());
        STATUS.with(|s| *s.get() = null());
    }
}

#[inline]
fn chkpt() -> Option<NonNull<sigjmp_buf>> {
    match CHKPT.try_with(|c| c.get()) {
        Ok(c) => NonNull::new(unsafe { *c }),
        Err(_) => None,
    }
}

#[inline]
fn status() -> Option<NonNull<AtomicU8>> {
    match STATUS.try_with(|s| s.get()) {
        Ok(s) => NonNull::new(unsafe { (*s).cast_mut() }),
        Err(_) => None,
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

pub(crate) struct RecoveryGuard {
    status: *const AtomicU8,
    chkpt: *mut sigjmp_buf,
}

impl RecoveryGuard {
    /// Creates a new `RecoveryGuard`.
    ///
    /// A new `RecoveryGuard` must always be created by calling `checkpoint` macro, not calling
    /// this `new` method directly.
    pub unsafe fn new(status: &AtomicU8, chkpt: &mut sigjmp_buf) -> Self {
        status.store(Status::InCs.bits(), Ordering::Relaxed);
        compiler_fence(Ordering::SeqCst);
        RecoveryGuard { status, chkpt }
    }

    /// Performs a given closure crash-atomically.
    ///
    /// It takes a mutable reference, so calling `atomic` recursively is not possible.
    #[inline]
    pub fn atomic<F, R>(&mut self, body: F) -> R
    where
        F: FnOnce(&Self) -> R,
    {
        unsafe { &*self.status }.store(Status::InCa.bits(), Ordering::Relaxed);

        compiler_fence(Ordering::SeqCst);
        let result = body(self);
        compiler_fence(Ordering::SeqCst);

        if unsafe { &*self.status }
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
        unsafe { siglongjmp(self.chkpt, 1) }
    }

    #[inline]
    pub fn must_rollback(&self) -> bool {
        compiler_fence(Ordering::SeqCst);
        Status::from_bits_truncate(unsafe { &*self.status }.load(Ordering::SeqCst))
            .contains(Status::InCaRb)
    }
}

impl Drop for RecoveryGuard {
    #[inline]
    fn drop(&mut self) {
        unsafe { &*self.status }.store(0, Ordering::Relaxed);
    }
}

/// Makes a checkpoint and create a `RecoveryGuard`.
macro_rules! guard {
    ($status:expr, $chkpt:expr) => {{
        compiler_fence(Ordering::SeqCst);

        // Make a checkpoint with `sigsetjmp` for recovering in this critical section.
        if unsafe { setjmp::sigsetjmp($chkpt, 0) } == 1 {
            compiler_fence(Ordering::SeqCst);

            // Unblock the signal before restarting the section.
            pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&SigSet::all()), None)
                .expect("Failed to unblock signal");
        }
        compiler_fence(Ordering::SeqCst);

        crate::crcu::RecoveryGuard::new($status, $chkpt)
    }};
}

pub(crate) use guard;

extern "C" fn handle_signal(_: i32, _: *mut siginfo_t, _: *mut c_void) {
    // In a signal handler, we are safe to access(and modify) Rust's Thread-local storage.
    // This is because all signal is blocked in a handler, so we don't have to worry about
    // unexpected restarts.
    //
    // `try_with` is used instead of `with` to prevent panics when a signal is handled while
    // destructing the thread local storage.
    let status: &AtomicU8 = match status() {
        Some(s) => unsafe { s.as_ref() },
        None => return,
    };
    let current = Status::from_bits_truncate(status.load(Ordering::Relaxed));

    // if we have made a checkpoint and are not in crash-atomic section, it is good to `longjmp`.
    if current == Status::InCs {
        if let Some(chkpt) = chkpt() {
            unsafe { siglongjmp(chkpt.as_ptr(), 1) }
        }
        return;
    }

    // If we are in crash-atomic section by `RecoveryGuard::atomic`, turn `InCaRb` on.
    if current == Status::InCa {
        status.store((Status::InCa | Status::InCaRb).bits(), Ordering::Relaxed);
    }
}
