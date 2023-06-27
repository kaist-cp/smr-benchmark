/// A thread-local recovery manager with signal handling
use nix::libc::{c_void, siginfo_t};
use nix::sys::pthread::{pthread_kill, Pthread};
use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler, SigSet, Signal};
use setjmp::{sigjmp_buf, siglongjmp};
use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{compiler_fence, AtomicBool, Ordering};

pub const EJECTION_SIGNAL: Signal = Signal::SIGUSR1;

thread_local! {
    static JMP_BUF: RefCell<MaybeUninit<sigjmp_buf>> = RefCell::new(MaybeUninit::uninit());
    static RESTARTABLE: AtomicBool = AtomicBool::new(false);
}

/// Install a signal handler.
///
/// Note that if a signal handler is installed for the parent thread before spawning childs, we
/// don't have to call `sigaction` for every child thread.
///
/// By default, SIGUSR1 is used as an ejection signal.
#[inline]
pub(crate) unsafe fn install() {
    sigaction(
        EJECTION_SIGNAL,
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

#[inline]
pub(crate) unsafe fn send_signal(pthread: Pthread) -> nix::Result<()> {
    pthread_kill(pthread, EJECTION_SIGNAL)
}

#[inline]
pub(crate) fn is_restartable() -> bool {
    let rest = RESTARTABLE.with(|rest| rest.load(Ordering::Relaxed));
    compiler_fence(Ordering::SeqCst);
    rest
}

#[inline]
pub(crate) fn set_restartable(set_rest: bool) {
    compiler_fence(Ordering::SeqCst);
    RESTARTABLE.with(|rest| rest.store(set_rest, Ordering::Relaxed));
}

/// Get a mutable thread-local pointer to `sigjmp_buf`, which is used for `sigsetjmp` at the
/// entrance of read phase.
#[inline]
pub(crate) fn jmp_buf() -> *mut sigjmp_buf {
    JMP_BUF.with(|buf| buf.borrow_mut().as_mut_ptr())
}

extern "C" fn handle_signal(_: i32, _: *mut siginfo_t, _: *mut c_void) {
    if !is_restartable() {
        return;
    }

    set_restartable(false);
    unsafe { perform_longjmp() };
}

/// Perform `siglongjmp` without changing any phase-related variables like `RESTARTABLE`.
///
/// It assume that the `jmp_buf` is properly initialized by calling `siglongjmp`.
#[inline]
pub(crate) unsafe fn perform_longjmp() -> ! {
    let buf = jmp_buf();
    compiler_fence(Ordering::SeqCst);

    siglongjmp(buf, 1)
}
