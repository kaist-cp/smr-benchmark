/// A thread-local recovery manager with signal handling
use nix::libc::{c_void, siginfo_t};
use nix::sys::pthread::{pthread_kill, Pthread};
use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler, SigSet, Signal};
use setjmp::{sigjmp_buf, siglongjmp};
use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{compiler_fence, fence, AtomicBool, Ordering};

static mut EJECTION_SIGNAL: Signal = Signal::SIGUSR1;
static mut SIG_ACTION: MaybeUninit<SigAction> = MaybeUninit::uninit();

thread_local! {
    static JMP_BUF: RefCell<MaybeUninit<sigjmp_buf>> = RefCell::new(MaybeUninit::uninit());
    static RESTARTABLE: AtomicBool = AtomicBool::new(false);
    static IN_WRITE: AtomicBool = AtomicBool::new(false);
    static DEFERRING_RESTART: AtomicBool = AtomicBool::new(false);
    static INVALIDATE_BACKUP: AtomicBool = AtomicBool::new(false);
}

/// Install a signal handler.
///
/// Note that if a signal handler is installed for the parent thread
/// before spawning childs, we don't have to call `sigaction` for every child thread.
///
/// By default, SIGUSR1 is used as a ejection signal.
/// To use the other signal, use `set_ejection_signal`.
#[inline]
pub(crate) unsafe fn install() {
    let sig_action = SigAction::new(
        SigHandler::SigAction(handle_signal),
        // Restart any interrupted sys calls instead of silently failing
        SaFlags::SA_RESTART | SaFlags::SA_SIGINFO,
        // Block signals during handler
        SigSet::all(),
    );
    SIG_ACTION.write(sig_action);
    if sigaction(EJECTION_SIGNAL, SIG_ACTION.assume_init_ref()).is_err() {
        panic!("failed to install signal handler");
    }
}

#[inline]
pub(crate) unsafe fn send_signal(pthread: Pthread) -> nix::Result<()> {
    pthread_kill(pthread, EJECTION_SIGNAL)
}

#[inline]
pub(crate) fn is_restartable() -> bool {
    RESTARTABLE.with(|rest| rest.load(Ordering::Acquire))
}

#[inline]
pub(crate) fn initialize_before_read() {
    RESTARTABLE.with(|rest| rest.store(true, Ordering::Relaxed));
    IN_WRITE.with(|wrt| wrt.store(false, Ordering::Relaxed));
    DEFERRING_RESTART.with(|def| def.store(false, Ordering::Relaxed));
    fence(Ordering::SeqCst);
}

#[inline]
pub(crate) fn set_restartable(set_rest: bool) {
    RESTARTABLE.with(|rest| rest.store(set_rest, Ordering::Relaxed));
    fence(Ordering::SeqCst);
}

#[inline]
pub(crate) fn set_in_write(in_write: bool) {
    compiler_fence(Ordering::SeqCst);
    IN_WRITE.with(|wrt| wrt.store(in_write, Ordering::SeqCst));
    compiler_fence(Ordering::SeqCst);
}

#[inline]
pub(crate) fn is_in_write() -> bool {
    IN_WRITE.with(|wrt| wrt.load(Ordering::Acquire))
}

#[inline]
pub(crate) fn defer_restart() {
    DEFERRING_RESTART.with(|def| def.store(true, Ordering::Release));
}

#[inline]
pub(crate) fn deferring_restart() -> bool {
    DEFERRING_RESTART.with(|def| def.load(Ordering::Acquire))
}

#[inline]
pub(crate) fn set_invalidate_backup(invalidate: bool) {
    INVALIDATE_BACKUP.with(|inv| inv.store(invalidate, Ordering::Release));
}

#[inline]
pub(crate) fn is_invalidated() -> bool {
    INVALIDATE_BACKUP.with(|inv| inv.load(Ordering::Acquire))
}

/// Get a current ejection signal.
///
/// By default, SIGUSR1 is used as a ejection signal.
/// To use the other signal, use `set_ejection_signal`.
///
/// # Safety
///
/// This function accesses and modify static variable.
/// To avoid potential race conditions, do not
/// call this function concurrently.
pub unsafe fn ejection_signal() -> Signal {
    EJECTION_SIGNAL
}

/// Set user-defined ejection signal.
/// This function allows a user to use the other signal
/// than SIGUSR1 for a ejection signal.
/// Note that it must called before creating
/// a Collector object.
///
/// # Safety
///
/// This function accesses and modify static variable.
/// To avoid potential race conditions, do not
/// call this function concurrently.
pub unsafe fn set_ejection_signal(signal: Signal) {
    EJECTION_SIGNAL = signal;
}

/// Get a mutable thread-local pointer to `sigjmp_buf`,
/// which is used for `sigsetjmp` at the entrance of
/// read phase.
#[inline]
pub(crate) fn jmp_buf() -> *mut sigjmp_buf {
    JMP_BUF.with(|buf| buf.borrow_mut().as_mut_ptr())
}

extern "C" fn handle_signal(_: i32, _: *mut siginfo_t, _: *mut c_void) {
    if is_in_write() {
        defer_restart();
        return;
    }
    if !is_restartable() {
        return;
    }

    set_restartable(false);
    unsafe { perform_longjmp() };
}

/// Perform `siglongjmp` without changing any phase-related variables
/// like `RESTARTABLE`.
///
/// It assume that the `jmp_buf` is properly initialized
/// by calling `siglongjmp`
#[inline]
pub(crate) unsafe fn perform_longjmp() -> ! {
    let buf = jmp_buf();
    compiler_fence(Ordering::SeqCst);

    siglongjmp(buf, 1)
}
