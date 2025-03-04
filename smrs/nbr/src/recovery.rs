/// A thread-local recovery manager with signal handling
use nix::libc::{c_void, siginfo_t};
use nix::sys::pthread::{pthread_kill, Pthread};
use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler, SigSet, Signal};
use setjmp::{jmp_buf, sigjmp_buf, siglongjmp};
use std::mem::{transmute, MaybeUninit};
use std::sync::atomic::{compiler_fence, AtomicBool, Ordering};

static mut NEUTRALIZE_SIGNAL: Signal = Signal::SIGUSR1;
static mut SIG_ACTION: MaybeUninit<SigAction> = MaybeUninit::uninit();

thread_local! {
    static JMP_BUF: Box<sigjmp_buf> = Box::new(unsafe { MaybeUninit::zeroed().assume_init() });
    static RESTARTABLE: Box<AtomicBool> = Box::new(AtomicBool::new(false));
}

/// Install a process-wide signal handler.
/// Note that we don't have to call `sigaction` for every child thread.
///
/// By default, SIGUSR1 is used as a neutralize signal.
/// To use the other signal, use `set_neutralize_signal`.
#[inline]
pub(crate) unsafe fn install() {
    let sig_action = SigAction::new(
        SigHandler::SigAction(handle_signal),
        // Restart any interrupted sys calls instead of silently failing
        SaFlags::SA_RESTART | SaFlags::SA_SIGINFO,
        // Block signals during handler
        SigSet::all(),
    );
    #[allow(static_mut_refs)]
    SIG_ACTION.write(sig_action);
    #[allow(static_mut_refs)]
    if sigaction(NEUTRALIZE_SIGNAL, SIG_ACTION.assume_init_ref()).is_err() {
        panic!("failed to install signal handler");
    }
}

#[inline]
pub(crate) unsafe fn send_signal(pthread: Pthread) -> nix::Result<()> {
    pthread_kill(pthread, NEUTRALIZE_SIGNAL)
}

pub(crate) struct Status {
    pub(crate) jmp_buf: *mut sigjmp_buf,
    pub(crate) rest: &'static AtomicBool,
}

impl Status {
    #[inline]
    pub unsafe fn new() -> Self {
        Self {
            jmp_buf: JMP_BUF.with(|buf| (&**buf as *const sigjmp_buf).cast_mut()),
            rest: RESTARTABLE.with(|rest| transmute(&**rest)),
        }
    }

    #[inline(always)]
    pub fn set_restartable(&self) {
        self.rest.store(true, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn unset_restartable(&self) {
        self.rest.store(false, Ordering::Relaxed);
    }
}

/// Get a current neutralize signal.
///
/// By default, SIGUSR1 is used as a neutralize signal.
/// To use the other signal, use `set_neutralize_signal`.
///
/// # Safety
///
/// This function accesses and modify static variable.
/// To avoid potential race conditions, do not
/// call this function concurrently.
#[inline]
pub unsafe fn neutralize_signal() -> Signal {
    NEUTRALIZE_SIGNAL
}

/// Set user-defined neutralize signal.
/// This function allows a user to use the other signal
/// than SIGUSR1 for a neutralize signal.
/// Note that it must called before creating
/// a Collector object.
///
/// # Safety
///
/// This function accesses and modify static variable.
/// To avoid potential race conditions, do not
/// call this function concurrently.
#[inline]
pub unsafe fn set_neutralize_signal(signal: Signal) {
    NEUTRALIZE_SIGNAL = signal;
}

#[inline]
pub unsafe fn jmp_buf() -> *mut jmp_buf {
    JMP_BUF.with(|buf| (&**buf as *const sigjmp_buf).cast_mut())
}

extern "C" fn handle_signal(_: i32, _: *mut siginfo_t, _: *mut c_void) {
    let rest: &AtomicBool = match RESTARTABLE.try_with(|rest| unsafe { transmute(&**rest) }) {
        Ok(rest) => rest,
        Err(_) => return,
    };

    if !rest.load(Ordering::Relaxed) {
        return;
    }

    let buf = match JMP_BUF.try_with(|buf| (&**buf as *const sigjmp_buf).cast_mut()) {
        Ok(buf) => buf,
        Err(_) => return,
    };
    rest.store(false, Ordering::Relaxed);
    compiler_fence(Ordering::SeqCst);

    unsafe { siglongjmp(buf, 1) };
}
