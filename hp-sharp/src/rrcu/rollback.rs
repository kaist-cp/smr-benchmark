/// A thread-local recovery manager with signal handling
use bitflags::bitflags;
use nix::libc::{c_void, siginfo_t};
use nix::sys::pthread::{pthread_kill, Pthread};
use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler, SigSet, Signal};
use setjmp::{sigjmp_buf, siglongjmp};
use std::ptr::NonNull;
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

#[inline]
fn local() -> Option<NonNull<Local>> {
    match LOCAL.try_with(|c| c.get()) {
        Ok(l) => NonNull::new(unsafe { *l }),
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

impl Rollbacker {
    /// Creates a new `Rollbacker`.
    ///
    /// A new `Rollbacker` must always be created by calling `checkpoint` macro, not calling
    /// this `new` method directly.
    #[inline(always)]
    pub unsafe fn new(status: &AtomicU8, chkpt: &mut sigjmp_buf) -> Self {
        status.store(Status::InCs.bits(), Ordering::Relaxed);
        Rollbacker { status, chkpt }
    }

    /// Performs a given closure crash-atomically.
    ///
    /// It takes a mutable reference, so calling `atomic` recursively is not possible.
    #[inline]
    pub fn atomic<F, R>(&self, body: F) -> R
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
        Status::from_bits_truncate(unsafe { &*self.status }.load(Ordering::Relaxed))
            .contains(Status::InCaRb)
    }
}

impl Drop for Rollbacker {
    #[inline]
    fn drop(&mut self) {
        if let Some(status) = unsafe { self.status.as_ref() } {
            if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
                status
                    .compare_exchange(Status::InCs.bits(), 0, Ordering::SeqCst, Ordering::SeqCst)
                    .expect("`Rollbacker` is dropped outside of a critical section");
            } else {
                status.store(0, Ordering::Relaxed);
            }
        }
    }
}

/// Makes a checkpoint and create a `Rollbacker`.
macro_rules! checkpoint {
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

        crate::Rollbacker::new($status, $chkpt)
    }};
}

pub(crate) use checkpoint;

use crate::{Local, Rollbacker, LOCAL};

extern "C" fn handle_signal(_: i32, _: *mut siginfo_t, _: *mut c_void) {
    // In a signal handler, we are safe to access(and modify) Rust's Thread-local storage.
    // This is because all signal is blocked in a handler, so we don't have to worry about
    // unexpected restarts.
    //
    // `try_with` is used instead of `with` to prevent panics when a signal is handled while
    // destructing the thread local storage.
    let local = match local() {
        Some(mut l) => unsafe { l.as_mut() },
        None => return,
    };
    let current = Status::from_bits_truncate(local.status.load(Ordering::Relaxed));

    // if we have made a checkpoint and are not in crash-atomic section, it is good to `longjmp`.
    if current == Status::InCs {
        unsafe { siglongjmp(&mut local.chkpt, 1) }
    }

    // If we are in crash-atomic section by `Rollbacker::atomic`, turn `InCaRb` on.
    if current == Status::InCa {
        local
            .status
            .store((Status::InCa | Status::InCaRb).bits(), Ordering::Relaxed);
    }
}