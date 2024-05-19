#![feature(cfg_sanitize)]
mod block_bag;
mod collector;
pub mod recovery;
mod stats;

pub use collector::{unprotected, Collector, Guard, Shield, ThreadId};
pub use stats::count_garbages;

pub use nix::sys::signal;
pub use setjmp;

/// Make a checkpoint with `sigsetjmp` for
/// recovering in read phase.
///
/// This macro is used only for `read_phase` macro, and
/// it is not recommended to use this manually.
#[macro_export]
macro_rules! set_checkpoint {
    ($guard:expr) => {{
        let buf = $guard.jmp_buf();
        if $crate::setjmp::sigsetjmp(buf, 0) == 1 {
            std::sync::atomic::fence(Ordering::SeqCst);
            let mut oldset = $crate::signal::SigSet::empty();
            oldset.add($crate::recovery::neutralize_signal());
            if $crate::signal::pthread_sigmask(
                $crate::signal::SigmaskHow::SIG_UNBLOCK,
                Some(&oldset),
                None,
            )
            .is_err()
            {
                panic!("Failed to unblock signal");
            }
        }
    }};
}

/// Automate starting, ending, protecting and barriering for read phase.
///
/// **In read phase, programmers must aware following restrictions.**
///
/// 1. Reading global variables is permitted and reading shared
///    records is permitted if pointers to them were obtained
///    during this phase.
///   - e.g., by traversing a sequence of shared objects by
///     following pointers starting from a global variable—i.e., a root
///
/// 2. Writes/CASs to shared records, writes/CASs to shared globals,
///    and system calls, are **not permitted.**
///
/// To understand the latter restriction, suppose an operation
/// allocates a node using malloc during its read phase, and before
/// it uses the node, the thread performing the operation is
/// neutralized. This would cause **a memory leak.**
///
/// Additionally, writes to thread local data structures are
/// not recommended. To see why, suppose a thread maintains
/// a thread local doubly-linked list, and also updates this list
/// as part of the read phase of some operation on the shared data
/// structure. If the thread is neutralized in middle of its update
/// to this local list, it might corrupt the structure of the list.
///
/// # Usage
///
/// ``` ignore
/// read_phase!(_Guard_of_NBR_, [_protectee1_, _protectee2_, ...] => {
///     /* traversing codes for read phase */
/// })
///
/// /* At this point, all _protecteei_ are protected. */
/// ```
#[macro_export]
macro_rules! read_phase {
    ($guard:expr => $($t:tt)*) => {{
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
        if cfg!(sanitize = "address") {
            /// # HACK: "Why a dummy loop with `black_box` is used?"
            ///
            /// It is not needed in normal builds, but
            /// when address-sanitizing, the address sanitizer often gives
            /// a false positive by recognizing `longjmp` as
            /// stack buffer overflow (or stack corruption).
            ///
            /// However, awkwardly, if it wrapped by a loop block,
            /// it seems that the sanitizer recognizes `longjmp` as
            /// normal `continue` operation and totally satisfies with it.
            ///
            /// So, they are added to avoid false positives from the sanitizer.
            loop {
                // `sigsetjmp` must called first. (in `set_checkpoint!()`)
                //
                // Since, if `sigsetjmp` is done later than other jobs
                // in `start_read`, the restartable would never be set to 1
                // and the upgrade assert will fail.
                //
                // Also, it "must be inlined" because longjmp can only jump up
                // the call stack, to functions that are still executing.
                unsafe { $crate::set_checkpoint!($guard) };
                std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
                ($guard).start_read();

                // The body of read phase
                std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
                { $($t)* }
                std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

                ($guard).end_read();
                std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);

                if std::hint::black_box(true) {
                    break;
                }
            }
        } else {
            // `sigsetjmp` must called first. (in `set_checkpoint!()`)
            //
            // Since, if `sigsetjmp` is done later than other jobs
            // in `start_read`, the restartable would never be set to 1
            // and the upgrade assert will fail.
            //
            // Also, it "must be inlined" because longjmp can only jump up
            // the call stack, to functions that are still executing.
            unsafe { $crate::set_checkpoint!($guard) };
            std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
            ($guard).start_read();

            // The body of read phase
            std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
            { $($t)* }
            std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

            ($guard).end_read();
            std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
        }

    }};
}
