//! The default garbage collector.
//!
//! For each thread, a participant is lazily initialized on its first use, when the current thread
//! is registered in the default collector.  If initialized, the thread's participant will get
//! destructed on thread exit, which in turn unregisters the thread.

use super::collector::{Collector, LocalHandle};
use super::guard::Guard;
use super::sync::once_lock::OnceLock;

fn collector() -> &'static Collector {
    /// The global data for the default garbage collector.
    static COLLECTOR: OnceLock<Collector> = OnceLock::new();
    COLLECTOR.get_or_init(Collector::new)
}

thread_local! {
    /// The per-thread participant for the default garbage collector.
    static HANDLE: LocalHandle = collector().register();
}

/// Pins the current thread.
#[inline]
pub fn pin() -> Guard {
    with_handle(|handle| handle.pin())
}

/// Returns `true` if the current thread is pinned.
#[inline]
pub fn is_pinned() -> bool {
    with_handle(|handle| handle.is_pinned())
}

/// Returns the default global collector.
pub fn default_collector() -> &'static Collector {
    collector()
}

#[inline]
fn with_handle<F, R>(mut f: F) -> R
where
    F: FnMut(&LocalHandle) -> R,
{
    HANDLE
        .try_with(|h| f(h))
        .unwrap_or_else(|_| f(&collector().register()))
}

#[cfg(test)]
mod tests {
    use crossbeam_utils::thread;

    #[test]
    fn pin_while_exiting() {
        struct Foo;

        impl Drop for Foo {
            fn drop(&mut self) {
                // Pin after `HANDLE` has been dropped. This must not panic.
                super::pin();
            }
        }

        thread_local! {
            static FOO: Foo = Foo;
        }

        thread::scope(|scope| {
            scope.spawn(|_| {
                // Initialize `FOO` and then `HANDLE`.
                FOO.with(|_| ());
                super::pin();
                // At thread exit, `HANDLE` gets dropped first and `FOO` second.
            });
        })
        .unwrap();
    }
}
