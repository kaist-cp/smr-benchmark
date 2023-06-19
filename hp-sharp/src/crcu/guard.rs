use std::{
    cell::Cell,
    sync::atomic::{compiler_fence, Ordering},
};

use crate::Deferred;

use super::{local::Local, recovery};

/// A crashable critical section guard.
///
/// Note that defering is not allowed in this critical section because a crash may occur while
/// writing data on non-atomic storage. To conduct jobs with side-effects, we must open a
/// non-crashable section by `write` method.
pub struct Guard {
    local: *const Local,
}

impl Guard {
    pub(crate) fn new(local: &Local) -> Self {
        Self { local }
    }

    /// Starts a non-crashable section where we can conduct operations with global side-effects.
    ///
    /// In this section, we do not restart immediately when we receive signals from reclaimers.
    /// The whole critical section restarts after this `write` section ends, if a reclaimer sent
    /// a signal, or we advanced our epoch to reclaim a full local garbage bag.
    pub fn write<F>(&mut self, body: F)
    where
        F: Fn(&mut WriteGuard),
    {
        recovery::set_restartable(false);
        compiler_fence(Ordering::SeqCst);

        let mut guard = WriteGuard::new(unsafe { &*self.local });
        body(&mut guard);
        compiler_fence(Ordering::SeqCst);

        recovery::set_restartable(true);
        if unsafe { !(*self.local).is_pinned() } || guard.advanced.get() {
            recovery::set_restartable(false);
            unsafe { recovery::perform_longjmp() };
        }
    }
}

/// A non-crashable write section guard.
///
/// Unlike a [`Guard`], it may perform jobs with side-effects such as retiring, or physical
/// deletion for a data structure.
pub struct WriteGuard {
    local: *const Local,
    advanced: Cell<bool>,
}

/// A non-crashable section guard.
impl WriteGuard {
    pub(crate) fn new(local: &Local) -> Self {
        Self {
            local,
            advanced: Cell::new(false),
        }
    }

    /// Defers a task which can be accessed after the current epoch ends.
    pub fn defer(&self, def: Deferred) {
        if unsafe { (*self.local).defer(def) } {
            self.advanced.set(true);
        }
    }
}
