use super::local::Local;

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
        todo!()
    }
}

/// A non-crashable write section guard.
///
/// Unlike a [`Guard`], it may perform jobs with side-effects such as retiring, or physical
/// deletion for a data structure.
pub struct WriteGuard {
    local: *const Local,
}

/// A non-crashable section guard.
impl WriteGuard {
    pub(crate) fn new(local: &Local) -> Self {
        Self { local }
    }

    /// Retires a detached pointer to reclaim after the current epoch ends.
    pub fn defer(&self, ptr: usize, deleter: unsafe fn(usize)) {
        todo!()
    }
}
