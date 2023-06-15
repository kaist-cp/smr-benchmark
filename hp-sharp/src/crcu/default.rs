use super::{Global, LocalHandle};

/// A default [`Global`] domain for CRCU.
pub static GLOBAL: Global = Global::new();

thread_local! {
    /// A default thread-local `LocalHandle` attached to [`Global`] domain.
    pub static LOCAL_HANDLE: LocalHandle = GLOBAL.register();
}
