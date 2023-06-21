use super::{Global, Handle};

/// A default [`Global`] domain for CRCU.
pub static GLOBAL: Global = Global::new();

thread_local! {
    /// A default thread-local `Handle` attached to [`Global`] domain.
    pub static HANDLE: Handle = GLOBAL.register();
}
