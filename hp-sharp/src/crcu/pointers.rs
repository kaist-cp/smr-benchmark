use std::marker::PhantomData;

/// A pointer to an shared object.
///
/// This pointer is valid for use only during the lifetime `'r`.
pub struct Shared<'r, T> {
    inner: usize,
    _marker: PhantomData<&'r T>,
}
