use std::{
    marker::PhantomData,
    mem::{self, forget, swap, transmute, zeroed, MaybeUninit},
    ops::{Deref, DerefMut},
    sync::atomic::{compiler_fence, fence, AtomicUsize, Ordering},
};

use crate::{
    hpsharp::{guard::EpochGuard, guard::Invalidate, handle::Handle, hazard::HazardPointer},
    Guard, Retire,
};

/// A result of unsuccessful `compare_exchange`.
pub struct CompareExchangeError<'g, T: ?Sized, P: Pointer> {
    /// The `new` pointer which was given as a parameter of `compare_exchange`.
    pub new: P,
    /// The actual pointer value inside the atomic pointer.
    pub actual: Shared<'g, T>,
}

pub struct Atomic<T> {
    link: AtomicUsize,
    _marker: PhantomData<*mut T>,
}

unsafe impl<T> Send for Atomic<T> {}
unsafe impl<T> Sync for Atomic<T> {}

impl<T> Atomic<T> {
    /// Allocates `init` on the heap and returns a new atomic pointer pointing to it.
    #[inline]
    pub fn new(init: T) -> Self {
        let ptr = Box::into_raw(Box::new(init));
        Self {
            link: AtomicUsize::new(ptr as _),
            _marker: PhantomData,
        }
    }

    /// Returns a new null atomic pointer.
    #[inline]
    pub const fn null() -> Self {
        Self {
            link: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    /// Stores a [`Shared`] pointer into the atomic pointer, returning the previous [`Shared`].
    #[inline]
    pub fn swap<G: Retire>(&self, ptr: Shared<T>, order: Ordering, _: &G) -> Shared<T> {
        let prev = self.link.swap(ptr.inner, order);
        Shared::new(prev)
    }

    /// Stores a [`Shared`] pointer into the atomic pointer.
    #[inline]
    pub fn store<G: Retire>(&self, ptr: Shared<T>, order: Ordering, _: &G) {
        self.link.store(ptr.inner, order);
    }

    /// Loads a [`Shared`] from the atomic pointer.
    #[inline]
    pub fn load<'r, G: Guard>(&self, order: Ordering, _: &G) -> Shared<'r, T> {
        let ptr = self.link.load(order);
        Shared::new(ptr)
    }

    /// Stores the pointer `new` into the atomic pointer if the current value is the
    /// same as `current`. The tag is also taken into account, so two pointers to the same object,
    /// but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success a
    /// [`Shared`] which is taken out from the atomic pointer is returned. On failure a
    /// [`CompareExchangeError`] which contains an actual value from the atomic pointer and
    /// the ownership of `new` pointer which was given as a parameter is returned.
    #[inline]
    pub fn compare_exchange<'g, P: Pointer, G: Retire>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        _: &G,
    ) -> Result<Shared<T>, CompareExchangeError<'g, T, P>> {
        let current = current.inner;
        let new = new.into_usize();

        match self.link.compare_exchange(current, new, success, failure) {
            Ok(actual) => Ok(Shared::new(actual)),
            Err(actual) => Err(CompareExchangeError {
                new: unsafe { P::from_usize(new) },
                actual: Shared::new(actual),
            }),
        }
    }

    /// Bitwise "or" with the current tag.
    ///
    /// Performs a bitwise "or" operation on the current tag and the argument `tag`, and sets the
    /// new tag to the result. Returns the previous pointer.
    #[inline]
    pub fn fetch_or<'r, G: Retire>(&self, tag: usize, order: Ordering, _: &'r G) -> Shared<'r, T> {
        Shared::new(self.link.fetch_or(decompose_data::<T>(tag).1, order))
    }

    /// Takes ownership of the pointee.
    ///
    /// This consumes the atomic and converts it into [`Owned`]. As [`Atomic`] doesn't have a
    /// destructor and doesn't drop the pointee while [`Owned`] does, this is suitable for
    /// destructors of data structures.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object.
    #[inline]
    pub unsafe fn into_owned(self) -> Owned<T> {
        Owned::from_usize(self.link.into_inner())
    }
}

/// A pointer to a shared object.
///
/// This pointer is valid for use only during the lifetime `'r`.
///
/// This is the most basic shared pointer type, which can be loaded directly from [`Atomic`].
/// Also it is worth noting that [`Shield`] can create a [`Shared`] which has a lifetime parameter
/// of the original pointer.
#[derive(Debug)]
pub struct Shared<'r, T: ?Sized> {
    inner: usize,
    _marker: PhantomData<(&'r (), *const T)>,
}

impl<'r, T> Clone for Shared<'r, T> {
    #[inline]
    fn clone(&self) -> Self {
        Shared {
            inner: self.inner,
            _marker: PhantomData,
        }
    }
}

impl<'r, T> Copy for Shared<'r, T> {}

impl<'r, T> PartialEq for Shared<'r, T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl<'r, T> Eq for Shared<'r, T> {}

impl<'r, T> Shared<'r, T> {
    #[inline]
    pub(crate) fn new(ptr: usize) -> Self {
        Self {
            inner: ptr,
            _marker: PhantomData,
        }
    }

    /// Returns a new null shared pointer.
    #[inline]
    pub fn null() -> Self {
        Self::new(0)
    }

    /// Returns `true` if the pointer is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        decompose_data::<T>(self.inner).0 as usize == 0
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    ///
    /// It is possible to directly dereference a [`Shared`] if and only if the current context is
    /// in a critical section which can be started by `traverse` and `traverse_loop` method.
    #[inline]
    pub fn as_ref(&self, _: &EpochGuard) -> Option<&'r T> {
        unsafe { decompose_data::<T>(self.inner).0.as_ref() }
    }

    /// Converts the pointer to a mutable reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    ///
    /// It is possible to directly dereference a [`Shared`] if and only if the current context is
    /// in a critical section which can be started by `traverse` and `traverse_loop` method.
    #[inline]
    pub fn as_mut(&mut self, _: &EpochGuard) -> Option<&'r mut T> {
        unsafe { decompose_data::<T>(self.inner).0.as_mut() }
    }

    /// Converts the pointer to a reference, without guaranteeing any safety.
    ///
    /// # Safety
    ///
    /// The `self` must be a valid memory location.
    #[inline]
    pub unsafe fn deref_unchecked(&self) -> &T {
        &*decompose_data::<T>(self.inner).0
    }

    /// Converts the pointer to a reference, without guaranteeing any safety.
    ///
    /// # Safety
    ///
    /// The `self` must be a valid memory location.
    #[inline]
    pub unsafe fn deref_mut_unchecked(&mut self) -> &mut T {
        &mut *decompose_data::<T>(self.inner).0
    }

    /// Returns the tag stored within the pointer.
    #[inline]
    pub fn tag(&self) -> usize {
        decompose_data::<T>(self.inner).1
    }

    /// Returns the same pointer, but the tag bits are cleared.
    #[inline]
    pub fn untagged(&self) -> Self {
        Shared::new(decompose_data::<T>(self.inner).0 as usize)
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn with_tag(&self, tag: usize) -> Self {
        Shared::new(data_with_tag::<T>(self.inner, tag))
    }

    /// Returns the machine representation of the pointer, including tag bits.
    #[inline]
    pub fn as_raw(&self) -> usize {
        self.inner
    }

    /// Takes ownership of the pointee.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object.
    #[inline]
    pub unsafe fn into_owned(self) -> Owned<T> {
        Owned::from_usize(self.inner)
    }
}

/// An owned heap-allocated object.
///
/// This type is very similar to `Box<T>`.
pub struct Owned<T> {
    inner: usize,
    _marker: PhantomData<*const T>,
}

unsafe impl<T> Sync for Owned<T> {}
unsafe impl<T> Send for Owned<T> {}

impl<T> Deref for Owned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*decompose_data::<T>(self.inner).0 }
    }
}

impl<T> DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *decompose_data::<T>(self.inner).0 }
    }
}

impl<T> Owned<T> {
    /// Allocates `init` on the heap and returns a new owned pointer pointing to it.
    pub fn new(init: T) -> Self {
        Self {
            inner: Box::into_raw(Box::new(init)) as usize,
            _marker: PhantomData,
        }
    }

    /// Returns the machine representation of the pointer, including tag bits.
    pub fn as_raw(&self) -> usize {
        self.inner
    }

    /// Returns the tag stored within the pointer.
    #[inline]
    pub fn tag(&self) -> usize {
        decompose_data::<T>(self.inner).1
    }

    /// Returns the same pointer, but the tag bits are cleared.
    #[inline]
    pub fn untagged(self) -> Self {
        self.with_tag(0)
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn with_tag(self, tag: usize) -> Self {
        let result = Self {
            inner: data_with_tag::<T>(self.inner, tag),
            _marker: PhantomData,
        };
        forget(self);
        result
    }

    /// Converts the owned pointer into a [`Shared`].
    #[inline]
    pub fn into_shared<'r>(self) -> Shared<'r, T> {
        unsafe { Shared::from_usize(self.into_usize()) }
    }
}

impl<T> Drop for Owned<T> {
    fn drop(&mut self) {
        drop(unsafe { Box::from_raw(self.inner as *const T as *mut T) });
    }
}

/// A pointer to a shared object, which is protected by a hazard pointer.
pub struct Shield<T> {
    hazptr: HazardPointer,
    inner: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T> Sync for Shield<T> {}

impl<T> Shield<T> {
    #[inline]
    pub fn null(handle: &mut Handle) -> Self {
        Self {
            hazptr: HazardPointer::new(handle),
            inner: 0,
            _marker: PhantomData,
        }
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    #[inline]
    pub fn as_ref<'s>(&'s self) -> Option<&'s T> {
        unsafe { decompose_data::<T>(self.inner).0.as_ref() }
    }

    #[inline]
    pub unsafe fn deref_unchecked<'s>(&'s self) -> &'s T {
        &*decompose_data::<T>(self.inner).0
    }

    #[inline]
    pub unsafe fn deref_mut_unchecked<'s>(&'s mut self) -> &'s mut T {
        &mut *decompose_data::<T>(self.inner).0
    }

    /// Converts the pointer to a mutable reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    #[inline]
    pub fn as_mut<'s>(&'s self) -> Option<&'s mut T> {
        unsafe { decompose_data::<T>(self.inner).0.as_mut() }
    }

    /// Returns `true` if the protected pointer is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        decompose_data::<T>(self.inner).0 as usize == 0
    }

    /// Returns the tag stored within the shield.
    #[inline]
    pub fn tag(&self) -> usize {
        decompose_data::<T>(self.inner).1
    }

    /// Changes the tag bits to `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn set_tag(&mut self, tag: usize) {
        self.inner = data_with_tag::<T>(self.inner, tag);
    }

    /// Releases the inner hazard pointer.
    #[inline]
    pub fn release(&mut self) {
        self.inner = 0;
        self.hazptr.reset_protection();
    }

    /// Returns the machine representation of the pointer, including tag bits.
    #[inline]
    pub fn as_raw(&self) -> usize {
        self.inner
    }

    /// Creates a `Shared` pointer whose lifetime is equal to `self`.
    #[inline]
    pub fn shared<'r>(&'r self) -> Shared<'r, T> {
        Shared::new(self.inner)
    }
}

/// A trait for either `Owned` or `Shared` pointers.
pub trait Pointer {
    /// Returns the machine representation of the pointer.
    fn into_usize(self) -> usize;

    /// Returns a new pointer pointing to the tagged pointer `data`.
    ///
    /// # Safety
    ///
    /// The given `data` should have been created by `Pointer::into_usize()`, and one `data` should
    /// not be converted back by `Pointer::from_usize()` multiple times.
    unsafe fn from_usize(data: usize) -> Self;
}

impl<'r, T> Pointer for Shared<'r, T> {
    #[inline]
    fn into_usize(self) -> usize {
        self.as_raw()
    }

    #[inline]
    unsafe fn from_usize(data: usize) -> Self {
        Self::new(data)
    }
}

impl<T> Pointer for Owned<T> {
    fn into_usize(self) -> usize {
        let inner = self.inner;
        forget(self);
        inner
    }

    unsafe fn from_usize(data: usize) -> Self {
        Self {
            inner: data,
            _marker: PhantomData,
        }
    }
}

/// A trait for `Shield` which can protect `Shared`.
pub trait Protector {
    /// A set of `Shared` pointers which is protected by an epoch.
    type Target<'r>: Clone + Copy;

    /// Returns a default [`Protector`] with nulls for [`Shield`]s and defaults for other types.
    fn empty(handle: &mut Handle) -> Self;

    /// Stores the given `Target` pointers in hazard slots without any validations.
    ///
    /// Just storing a pointer in a hazard slot doesn't guarantee that the pointer is
    /// truly protected, as the memory block may already be reclaimed. We must validate whether
    /// the memory block is reclaimed or not, by reloading the atomic pointer or checking the
    /// local CRCU epoch.
    fn protect_unchecked(&mut self, read: &Self::Target<'_>);

    /// Loads currently protected pointers and checks whether any of them are invalidated.
    /// If not, creates a new `Target` and returns it.
    fn as_target<'r>(&self, guard: &'r EpochGuard) -> Option<Self::Target<'r>>;

    /// Resets all hazard slots, allowing the previous memory block to be reclaimed.
    fn release(&mut self);

    /// Starts a crashable critical section where we cannot perform operations with side-effects,
    /// such as system calls, non-atomic write on a global variable, etc.
    ///
    /// After finishing the section, it protects the returned `Target` pointers, so that they can be
    /// dereferenced outside of the phase.
    ///
    /// # Safety
    ///
    /// In a section body, only *rollback-safe* operations are allowed. For example, non-atomic
    /// writes on a global variable and system-calls(File I/O and etc.) are dangerous, as they
    /// may cause an unexpected inconsistency on the whole system after a crash.
    #[inline(always)]
    unsafe fn traverse<F>(&mut self, handle: &mut Handle, body: F)
    where
        F: for<'r> Fn(&'r mut EpochGuard) -> Self::Target<'r>,
    {
        handle.crcu_handle.borrow_mut().pin(|guard| {
            // Execute the body of this read phase.
            let mut guard = EpochGuard::new(guard, handle, None);
            let result = body(&mut guard);
            compiler_fence(Ordering::SeqCst);

            // Store pointers in hazard slots and issue a fence.
            self.protect_unchecked(&result);
            fence(Ordering::SeqCst);

            // If we successfully protected pointers without an intermediate crash,
            // it has the same meaning with a well-known HP validation:
            // we can safely assume that the pointers are not reclaimed yet.
        })
    }

    /// Starts a crashable critical section where we cannot perform operations with side-effects,
    /// such as system calls, non-atomic write on a global variable, etc.
    ///
    /// This is similar to `traverse`, as it manages CRCU critical section. However, this
    /// `traverse_loop` prevents a starvation in crash-intensive workload by saving intermediate
    /// results on a backup [`Protector`].
    ///
    /// After finishing the section, it protects the final `Target` pointers, so that they can be
    /// dereferenced outside of the phase.
    ///
    /// # Safety
    ///
    /// In a section body, only *rollback-safe* operations are allowed. For example, non-atomic
    /// writes on a global variable and system-calls(File I/O and etc.) are dangerous, as they
    /// may cause an unexpected inconsistency on the whole system after a crash.
    #[inline(always)]
    unsafe fn traverse_loop<F1, F2>(
        &mut self,
        backup: &mut Self,
        handle: &mut Handle,
        init_result: F1,
        step_forward: F2,
    ) where
        F1: for<'r> Fn(&'r mut EpochGuard) -> Self::Target<'r>,
        F2: for<'r> Fn(&mut Self::Target<'r>, &'r mut EpochGuard) -> TraverseStatus,
        Self: Sized,
    {
        const ITER_BETWEEN_CHECKPOINTS: usize = 512;

        // `backup_idx` indicates where we have stored the latest backup to `backup_def`.
        // 0 = `defs[0]`, 1 = `defs[1]`, otherwise = no backup
        // We use an atomic type instead of regular one, to perform writes atomically,
        // so that stored data is consistent even if a crash occured during a write.
        let backup_idx = AtomicUsize::new(2);
        {
            let defs = [&mut *self, backup];

            handle.crcu_handle.borrow_mut().pin(|guard| {
                // Load the saved intermediate result, if one exists.
                let mut guard = EpochGuard::new(guard, handle, Some(&backup_idx));

                // Initialize the first `result`. It is either a checkpointed result or an very
                // first result(probably pointing a root of a data structure) returned by
                // `init_result`.
                let mut result = defs
                    .get(backup_idx.load(Ordering::Relaxed))
                    .and_then(|def| transmute(def.as_target(&guard)))
                    .unwrap_or_else(|| {
                        // As `F1` takes a mutable reference to `guard`, `init_result` returns
                        // `Read<'r>` and `guard`'s lifetime becomes an another arbitrary value.
                        // They must be synchronized to use them on `step_forward`.
                        transmute(init_result(&mut guard))
                    });

                for iter in 0.. {
                    // Execute a single step.
                    //
                    // `transmute` synchronizes the lifetime parameters of `result` and `guard`.
                    // After a single `step_forward`, the lifetimes become different to each other,
                    // for example, `'r` and `'g` respectively. On the next iteration, they are
                    // synchronized again with the same value.
                    let step_result = step_forward(transmute(&mut result), &mut guard);

                    let finished = step_result == TraverseStatus::Finished;
                    // TODO(@jeonghyeon): Apply an adaptive checkpointing.
                    let should_checkpoint = step_result == TraverseStatus::Continue
                        && iter % ITER_BETWEEN_CHECKPOINTS == 0;

                    if finished || should_checkpoint {
                        // Select an available protector to protect a backup.
                        let (curr_idx, next_idx) = {
                            let backup_idx = backup_idx.load(Ordering::Relaxed);
                            (backup_idx % 2, (backup_idx + 1) % 2)
                        };

                        // Store pointers in hazard slots and issue a fence.
                        defs[next_idx].protect_unchecked(&result);
                        fence(Ordering::SeqCst);

                        // Success! We are not ejected so the protection is valid!
                        // Finalize backup process by storing a new backup index to `backup_idx`
                        backup_idx.store(next_idx, Ordering::Relaxed);
                        compiler_fence(Ordering::SeqCst);
                        defs[curr_idx].release();
                    }

                    // The task is finished! Break the loop and return the result.
                    if finished {
                        break;
                    }
                }
            });
        }

        // We want that `self` contains the final result.
        // If the latest backup is `backup`, than swap `self` and `backup`.
        let backup_idx = backup_idx.load(Ordering::Relaxed);
        assert!(backup_idx < 2);
        if backup_idx == 1 {
            swap(self, backup);
        }
    }
}

/// An empty [`Protector`].
impl Protector for () {
    type Target<'r> = ();

    #[inline]
    fn empty(_: &mut Handle) -> Self {
        ()
    }

    #[inline]
    fn protect_unchecked(&mut self, _: &Self::Target<'_>) {}

    #[inline]
    fn as_target<'r>(&self, _: &'r EpochGuard) -> Option<Self::Target<'r>> {
        Some(())
    }

    #[inline]
    fn release(&mut self) {}
}

/// A unit [`Protector`] with a single [`Shield`].
impl<T: Invalidate> Protector for Shield<T> {
    type Target<'r> = Shared<'r, T>;

    #[inline]
    fn empty(handle: &mut Handle) -> Self {
        Shield::null(handle)
    }

    #[inline]
    fn protect_unchecked(&mut self, read: &Self::Target<'_>) {
        let raw = read.untagged().as_raw();
        self.hazptr
            .protect_raw(raw as *const T as *mut T, Ordering::Relaxed);
        self.inner = raw;
    }

    #[inline]
    fn as_target<'r>(&self, guard: &'r EpochGuard) -> Option<Self::Target<'r>> {
        let read = Shared::new(self.inner);
        if let Some(value) = self.as_ref() {
            if value.is_invalidated(guard) {
                return None;
            }
        }
        Some(read)
    }

    #[inline]
    fn release(&mut self) {
        Shield::release(self)
    }
}

macro_rules! impl_protector_for_array {(
    $($N:literal)*
) => (
    $(
        impl<T: Invalidate> Protector for [Shield<T>; $N] {
            type Target<'r> = [Shared<'r, T>; $N];

            #[inline]
            fn empty(handle: &mut Handle) -> Self {
                let mut result: [MaybeUninit<Shield<T>>; $N] = unsafe { zeroed() };
                for shield in result.iter_mut() {
                    shield.write(Shield::null(handle));
                }
                unsafe { transmute(result) }
            }

            #[inline]
            fn protect_unchecked(&mut self, read: &Self::Target<'_>) {
                for (shield, shared) in self.iter_mut().zip(read) {
                    shield.protect_unchecked(shared);
                }
            }

            #[inline]
            fn as_target<'r>(&self, guard: &'r EpochGuard) -> Option<Self::Target<'r>> {
                let mut result: [MaybeUninit<Shared<'r, T>>; $N] = unsafe { zeroed() };
                for (shield, shared) in self.iter().zip(result.iter_mut()) {
                    match shield.as_target(guard) {
                        Some(read) => shared.write(read),
                        None => return None,
                    };
                }
                Some(unsafe { transmute(result) })
            }

            #[inline]
            fn release(&mut self) {
                for shield in self {
                    shield.release();
                }
            }
        }
    )*
)}

impl_protector_for_array! {
    00
    01 02 03 04 05 06 07 08
    09 10 11 12 13 14 15 16
    17 18 19 20 21 22 23 24
    25 26 27 28 29 30 31 32
}

/// A result of a single step of an iterative critical section.
#[derive(PartialEq, Eq)]
pub enum TraverseStatus {
    /// The entire task is finished.
    Finished,
    /// We need to take one or more steps.
    Continue,
}

/// A result of a non-crashable section.
#[derive(PartialEq, Eq)]
pub enum WriteResult {
    /// The section is normally finished. Resume the task.
    Finished,
    /// Give up the current epoch and restart the whole critical section.
    RepinEpoch,
}

/// Panics if the pointer is not properly unaligned.
#[inline]
pub fn ensure_aligned<T>(raw: *const T) {
    assert_eq!(raw as usize & low_bits::<T>(), 0, "unaligned pointer");
}

/// Returns a bitmask containing the unused least significant bits of an aligned pointer to `T`.
#[inline]
pub fn low_bits<T>() -> usize {
    (1 << mem::align_of::<T>().trailing_zeros()) - 1
}

/// Given a tagged pointer `data`, returns the same pointer, but tagged with `tag`.
///
/// `tag` is truncated to fit into the unused bits of the pointer to `T`.
#[inline]
pub fn data_with_tag<T>(data: usize, tag: usize) -> usize {
    (data & !low_bits::<T>()) | (tag & low_bits::<T>())
}

/// Decomposes a tagged pointer `data` into the pointer and the tag.
#[inline]
pub fn decompose_data<T>(data: usize) -> (*mut T, usize) {
    let raw = (data & !low_bits::<T>()) as *mut T;
    let tag = data & low_bits::<T>();
    (raw, tag)
}

/// Returns a highest tag bit in a memory representation of `T`.
#[inline]
pub fn highest_tag<T>() -> usize {
    1 << (usize::BITS - low_bits::<T>().leading_zeros() - 1)
}

#[cfg(test)]
mod test {
    use std::thread::scope;

    use atomic::Ordering;

    use crate::{
        Atomic, CrashGuard, EpochGuard, Global, Invalidate, Owned, Protector, Retire, Shared,
        Shield,
    };

    struct Node {
        next: Atomic<Node>,
    }

    impl Invalidate for Node {
        fn invalidate(&self) {
            let guard = &unsafe { EpochGuard::unprotected() };
            let ptr = self.next.load(Ordering::Relaxed, guard);
            self.next
                .store(ptr.with_tag(1), Ordering::Relaxed, &unsafe {
                    CrashGuard::unprotected()
                });
        }

        fn is_invalidated(&self, guard: &EpochGuard) -> bool {
            (self.next.load(Ordering::Relaxed, guard).tag() & 1) != 0
        }
    }

    struct Cursor {
        prev: Shield<Node>,
        curr: Shield<Node>,
    }

    impl Protector for Cursor {
        type Target<'r> = SharedCursor<'r>;

        fn empty(handle: &mut crate::Handle) -> Self {
            Self {
                prev: Shield::null(handle),
                curr: Shield::null(handle),
            }
        }

        unsafe fn protect_unchecked(&mut self, read: &Self::Target<'_>) {
            self.prev.protect_unchecked(&read.prev);
            self.curr.protect_unchecked(&read.curr);
        }

        unsafe fn as_target<'r>(&self, guard: &'r EpochGuard) -> Option<Self::Target<'r>> {
            Some(SharedCursor {
                prev: self.prev.as_target(guard)?,
                curr: self.curr.as_target(guard)?,
            })
        }

        fn release(&mut self) {
            self.prev.release();
            self.curr.release();
        }
    }

    #[derive(Clone, Copy)]
    struct SharedCursor<'r> {
        prev: Shared<'r, Node>,
        curr: Shared<'r, Node>,
    }

    #[test]
    fn double_node() {
        const THREADS: usize = 30;
        const COUNT_PER_THREAD: usize = 1 << 20;

        let head = Atomic::new(Node {
            next: Atomic::new(Node {
                next: Atomic::null(),
            }),
        });

        unsafe {
            let global = &Global::new();
            scope(|s| {
                for _ in 0..THREADS {
                    s.spawn(|| {
                        let mut handle = global.register();
                        let mut cursor = Cursor::empty(&mut handle);
                        for _ in 0..COUNT_PER_THREAD {
                            loop {
                                cursor.traverse(&mut handle, |guard| {
                                    let mut cursor = SharedCursor {
                                        prev: Shared::null(),
                                        curr: Shared::null(),
                                    };

                                    cursor.prev = head.load(Ordering::Acquire, guard);
                                    cursor.curr = cursor
                                        .prev
                                        .as_ref(guard)
                                        .unwrap()
                                        .next
                                        .load(Ordering::Acquire, guard);

                                    cursor
                                });

                                let new = Owned::new(Node {
                                    next: Atomic::new(Node {
                                        next: Atomic::null(),
                                    }),
                                });

                                match head.compare_exchange(
                                    cursor.prev.shared(),
                                    new,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                    &handle,
                                ) {
                                    Ok(_) => {
                                        handle.retire(cursor.prev.shared());
                                        handle.retire(cursor.curr.shared());
                                        break;
                                    }
                                    Err(e) => {
                                        let new = e.new;
                                        drop(
                                            new.next
                                                .load(Ordering::Relaxed, &EpochGuard::unprotected())
                                                .into_owned(),
                                        );
                                    }
                                }
                            }
                        }
                    });
                }
            });
            let head = head.into_owned();
            drop(
                head.next
                    .load(Ordering::Relaxed, &EpochGuard::unprotected())
                    .into_owned(),
            );
        }
    }
}
