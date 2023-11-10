use std::{
    marker::PhantomData,
    mem::{align_of, forget, swap, transmute, zeroed, MaybeUninit},
    ops::{Deref, DerefMut},
    sync::atomic::{compiler_fence, AtomicUsize},
};

use atomic::Ordering;

use crate::{
    rrcu::{CsGuardRRCU, RaGuardRRCU, ThreadRRCU},
    CsGuard, RaGuard, Thread,
};

use super::{hazard::HazardPointer, Guard, Invalidate, Retire};

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

impl<T> Default for Atomic<T> {
    fn default() -> Self {
        Self::null()
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

impl<'r, T> Default for Shared<'r, T> {
    fn default() -> Self {
        Self {
            inner: 0,
            _marker: PhantomData,
        }
    }
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
    pub fn as_ref(&self, _: &CsGuard) -> Option<&'r T> {
        unsafe { decompose_data::<T>(self.inner).0.as_ref() }
    }

    /// Converts the pointer to a mutable reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    ///
    /// It is possible to directly dereference a [`Shared`] if and only if the current context is
    /// in a critical section which can be started by `traverse` and `traverse_loop` method.
    #[inline]
    pub fn as_mut(&mut self, _: &CsGuard) -> Option<&'r mut T> {
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
        drop(unsafe { Box::from_raw(decompose_data::<T>(self.inner).0) });
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
    pub fn null(thread: &mut Thread) -> Self {
        Self {
            hazptr: HazardPointer::new(thread),
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

    /// Returns the same pointer, but wrapped with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    #[inline]
    pub fn with_tag<'s>(&'s self, tag: usize) -> TaggedShield<'s, T> {
        TaggedShield { inner: self, tag }
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

    /// Stores a pointer value only to an inner variable. It doesn't protect the pointer.
    #[inline]
    pub unsafe fn store(&mut self, ptr: Shared<T>) {
        self.inner = ptr.into_usize();
    }
}

/// A reference of a [`Shield`] with a overwriting tag value.
///
/// # Motivation
///
/// `with_tag` for [`Rc`] and [`Shared`] can be implemented by taking the ownership of the original
/// pointer and returning the same one but tagged with the given value. However, for [`Shield`],
/// taking ownership is not a good option because [`Shield`]s usually live as fields of [`Defender`]
/// and we cannot take partial ownership in most cases.
///
/// Before proposing [`TaggedShield`], we just provided `set_tag` only for a [`Shield`], which changes
/// a tag value only. Unfortunately, it was not easy to use because there were many circumstances
/// where we just want to make a temporary tagged operand for atomic operators.
/// (especially, [`AtomicRc`].)
///
/// For this reason, a method to easily produce a tagged [`Shield`] pointer for a temporary use is
/// needed.
pub struct TaggedShield<'s, T> {
    pub(crate) inner: &'s Shield<T>,
    pub(crate) tag: usize,
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
    fn empty(thread: &mut Thread) -> Self;

    /// Stores the given `Target` pointers in hazard slots without any validations.
    ///
    /// Just storing a pointer in a hazard slot doesn't guarantee that the pointer is
    /// truly protected, as the memory block may already be reclaimed. We must validate whether
    /// the memory block is reclaimed or not, by reloading the atomic pointer or checking the
    /// local CRCU epoch.
    fn protect_unchecked(&mut self, read: &Self::Target<'_>);

    /// Loads currently protected pointers and checks whether any of them are invalidated.
    /// If not, creates a new `Target` and returns it.
    fn as_target<'r>(&self, guard: &'r CsGuard) -> Option<Self::Target<'r>>;

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
    #[inline]
    unsafe fn traverse<F>(&mut self, thread: &mut Thread, mut body: F)
    where
        F: for<'r> FnMut(&'r mut CsGuard) -> Self::Target<'r>,
    {
        thread.pin(
            #[inline]
            |guard| {
                // Execute the body of this read phase.
                let result = body(guard);
                // Store pointers in hazard slots. (A fence is not necessary.)
                self.protect_unchecked(&result);

                // If we successfully protected pointers without an intermediate crash,
                // it has the same meaning with a well-known HP validation:
                // we can safely assume that the pointers are not reclaimed yet.
            },
        )
    }

    /// Starts a non-crashable section where we can conduct operations with global side-effects.
    ///
    /// In this section, we do not restart immediately when we receive signals from reclaimers.
    /// The whole critical section restarts after this `mask` section ends, if a reclaimer sent
    /// a signal, or we advanced our epoch to reclaim a full local garbage bag.
    #[inline]
    unsafe fn traverse_mask<'r, F>(&mut self, cs: &CsGuard, to_deref: Self::Target<'r>, body: F)
    where
        F: FnOnce(&Self, &mut RaGuard) -> WriteResult,
    {
        let backup_idx = cs.backup_idx.clone();
        cs.mask(|guard| {
            let result = {
                // Store pointers in hazard slots and issue a fence.
                self.protect_unchecked(&to_deref);
                compiler_fence(Ordering::SeqCst);

                // Restart if the thread is crashed while protecting.
                if guard.must_rollback() {
                    guard.repin();
                }

                body(self, guard)
            };

            compiler_fence(Ordering::SeqCst);
            if result == WriteResult::RepinEpoch {
                // Invalidate any saved checkpoints.
                if let Some(backup_idx) = backup_idx {
                    unsafe { backup_idx.as_ref() }.store(2, Ordering::Relaxed);
                }
                guard.repin();
            }
        });
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
    #[inline]
    unsafe fn traverse_loop<F1, F2>(
        &mut self,
        backup: &mut Self,
        thread: &mut Thread,
        init_result: F1,
        step_forward: F2,
    ) where
        F1: for<'r> Fn(&'r mut CsGuard) -> Self::Target<'r>,
        F2: for<'r> Fn(&mut Self::Target<'r>, &'r mut CsGuard) -> TraverseStatus,
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

            thread.pin(|guard| {
                // Load the saved intermediate result, if one exists.
                guard.set_backup(&backup_idx);

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
                        transmute(init_result(guard))
                    });

                for iter in 0.. {
                    // Execute a single step.
                    //
                    // `transmute` synchronizes the lifetime parameters of `result` and `guard`.
                    // After a single `step_forward`, the lifetimes become different to each other,
                    // for example, `'r` and `'g` respectively. On the next iteration, they are
                    // synchronized again with the same value.
                    let step_result = step_forward(transmute(&mut result), guard);

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
                        compiler_fence(Ordering::SeqCst);

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
    fn empty(_: &mut Thread) -> Self {
        ()
    }

    #[inline]
    fn protect_unchecked(&mut self, _: &Self::Target<'_>) {}

    #[inline]
    fn as_target<'r>(&self, _: &'r CsGuard) -> Option<Self::Target<'r>> {
        Some(())
    }

    #[inline]
    fn release(&mut self) {}
}

/// A unit [`Protector`] with a single [`Shield`].
impl<T: Invalidate> Protector for Shield<T> {
    type Target<'r> = Shared<'r, T>;

    #[inline]
    fn empty(thread: &mut Thread) -> Self {
        Shield::null(thread)
    }

    #[inline]
    fn protect_unchecked(&mut self, read: &Self::Target<'_>) {
        let raw = read.untagged().as_raw();
        self.hazptr
            .protect_raw(raw as *const T as *mut T, Ordering::Relaxed);
        self.inner = raw;
    }

    #[inline]
    fn as_target<'r>(&self, guard: &'r CsGuard) -> Option<Self::Target<'r>> {
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
            fn empty(thread: &mut Thread) -> Self {
                let mut result: [MaybeUninit<Shield<T>>; $N] = unsafe { zeroed() };
                for shield in result.iter_mut() {
                    shield.write(Shield::null(thread));
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
            fn as_target<'r>(&self, guard: &'r CsGuard) -> Option<Self::Target<'r>> {
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
    (1 << align_of::<T>().trailing_zeros()) - 1
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
        hpsharp::{GlobalHPSharp, Invalidate, Owned, Retire},
        CsGuard, Global,
    };

    use super::{Atomic, Protector, Shared, Shield};

    struct Node {
        next: Atomic<Node>,
    }

    impl Invalidate for Node {
        fn invalidate(&self) {
            // We do not use traverse_loop here.
        }

        fn is_invalidated(&self, _: &CsGuard) -> bool {
            // We do not use traverse_loop here.
            false
        }
    }

    struct Cursor {
        prev: Shield<Node>,
        curr: Shield<Node>,
    }

    impl Protector for Cursor {
        type Target<'r> = SharedCursor<'r>;

        fn empty(thread: &mut crate::Thread) -> Self {
            Self {
                prev: Shield::null(thread),
                curr: Shield::null(thread),
            }
        }

        fn protect_unchecked(&mut self, read: &Self::Target<'_>) {
            self.prev.protect_unchecked(&read.prev);
            self.curr.protect_unchecked(&read.curr);
        }

        fn as_target<'r>(&self, guard: &'r CsGuard) -> Option<Self::Target<'r>> {
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
            let global = Global::new();
            scope(|s| {
                for _ in 0..THREADS {
                    s.spawn(|| {
                        let mut thread = global.register();
                        let mut cursor = Cursor::empty(&mut thread);
                        for _ in 0..COUNT_PER_THREAD {
                            loop {
                                cursor.traverse(&mut thread, |guard| {
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
                                    &thread,
                                ) {
                                    Ok(_) => {
                                        thread.retire(cursor.prev.shared());
                                        thread.retire(cursor.curr.shared());
                                        break;
                                    }
                                    Err(e) => {
                                        let new = e.new;
                                        drop(
                                            new.next
                                                .load(Ordering::Relaxed, &CsGuard::unprotected())
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
                    .load(Ordering::Relaxed, &CsGuard::unprotected())
                    .into_owned(),
            );
        }
    }
}
