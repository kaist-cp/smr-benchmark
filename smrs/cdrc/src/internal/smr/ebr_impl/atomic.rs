use core::borrow::{Borrow, BorrowMut};
use core::cmp;
use core::fmt;
use core::marker::PhantomData;
use core::mem::{self, MaybeUninit};
use core::ops::{Deref, DerefMut};
use core::slice;
use core::sync::atomic::{AtomicUsize, Ordering};

use super::guard::Guard;
use crossbeam_utils::atomic::AtomicConsume;
use std::alloc;

/// Given ordering for the success case in a compare-exchange operation, returns the strongest
/// appropriate ordering for the failure case.
#[inline]
fn strongest_failure_ordering(ord: Ordering) -> Ordering {
    use self::Ordering::*;
    match ord {
        Relaxed | Release => Relaxed,
        Acquire | AcqRel => Acquire,
        _ => SeqCst,
    }
}

/// The error returned on failed compare-and-set operation.
// TODO: remove in the next major version.
#[deprecated(note = "Use `CompareExchangeError` instead")]
pub type CompareAndSetError<'g, T, P> = CompareExchangeError<'g, T, P>;

/// The error returned on failed compare-and-swap operation.
pub struct CompareExchangeError<'g, T: ?Sized + Pointable, P: Pointer<T>> {
    /// The value in the atomic pointer at the time of the failed operation.
    pub current: Shared<'g, T>,

    /// The new value, which the operation failed to store.
    pub new: P,
}

impl<T, P: Pointer<T> + fmt::Debug> fmt::Debug for CompareExchangeError<'_, T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompareExchangeError")
            .field("current", &self.current)
            .field("new", &self.new)
            .finish()
    }
}

/// Memory orderings for compare-and-set operations.
///
/// A compare-and-set operation can have different memory orderings depending on whether it
/// succeeds or fails. This trait generalizes different ways of specifying memory orderings.
///
/// The two ways of specifying orderings for compare-and-set are:
///
/// 1. Just one `Ordering` for the success case. In case of failure, the strongest appropriate
///    ordering is chosen.
/// 2. A pair of `Ordering`s. The first one is for the success case, while the second one is
///    for the failure case.
// TODO: remove in the next major version.
#[deprecated(
    note = "`compare_and_set` and `compare_and_set_weak` that use this trait are deprecated, \
            use `compare_exchange` or `compare_exchange_weak instead`"
)]
pub trait CompareAndSetOrdering {
    /// The ordering of the operation when it succeeds.
    fn success(&self) -> Ordering;

    /// The ordering of the operation when it fails.
    ///
    /// The failure ordering can't be `Release` or `AcqRel` and must be equivalent or weaker than
    /// the success ordering.
    fn failure(&self) -> Ordering;
}

#[allow(deprecated)]
impl CompareAndSetOrdering for Ordering {
    #[inline]
    fn success(&self) -> Ordering {
        *self
    }

    #[inline]
    fn failure(&self) -> Ordering {
        strongest_failure_ordering(*self)
    }
}

#[allow(deprecated)]
impl CompareAndSetOrdering for (Ordering, Ordering) {
    #[inline]
    fn success(&self) -> Ordering {
        self.0
    }

    #[inline]
    fn failure(&self) -> Ordering {
        self.1
    }
}

/// Returns a bitmask containing the unused least significant bits of an aligned pointer to `T`.
#[inline]
fn low_bits<T: ?Sized + Pointable>() -> usize {
    (1 << T::ALIGN.trailing_zeros()) - 1
}

/// Panics if the pointer is not properly unaligned.
#[inline]
fn ensure_aligned<T: ?Sized + Pointable>(raw: usize) {
    assert_eq!(raw & low_bits::<T>(), 0, "unaligned pointer");
}

/// Given a tagged pointer `data`, returns the same pointer, but tagged with `tag`.
///
/// `tag` is truncated to fit into the unused bits of the pointer to `T`.
#[inline]
fn compose_tag<T: ?Sized + Pointable>(data: usize, tag: usize) -> usize {
    (data & !low_bits::<T>()) | (tag & low_bits::<T>())
}

/// Decomposes a tagged pointer `data` into the pointer and the tag.
#[inline]
pub(crate) fn decompose_tag<T: ?Sized + Pointable>(data: usize) -> (usize, usize) {
    (data & !low_bits::<T>(), data & low_bits::<T>())
}

/// Types that are pointed to by a single word.
///
/// In concurrent programming, it is necessary to represent an object within a word because atomic
/// operations (e.g., reads, writes, read-modify-writes) support only single words.  This trait
/// qualifies such types that are pointed to by a single word.
///
/// The trait generalizes `Box<T>` for a sized type `T`.  In a box, an object of type `T` is
/// allocated in heap and it is owned by a single-word pointer.  This trait is also implemented for
/// `[MaybeUninit<T>]` by storing its size along with its elements and pointing to the pair of array
/// size and elements.
///
/// Pointers to `Pointable` types can be stored in [`Atomic`], [`Owned`], and [`Shared`].  In
/// particular, Crossbeam supports dynamically sized slices as follows.
pub trait Pointable {
    /// The alignment of pointer.
    const ALIGN: usize;

    /// The type for initializers.
    type Init;

    /// Initializes a with the given initializer.
    ///
    /// # Safety
    ///
    /// The result should be a multiple of `ALIGN`.
    unsafe fn init(init: Self::Init) -> usize;

    /// Dereferences the given pointer.
    ///
    /// # Safety
    ///
    /// - The given `ptr` should have been initialized with [`Pointable::init`].
    /// - `ptr` should not have yet been dropped by [`Pointable::drop`].
    /// - `ptr` should not be mutably dereferenced by [`Pointable::deref_mut`] concurrently.
    unsafe fn deref<'a>(ptr: usize) -> &'a Self;

    /// Mutably dereferences the given pointer.
    ///
    /// # Safety
    ///
    /// - The given `ptr` should have been initialized with [`Pointable::init`].
    /// - `ptr` should not have yet been dropped by [`Pointable::drop`].
    /// - `ptr` should not be dereferenced by [`Pointable::deref`] or [`Pointable::deref_mut`]
    ///   concurrently.
    unsafe fn deref_mut<'a>(ptr: usize) -> &'a mut Self;

    /// Drops the object pointed to by the given pointer.
    ///
    /// # Safety
    ///
    /// - The given `ptr` should have been initialized with [`Pointable::init`].
    /// - `ptr` should not have yet been dropped by [`Pointable::drop`].
    /// - `ptr` should not be dereferenced by [`Pointable::deref`] or [`Pointable::deref_mut`]
    ///   concurrently.
    unsafe fn drop(ptr: usize);
}

impl<T> Pointable for T {
    const ALIGN: usize = mem::align_of::<T>();

    type Init = T;

    unsafe fn init(init: Self::Init) -> usize {
        Box::into_raw(Box::new(init)) as usize
    }

    unsafe fn deref<'a>(ptr: usize) -> &'a Self {
        &*(ptr as *const T)
    }

    unsafe fn deref_mut<'a>(ptr: usize) -> &'a mut Self {
        &mut *(ptr as *mut T)
    }

    unsafe fn drop(ptr: usize) {
        drop(Box::from_raw(ptr as *mut T));
    }
}

/// Array with size.
///
/// # Memory layout
///
/// An array consisting of size and elements:
///
/// ```text
///          elements
///          |
///          |
/// ------------------------------------
/// | size | 0 | 1 | 2 | 3 | 4 | 5 | 6 |
/// ------------------------------------
/// ```
///
/// Its memory layout is different from that of `Box<[T]>` in that size is in the allocation (not
/// along with pointer as in `Box<[T]>`).
///
/// Elements are not present in the type, but they will be in the allocation.
/// ```
///
// TODO(): once we bump the minimum required Rust version to 1.44 or newer, use
// [`alloc::alloc::Layout::extend`] instead.
#[repr(C)]
struct Array<T> {
    /// The number of elements (not the number of bytes).
    len: usize,
    elements: [MaybeUninit<T>; 0],
}

impl<T> Pointable for [MaybeUninit<T>] {
    const ALIGN: usize = mem::align_of::<Array<T>>();

    type Init = usize;

    unsafe fn init(len: Self::Init) -> usize {
        let size = mem::size_of::<Array<T>>() + mem::size_of::<MaybeUninit<T>>() * len;
        let align = mem::align_of::<Array<T>>();
        let layout = alloc::Layout::from_size_align(size, align).unwrap();
        let ptr = alloc::alloc(layout).cast::<Array<T>>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        (*ptr).len = len;
        ptr as usize
    }

    unsafe fn deref<'a>(ptr: usize) -> &'a Self {
        let array = &*(ptr as *const Array<T>);
        slice::from_raw_parts(array.elements.as_ptr() as *const _, array.len)
    }

    unsafe fn deref_mut<'a>(ptr: usize) -> &'a mut Self {
        let array = &*(ptr as *mut Array<T>);
        slice::from_raw_parts_mut(array.elements.as_ptr() as *mut _, array.len)
    }

    unsafe fn drop(ptr: usize) {
        let array = &*(ptr as *mut Array<T>);
        let size = mem::size_of::<Array<T>>() + mem::size_of::<MaybeUninit<T>>() * array.len;
        let align = mem::align_of::<Array<T>>();
        let layout = alloc::Layout::from_size_align(size, align).unwrap();
        alloc::dealloc(ptr as *mut u8, layout);
    }
}

/// An atomic pointer that can be safely shared between threads.
///
/// The pointer must be properly aligned. Since it is aligned, a tag can be stored into the unused
/// least significant bits of the address. For example, the tag for a pointer to a sized type `T`
/// should be less than `(1 << mem::align_of::<T>().trailing_zeros())`.
///
/// Any method that loads the pointer must be passed a reference to a [`Guard`].
///
/// Crossbeam supports dynamically sized types.  See [`Pointable`] for details.
pub struct Atomic<T: ?Sized + Pointable> {
    data: AtomicUsize,
    _marker: PhantomData<*mut T>,
}

unsafe impl<T: ?Sized + Pointable + Send + Sync> Send for Atomic<T> {}
unsafe impl<T: ?Sized + Pointable + Send + Sync> Sync for Atomic<T> {}

impl<T> Atomic<T> {
    /// Allocates `value` on the heap and returns a new atomic pointer pointing to it.
    pub fn new(init: T) -> Atomic<T> {
        Self::init(init)
    }
}

impl<T: ?Sized + Pointable> Atomic<T> {
    /// Allocates `value` on the heap and returns a new atomic pointer pointing to it.
    pub fn init(init: T::Init) -> Atomic<T> {
        Self::from(Owned::init(init))
    }

    /// Returns a new atomic pointer pointing to the tagged pointer `data`.
    fn from_usize(data: usize) -> Self {
        Self {
            data: AtomicUsize::new(data),
            _marker: PhantomData,
        }
    }

    /// Returns a new null atomic pointer.
    #[cfg(all(not(crossbeam_no_const_fn_trait_bound), not(crossbeam_loom)))]
    pub const fn null() -> Atomic<T> {
        Self {
            data: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    /// Returns a new null atomic pointer.
    #[cfg(not(all(not(crossbeam_no_const_fn_trait_bound), not(crossbeam_loom))))]
    pub fn null() -> Atomic<T> {
        Self {
            data: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    /// Loads a `Shared` from the atomic pointer.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    pub fn load<'g>(&self, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.load(ord)) }
    }

    /// Loads a `Shared` from the atomic pointer using a "consume" memory ordering.
    ///
    /// This is similar to the "acquire" ordering, except that an ordering is
    /// only guaranteed with operations that "depend on" the result of the load.
    /// However consume loads are usually much faster than acquire loads on
    /// architectures with a weak memory model since they don't require memory
    /// fence instructions.
    ///
    /// The exact definition of "depend on" is a bit vague, but it works as you
    /// would expect in practice since a lot of software, especially the Linux
    /// kernel, rely on this behavior.
    pub fn load_consume<'g>(&self, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.load_consume()) }
    }

    /// Stores a `Shared` or `Owned` pointer into the atomic pointer.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    pub fn store<P: Pointer<T>>(&self, new: P, ord: Ordering) {
        self.data.store(new.into_usize(), ord);
    }

    /// Stores a `Shared` or `Owned` pointer into the atomic pointer, returning the previous
    /// `Shared`.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    pub fn swap<'g, P: Pointer<T>>(&self, new: P, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.swap(new.into_usize(), ord)) }
    }

    /// Stores the pointer `new` (either `Shared` or `Owned`) into the atomic pointer if the current
    /// value is the same as `current`. The tag is also taken into account, so two pointers to the
    /// same object, but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success the
    /// pointer that was written is returned. On failure the actual current value and `new` are
    /// returned.
    ///
    /// This method takes two `Ordering` arguments to describe the memory
    /// ordering of this operation. `success` describes the required ordering for the
    /// read-modify-write operation that takes place if the comparison with `current` succeeds.
    /// `failure` describes the required ordering for the load operation that takes place when
    /// the comparison fails. Using `Acquire` as success ordering makes the store part
    /// of this operation `Relaxed`, and using `Release` makes the successful load
    /// `Relaxed`. The failure ordering can only be `SeqCst`, `Acquire` or `Relaxed`
    /// and must be equivalent to or weaker than the success ordering.
    pub fn compare_exchange<'g, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        _: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T, P>>
    where
        P: Pointer<T>,
    {
        let new = new.into_usize();
        self.data
            .compare_exchange(current.into_usize(), new, success, failure)
            .map(|_| unsafe { Shared::from_usize(new) })
            .map_err(|current| unsafe {
                CompareExchangeError {
                    current: Shared::from_usize(current),
                    new: P::from_usize(new),
                }
            })
    }

    /// Stores the pointer `new` (either `Shared` or `Owned`) into the atomic pointer if the current
    /// value is the same as `current`. The tag is also taken into account, so two pointers to the
    /// same object, but with different tags, will not be considered equal.
    ///
    /// Unlike [`compare_exchange`], this method is allowed to spuriously fail even when comparison
    /// succeeds, which can result in more efficient code on some platforms.  The return value is a
    /// result indicating whether the new pointer was written. On success the pointer that was
    /// written is returned. On failure the actual current value and `new` are returned.
    ///
    /// This method takes two `Ordering` arguments to describe the memory
    /// ordering of this operation. `success` describes the required ordering for the
    /// read-modify-write operation that takes place if the comparison with `current` succeeds.
    /// `failure` describes the required ordering for the load operation that takes place when
    /// the comparison fails. Using `Acquire` as success ordering makes the store part
    /// of this operation `Relaxed`, and using `Release` makes the successful load
    /// `Relaxed`. The failure ordering can only be `SeqCst`, `Acquire` or `Relaxed`
    /// and must be equivalent to or weaker than the success ordering.
    pub fn compare_exchange_weak<'g, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        _: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T, P>>
    where
        P: Pointer<T>,
    {
        let new = new.into_usize();
        self.data
            .compare_exchange_weak(current.into_usize(), new, success, failure)
            .map(|_| unsafe { Shared::from_usize(new) })
            .map_err(|current| unsafe {
                CompareExchangeError {
                    current: Shared::from_usize(current),
                    new: P::from_usize(new),
                }
            })
    }

    /// Fetches the pointer, and then applies a function to it that returns a new value.
    /// Returns a `Result` of `Ok(previous_value)` if the function returned `Some`, else `Err(_)`.
    ///
    /// Note that the given function may be called multiple times if the value has been changed by
    /// other threads in the meantime, as long as the function returns `Some(_)`, but the function
    /// will have been applied only once to the stored value.
    ///
    /// `fetch_update` takes two [`Ordering`] arguments to describe the memory
    /// ordering of this operation. The first describes the required ordering for
    /// when the operation finally succeeds while the second describes the
    /// required ordering for loads. These correspond to the success and failure
    /// orderings of [`Atomic::compare_exchange`] respectively.
    ///
    /// Using [`Acquire`] as success ordering makes the store part of this
    /// operation [`Relaxed`], and using [`Release`] makes the final successful
    /// load [`Relaxed`]. The (failed) load ordering can only be [`SeqCst`],
    /// [`Acquire`] or [`Relaxed`] and must be equivalent to or weaker than the
    /// success ordering.
    ///
    /// [`Relaxed`]: Ordering::Relaxed
    /// [`Acquire`]: Ordering::Acquire
    /// [`Release`]: Ordering::Release
    /// [`SeqCst`]: Ordering::SeqCst
    pub fn fetch_update<'g, F>(
        &self,
        set_order: Ordering,
        fail_order: Ordering,
        guard: &'g Guard,
        mut func: F,
    ) -> Result<Shared<'g, T>, Shared<'g, T>>
    where
        F: FnMut(Shared<'g, T>) -> Option<Shared<'g, T>>,
    {
        let mut prev = self.load(fail_order, guard);
        while let Some(next) = func(prev) {
            match self.compare_exchange_weak(prev, next, set_order, fail_order, guard) {
                Ok(shared) => return Ok(shared),
                Err(next_prev) => prev = next_prev.current,
            }
        }
        Err(prev)
    }

    /// Stores the pointer `new` (either `Shared` or `Owned`) into the atomic pointer if the current
    /// value is the same as `current`. The tag is also taken into account, so two pointers to the
    /// same object, but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success the
    /// pointer that was written is returned. On failure the actual current value and `new` are
    /// returned.
    ///
    /// This method takes a [`CompareAndSetOrdering`] argument which describes the memory
    /// ordering of this operation.
    ///
    /// # Migrating to `compare_exchange`
    ///
    /// `compare_and_set` is equivalent to `compare_exchange` with the following mapping for
    /// memory orderings:
    ///
    /// Original | Success | Failure
    /// -------- | ------- | -------
    /// Relaxed  | Relaxed | Relaxed
    /// Acquire  | Acquire | Acquire
    /// Release  | Release | Relaxed
    /// AcqRel   | AcqRel  | Acquire
    /// SeqCst   | SeqCst  | SeqCst
    // TODO: remove in the next major version.
    #[allow(deprecated)]
    #[deprecated(note = "Use `compare_exchange` instead")]
    pub fn compare_and_set<'g, O, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        ord: O,
        guard: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareAndSetError<'g, T, P>>
    where
        O: CompareAndSetOrdering,
        P: Pointer<T>,
    {
        self.compare_exchange(current, new, ord.success(), ord.failure(), guard)
    }

    /// Stores the pointer `new` (either `Shared` or `Owned`) into the atomic pointer if the current
    /// value is the same as `current`. The tag is also taken into account, so two pointers to the
    /// same object, but with different tags, will not be considered equal.
    ///
    /// Unlike [`compare_and_set`], this method is allowed to spuriously fail even when comparison
    /// succeeds, which can result in more efficient code on some platforms.  The return value is a
    /// result indicating whether the new pointer was written. On success the pointer that was
    /// written is returned. On failure the actual current value and `new` are returned.
    ///
    /// This method takes a [`CompareAndSetOrdering`] argument which describes the memory
    /// ordering of this operation.
    ///
    /// [`compare_and_set`]: Atomic::compare_and_set
    ///
    /// # Migrating to `compare_exchange_weak`
    ///
    /// `compare_and_set_weak` is equivalent to `compare_exchange_weak` with the following mapping for
    /// memory orderings:
    ///
    /// Original | Success | Failure
    /// -------- | ------- | -------
    /// Relaxed  | Relaxed | Relaxed
    /// Acquire  | Acquire | Acquire
    /// Release  | Release | Relaxed
    /// AcqRel   | AcqRel  | Acquire
    /// SeqCst   | SeqCst  | SeqCst
    // TODO: remove in the next major version.
    #[allow(deprecated)]
    #[deprecated(note = "Use `compare_exchange_weak` instead")]
    pub fn compare_and_set_weak<'g, O, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        ord: O,
        guard: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareAndSetError<'g, T, P>>
    where
        O: CompareAndSetOrdering,
        P: Pointer<T>,
    {
        self.compare_exchange_weak(current, new, ord.success(), ord.failure(), guard)
    }

    /// Bitwise "and" with the current tag.
    ///
    /// Performs a bitwise "and" operation on the current tag and the argument `val`, and sets the
    /// new tag to the result. Returns the previous pointer.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    pub fn fetch_and<'g>(&self, val: usize, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.fetch_and(val | !low_bits::<T>(), ord)) }
    }

    /// Bitwise "or" with the current tag.
    ///
    /// Performs a bitwise "or" operation on the current tag and the argument `val`, and sets the
    /// new tag to the result. Returns the previous pointer.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    pub fn fetch_or<'g>(&self, val: usize, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.fetch_or(val & low_bits::<T>(), ord)) }
    }

    /// Bitwise "xor" with the current tag.
    ///
    /// Performs a bitwise "xor" operation on the current tag and the argument `val`, and sets the
    /// new tag to the result. Returns the previous pointer.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    pub fn fetch_xor<'g>(&self, val: usize, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.fetch_xor(val & low_bits::<T>(), ord)) }
    }

    /// Takes ownership of the pointee.
    ///
    /// This consumes the atomic and converts it into [`Owned`]. As [`Atomic`] doesn't have a
    /// destructor and doesn't drop the pointee while [`Owned`] does, this is suitable for
    /// destructors of data structures.
    ///
    /// # Panics
    ///
    /// Panics if this pointer is null, but only in debug mode.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object.
    pub unsafe fn into_owned(self) -> Owned<T> {
        #[cfg(crossbeam_loom)]
        {
            // FIXME: loom does not yet support into_inner, so we use unsync_load for now,
            // which should have the same synchronization properties:
            // https://github.com/tokio-rs/loom/issues/117
            Owned::from_usize(self.data.unsync_load())
        }
        #[cfg(not(crossbeam_loom))]
        {
            Owned::from_usize(self.data.into_inner())
        }
    }

    /// Takes ownership of the pointee if it is non-null.
    ///
    /// This consumes the atomic and converts it into [`Owned`]. As [`Atomic`] doesn't have a
    /// destructor and doesn't drop the pointee while [`Owned`] does, this is suitable for
    /// destructors of data structures.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object, or the pointer is null.
    pub unsafe fn try_into_owned(self) -> Option<Owned<T>> {
        // FIXME: See self.into_owned()
        #[cfg(crossbeam_loom)]
        let data = self.data.unsync_load();
        #[cfg(not(crossbeam_loom))]
        let data = self.data.into_inner();
        if decompose_tag::<T>(data).0 == 0 {
            None
        } else {
            Some(Owned::from_usize(data))
        }
    }
}

impl<T: ?Sized + Pointable> fmt::Debug for Atomic<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data = self.data.load(Ordering::SeqCst);
        let (raw, tag) = decompose_tag::<T>(data);

        f.debug_struct("Atomic")
            .field("raw", &raw)
            .field("tag", &tag)
            .finish()
    }
}

impl<T: ?Sized + Pointable> fmt::Pointer for Atomic<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data = self.data.load(Ordering::SeqCst);
        let (raw, _) = decompose_tag::<T>(data);
        fmt::Pointer::fmt(&(unsafe { T::deref(raw) as *const _ }), f)
    }
}

impl<T: ?Sized + Pointable> Clone for Atomic<T> {
    /// Returns a copy of the atomic value.
    ///
    /// Note that a `Relaxed` load is used here. If you need synchronization, use it with other
    /// atomics or fences.
    fn clone(&self) -> Self {
        let data = self.data.load(Ordering::Relaxed);
        Atomic::from_usize(data)
    }
}

impl<T: ?Sized + Pointable> Default for Atomic<T> {
    fn default() -> Self {
        Atomic::null()
    }
}

impl<T: ?Sized + Pointable> From<Owned<T>> for Atomic<T> {
    /// Returns a new atomic pointer pointing to `owned`.
    fn from(owned: Owned<T>) -> Self {
        let data = owned.data;
        mem::forget(owned);
        Self::from_usize(data)
    }
}

impl<T> From<Box<T>> for Atomic<T> {
    fn from(b: Box<T>) -> Self {
        Self::from(Owned::from(b))
    }
}

impl<T> From<T> for Atomic<T> {
    fn from(t: T) -> Self {
        Self::new(t)
    }
}

impl<'g, T: ?Sized + Pointable> From<Shared<'g, T>> for Atomic<T> {
    /// Returns a new atomic pointer pointing to `ptr`.
    fn from(ptr: Shared<'g, T>) -> Self {
        Self::from_usize(ptr.data)
    }
}

impl<T> From<*const T> for Atomic<T> {
    /// Returns a new atomic pointer pointing to `raw`.
    fn from(raw: *const T) -> Self {
        Self::from_usize(raw as usize)
    }
}

/// A trait for either `Owned` or `Shared` pointers.
pub trait Pointer<T: ?Sized + Pointable> {
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

/// An owned heap-allocated object.
///
/// This type is very similar to `Box<T>`.
///
/// The pointer must be properly aligned. Since it is aligned, a tag can be stored into the unused
/// least significant bits of the address.
pub struct Owned<T: ?Sized + Pointable> {
    data: usize,
    _marker: PhantomData<Box<T>>,
}

impl<T: ?Sized + Pointable> Pointer<T> for Owned<T> {
    #[inline]
    fn into_usize(self) -> usize {
        let data = self.data;
        mem::forget(self);
        data
    }

    /// Returns a new pointer pointing to the tagged pointer `data`.
    ///
    /// # Panics
    ///
    /// Panics if the data is zero in debug mode.
    #[inline]
    unsafe fn from_usize(data: usize) -> Self {
        debug_assert!(data != 0, "converting zero into `Owned`");
        Owned {
            data,
            _marker: PhantomData,
        }
    }
}

impl<T> Owned<T> {
    /// Returns a new owned pointer pointing to `raw`.
    ///
    /// This function is unsafe because improper use may lead to memory problems. Argument `raw`
    /// must be a valid pointer. Also, a double-free may occur if the function is called twice on
    /// the same raw pointer.
    ///
    /// # Panics
    ///
    /// Panics if `raw` is not properly aligned.
    ///
    /// # Safety
    ///
    /// The given `raw` should have been derived from `Owned`, and one `raw` should not be converted
    /// back by `Owned::from_raw()` multiple times.
    pub unsafe fn from_raw(raw: *mut T) -> Owned<T> {
        let raw = raw as usize;
        ensure_aligned::<T>(raw);
        Self::from_usize(raw)
    }

    /// Converts the owned pointer into a `Box`.
    pub fn into_box(self) -> Box<T> {
        let (raw, _) = decompose_tag::<T>(self.data);
        mem::forget(self);
        unsafe { Box::from_raw(raw as *mut _) }
    }

    /// Allocates `value` on the heap and returns a new owned pointer pointing to it.
    pub fn new(init: T) -> Owned<T> {
        Self::init(init)
    }
}

impl<T: ?Sized + Pointable> Owned<T> {
    /// Allocates `value` on the heap and returns a new owned pointer pointing to it.
    pub fn init(init: T::Init) -> Owned<T> {
        unsafe { Self::from_usize(T::init(init)) }
    }

    /// Converts the owned pointer into a [`Shared`].
    #[allow(clippy::needless_lifetimes)]
    pub fn into_shared<'g>(self, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.into_usize()) }
    }

    /// Returns the tag stored within the pointer.
    pub fn tag(&self) -> usize {
        let (_, tag) = decompose_tag::<T>(self.data);
        tag
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    pub fn with_tag(self, tag: usize) -> Owned<T> {
        let data = self.into_usize();
        unsafe { Self::from_usize(compose_tag::<T>(data, tag)) }
    }
}

impl<T: ?Sized + Pointable> Drop for Owned<T> {
    fn drop(&mut self) {
        let (raw, _) = decompose_tag::<T>(self.data);
        unsafe {
            T::drop(raw);
        }
    }
}

impl<T: ?Sized + Pointable> fmt::Debug for Owned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (raw, tag) = decompose_tag::<T>(self.data);

        f.debug_struct("Owned")
            .field("raw", &raw)
            .field("tag", &tag)
            .finish()
    }
}

impl<T: Clone> Clone for Owned<T> {
    fn clone(&self) -> Self {
        Owned::new((**self).clone()).with_tag(self.tag())
    }
}

impl<T: ?Sized + Pointable> Deref for Owned<T> {
    type Target = T;

    fn deref(&self) -> &T {
        let (raw, _) = decompose_tag::<T>(self.data);
        unsafe { T::deref(raw) }
    }
}

impl<T: ?Sized + Pointable> DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut T {
        let (raw, _) = decompose_tag::<T>(self.data);
        unsafe { T::deref_mut(raw) }
    }
}

impl<T> From<T> for Owned<T> {
    fn from(t: T) -> Self {
        Owned::new(t)
    }
}

impl<T> From<Box<T>> for Owned<T> {
    /// Returns a new owned pointer pointing to `b`.
    ///
    /// # Panics
    ///
    /// Panics if the pointer (the `Box`) is not properly aligned.
    fn from(b: Box<T>) -> Self {
        unsafe { Self::from_raw(Box::into_raw(b)) }
    }
}

impl<T: ?Sized + Pointable> Borrow<T> for Owned<T> {
    fn borrow(&self) -> &T {
        self.deref()
    }
}

impl<T: ?Sized + Pointable> BorrowMut<T> for Owned<T> {
    fn borrow_mut(&mut self) -> &mut T {
        self.deref_mut()
    }
}

impl<T: ?Sized + Pointable> AsRef<T> for Owned<T> {
    fn as_ref(&self) -> &T {
        self.deref()
    }
}

impl<T: ?Sized + Pointable> AsMut<T> for Owned<T> {
    fn as_mut(&mut self) -> &mut T {
        self.deref_mut()
    }
}

/// A pointer to an object protected by the epoch GC.
///
/// The pointer is valid for use only during the lifetime `'g`.
///
/// The pointer must be properly aligned. Since it is aligned, a tag can be stored into the unused
/// least significant bits of the address.
pub struct Shared<'g, T: 'g + ?Sized + Pointable> {
    data: usize,
    _marker: PhantomData<(&'g (), *const T)>,
}

impl<T: ?Sized + Pointable> Clone for Shared<'_, T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Pointable> Copy for Shared<'_, T> {}

impl<T: ?Sized + Pointable> Pointer<T> for Shared<'_, T> {
    #[inline]
    fn into_usize(self) -> usize {
        self.data
    }

    #[inline]
    unsafe fn from_usize(data: usize) -> Self {
        Shared {
            data,
            _marker: PhantomData,
        }
    }
}

impl<'g, T> Shared<'g, T> {
    /// Converts the pointer to a raw pointer (without the tag).
    pub fn as_raw(&self) -> *const T {
        let (raw, _) = decompose_tag::<T>(self.data);
        raw as *const _
    }
}

impl<'g, T: ?Sized + Pointable> Shared<'g, T> {
    /// Returns a new null pointer.
    pub fn null() -> Shared<'g, T> {
        Shared {
            data: 0,
            _marker: PhantomData,
        }
    }

    /// Returns `true` if the pointer is null.
    pub fn is_null(&self) -> bool {
        let (raw, _) = decompose_tag::<T>(self.data);
        raw == 0
    }

    /// Dereferences the pointer.
    ///
    /// Returns a reference to the pointee that is valid during the lifetime `'g`.
    ///
    /// # Safety
    ///
    /// Dereferencing a pointer is unsafe because it could be pointing to invalid memory.
    ///
    /// Another concern is the possibility of data races due to lack of proper synchronization.
    /// For example, consider the following scenario:
    ///
    /// 1. A thread creates a new object: `a.store(Owned::new(10), Relaxed)`
    /// 2. Another thread reads it: `*a.load(Relaxed, guard).as_ref().unwrap()`
    ///
    /// The problem is that relaxed orderings don't synchronize initialization of the object with
    /// the read from the second thread. This is a data race. A possible solution would be to use
    /// `Release` and `Acquire` orderings.
    pub unsafe fn deref(&self) -> &'g T {
        let (raw, _) = decompose_tag::<T>(self.data);
        T::deref(raw)
    }

    /// Dereferences the pointer.
    ///
    /// Returns a mutable reference to the pointee that is valid during the lifetime `'g`.
    ///
    /// # Safety
    ///
    /// * There is no guarantee that there are no more threads attempting to read/write from/to the
    ///   actual object at the same time.
    ///
    ///   The user must know that there are no concurrent accesses towards the object itself.
    ///
    /// * Other than the above, all safety concerns of `deref()` applies here.
    pub unsafe fn deref_mut(&mut self) -> &'g mut T {
        let (raw, _) = decompose_tag::<T>(self.data);
        T::deref_mut(raw)
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    ///
    /// # Safety
    ///
    /// Dereferencing a pointer is unsafe because it could be pointing to invalid memory.
    ///
    /// Another concern is the possibility of data races due to lack of proper synchronization.
    /// For example, consider the following scenario:
    ///
    /// 1. A thread creates a new object: `a.store(Owned::new(10), Relaxed)`
    /// 2. Another thread reads it: `*a.load(Relaxed, guard).as_ref().unwrap()`
    ///
    /// The problem is that relaxed orderings don't synchronize initialization of the object with
    /// the read from the second thread. This is a data race. A possible solution would be to use
    /// `Release` and `Acquire` orderings.
    pub unsafe fn as_ref(&self) -> Option<&'g T> {
        let (raw, _) = decompose_tag::<T>(self.data);
        if raw == 0 {
            None
        } else {
            Some(T::deref(raw))
        }
    }

    /// Takes ownership of the pointee.
    ///
    /// # Panics
    ///
    /// Panics if this pointer is null, but only in debug mode.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object.
    pub unsafe fn into_owned(self) -> Owned<T> {
        debug_assert!(!self.is_null(), "converting a null `Shared` into `Owned`");
        Owned::from_usize(self.data)
    }

    /// Takes ownership of the pointee if it is not null.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object, or if the pointer is null.
    pub unsafe fn try_into_owned(self) -> Option<Owned<T>> {
        if self.is_null() {
            None
        } else {
            Some(Owned::from_usize(self.data))
        }
    }

    /// Returns the tag stored within the pointer.
    pub fn tag(&self) -> usize {
        let (_, tag) = decompose_tag::<T>(self.data);
        tag
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    pub fn with_tag(&self, tag: usize) -> Shared<'g, T> {
        unsafe { Self::from_usize(compose_tag::<T>(self.data, tag)) }
    }
}

impl<T> From<*const T> for Shared<'_, T> {
    /// Returns a new pointer pointing to `raw`.
    ///
    /// # Panics
    ///
    /// Panics if `raw` is not properly aligned.
    fn from(raw: *const T) -> Self {
        let raw = raw as usize;
        ensure_aligned::<T>(raw);
        unsafe { Self::from_usize(raw) }
    }
}

impl<'g, T: ?Sized + Pointable> PartialEq<Shared<'g, T>> for Shared<'g, T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T: ?Sized + Pointable> Eq for Shared<'_, T> {}

impl<'g, T: ?Sized + Pointable> PartialOrd<Shared<'g, T>> for Shared<'g, T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.data.partial_cmp(&other.data)
    }
}

impl<T: ?Sized + Pointable> Ord for Shared<'_, T> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

impl<T: ?Sized + Pointable> fmt::Debug for Shared<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (raw, tag) = decompose_tag::<T>(self.data);

        f.debug_struct("Shared")
            .field("raw", &raw)
            .field("tag", &tag)
            .finish()
    }
}

impl<T: ?Sized + Pointable> fmt::Pointer for Shared<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Pointer::fmt(&(unsafe { self.deref() as *const _ }), f)
    }
}

impl<T: ?Sized + Pointable> Default for Shared<'_, T> {
    fn default() -> Self {
        Shared::null()
    }
}
