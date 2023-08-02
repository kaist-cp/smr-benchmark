// To use  `#[cfg(sanitize = "address")]`
#![feature(cfg_sanitize)]

mod cdrc;
mod hpsharp;
mod rrcu;
pub use cdrc::*;
pub use hpsharp::*;

use std::{cell::RefCell, mem::ManuallyDrop};

pub static GLOBAL: Global = Global::new();

thread_local! {
    pub static THREAD: RefCell<Box<Thread>> = RefCell::new(Box::new(GLOBAL.register()));
}

use arrayvec::{ArrayVec, IntoIter};
use crossbeam_utils::CachePadded;
use nix::sys::pthread::{pthread_self, Pthread};
use setjmp::sigjmp_buf;
use static_assertions::const_assert;
use std::{
    cell::{Cell, UnsafeCell},
    marker::PhantomData,
    mem::{forget, take, zeroed},
    ptr::{null, null_mut, NonNull},
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering},
};

/// Maximum number of objects a bag can contain.
#[cfg(not(sanitize = "address"))]
const MAX_OBJECTS: usize = 128;
#[cfg(sanitize = "address")]
const MAX_OBJECTS: usize = 4;

/// A deferred task consisted of data and a callable function.
///
/// Note that a [`Deferred`] must be finalized by `execute` function, and `drop`ping this object
/// will trigger a panic!
///
/// Also, [`Deferred`] is `Send` because it may be executed by an arbitrary thread.
#[derive(Debug)]
pub struct Deferred {
    data: *mut u8,
    task: unsafe fn(*mut u8),
}

impl Deferred {
    #[inline]
    #[must_use]
    fn new(data: *mut u8, task: unsafe fn(*mut u8)) -> Self {
        Self { data, task }
    }

    /// Executes and finalizes this deferred task.
    #[inline]
    unsafe fn execute(self) {
        (self.task)(self.data);
        // Prevent calling the `drop` for this object.
        forget(self);
    }

    /// Returns a copy of inner `data`.
    #[inline]
    fn data(&self) -> *mut u8 {
        self.data
    }
}

impl Drop for Deferred {
    fn drop(&mut self) {
        // Note that a `Deferred` must be finalized by `execute` function.
        // In other words, we must make sure that all deferred tasks are executed consequently!
        panic!("`Deferred` task must be finalized by `execute`!");
    }
}

/// [`Deferred`] can be collected by arbitrary threads.
unsafe impl Send for Deferred {}

/// A bag of deferred functions.
#[derive(Default)]
struct Bag {
    /// Stashed garbages.
    defs: ArrayVec<Deferred, MAX_OBJECTS>,
}

/// `Bag::try_push()` requires that it is safe for another thread to execute the given functions.
unsafe impl Send for Bag {}

impl Bag {
    /// Returns a new, empty bag.
    #[inline]
    fn new() -> Self {
        Self::default()
    }

    /// Attempts to insert a deferred function into the bag.
    ///
    /// Returns `Ok(())` if successful, and `Err(deferred)` for the given `deferred` if the bag is
    /// full.
    #[inline]
    fn try_push(&mut self, def: Deferred) -> Result<(), Deferred> {
        self.defs.try_push(def).map_err(|e| e.element())
    }

    /// Creates an iterator of [`Deferred`] from a [`Bag`].
    #[must_use]
    #[inline]
    fn into_iter(self) -> IntoIter<Deferred, MAX_OBJECTS> {
        self.defs.into_iter()
    }

    #[inline]
    fn len(&self) -> usize {
        self.defs.len()
    }
}

/// An epoch that can be marked as pinned or unpinned.
///
/// Internally, the epoch is represented as an integer that wraps around at some unspecified point
/// and a flag that represents whether it is pinned or unpinned.
#[derive(Copy, Clone, Default, Debug, Eq, PartialEq)]
pub struct Epoch {
    /// The least significant bit is set if pinned. The rest of the bits hold the epoch.
    data: usize,
}

impl Epoch {
    /// Returns the starting epoch in unpinned state.
    #[inline]
    const fn starting() -> Self {
        Epoch { data: 0 }
    }

    /// Returns the number of epoch as `isize`.
    #[inline]
    fn value(&self) -> isize {
        (self.data & !1) as isize >> 1
    }

    /// Returns `true` if the epoch is marked as pinned.
    #[inline]
    fn is_pinned(self) -> bool {
        (self.data & 1) == 1
    }

    /// Returns the same epoch, but marked as pinned.
    #[inline]
    fn pinned(self) -> Epoch {
        Epoch {
            data: self.data | 1,
        }
    }

    /// Returns the same epoch, but marked as unpinned.
    #[inline]
    fn unpinned(self) -> Epoch {
        Epoch {
            data: self.data & !1,
        }
    }

    /// Returns the successor epoch.
    ///
    /// The returned epoch will be marked as pinned only if the previous one was as well.
    #[inline]
    fn successor(self) -> Epoch {
        Epoch {
            data: self.data.wrapping_add(2),
        }
    }
}

/// An atomic value that holds an `Epoch`.
#[derive(Default, Debug)]
#[repr(transparent)]
struct AtomicEpoch {
    /// Since `Epoch` is just a wrapper around `usize`, an `AtomicEpoch` is similarly represented
    /// using an `AtomicUsize`.
    data: AtomicUsize,
}

impl AtomicEpoch {
    /// Creates a new atomic epoch.
    #[inline]
    const fn new(epoch: Epoch) -> Self {
        let data = AtomicUsize::new(epoch.data);
        AtomicEpoch { data }
    }

    /// Loads a value from the atomic epoch.
    #[inline]
    fn load(&self, ord: Ordering) -> Epoch {
        Epoch {
            data: self.data.load(ord),
        }
    }

    /// Stores a value into the atomic epoch.
    #[inline]
    fn store(&self, epoch: Epoch, ord: Ordering) {
        self.data.store(epoch.data, ord);
    }

    /// Stores a value into the atomic epoch if the current value is the same as `current`.
    ///
    /// The return value is a result indicating whether the new value was written and containing
    /// the previous value. On success this value is guaranteed to be equal to `current`.
    ///
    /// This method takes two `Ordering` arguments to describe the memory
    /// ordering of this operation. `success` describes the required ordering for the
    /// read-modify-write operation that takes place if the comparison with `current` succeeds.
    /// `failure` describes the required ordering for the load operation that takes place when
    /// the comparison fails. Using `Acquire` as success ordering makes the store part
    /// of this operation `Relaxed`, and using `Release` makes the successful load
    /// `Relaxed`. The failure ordering can only be `SeqCst`, `Acquire` or `Relaxed`
    /// and must be equivalent to or weaker than the success ordering.
    #[inline]
    fn compare_exchange(
        &self,
        current: Epoch,
        new: Epoch,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Epoch, Epoch> {
        match self
            .data
            .compare_exchange(current.data, new.data, success, failure)
        {
            Ok(data) => Ok(Epoch { data }),
            Err(data) => Err(Epoch { data }),
        }
    }
}

/// The width of the number of bags.
const BAGS_WIDTH: u32 = 3;

/// A pair of an epoch and a bag.
struct SealedBag {
    epoch: Epoch,
    inner: Bag,
}

/// It is safe to share `SealedBag` because `is_expired` only inspects the epoch.
unsafe impl Sync for SealedBag {}

impl SealedBag {
    /// Checks if it is safe to drop the bag w.r.t. the given global epoch.
    #[inline]
    fn is_expired(&self, global_epoch: Epoch) -> bool {
        global_epoch.value() - self.epoch.value() >= 2
    }

    #[inline]
    fn into_inner(self) -> Bag {
        self.inner
    }
}

/// The global data for a garbage collector.
pub struct Global {
    /// The intrusive linked list of `Local`s.
    locals: LocalList,

    /// The global pool of bags of deferred functions.
    epoch_bags: [CachePadded<Pile<SealedBag>>; 1 << BAGS_WIDTH],

    /// The global epoch.
    epoch: CachePadded<AtomicEpoch>,

    /// Collected from RRCU, but not executed because of hazard pointers.
    hp_bags: CachePadded<Pile<Deferred>>,

    garbage_count: AtomicUsize,
}

impl Global {
    /// Creates a new global data for garbage collection.
    #[inline]
    pub const fn new() -> Self {
        Self {
            locals: LocalList::new(),
            epoch_bags: [
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
                CachePadded::new(Pile::new()),
            ],
            epoch: CachePadded::new(AtomicEpoch::new(Epoch::starting())),
            hp_bags: CachePadded::new(Pile::new()),
            garbage_count: AtomicUsize::new(0),
        }
    }

    /// Registers a current thread as a participant associated with this [`Global`] epoch
    /// manager.
    #[inline]
    #[must_use]
    fn acquire<'s>(&'s self) -> &'s mut Local {
        let tid = pthread_self();
        self.locals.acquire(tid, self)
    }

    #[inline]
    pub fn garbage_count(&self) -> usize {
        self.garbage_count.load(Ordering::Acquire)
    }
}

impl Drop for Global {
    fn drop(&mut self) {
        self.locals
            .iter()
            .flat_map(|local| take(unsafe { &mut *local.bag.get() }).into_iter())
            .chain(self.epoch_bags.iter_mut().flat_map(|pile| {
                pile.pop_all()
                    .into_iter()
                    .flat_map(|bag| bag.inner.into_iter())
            }))
            .for_each(|def| unsafe { def.execute() });
        let mut deferred = self.hp_bags.pop_all();
        for r in deferred.drain(..) {
            unsafe { r.execute() };
        }
    }
}

/// A thread-local handle managing local epoch and defering.
pub struct Thread {
    local: *mut Local,
}

impl Thread {
    /// Creates an unprotected `Thread`.
    pub unsafe fn unprotected() -> Self {
        Self { local: null_mut() }
    }

    pub(crate) unsafe fn internal() -> ManuallyDrop<Self> {
        ManuallyDrop::new(Self {
            local: LOCAL.try_with(|local| *local.get()).unwrap_or(null_mut()),
        })
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.epoch.store(Epoch::starting(), Ordering::Release);
            local.using.store(false, Ordering::Release);
            clear_data();
        }
    }
}

/// A crashable critical section guard.
///
/// Note that defering is not allowed in this critical section because a crash may occur while
/// writing data on non-atomic storage. To conduct jobs with side-effects, we must open a
/// non-crashable section by `mask` method.
pub struct CsGuard {
    local: *mut Local,
    rb: Rollbacker,
    backup_idx: Option<NonNull<AtomicUsize>>,
}

impl CsGuard {
    fn new(local: &mut Local, rb: Rollbacker) -> Self {
        Self {
            local,
            rb,
            backup_idx: None,
        }
    }

    fn set_backup(&mut self, backup_idx: &AtomicUsize) {
        unsafe {
            self.backup_idx = Some(NonNull::new_unchecked(
                (backup_idx as *const AtomicUsize).cast_mut(),
            ))
        };
    }

    /// Creates an unprotected `CsGuard`.
    pub unsafe fn unprotected() -> Self {
        Self {
            local: null_mut(),
            rb: Rollbacker {
                status: null(),
                chkpt: null_mut(),
            },
            backup_idx: None,
        }
    }
}

/// A non-crashable write section guard.
///
/// Unlike a [`CsGuard`], it may perform jobs with side-effects such as retiring, or physical
/// deletion for a data structure.
pub struct RaGuard {
    local: *mut Local,
    rb: *const Rollbacker,
}

impl RaGuard {
    /// Creates an unprotected `RaGuard`.
    pub unsafe fn unprotected() -> Self {
        Self {
            local: null_mut(),
            rb: null(),
        }
    }
}

impl Clone for Rollbacker {
    fn clone(&self) -> Self {
        Self {
            status: self.status,
            chkpt: self.chkpt,
        }
    }
}

const_assert!(atomic::Atomic::<Pthread>::is_lock_free());

pub struct Local {
    /// Represents a thread-local status.
    ///
    /// A thread entering a crashable section sets its `STATUS` by calling `guard!` macro.
    ///
    /// Note that `STATUS` must be a type which can be read and written in tear-free manner.
    /// For this reason, using non-atomic type such as `u8` is not safe, as any writes on this
    /// variable may be splitted into multiple instructions and the thread may read inconsistent
    /// value in its signal handler.
    ///
    /// According to ISO/IEC TS 17961 C Secure Coding Rules, accessing values of objects that are
    /// neither lock-free atomic objects nor of type `volatile sig_atomic_t` in a signal handler
    /// results in undefined behavior.
    ///
    /// `AtomicU8` is likely to be safe, because any accesses to it will be compiled into a
    /// single ISA instruction. On top of that, by preventing reordering instructions across this
    /// variable by issuing `compiler_fence`, we can have the same restrictions which
    /// `volatile sig_atomic_t` has.
    status: CachePadded<AtomicU8>,
    chkpt: sigjmp_buf,
    epoch: CachePadded<AtomicEpoch>,

    owner: atomic::Atomic<Pthread>,
    using: AtomicBool,
    next: AtomicPtr<Local>,

    bag: UnsafeCell<Bag>,
    push_count: Cell<usize>,
    global: *const Global,

    hazptrs: AtomicPtr<HazardArray>,
    /// Available slots of the hazard array.
    available_indices: Vec<usize>,
    local_deferred: Vec<Deferred>,
}

impl Local {
    #[must_use]
    fn new(using: bool, global: &Global) -> Self {
        const HAZARD_ARRAY_INIT_SIZE: usize = 16;
        let array = Vec::from(unsafe { zeroed::<[AtomicPtr<u8>; HAZARD_ARRAY_INIT_SIZE]>() });

        Self {
            epoch: CachePadded::new(AtomicEpoch::new(Epoch::starting())),
            owner: unsafe { core::mem::zeroed() },
            bag: UnsafeCell::new(Bag::new()),
            status: CachePadded::new(AtomicU8::new(0)),
            chkpt: unsafe { core::mem::zeroed() },
            next: AtomicPtr::new(null_mut()),
            using: AtomicBool::new(using),
            global,
            push_count: Cell::new(0),
            hazptrs: AtomicPtr::new(Box::into_raw(Box::new(array))),
            available_indices: (0..HAZARD_ARRAY_INIT_SIZE).collect(),
            local_deferred: vec![],
        }
    }
}

impl Drop for Local {
    fn drop(&mut self) {
        drop(unsafe { Box::from_raw(self.hazptrs.load(Ordering::Acquire)) });
    }
}

type HazardArray = Vec<AtomicPtr<u8>>;

/// A grow-only linked list for [`Local`] registration.
struct LocalList {
    head: AtomicPtr<Local>,
}

impl LocalList {
    const fn new() -> Self {
        Self {
            head: AtomicPtr::new(null_mut()),
        }
    }

    /// Acquire an empty slot for a new participant.
    ///
    /// If there is an available slot, it returns a reference to that slot.
    /// Otherwise, it tries to append a new slot at the end of the list,
    /// and if it succeeds, returns the allocated slot.
    #[inline]
    fn acquire<'c>(&'c self, tid: Pthread, global: &Global) -> &'c mut Local {
        let mut prev_link = &self.head;
        let local = loop {
            match unsafe { prev_link.load(Ordering::Acquire).as_mut() } {
                Some(curr) => {
                    if !curr.using.load(Ordering::Acquire)
                        && curr
                            .using
                            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
                            .is_ok()
                    {
                        break curr;
                    }
                    prev_link = &curr.next;
                }
                None => {
                    let new_local = Box::into_raw(Box::new(Local::new(true, global)));
                    if prev_link
                        .compare_exchange(
                            null_mut(),
                            new_local,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break unsafe { &mut *new_local };
                    } else {
                        unsafe { drop(Box::from_raw(new_local)) };
                    }
                }
            }
        };
        local.owner.store(tid, Ordering::Release);
        local
    }

    /// Returns an iterator over all using `Local`s.
    fn iter_using<'g>(&'g self) -> impl Iterator<Item = &'g Local> {
        LocalIter {
            curr: self.head.load(Ordering::Acquire),
            predicate: |local| local.using.load(Ordering::Acquire),
            _marker: PhantomData,
        }
    }

    /// Returns an iterator over all `Local`s.
    fn iter<'g>(&'g self) -> impl Iterator<Item = &'g Local> {
        LocalIter {
            curr: self.head.load(Ordering::Acquire),
            predicate: |_| true,
            _marker: PhantomData,
        }
    }
}

impl Drop for LocalList {
    fn drop(&mut self) {
        let mut curr = self.head.load(Ordering::Acquire);
        while let Some(curr_ref) = unsafe { curr.as_ref() } {
            let next = curr_ref.next.load(Ordering::Acquire);
            let mut curr_local = unsafe { Box::from_raw(curr) };
            for def in curr_local.local_deferred.drain(..) {
                unsafe { def.execute() };
            }
            curr = next;
        }
    }
}

struct LocalIter<'g, F>
where
    F: Fn(&Local) -> bool,
{
    curr: *const Local,
    predicate: F,
    _marker: PhantomData<&'g ()>,
}

impl<'g, F> Iterator for LocalIter<'g, F>
where
    F: Fn(&Local) -> bool,
{
    type Item = &'g Local;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(curr_ref) = unsafe { self.curr.as_ref() } {
            self.curr = curr_ref.next.load(Ordering::Acquire);
            if (self.predicate)(curr_ref) {
                return Some(curr_ref);
            }
        }
        None
    }
}

pub(crate) struct Rollbacker {
    status: *const AtomicU8,
    chkpt: *mut sigjmp_buf,
}

thread_local! {
    static LOCAL: UnsafeCell<*mut Local> = UnsafeCell::new(null_mut());
}

fn set_data(local: &mut Local) {
    LOCAL.with(|l| unsafe { *l.get() = local });
}

fn clear_data() {
    LOCAL.with(|l| unsafe { *l.get() = null_mut() });
}

/// A lock-free pile, which we can push an element or pop all elements.
#[derive(Debug)]
#[repr(transparent)]
struct Pile<T> {
    head: AtomicPtr<Node<T>>,
}

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: AtomicPtr<Node<T>>,
}

impl<T> Pile<T> {
    /// Creates a new, empty pile.
    const fn new() -> Pile<T> {
        Pile {
            head: AtomicPtr::new(null_mut()),
        }
    }

    /// Pushes a value on top of the pile.
    fn push(&self, t: T) {
        let n = Box::into_raw(Box::new(Node {
            data: t,
            next: AtomicPtr::new(null_mut()),
        }));

        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            unsafe { &*n }.next.store(head, Ordering::Relaxed);

            match self
                .head
                .compare_exchange(head, n, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(new_head) => head = new_head,
            }
        }
    }

    fn append(&self, mut iter: impl Iterator<Item = T>) {
        let Some(first_value) = iter.next() else { return; };
        let first_node = Box::into_raw(Box::new(Node {
            data: first_value,
            next: AtomicPtr::new(null_mut()),
        }));
        let mut last_node = first_node;

        while let Some(value) = iter.next() {
            let node = Box::into_raw(Box::new(Node {
                data: value,
                next: AtomicPtr::new(null_mut()),
            }));
            unsafe { &*last_node }.next.store(node, Ordering::Relaxed);
            last_node = node;
        }

        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            unsafe { &*last_node }.next.store(head, Ordering::Relaxed);

            match self
                .head
                .compare_exchange(head, first_node, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(new_head) => head = new_head,
            }
        }
    }

    /// Pops and returns all elements from the pile.
    #[must_use]
    fn pop_all(&self) -> Vec<T> {
        let mut result = vec![];
        let mut node = self.head.swap(null_mut(), Ordering::AcqRel);
        while !node.is_null() {
            let node_owned = unsafe { Box::from_raw(node) };
            let data = node_owned.data;
            result.push(data);
            node = node_owned.next.load(Ordering::Acquire);
        }
        result
    }
}

impl<T> Default for Pile<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Pile<T> {
    fn drop(&mut self) {
        drop(self.pop_all());
    }
}

#[test]
fn seq_append_pop() {
    let pile = Pile::new();
    pile.push(1);
    pile.push(2);
    pile.append(vec![6, 5, 4, 3].into_iter());
    pile.push(7);
    assert_eq!(pile.pop_all(), vec![7, 6, 5, 4, 3, 2, 1]);
}

#[test]
fn con_push_pop() {
    use std::thread::scope;
    const THREADS: usize = 32;
    const PUSH_COUNT: usize = 1000;

    let pile = Pile::new();

    scope(|s| {
        for i in 0..THREADS {
            let pile = &pile;
            s.spawn(move || {
                for j in 0..PUSH_COUNT {
                    pile.push(i * PUSH_COUNT + j);
                }
            });
        }

        let mut appeared = [false; THREADS * PUSH_COUNT];
        while appeared.iter().any(|v| !*v) {
            for v in pile.pop_all() {
                assert!(!appeared[v]);
                appeared[v] = true;
            }
        }
    });
}
