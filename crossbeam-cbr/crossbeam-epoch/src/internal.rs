//! The global data and participant for garbage collection.
//!
//! # Registration
//!
//! In order to track all participants in one place, we need some form of participant
//! registration. When a participant is created, it is registered to a global lock-free
//! singly-linked list of registries; and when a participant is leaving, it is unregistered from the
//! list.
//!
//! # Pinning
//!
//! Every participant contains an integer that tells whether the participant is pinned and if so,
//! what was the global epoch at the time it was pinned. Participants also hold a pin counter that
//! aids in periodic global epoch advancement.
//!
//! When a participant is pinned, a `Guard` is returned as a witness that the participant is pinned.
//! Guards are necessary for performing atomic operations, and for freeing/dropping locations.
//!
//! # Thread-local bag
//!
//! Objects that get unlinked from concurrent data structures must be stashed away until the global
//! epoch sufficiently advances so that they become safe for destruction. Pointers to such objects
//! are pushed into a thread-local bag, and when it becomes full, the bag is marked with the current
//! global epoch and pushed into the global queue of bags. We store objects in thread-local storages
//! for amortizing the synchronization cost of pushing the garbages to a global queue.
//!
//! # Global queue
//!
//! Whenever a bag is pushed into a queue, the objects in some bags in the queue are collected and
//! destroyed along the way. This design reduces contention on data structures. The global queue
//! cannot be explicitly accessed: the only way to interact with it is by calling functions
//! `defer()` that adds an object tothe thread-local bag, or `collect()` that manually triggers
//! garbage collection.
//!
//! Ideally each instance of concurrent data structure may have its own queue that gets fully
//! destroyed as soon as the data structure gets dropped.

use core::cell::{Cell, UnsafeCell};
use core::cmp;
use core::mem::{self, ManuallyDrop};
use core::ptr;
use core::sync::atomic::{self, Ordering, AtomicUsize};
use core::ops::Deref;

use crossbeam_utils::CachePadded;
use membarrier;

use crate::atomic::{Atomic, Owned, Shared};
use crate::bloom_filter::BloomFilter;
use crate::collector::{Collector, LocalHandle};
use crate::garbage::{Bag, Garbage};
use crate::guard::{unprotected, Guard};
use crate::hazard::{HazardSet, Shield, ShieldError};
use nix::sys::pthread::{Pthread, pthread_self};
use crate::sync::list::{repeat_iter, Entry, IsElement, IterError, List};
use crate::sync::stack::Stack;
use crate::tag::*;

use crate::recovery;

#[allow(missing_docs)]
pub static GLOBAL_GARBAGE_COUNT: AtomicUsize = AtomicUsize::new(0);

/// The width of epoch's representation. In other words, there can be `1 << EPOCH_WIDTH` epochs that
/// are wrapping around.
const EPOCH_WIDTH: u32 = 5;

/// The width of the number of bags.
const BAGS_WIDTH: u32 = 3;

const_assert!(BAGS_WIDTH <= EPOCH_WIDTH);
const_assert!(mem::align_of::<CachePadded<BloomFilter>>() % (1 << (EPOCH_WIDTH + 2)) == 0);

/// Compares two epochs.
fn epoch_cmp(a: usize, b: usize) -> cmp::Ordering {
    let diff = b.wrapping_sub(a) % (1 << EPOCH_WIDTH);
    if diff == 0 {
        cmp::Ordering::Equal
    } else if diff < (1 << (EPOCH_WIDTH - 1)) {
        cmp::Ordering::Less
    } else {
        cmp::Ordering::Greater
    }
}

bitflags! {
    /// Status flags tagged in a pointer to hazard pointer summary.
    struct StatusFlags: usize {
        const EJECTING = 1 << (EPOCH_WIDTH + 1);
        const PINNED   = 1 << EPOCH_WIDTH;
        const EPOCH    = (1 << EPOCH_WIDTH) - 1;
    }
}

impl StatusFlags {
    #[inline(always)]
    pub fn new(is_ejecting: bool, is_pinned: bool, epoch: usize) -> Self {
        debug_assert!(
            StatusFlags::all().bits() <= low_bits::<CachePadded<BloomFilter>>(),
            "StatusFlags should be tagged in a pointer to hazard pointer summary.",
        );

        let is_ejecting = if is_ejecting {
            Self::EJECTING
        } else {
            Self::empty()
        };
        let is_pinned = if is_pinned {
            Self::PINNED
        } else {
            Self::empty()
        };
        let epoch = Self::from_bits_truncate(epoch) & Self::EPOCH;
        is_ejecting | is_pinned | epoch
    }

    #[inline(always)]
    pub fn is_ejecting(self) -> bool {
        self.contains(Self::EJECTING)
    }

    #[inline(always)]
    pub fn is_pinned(self) -> bool {
        self.contains(Self::PINNED)
    }

    #[inline(always)]
    pub fn epoch(self) -> usize {
        (self & Self::EPOCH).bits()
    }
}

/// The global data for a garbage collector.
pub struct Global {
    /// The intrusive linked list of `Local`s.
    locals: List<Local>,

    /// The global pool of bags of deferred functions.
    bags: [CachePadded<Stack<Bag>>; 1 << BAGS_WIDTH],

    /// The global status consisting of (1) the (approximate) summary of hazard pointers, and (2)
    /// the epoch.
    pub(crate) status: Atomic<CachePadded<BloomFilter>>,
}

impl Drop for Global {
    fn drop(&mut self) {
        unsafe {
            let status = self.status.load(Ordering::Relaxed, unprotected());
            if !status.is_null() {
                drop(status.into_owned());
            }
        }
    }
}

impl Global {
    /// Number of bags to destroy.
    const COLLECT_STEPS: usize = 8;

    /// Creates a new global data for garbage collection.
    #[inline]
    pub fn new() -> Self {
        // TODO(@jeehoonkang): it has a very ugly invariant...
        membarrier::heavy();

        Self {
            locals: List::new(),
            bags: [
                CachePadded::new(Stack::new()),
                CachePadded::new(Stack::new()),
                CachePadded::new(Stack::new()),
                CachePadded::new(Stack::new()),
                CachePadded::new(Stack::new()),
                CachePadded::new(Stack::new()),
                CachePadded::new(Stack::new()),
                CachePadded::new(Stack::new()),
            ],
            status: Atomic::null(),
        }
    }

    /// Pushes the bag into the global queue and replaces the bag with a new empty bag.
    pub fn push_bag(&self, bag: &mut Bag, index: usize) {
        GLOBAL_GARBAGE_COUNT.fetch_add(bag.len(), Ordering::AcqRel);
        let bags = unsafe { &*self.bags.get_unchecked(index % (1 << BAGS_WIDTH)) };
        let bag = mem::replace(bag, Bag::new());
        bags.push(bag);
    }

    #[inline]
    #[must_use]
    pub fn collect_inner<'g>(
        &'g self,
        status: Shared<'g, CachePadded<BloomFilter>>,
        guard: &'g Guard,
    ) -> Result<bool, ShieldError> {
        let shield = Shield::new(status, guard)?;
        let summary = unsafe { shield.as_ref() }.map(Deref::deref);

        let flags = StatusFlags::from_bits_truncate(status.tag());
        let index = flags.epoch().wrapping_sub(3);
        let bags = unsafe { &*self.bags.get_unchecked(index % (1 << BAGS_WIDTH)) };
        let steps = if cfg!(feature = "sanitize") {
            usize::max_value()
        } else {
            Self::COLLECT_STEPS
        };

        for _ in 0..steps {
            if let Some(mut bag) = bags.try_pop(guard)? {
                // Disposes the garbages (except for hazard pointers) in the bag popped from the
                // global queue.
                GLOBAL_GARBAGE_COUNT.fetch_sub(bag.len(), Ordering::AcqRel);
                bag.dispose(summary);

                // If the bag is not empty (due to hazard pointers), push it back to the global
                // queue.
                if !bag.is_empty() {
                    self.push_bag(&mut bag, index.wrapping_add(1));
                }
            } else {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Collects several bags from the global queue and executes deferred functions in them.
    ///
    /// Note: This may itself produce garbage and in turn allocate new bags.
    ///
    /// `pin()` rarely calls `collect()`, so we want the compiler to place that call on a cold
    /// path. In other words, we want the compiler to optimize branching for the case when
    /// `collect()` is not called.
    #[cold]
    #[must_use]
    pub fn collect(&self, guard: &Guard) -> Result<(), ShieldError> {
        let global_status = self.status.load(Ordering::Acquire, guard);
        let global_flags = StatusFlags::from_bits_truncate(global_status.tag());
        if self.collect_inner(global_status, guard)? {
            self.advance(global_flags.epoch(), false, guard)?;
        }
        Ok(())
    }

    /// Attempts to advance the global epoch.
    ///
    /// The global epoch can advance only if all currently pinned participants have been pinned in
    /// the current epoch.
    ///
    /// Returns whether the global epoch has advanced.
    ///
    /// `advance()` is annotated `#[cold]` because it is rarely called.
    #[cold]
    #[must_use]
    pub fn advance(
        &self,
        epoch: usize,
        is_forcing: bool,
        guard: &Guard,
    ) -> Result<(), ShieldError> {
        let global_status = self.status.load(Ordering::Relaxed, guard);
        let global_flags = StatusFlags::from_bits_truncate(global_status.tag());

        if epoch != global_flags.epoch() {
            return Ok(());
        }

        // Collects all the old garbage bags.
        while !self.collect_inner(global_status, guard)? {}

        atomic::fence(Ordering::SeqCst);

        let mut new_summary = BloomFilter::new();
        {
            let mut local_summary = Shield::null(guard);
            let mut pred = Shield::null(guard);
            let mut curr = Shield::null(guard);

            // TODO(stjepang): `Local`s are stored in a linked list because linked lists are fairly easy
            // to implement in a lock-free manner. However, traversal can be slow due to cache misses
            // and data dependencies. We should experiment with other data structures as well.
            for local in self.locals.iter(&mut pred, &mut curr, true, guard)? {
                match local {
                    Err(IterError::Stalled) => {
                        // A concurrent thread stalled this iteration. That thread might also try to
                        // advance the epoch, in which case we leave the job to it. Otherwise, the
                        // epoch will not be advanced.
                        return Ok(());
                    }
                    Err(IterError::ShieldError(e)) => {
                        return Err(e);
                    }
                    Ok(local) => {
                        let local = unsafe { &*local };

                        let mut local_status = local.status.load(Ordering::Acquire, guard);
                        loop {
                            let local_flags = StatusFlags::from_bits_truncate(local_status.tag());

                            // If `local` is not pinned, we're okay.
                            if !local_flags.is_pinned() {
                                break;
                            }

                            // Compares the local epoch with the target epoch.
                            match epoch_cmp(local_flags.epoch(), epoch) {
                                // If they're the same, we're okay.
                                cmp::Ordering::Equal => break,
                                // If the local epoch is greater, we cannot advance.
                                cmp::Ordering::Greater => return Ok(()),
                                cmp::Ordering::Less => (),
                            }
                            // Now we know local epoch < target epoch.

                            // If it's not forcing to advance, bail out.
                            if !is_forcing {
                                return Ok(());
                            }

                            // Ejects `local` and retries.
                            local_status = local.eject(local_status, epoch, guard)?;
                        }

                        // Reads the local summary and add it to the new summary.
                        if !local_status.is_null() {
                            local_summary.defend(local_status, guard)?;
                            new_summary.union(unsafe { local_summary.deref() });
                        }
                    }
                }
            }
        }

        // If the global epoch already has advanced, we cannot advance it again.
        let global_status_validation = self.status.load(Ordering::Relaxed, guard);
        if global_status != global_status_validation {
            return Ok(());
        }

        // Protects the old global summary to prevent the ABA problem.
        let _shield = Shield::new(global_status, guard)?;

        // All pinned participants were pinned in the current global epoch, and we have removed all
        // the old garbages. Now let's advance the global epoch. First, calculates the new global
        // status.
        let new_flags = StatusFlags::new(false, false, epoch.wrapping_add(1));
        let new_status = Owned::new(CachePadded::new(new_summary)).with_tag(new_flags.bits());

        // Tries to replace the global status.
        if self
            .status
            .compare_and_set(global_status, new_status, Ordering::Release, guard)
            .is_ok()
        {
            // If successful, destroys the old summary.
            if !global_status.is_null() {
                unsafe {
                    guard.defer_destroy(global_status);
                }
            }
        }

        Ok(())
    }
}

/// Participant for garbage collection.
#[derive(Debug)]
pub struct Local {
    /// A node in the intrusive linked list of `Local`s.
    entry: Entry,

    /// The local status consisting of (1) the (approximate) summary of hazard pointers, and (2)
    /// `StatusFlags`.
    status: CachePadded<Atomic<CachePadded<BloomFilter>>>,

    /// A reference to the global data.
    ///
    /// When all guards and handles get dropped, this reference is destroyed.
    collector: UnsafeCell<ManuallyDrop<Collector>>,

    /// The local bag of deferred functions.
    pub(crate) bag: UnsafeCell<Bag>,

    /// The number of guards keeping this participant pinned.
    guard_count: Cell<usize>,

    /// The number of active handles.
    handle_count: Cell<usize>,

    /// The previous epoch.
    prev_epoch: Cell<usize>,

    /// Total number of bags in this epoch.
    ///
    /// This is just an auxilliary counter that sometimes kicks off collection.
    collect_count: Cell<usize>,
    advance_count: Cell<usize>,

    /// The set of hazard pointers.
    pub(crate) hazards: HazardSet,
    /// The Pthread ID of registered thread.
    pub(crate) pthread: Pthread,
}

const_assert_eq!(Local::COUNTS_BETWEEN_FORCE_ADVANCE % Local::COUNTS_BETWEEN_TRY_ADVANCE, 0);

impl Local {
    /// Number of pinnings after which a participant will execute some deferred functions from the
    /// global queue.
    #[cfg(not(feature = "sanitize"))]
    const COUNTS_BETWEEN_COLLECT: usize = 64;
    #[cfg(feature = "sanitize")]
    const COUNTS_BETWEEN_COLLECT: usize = 2;

    /// Number of pinnings after which a participant will try to advance the global epoch.
    #[cfg(not(feature = "sanitize"))]
    const COUNTS_BETWEEN_TRY_ADVANCE: usize = 128;
    #[cfg(feature = "sanitize")]
    const COUNTS_BETWEEN_TRY_ADVANCE: usize = 4;

    /// Number of pinnings after which a participant will force to advance the global epoch.
    #[cfg(not(feature = "sanitize"))]
    const COUNTS_BETWEEN_FORCE_ADVANCE: usize = 4 * 128;
    #[cfg(feature = "sanitize")]
    const COUNTS_BETWEEN_FORCE_ADVANCE: usize = 8;

    /// Registers a new `Local` in the provided `Global`.
    pub fn register(collector: &Collector) -> LocalHandle {
        unsafe {
            // Install a signal handler to receive a signal after being ejected.
            recovery::install();

            // Since we dereference no pointers in this block, it is safe to use `unprotected`.

            let local = Owned::new(Local {
                entry: Entry::default(),
                status: CachePadded::new(Atomic::null()),
                collector: UnsafeCell::new(ManuallyDrop::new(collector.clone())),
                hazards: HazardSet::new(),
                bag: UnsafeCell::new(Bag::new()),
                guard_count: Cell::new(0),
                handle_count: Cell::new(1),
                prev_epoch: Cell::new(0),
                collect_count: Cell::new(0),
                advance_count: Cell::new(0),
                pthread: pthread_self(),
            })
            .into_shared(unprotected());
            collector.global.locals.insert(local);
            LocalHandle {
                local: local.as_raw(),
            }
        }
    }

    /// Returns a reference to the `Global` in which this `Local` resides.
    #[inline]
    pub fn global(&self) -> &Global {
        &self.collector().global
    }

    /// Returns a reference to the `Collector` in which this `Local` resides.
    #[inline]
    pub fn collector(&self) -> &Collector {
        unsafe { &**self.collector.get() }
    }

    /// Returns `true` if the current participant is pinned.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.guard_count.get() > 0
    }

    /// Returns the current epoch if `self` is not ejected yet.
    #[must_use]
    #[inline]
    pub fn get_epoch(&self, guard: &Guard) -> Result<usize, ShieldError> {
        // Light fence to synchronize with `Self::eject()`.
        membarrier::light_membarrier();

        let local_status = self.status.load(Ordering::Relaxed, guard);
        // let local_flags = StatusFlags::from_bits_truncate(local_status.tag());

        // if local_flags.is_pinned() && !local_flags.is_ejecting() {
        //     Ok(local_flags.epoch())

        // HACK(@jeehoonkang): It is inside a very hot loop, but LLVM cannot optimize the above
        // lines...
        let tag = local_status.tag();
        if tag & StatusFlags::PINNED.bits() != 0 &&
            tag & StatusFlags::EJECTING.bits() == 0
        {
            Ok(tag & StatusFlags::EPOCH.bits())
        } else {
            Err(ShieldError::Ejected)
        }
    }

    fn get_epoch_resilient(&self, guard: &Guard) -> usize {
        self.get_epoch(guard).unwrap_or_else(|_| {
            atomic::fence(Ordering::SeqCst);
            let global_status = self
                .global()
                .status
                .load(Ordering::Acquire, unsafe { unprotected() });
            let global_flags = StatusFlags::from_bits_truncate(global_status.tag());
            global_flags.epoch()
        })
    }

    // TODO: name
    fn incr_counts(&self, is_forcing: bool, guard: &Guard) {
        let collect_count = self.collect_count.get().wrapping_add(1);
        self.collect_count.set(collect_count);

        let advance_count = self.advance_count.get().wrapping_add(1);
        self.advance_count.set(advance_count);

        if advance_count % Self::COUNTS_BETWEEN_TRY_ADVANCE == 0 {
            let local_status = self.status.load(Ordering::Acquire, guard);
            let local_flags = StatusFlags::from_bits_truncate(local_status.tag());
            let is_forcing = advance_count % Self::COUNTS_BETWEEN_FORCE_ADVANCE == 0;
            let _ = self.global().advance(local_flags.epoch(), is_forcing, &guard);
        }
        // After every `COUNTS_BETWEEN_COLLECT` try collecting some old garbage bags.
        else if is_forcing || collect_count % Self::COUNTS_BETWEEN_COLLECT == 0 {
            let _ = self.global().collect(&guard);
        }
    }

    /// Adds `deferred` to the thread-local bag.
    ///
    /// # Safety
    ///
    /// It should be safe for another thread to execute the given function.
    pub unsafe fn defer(&self, mut garbage: Garbage, guard: &Guard, internal: bool) {
        let bag = &mut *self.bag.get();

        while let Err(g) = bag.try_push(garbage) {
            let epoch = self.get_epoch_resilient(guard);
            self.global().push_bag(bag, epoch);
            garbage = g;
        }

        if !internal {
            self.incr_counts(false, guard);
        }
    }

    pub fn flush(&self, guard: &Guard) {
        let epoch = self.get_epoch_resilient(guard);

        let bag = unsafe { &mut *self.bag.get() };
        if !bag.is_empty() {
            self.global().push_bag(bag, epoch);
        }

        self.incr_counts(true, guard);
    }

    /// Pins the `Local`.
    #[inline]
    pub fn pin(&self) -> Guard {
        let guard = Guard { local: self };

        let guard_count = self.guard_count.get();
        self.guard_count.set(guard_count.checked_add(1).unwrap());

        if guard_count == 0 {
            // Loads the current local status. It's safe not to protect the access because no other
            // threads are modifying it.
            let mut local_status = unsafe { self.status.load(Ordering::Relaxed, unprotected()) };

            // Loads the current global status. It's safe not to protect the load because we're not
            // accessing its contents.
            let mut global_status =
                unsafe { self.global().status.load(Ordering::Relaxed, unprotected()) };

            let local_flags = StatusFlags::from_bits_truncate(local_status.tag());
            debug_assert!(
                !local_flags.is_pinned(),
                "[Local::pin()] `self` should be unpinned"
            );

            // Now we must store the new status into `self.status` and execute a `SeqCst` fence.
            // The fence makes sure that any future loads from `Atomic`s will not happen before this
            // store.
            loop {
                let global_flags = StatusFlags::from_bits_truncate(global_status.tag());

                // Stores the local status as pinned at the epoch.
                let new_status = local_status
                    .with_tag(StatusFlags::new(false, true, global_flags.epoch()).bits());

                if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
                    // HACK(stjepang): On x86 architectures there are two different ways of
                    // executing a `SeqCst` fence.
                    //
                    // 1. `atomic::fence(SeqCst)`, which compiles into a `mfence` instruction.
                    // 2. `_.compare_and_swap(_, _, SeqCst)`, which compiles into a `lock cmpxchg`
                    //    instruction.
                    //
                    // Both instructions have the effect of a full barrier, but benchmarks have
                    // shown that the second one makes pinning faster in this particular case.  It
                    // is not clear that this is permitted by the C++ memory model (SC fences work
                    // very differently from SC accesses), but experimental evidence suggests that
                    // this works fine.  Using inline assembly would be a viable (and correct)
                    // alternative, but alas, that is not possible on stable Rust.
                    if let Err(e) = self
                        .status
                        .compare_and_set(local_status, new_status, Ordering::SeqCst, &guard)
                    {
                        local_status = e.current;
                        continue;
                    } else {
                        local_status = new_status;
                    }

                    // We add a compiler fence to make it less likely for LLVM to do something wrong
                    // here.  Formally, this is not enough to get rid of data races; practically, it
                    // should go a long way.
                    atomic::compiler_fence(Ordering::SeqCst);
                } else {
                    if let Err(e) = self
                        .status
                        .compare_and_set(local_status, new_status, Ordering::AcqRel, &guard)
                    {
                        local_status = e.current;
                        continue;
                    } else {
                        local_status = new_status;
                    }

                    atomic::fence(Ordering::SeqCst);
                }

                // Validates that the global status did not change.
                let global_status_validation =
                    unsafe { self.global().status.load(Ordering::Relaxed, unprotected()) };
                if global_status == global_status_validation {
                    break;
                }

                // Retries with a more recent value of global status.
                global_status = global_status_validation;
            }

            // Reset the garbage counter if epoch has advanced.
            let global_flags = StatusFlags::from_bits_truncate(global_status.tag());
            let new_epoch = global_flags.epoch();
            if new_epoch != self.prev_epoch.get() {
                self.prev_epoch.set(new_epoch);
                self.advance_count.set(0);
            }
        }

        guard
    }

    /// Unpins the `Local`.
    #[inline]
    pub fn unpin(&self) {
        let guard_count = self.guard_count.get();
        debug_assert_ne!(guard_count, 0, "[Local::unpin()] guard count should be > 0");

        if guard_count == 1 {
            unsafe {
                // We don't need to be protected because we're not accessing the shared memory.
                let guard = unprotected();

                // Loads the current status.
                let status = self.status.load(Ordering::Acquire, guard);
                let flags = StatusFlags::from_bits_truncate(status.tag());

                // Update status only if `self` is not already unpinned.
                if flags.is_pinned() {
                    // Creates a summary of the set of hazard pointers.
                    let new_status = self.hazards.make_summary(true, guard)
                        // `IterError` is impossible with the `unprotected()` guard.
                        .unwrap()
                        .map(|summary| Owned::new(CachePadded::new(summary)).into_shared(guard))
                        .unwrap_or_else(|| Shared::null())
                        .with_tag(StatusFlags::new(false, false, flags.epoch()).bits());

                    // Replaces `self.status` with the new status.
                    let old_status = self.status.swap(new_status, Ordering::AcqRel, guard);

                    // Defers to destroy the old summary with a "fake" guard, and returns the new
                    // status.
                    if !old_status.is_null() {
                        let guard = ManuallyDrop::new(Guard { local: self });
                        guard.defer_destroy(old_status);
                    }
                }
            }
        }

        self.guard_count.set(guard_count - 1);

        if self.handle_count.get() == 0 {
            self.finalize();
        }
    }

    /// Unpins and then pins the `Local`.
    #[inline]
    pub fn repin(&self) {
        // TODO(@jeehoonkang): optimize it.
        self.unpin();
        mem::forget(self.pin());
    }

    /// Increments the handle count.
    #[inline]
    pub fn acquire_handle(&self) {
        let handle_count = self.handle_count.get();
        self.handle_count.set(handle_count + 1);
    }

    /// Decrements the handle count.
    #[inline]
    pub fn release_handle(&self) {
        let guard_count = self.guard_count.get();
        let handle_count = self.handle_count.get();
        debug_assert!(handle_count >= 1);
        self.handle_count.set(handle_count - 1);

        if guard_count == 0 && handle_count == 1 {
            self.finalize();
        }
    }

    /// Ejects `self` from the current epoch, and returns its new status.
    #[must_use]
    fn eject<'g>(
        &self,
        mut status: Shared<'g, CachePadded<BloomFilter>>,
        target_epoch: usize,
        guard: &'g Guard,
    ) -> Result<Shared<'g, CachePadded<BloomFilter>>, ShieldError> {
        // Marks `self` as ejected.
        loop {
            let flags = StatusFlags::from_bits_truncate(status.tag());
            let epoch = flags.epoch();

            // If `self` is not pinned at an epoch less than the target epoch, we're done.
            if !(flags.is_pinned() && epoch_cmp(epoch, target_epoch) == cmp::Ordering::Less) {
                return Ok(status);
            }

            // If it's marked as being ejected, proceeds to the next stage.
            if flags.is_ejecting() {
                break;
            }

            // Tries to mark the status as being ejected.
            let new_status = status.with_tag((flags | StatusFlags::EJECTING).bits());
            match self
                .status
                .compare_and_set(status, new_status, Ordering::AcqRel, guard)
            {
                // If successful, proceeds to the next stage.
                Ok(_) => {
                    status = new_status;
                    // Send a signal to make the owner thread of `self` restart its read phase.
                    if let Err(err) = unsafe { recovery::send_signal(self.pthread) } {
                        panic!("Failed to restart the ejected thread: {}", err);
                    }
                    break;
                }
                // If not, retries.
                Err(e) => status = e.current,
            }
        }

        // Heavy fence to synchronize with `Self::get_epoch()`.
        unsafe { membarrier::heavy_membarrier(); }

        // Protects the current status to prevent the ABA problem.
        let _shield = Shield::new(status, guard)?;

        // Now `self` is pinned at an epoch less than `target_epoch`, and it's marked as being
        // ejected. Finishes ejecting `self`.
        let flags = StatusFlags::from_bits_truncate(status.tag());
        debug_assert!(
            flags.is_pinned(),
            "[Local::help_eject()] `self` should be pinned"
        );
        debug_assert!(
            flags.is_ejecting(),
            "[Local::help_eject()] `self` should be ejecting"
        );

        // Creates a summary of the set of hazard pointers.
        let new_status = repeat_iter(|| self.hazards.make_summary(false, &guard))?
            .map(|summary| Owned::new(CachePadded::new(summary)).into_shared(&guard))
            .unwrap_or_else(|| Shared::null())
            .with_tag(StatusFlags::new(true, false, flags.epoch()).bits());

        // Replaces the old status with the new one.
        let return_status = match self
            .status
            .compare_and_set(status, new_status, Ordering::AcqRel, guard)
        {
            Ok(_) => unsafe {
                if !status.is_null() {
                    guard.defer_destroy(status);
                }
                new_status
            },
            Err(e) => unsafe {
                if !e.new.is_null() {
                    drop(e.new.into_owned());
                }
                e.current
            },
        };

        Ok(return_status)
    }

    /// Removes the `Local` from the global linked list.
    #[cold]
    fn finalize(&self) {
        debug_assert_eq!(self.guard_count.get(), 0);
        debug_assert_eq!(self.handle_count.get(), 0);

        // Temporarily increment handle count. This is required so that the following call to `pin`
        // doesn't call `finalize` again.
        self.handle_count.set(1);
        {
            let guard = ManuallyDrop::new(Guard { local: self });

            // Flushes the local garbages.
            self.flush(&guard);

            // Defers to destroy the local summary.
            let local_status = self.status.load(Ordering::Relaxed, &guard);
            let local_flags = StatusFlags::from_bits_truncate(local_status.tag());
            debug_assert!(
                !local_flags.is_pinned(),
                "[Local::finalize()] `self` should be unpinned"
            );
            if !local_status.is_null() {
                unsafe {
                    guard.defer_destroy(local_status);
                }
            }
        }
        // Revert the handle count back to zero.
        self.handle_count.set(0);

        unsafe {
            // Take the reference to the `Global` out of this `Local`. Since we're not protected
            // by a guard at this time, it's crucial that the reference is read before marking the
            // `Local` as deleted.
            let collector: Collector = ptr::read(&*(*self.collector.get()));

            // Mark this node in the linked list as deleted.
            self.entry.delete();

            // Finally, drop the reference to the global. Note that this might be the last reference
            // to the `Global`. If so, the global data will be destroyed and all deferred functions
            // in its queue will be executed.
            drop(collector);
        }
    }
}

impl IsElement<Local> for Local {
    fn entry_of(local: &Local) -> &Entry {
        let entry_ptr = (local as *const Local as usize + offset_of!(Local, entry)) as *const Entry;
        unsafe { &*entry_ptr }
    }

    unsafe fn element_of(entry: &Entry) -> &Local {
        // offset_of! macro uses unsafe, but it's unnecessary in this context.
        #[allow(unused_unsafe)]
        let local_ptr = (entry as *const Entry as usize - offset_of!(Local, entry)) as *const Local;
        &*local_ptr
    }

    unsafe fn finalize(entry: &Entry, guard: &Guard) {
        guard.defer_destroy(Shared::from(Self::element_of(entry) as *const _));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::deferred::Deferred;

    #[test]
    fn check_defer() {
        static FLAG: AtomicUsize = AtomicUsize::new(0);
        fn set() {
            FLAG.store(42, Ordering::Relaxed);
        }

        let d = Deferred::new(set);
        assert_eq!(FLAG.load(Ordering::Relaxed), 0);
        d.call();
        assert_eq!(FLAG.load(Ordering::Relaxed), 42);
    }
}
