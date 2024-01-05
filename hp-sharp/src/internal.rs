use crossbeam_utils::CachePadded;
use nix::errno::Errno;
use nix::sys::pthread::{pthread_self, Pthread};
use static_assertions::const_assert;
use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::{take, zeroed};
use std::ptr::null_mut;
use std::sync::atomic::{compiler_fence, fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};

use crate::deferred::{Bag, Deferred, SealedBag};
use crate::epoch::{AtomicEpoch, Epoch};
use crate::handle::{CsGuard, Thread};
use crate::hazard::HazardPointer;
use crate::queue::DoubleLink;
use crate::rollback;

pub(crate) static mut USE_ROLLBACK: bool = true;

/// Turn on or off the functionality to rollback. By default, HP# will use rollbacks.
///
/// # Safety
///
/// Set this configuration before actually running an application with HP#.
pub unsafe fn set_rollback(enable: bool) {
    unsafe { USE_ROLLBACK = enable };
}

/// The global data for a garbage collector.
pub struct Global {
    /// The intrusive linked list of `Local`s.
    locals: LocalList,

    /// The global pool of bags of deferred functions.
    epoch_bags: DoubleLink,

    /// The global epoch.
    epoch: CachePadded<AtomicEpoch>,

    garbage_count: AtomicUsize,
}

impl Global {
    /// Creates a new global data for garbage collection.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            locals: LocalList::new(),
            epoch_bags: DoubleLink::new(),
            epoch: CachePadded::new(AtomicEpoch::new(Epoch::starting())),
            garbage_count: AtomicUsize::new(0),
        }
    }

    /// Registers a current thread as a participant associated with this [`Global`] epoch
    /// manager.
    #[inline]
    #[must_use]
    fn acquire(&self) -> &mut Local {
        self.locals.acquire(self)
    }

    #[inline]
    pub fn garbage_count(&self) -> usize {
        self.garbage_count.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn push_bag(&self, bag: &mut Bag) {
        self.garbage_count.fetch_add(bag.len(), Ordering::SeqCst);
        let bag = take(bag);

        fence(Ordering::SeqCst);

        let epoch = self.epoch.load(Ordering::Relaxed);
        self.epoch_bags.push(SealedBag::new(epoch, bag));
    }

    #[must_use]
    pub(crate) fn collect(&self, global_epoch: Epoch) -> Vec<Bag> {
        let mut collected = Vec::new();
        for _ in 0..8 {
            if let Some(bag) = self.epoch_bags.pop_if(|bag| bag.is_expired(global_epoch)) {
                collected.push(bag.into_inner());
            } else {
                break;
            }
        }
        collected
    }

    /// Registers a current thread as a participant associated with this [`GlobalRRCU`] epoch
    /// manager.
    #[inline]
    pub(crate) fn register(&self) -> Thread {
        // Install a signal handler to handle manual crash triggered by a reclaimer.
        unsafe { rollback::install() };
        Thread::from_raw(self.acquire())
    }

    /// Attempts to advance the global epoch.
    ///
    /// The global epoch can advance if all currently pinned participants have been pinned in
    /// the current epoch.
    ///
    /// Returns the current global epoch.
    ///
    /// `try_advance()` is annotated `#[cold]` because it is rarely called.
    #[cold]
    fn try_advance(&self) -> Result<Epoch, Epoch> {
        let global_epoch = self.epoch.load(Ordering::Relaxed);
        fence(Ordering::SeqCst);

        for local in self.locals.iter_using() {
            let local_epoch = local.epoch.load(Ordering::Relaxed);

            // Someone has advanced the global epoch already.
            if local_epoch.value() > global_epoch.value() {
                return Ok(global_epoch.successor());
            }

            // If the participant was pinned in a different epoch, we cannot advance the
            // global epoch just yet.
            if local_epoch.is_pinned() && local_epoch.unpinned().value() < global_epoch.value() {
                return Err(global_epoch);
            }
        }
        fence(Ordering::Acquire);

        // All pinned participants were pinned in the current global epoch.
        // Now let's advance the global epoch...
        //
        // Note that advancing here may fail if other thread already have advanced the epoch.
        let new_epoch = global_epoch.successor();
        let _ = self.epoch.compare_exchange(
            global_epoch,
            new_epoch,
            Ordering::Release,
            Ordering::Relaxed,
        );
        Ok(new_epoch)
    }

    /// Force advancing the global epoch.
    ///
    /// if currently there are pinned participants have been pinned in the other epoch than
    /// the current global epoch, sends signals to restart the slow participants.
    ///
    /// Returns the advanced global epoch.
    ///
    /// `advance()` is annotated `#[cold]` because it is rarely called.
    #[cold]
    fn advance(&self, advancer: &mut Local) -> Epoch {
        debug_assert!(unsafe { USE_ROLLBACK });
        let advancer_owner = advancer.owner.load(Ordering::Relaxed);
        let global_epoch = self.epoch.load(Ordering::Relaxed);
        fence(Ordering::SeqCst);

        for local in self.locals.iter_using() {
            let local_epoch = local.epoch.load(Ordering::Relaxed);

            // Someone has advanced the global epoch already.
            if local_epoch.value() > global_epoch.value() {
                return global_epoch.successor();
            }

            // If the participant was pinned in a different epoch, we eject its epoch.
            if local_epoch.is_pinned() && local_epoch.unpinned().value() < global_epoch.value() {
                let owner = local.owner.load(Ordering::Relaxed);
                if advancer_owner == owner {
                    advancer.unpin_inner();
                }
                match unsafe { rollback::send_signal(owner) } {
                    // `ESRCH` indicates that the given pthread is already exited.
                    Ok(_) | Err(Errno::ESRCH) => {}
                    Err(err) => panic!("Failed to restart the thread: {}", err),
                }
            }
        }
        fence(Ordering::Acquire);

        // All pinned participants were pinned in the current global epoch.
        // Now let's advance the global epoch...
        //
        // Note that advancing here may fail if other thread already have advanced the epoch.
        let new_epoch = global_epoch.successor();
        let _ = self.epoch.compare_exchange(
            global_epoch,
            new_epoch,
            Ordering::Release,
            Ordering::Relaxed,
        );
        new_epoch
    }

    #[inline]
    fn iter_guarded_ptrs<'s>(
        &'s self,
        reader: &'s mut Local,
    ) -> impl Iterator<Item = *mut u8> + 's {
        self.locals
            .iter_using()
            .flat_map(|local| HazardArrayIter::new(local, reader))
    }
}

const_assert!(atomic::Atomic::<Pthread>::is_lock_free());

type HazardArray = Vec<AtomicPtr<u8>>;

pub struct Local {
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
    hp_bag: Vec<Deferred>,
}

impl Local {
    const COUNTS_BETWEEN_FORCE_ADVANCE: usize = 2;
    const HAZARD_ARRAY_INIT_SIZE: usize = 16;

    #[must_use]
    fn new(using: bool, global: &Global) -> Self {
        let array = Vec::from(unsafe { zeroed::<[AtomicPtr<u8>; Self::HAZARD_ARRAY_INIT_SIZE]>() });

        Self {
            epoch: CachePadded::new(AtomicEpoch::new(Epoch::starting())),
            owner: unsafe { core::mem::zeroed() },
            bag: UnsafeCell::new(Bag::new()),
            next: AtomicPtr::new(null_mut()),
            using: AtomicBool::new(using),
            global,
            push_count: Cell::new(0),
            hazptrs: AtomicPtr::new(Box::into_raw(Box::new(array))),
            available_indices: (0..Self::HAZARD_ARRAY_INIT_SIZE).collect(),
            hp_bag: vec![],
        }
    }

    #[inline]
    fn global(&self) -> &Global {
        unsafe { &*self.global }
    }

    /// Unpins and then pins the [`Local`].
    #[inline]
    fn repin(&mut self) {
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            // HACK: On x86 architectures we optimize out `fence(SeqCst)` with `compare_exchange`.
            // In this case, manually unpinning my epoch must be proceeded.
            // Check out comments on `pin_inner` for more information.
            self.unpin_inner();
        }
        self.pin_inner();
    }

    /// Pins the [`Local`].
    #[inline]
    fn pin_inner(&mut self) {
        let global_epoch = self.global().epoch.load(Ordering::Relaxed);
        let new_epoch = global_epoch.pinned();

        // Now we must store `new_epoch` into `self.epoch` and execute a `SeqCst` fence.
        // The fence makes sure that any future loads from `Atomic`s will not happen before
        // this store.
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            // HACK(stjepang): On x86 architectures there are two different ways of executing
            // a `SeqCst` fence.
            //
            // 1. `atomic::fence(SeqCst)`, which compiles into a `mfence` instruction.
            // 2. `_.compare_exchange(_, _, SeqCst, SeqCst)`, which compiles into a `lock cmpxchg`
            //    instruction.
            //
            // Both instructions have the effect of a full barrier, but benchmarks have shown
            // that the second one makes pinning faster in this particular case.  It is not
            // clear that this is permitted by the C++ memory model (SC fences work very
            // differently from SC accesses), but experimental evidence suggests that this
            // works fine.  Using inline assembly would be a viable (and correct) alternative,
            // but alas, that is not possible on stable Rust.
            let current = Epoch::starting();
            let res =
                self.epoch
                    .compare_exchange(current, new_epoch, Ordering::SeqCst, Ordering::SeqCst);
            debug_assert!(res.is_ok(), "participant was expected to be unpinned");
            // We add a compiler fence to make it less likely for LLVM to do something wrong
            // here.  Formally, this is not enough to get rid of data races; practically,
            // it should go a long way.
            compiler_fence(Ordering::SeqCst);
        } else {
            self.epoch.store(new_epoch, Ordering::Relaxed);
            fence(Ordering::SeqCst);
        }
    }

    /// Unpins the [`Local`].
    #[inline]
    pub(crate) fn unpin_inner(&mut self) {
        self.epoch.store(Epoch::starting(), Ordering::Release);
    }

    /// `#[inline(never)]` is used to reduce the chances for misoptimizations.
    ///
    /// In Rust, there is no concept of functions that return multiple times, like `setjmp`, so
    /// it's easy to generate incorrect code around such a function in Rust.
    ///
    /// Rust uses LLVM during compilation, which needs to be made aware of functions that return
    /// multiple times by using the `returns_twice` attribute; but Rust has no way to propagate
    /// that attribute to LLVM.
    ///
    /// Reference:
    /// * https://github.com/rust-lang/rfcs/issues/2625#issuecomment-460849462
    /// * https://github.com/jeff-davis/setjmp.rs#problems
    #[cfg(not(sanitize = "address"))]
    #[inline(never)]
    pub(crate) unsafe fn pin<F, R>(&mut self, mut body: F) -> R
    where
        F: FnMut(&CsGuard) -> R,
        R: Copy,
    {
        // Makes a checkpoint and create a `Rollbacker`.
        let rb = rollback::checkpoint!();
        compiler_fence(Ordering::SeqCst);

        // Repin the current epoch.
        // Acquiring an epoch must be proceeded after starting the crashable section,
        // not before. This is because if we acquire it before allowing a crash,
        // it is possible to be ejected before allowing. Although an ejection is occured,
        // the critical section would continues, as we would not `longjmp` from
        // the signal handler.
        self.repin();
        compiler_fence(Ordering::SeqCst);

        // Execute the body of this section.
        let result = body(&CsGuard::new(self, rb));
        compiler_fence(Ordering::SeqCst);

        // We are now out of the critical(crashable) section.
        // Unpin the local epoch to help reclaimers to freely collect bags.
        self.unpin_inner();
        compiler_fence(Ordering::SeqCst);

        // Finaly, close this critical section by dropping `rb`.
        result
    }

    #[cfg(sanitize = "address")]
    #[inline(never)]
    pub(crate) unsafe fn pin<F, R>(&mut self, mut body: F) -> R
    where
        F: FnMut(&CsGuard) -> R,
        R: Copy,
    {
        // A dummy loop to bypass a false stack overflow from AdressSanitizer.
        //
        // # HACK: A dummy loop and `blackbox`
        //
        // It is not needed in normal builds, but when address-sanitizing,
        // the sanitizer often gives a false positive by recognizing `longjmp` as
        // stack buffer overflow (or stack corruption).
        //
        // However, awkwardly, if it wrapped by a loop block,
        // it seems that the sanitizer recognizes `longjmp` as
        // normal `continue` operation and totally satisfies with it.
        //
        // So, they are added to avoid false positives from the sanitizer.
        loop {
            // Makes a checkpoint and create a `Rollbacker`.
            let rb = rollback::checkpoint!();
            compiler_fence(Ordering::SeqCst);

            // Repin the current epoch.
            // Acquiring an epoch must be proceeded after starting the crashable section,
            // not before. This is because if we acquire it before allowing a crash,
            // it is possible to be ejected before allowing. Although an ejection is occured,
            // the critical section would continues, as we would not `longjmp` from
            // the signal handler.
            self.repin();
            compiler_fence(Ordering::SeqCst);

            // Execute the body of this section.
            let result = body(&CsGuard::new(self, rb));
            compiler_fence(Ordering::SeqCst);

            // We are now out of the critical(crashable) section.
            // Unpin the local epoch to help reclaimers to freely collect bags.
            self.unpin_inner();
            compiler_fence(Ordering::SeqCst);

            // Finaly, close this critical section by dropping `rb`.

            // # HACK: A dummy loop and `blackbox`
            // (See comments on the loop for more information.)
            if core::hint::black_box(true) {
                break result;
            }
        }
    }

    /// Adds `deferred` to the thread-local bag.
    ///
    /// It returns a `Some(Vec<Deferred>)` if the global epoch is advanced and we have collected
    /// some expired deferred tasks.
    #[inline]
    pub(crate) fn defer(&mut self, def: Deferred) -> Option<Vec<Deferred>> {
        let bag = unsafe { &mut *self.bag.get() };

        if let Err(d) = bag.try_push(def) {
            self.global().push_bag(bag);
            bag.try_push(d).unwrap();

            let push_count = self.push_count.get() + 1;
            self.push_count.set(push_count);

            let collected =
                if unsafe { USE_ROLLBACK } && push_count >= Self::COUNTS_BETWEEN_FORCE_ADVANCE {
                    self.push_count.set(0);
                    let epoch = unsafe { &*self.global }.advance(self);
                    Some(self.global().collect(epoch))
                } else {
                    let epoch = match self.global().try_advance() {
                        Ok(epoch) => {
                            self.push_count.set(0);
                            epoch
                        }
                        Err(epoch) => epoch,
                    };
                    Some(self.global().collect(epoch))
                };

            return collected
                .map(|bags| bags.into_iter().flat_map(|bag| bag.into_iter()).collect());
        }
        None
    }

    #[inline]
    pub(crate) fn with_local_defs<F>(&mut self, f: F)
    where
        F: FnOnce(Vec<Deferred>) -> Vec<Deferred>,
    {
        let defs = take(&mut self.hp_bag);
        self.hp_bag = f(defs);
    }

    #[inline]
    pub(crate) fn iter_guarded_ptrs<'s>(&'s mut self) -> impl Iterator<Item = *mut u8> + 's {
        unsafe { &*self.global }.iter_guarded_ptrs(self)
    }

    #[inline]
    pub(crate) fn decr_garb_stat(&self, val: usize) {
        self.global().garbage_count.fetch_sub(val, Ordering::AcqRel);
    }

    #[inline]
    pub(crate) fn acquire_slot(&mut self) -> usize {
        loop {
            if let Some(idx) = self.available_indices.pop() {
                return idx;
            }
            self.grow_array();
        }
    }

    #[inline]
    fn grow_array(&mut self) {
        let array_ptr = self.hazptrs.load(Ordering::Relaxed);
        let array = unsafe { &*array_ptr };
        let prev_len = array.len();

        // Double the size of the array and fill the empty slots with `null_mut`s.
        let origin_iter = array.iter().map(|slot| slot.load(Ordering::Relaxed));
        let new_iter = std::iter::repeat(null_mut()).take(prev_len);
        let extended = origin_iter.chain(new_iter).map(AtomicPtr::new).collect();

        self.hazptrs
            .store(Box::into_raw(Box::new(extended)), Ordering::Release);
        self.available_indices.extend(prev_len..prev_len * 2);

        // Must defer the destruction of the old array.
        self.global().garbage_count.fetch_add(1, Ordering::AcqRel);
        self.hp_bag
            .push(Deferred::new(array_ptr as _, free::<Vec<AtomicPtr<u8>>>));
    }

    #[inline]
    pub(crate) fn release_slot(&mut self, idx: usize) {
        self.available_indices.push(idx);
    }

    #[inline]
    pub(crate) unsafe fn slot_unchecked(&self, idx: usize) -> &AtomicPtr<u8> {
        (*self.hazptrs.load(Ordering::Relaxed)).get_unchecked(idx)
    }

    /// # Safety
    ///
    /// The released `Local` must not be used afterwards.
    #[inline]
    pub(crate) unsafe fn release(&mut self) {
        self.epoch.store(Epoch::starting(), Ordering::Release);

        if cfg!(debug_assertions) {
            self.available_indices.sort_unstable();
            let len = self.available_indices.len();
            debug_assert!(Self::HAZARD_ARRAY_INIT_SIZE <= len);
            debug_assert_eq!((len / Self::HAZARD_ARRAY_INIT_SIZE).count_ones(), 1);
            debug_assert_eq!(self.available_indices, (0..len).collect::<Vec<_>>());
        }

        // Sync with `LocalList::acquire`.
        fence(Ordering::Release);
        self.using.store(false, Ordering::Relaxed);
    }
}

struct HazardArrayIter {
    array: *const [AtomicPtr<u8>],
    idx: usize,
    _hp: HazardPointer,
}

impl HazardArrayIter {
    fn new(owner: &Local, reader: &mut Local) -> Self {
        let hp = HazardPointer::new(reader);
        let array = hp.protect(&owner.hazptrs);
        HazardArrayIter {
            array: unsafe { &*array }.as_slice(),
            idx: 0,
            _hp: hp,
        }
    }
}

impl Iterator for HazardArrayIter {
    type Item = *mut u8;

    fn next(&mut self) -> Option<Self::Item> {
        let array = unsafe { &*self.array };
        for i in self.idx..array.len() {
            self.idx += 1;
            let slot = unsafe { array.get_unchecked(i) };
            let value = slot.load(Ordering::Acquire);
            if !value.is_null() {
                return Some(value);
            }
        }
        None
    }
}

impl Drop for Local {
    fn drop(&mut self) {
        drop(unsafe { Box::from_raw(self.hazptrs.load(Ordering::Acquire)) });
    }
}

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
    #[allow(clippy::mut_from_ref)]
    fn acquire<'c>(&'c self, global: &Global) -> &'c mut Local {
        let tid = pthread_self();
        let mut prev_link = &self.head;

        // Sync with `Local::release`.
        fence(Ordering::Acquire);
        let local = loop {
            match unsafe { prev_link.load(Ordering::Acquire).as_mut() } {
                Some(curr) => {
                    if !curr.using.load(Ordering::Relaxed)
                        && curr
                            .using
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
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
                            Ordering::AcqRel,
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
    fn iter_using(&self) -> impl Iterator<Item = &Local> {
        LocalIter {
            curr: self.head.load(Ordering::Acquire),
            predicate: |local| local.using.load(Ordering::Acquire),
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
            for def in curr_local.hp_bag.drain(..) {
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

pub(crate) unsafe fn free<T>(ptr: *mut u8) {
    let ptr = ptr as *mut T;
    drop(Box::from_raw(ptr));
}
