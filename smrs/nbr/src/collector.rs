/// A concurrent garbage collector
/// with Neutralization Based Reclamation (NBR+).
use atomic::Atomic;
use nix::errno::Errno;
use nix::sys::pthread::{pthread_self, Pthread};
use rustc_hash::FxHashSet;
use setjmp::jmp_buf;
use static_assertions::const_assert;
use std::sync::atomic::{compiler_fence, fence, AtomicBool};
use std::sync::Barrier;
use std::{
    cell::{Cell, RefCell},
    ptr::{null_mut, NonNull},
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use crate::block_bag::{BlockBag, BlockPool, BLOCK_SIZE};
use crate::recovery::{self, Status};
use crate::stats;

const_assert!(Atomic::<Pthread>::is_lock_free());

const HAZARD_ARRAY_INIT_SIZE: usize = 16;

/// 0-indexed thread identifier.
/// Note that this ThreadId is not same with pthread_t,
/// and it is used for only NBR internally.
pub type ThreadId = usize;

/// Thread-local variables of NBR+
struct Thread {
    #[allow(unused)]
    tid: ThreadId,
    // Retired records collected by a delete operation
    retired: *mut BlockBag,
    // Each reclaimer scans hazard pointers across threads
    // to free retired its bag such that any hazard pointers aren't freed.
    scanned_hazptrs: RefCell<FxHashSet<*mut u8>>,
    // A helper to allocate and recycle blocks
    //
    // In the original implementation, `reclaimer_nbr` has
    // `pool` as a member, which is a subclass of `pool_interface`.
    // And `pool_interface` has `blockpools` to serve a thread-local
    // block pool to each worker.
    // We don't have to write any codes which is equivalent to
    // `pool_interface`, as `pool_interface` is just a simple
    // wrapper for convinient allocating & deallocating.
    // It can be replaced with a simple `BlockPool`.
    pool: *mut BlockPool,
    capacity: usize,
    #[allow(unused)]
    lowatermark: usize,

    // Used for NBR+ signal optimization
    announced_ts: AtomicUsize,
    saved_retired_head: Option<NonNull<u8>>,
    saved_ts: Vec<usize>,
    first_lo_entry_flag: bool,
    retires_since_lo_watermark: usize,

    // Saves the discovered records before upgrading to write
    // to protect records from concurrent reclaimer threads.
    // (Single-Writer Multi-Reader)
    using_hazptrs: usize,
    proposed_hazptrs: Vec<AtomicPtr<u8>>,
}

impl Thread {
    pub fn new(
        tid: usize,
        num_threads: usize,
        bag_cap_pow2: usize,
        lowatermark: usize,
        max_hazptrs: usize,
    ) -> Self {
        let pool = Box::into_raw(Box::<BlockPool>::default());
        let retired: *mut BlockBag = Box::into_raw(Box::new(BlockBag::new(pool)));
        let mut proposed_hazptrs = Vec::with_capacity(HAZARD_ARRAY_INIT_SIZE);
        proposed_hazptrs.resize_with(max_hazptrs, || AtomicPtr::new(null_mut()));

        Self {
            tid,
            retired,
            scanned_hazptrs: RefCell::default(),
            pool,
            capacity: bag_cap_pow2 / BLOCK_SIZE,
            lowatermark,
            announced_ts: AtomicUsize::new(0),
            saved_retired_head: None,
            saved_ts: vec![0; num_threads],
            first_lo_entry_flag: true,
            retires_since_lo_watermark: 0,
            using_hazptrs: 0,
            proposed_hazptrs,
        }
    }

    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    fn retired_mut(&self) -> &mut BlockBag {
        unsafe { &mut *self.retired }
    }

    // This out of patience decision has been improved
    // in the latest NBR+ version on setbench.
    #[inline]
    fn is_out_of_patience(&mut self) -> bool {
        self.retired_mut().size_in_blocks() > self.capacity
    }

    #[inline]
    fn is_past_lo_watermark(&mut self) -> bool {
        cfg_if::cfg_if! {
            // When sanitizing, reclaim more aggressively.
            if #[cfg(sanitize = "address")] {
                !self.retired_mut().is_empty()
            } else {
                let blocks = self.retired_mut().size_in_blocks();
                (blocks as f32
                    > (self.capacity as f32 * (1.0f32 / ((self.tid % 3) + 2) as f32)))
                && ((self.retires_since_lo_watermark % self.lowatermark) == 0)
            }
        }
    }

    #[inline]
    fn set_lo_watermark(&mut self) {
        self.saved_retired_head = Some(NonNull::new(self.retired_mut().peek().ptr()).unwrap());
    }

    #[inline]
    fn send_freeable_to_pool(&mut self) {
        let retired = self.retired_mut();

        let mut spare_bag = BlockBag::new(self.pool);

        if let Some(saved) = self.saved_retired_head {
            // Reclaim due to a lo-watermark path
            while retired.peek().ptr() != saved.as_ptr() {
                let ret = retired.pop();
                spare_bag.push_retired(ret);
            }
        }

        // Deallocate freeable records.
        // Note that even if we are on a lo-watermark path,
        // we must check a pointer is protected or not anyway.
        let mut reclaimed = 0;
        while !retired.is_empty() {
            let ret = retired.pop();
            if self.scanned_hazptrs.borrow().contains(&ret.ptr()) {
                spare_bag.push_retired(ret);
            } else {
                reclaimed += 1;
                unsafe { ret.deallocate() };
            }
        }
        stats::decr_garb(reclaimed);

        // Add all collected but protected records back to `retired`
        while !spare_bag.is_empty() {
            retired.push_retired(spare_bag.pop());
        }
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        unsafe {
            let mut retired = Box::from_raw(self.retired);
            retired.deallocate_all();
            drop(retired);
            drop(Box::from_raw(self.pool));
        }
    }
}

pub struct Collector {
    num_threads: usize,
    max_hazptrs: usize,
    threads: Vec<Thread>,
    // Map from Thread ID into pthread_t(u64 or usize, which depends on platforms)
    // for each registered thread
    registered_map: Vec<Atomic<Pthread>>,
    registered_count: AtomicUsize,
    barrier: Barrier,
}

impl Collector {
    pub fn new(
        num_threads: usize,
        bag_cap_pow2: usize,
        lowatermark: usize,
        max_hazptrs: usize,
    ) -> Self {
        assert!(bag_cap_pow2.count_ones() == 1);
        unsafe { recovery::install() };

        let threads = (0..num_threads)
            .map(|tid| Thread::new(tid, num_threads, bag_cap_pow2, lowatermark, max_hazptrs))
            .collect();

        Self {
            num_threads,
            max_hazptrs,
            threads,
            registered_map: (0..num_threads).map(|_| Atomic::new(0)).collect(),
            registered_count: AtomicUsize::new(0),
            barrier: Barrier::new(num_threads),
        }
    }

    #[cold]
    unsafe fn restart_all_threads(&self, reclaimer: ThreadId) -> Result<(), Errno> {
        for other_tid in 0..self.num_threads {
            if other_tid == reclaimer {
                continue;
            }
            let pthread = self.registered_map[other_tid].load(Ordering::Acquire);
            recovery::send_signal(pthread)?;
        }
        Ok(())
    }

    #[cold]
    fn collect_all_saved_records(&self, reclaimer: ThreadId) {
        // Set where record would be collected in.
        let mut scanned = self.threads[reclaimer].scanned_hazptrs.borrow_mut();
        scanned.clear();
        fence(Ordering::SeqCst);

        for other_tid in 0..self.num_threads {
            for i in 0..self.max_hazptrs {
                let hazptr = &self.threads[other_tid].proposed_hazptrs[i];
                let ptr = hazptr.load(Ordering::Acquire);
                scanned.insert(ptr);
            }
        }
    }

    fn reclaim_freeable(&mut self, reclaimer: ThreadId) {
        self.collect_all_saved_records(reclaimer);
        self.threads[reclaimer].send_freeable_to_pool();
    }

    pub fn register(&self) -> Guard {
        // Initialize current thread.
        // (ref: `initThread(tid)` in `recovery_manager.h`
        //  from original nbr_setbench)
        let tid = self.registered_count.fetch_add(1, Ordering::SeqCst);
        assert!(
            tid < self.num_threads,
            "Attempted to exceed the maximum number of threads"
        );
        self.registered_map[tid].store(pthread_self(), Ordering::Release);
        let guard = Guard::register(self, tid);

        // Wait until all threads are ready.
        self.barrier.wait();
        guard
    }

    pub fn reset_registrations(&mut self) {
        self.registered_count.store(0, Ordering::SeqCst);
        self.barrier = Barrier::new(self.num_threads);
    }
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

const SYNC_RETIRED_COUNT_BY: usize = 32;

pub struct Guard {
    collector: *mut Collector,
    tid: ThreadId,
    retired_cnt_buff: Cell<usize>,
    status: Status,
}

impl Guard {
    fn register(collector: &Collector, tid: ThreadId) -> Self {
        Self {
            collector: collector as *const _ as _,
            tid,
            retired_cnt_buff: Cell::new(0),
            status: unsafe { Status::new() },
        }
    }

    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    fn coll_mut(&self) -> &mut Collector {
        unsafe { &mut *self.collector }
    }

    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    fn thread_mut(&self) -> &mut Thread {
        &mut self.coll_mut().threads[self.tid]
    }

    #[inline]
    pub fn is_unprotected(&self) -> bool {
        self.collector.is_null()
    }

    fn incr_cnt_buff(&self) -> usize {
        let new_cnt = self.retired_cnt_buff.get() + 1;
        self.retired_cnt_buff.set(new_cnt);
        new_cnt
    }

    fn flush_cnt_buff(&self) {
        let cnt = self.retired_cnt_buff.get();
        self.retired_cnt_buff.set(0);
        stats::incr_garb(cnt);
    }

    /// Start read phase.
    ///
    /// In read phase, programmers must aware following restrictions.
    ///
    /// 1. Reading global variables is permitted and reading shared
    ///    records is permitted if pointers to them were obtained
    ///    during this phase.
    ///   - e.g., by traversing a sequence of shared objects by
    ///     following pointers starting from a global variable—i.e., a root
    ///
    /// 2. Writes/CASs to shared records, writes/CASs to shared globals,
    ///    and system calls, are **not permitted.**
    ///
    /// To understand the latter restriction, suppose an operation
    /// allocates a node using malloc during its read phase, and before
    /// it uses the node, the thread performing the operation is
    /// neutralized. This would cause **a memory leak.**
    ///
    /// Additionally, writes to thread local data structures are
    /// not recommended. To see why, suppose a thread maintains
    /// a thread local doubly-linked list, and also updates this list
    /// as part of the read phase of some operation on the shared data
    /// structure. If the thread is neutralized in middle of its update
    /// to this local list, it might corrupt the structure of the list.
    #[inline]
    pub fn start_read(&self) {
        self.status.set_restartable();
    }

    #[inline]
    pub fn jmp_buf(&self) -> *mut jmp_buf {
        self.status.jmp_buf
    }

    /// End read phase.
    ///
    /// Note that it is also equivalent to upgrading to write phase.
    #[inline]
    pub fn end_read(&self) {
        self.status.unset_restartable();
    }

    /// Retire a pointer.
    /// It may trigger other threads to restart.
    ///
    /// # Safety
    /// * The given memory block is no longer modified.
    /// * It is no longer possible to reach the block from
    ///   the data structure.
    /// * The same block is not retired more than once.
    pub unsafe fn retire<T>(&self, ptr: *mut T) {
        if self.is_unprotected() {
            drop(Box::from_raw(ptr));
            return;
        }

        let cnt_buff = self.incr_cnt_buff();
        let collector = self.coll_mut();
        let num_threads = collector.num_threads;

        if self.thread_mut().is_out_of_patience() {
            // Tell other threads that I'm starting signaling.
            self.thread_mut()
                .announced_ts
                .fetch_add(1, Ordering::SeqCst);
            compiler_fence(Ordering::SeqCst);

            if let Err(err) = collector.restart_all_threads(self.tid) {
                panic!("Failed to restart other threads: {err}");
            } else {
                // Tell other threads that I have done signaling.
                self.thread_mut()
                    .announced_ts
                    .fetch_add(1, Ordering::SeqCst);

                self.flush_cnt_buff();

                // Full bag shall be reclaimed so clear any bag head.
                // Avoiding changes to arg of this reclaim_freeable.
                self.thread_mut().saved_retired_head = None;
                collector.reclaim_freeable(self.tid);

                self.thread_mut().first_lo_entry_flag = true;
                self.thread_mut().retires_since_lo_watermark = 0;

                for i in 0..num_threads {
                    self.thread_mut().saved_ts[i] = 0;
                }
            }
        } else if self.thread_mut().is_past_lo_watermark() {
            // On the first entry to lo-path, I shall save my baghead.
            // Up to this baghead, I can reclaim upon detecting that someone
            // has started and finished signalling after I saved Baghead.
            // That is a condition where all threads have gone Quiescent
            // at least once after I saved my baghead.
            if self.thread_mut().first_lo_entry_flag {
                self.thread_mut().first_lo_entry_flag = false;
                self.thread_mut().set_lo_watermark();

                // Take a snapshot of all other announce_ts,
                // to be used to know if its time to reclaim at lo-path.
                for i in 0..num_threads {
                    let ts = collector.threads[i].announced_ts.load(Ordering::SeqCst);
                    self.thread_mut().saved_ts[i] = ts + (ts % 2);
                }
            }

            for i in 0..num_threads {
                if collector.threads[i].announced_ts.load(Ordering::SeqCst)
                    >= self.thread_mut().saved_ts[i] + 2
                {
                    self.flush_cnt_buff();

                    // If the baghead is not `None`, then reclamation shall happen
                    // from the baghead to tail in functions depicting reclamation of lo-watermark path.
                    collector.reclaim_freeable(self.tid);

                    self.thread_mut().first_lo_entry_flag = true;
                    self.thread_mut().retires_since_lo_watermark = 0;
                    for j in 0..num_threads {
                        self.thread_mut().saved_ts[j] = 0;
                    }
                    break;
                }
            }
        }

        if !self.thread_mut().first_lo_entry_flag {
            self.thread_mut().retires_since_lo_watermark += 1;
        }

        self.thread_mut().retired_mut().push(ptr);

        if cnt_buff % SYNC_RETIRED_COUNT_BY == 0 {
            self.flush_cnt_buff();
        }
    }

    #[inline]
    pub fn acquire_shield(&mut self) -> Option<Shield> {
        let thread = self.thread_mut();
        let len = thread.using_hazptrs;
        thread.using_hazptrs += 1;
        thread.proposed_hazptrs.get(len).map(|slot| Shield { slot })
    }
}

pub struct Shield {
    slot: *const AtomicPtr<u8>,
}

impl Shield {
    #[inline(always)]
    pub fn protect<T>(&self, ptr: *mut T) {
        unsafe { &*self.slot }.store(ptr as _, Ordering::Relaxed);
    }
}

/// Get a dummy `Guard` associated with no collector.
///
/// In a dummy `Guard`, `start_read`, `end_read` and `protect`
/// have no effect, and `retire` reclaims a block immediately.
///
/// # Safety
///
/// Use the unprotected `Guard` only if the data structure
/// is not accessed concurrently.
pub unsafe fn unprotected() -> &'static Guard {
    static DUMMY_REST: AtomicBool = AtomicBool::new(false);
    struct GuardWrapper(Guard);
    unsafe impl Sync for GuardWrapper {}
    static UNPROTECTED: GuardWrapper = GuardWrapper(Guard {
        collector: null_mut(),
        tid: 0,
        retired_cnt_buff: Cell::new(0),
        status: Status {
            jmp_buf: null_mut(),
            rest: &DUMMY_REST,
        },
    });
    &UNPROTECTED.0
}
