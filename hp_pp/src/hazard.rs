use core::marker::PhantomData;
use core::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use core::{mem, ptr};

use crossbeam_utils::CachePadded;

use crate::thread::Thread;
use crate::untagged;
use crate::DEFAULT_THREAD;

#[derive(Debug)]
pub struct HazardPointer<'domain> {
    thread: *const Thread<'domain>,
    idx: usize,
}

pub enum ProtectError<T> {
    Stopped,
    Changed(*mut T),
}

impl Default for HazardPointer<'static> {
    fn default() -> Self {
        DEFAULT_THREAD.with(|t| HazardPointer::new(&mut t.borrow_mut()))
    }
}

impl<'domain> HazardPointer<'domain> {
    /// Create a hazard pointer in the given thread
    pub fn new(thread: &mut Thread<'domain>) -> Self {
        let idx = thread.acquire();
        Self { thread, idx }
    }

    #[inline]
    fn slot(&self) -> &AtomicPtr<u8> {
        unsafe {
            let array = &*(*self.thread).hazards.hazptrs.load(Ordering::Relaxed);
            array.get_unchecked(self.idx)
        }
    }

    /// Protect the given address.
    ///
    /// You will very rarely want to use this method, and should prefer the other protection
    /// methods instead, as they guard against races between when the value of a shared pointer was
    /// read and any changes to the shared pointer address.
    ///
    /// Note that protecting a given pointer only has an effect if any thread that may drop the
    /// pointer does so through the same [`Domain`] as this hazard pointer is associated with.
    ///
    pub fn protect_raw<T>(&mut self, ptr: *mut T) {
        self.slot().store(ptr as *mut u8, Ordering::Release);
    }

    /// Release the protection awarded by this hazard pointer, if any.
    ///
    /// If the hazard pointer was protecting an object, that object may now be reclaimed when
    /// retired (assuming it isn't protected by any _other_ hazard pointers).
    pub fn reset_protection(&mut self) {
        self.slot().store(ptr::null_mut(), Ordering::Release);
    }

    /// Check if `src` still points to `pointer`. If not, returns the current value.
    ///
    /// For a pointer `p`, if "`src` still pointing to `pointer`" implies that `p` is not retired,
    /// then `Ok(())` means that shields set to `p` are validated.
    pub fn validate<T>(pointer: *mut T, src: &AtomicPtr<T>) -> Result<(), *mut T> {
        membarrier::light_membarrier();
        let new = src.load(Ordering::Acquire);
        if pointer == new {
            Ok(())
        } else {
            Err(new)
        }
    }

    /// Try protecting `pointer` obtained from `src`. If not, returns the current value.
    ///
    /// If "`src` still pointing to `pointer`" implies that `pointer` is not retired, then `Ok(())`
    /// means that this shield is validated.
    pub fn try_protect<T>(&mut self, pointer: *mut T, src: &AtomicPtr<T>) -> Result<(), *mut T> {
        self.protect_raw(pointer);
        Self::validate(pointer, src)
    }

    /// Get a protected pointer from `src`.
    ///
    /// See `try_protect()`.
    pub fn protect<T>(&mut self, src: &AtomicPtr<T>) -> *mut T {
        let mut pointer = src.load(Ordering::Relaxed);
        while let Err(new) = self.try_protect(pointer, src) {
            pointer = new;
        }
        pointer
    }

    /// hp++ protection
    pub fn try_protect_pp<T, S, F1, F2>(
        &mut self,
        ptr: *mut T,
        src: &S,
        src_link: &F1,
        check_stop: &F2,
    ) -> Result<(), ProtectError<T>>
    where
        F1: Fn(&S) -> &AtomicPtr<T>,
        F2: Fn(&S) -> bool,
    {
        self.protect_raw(ptr);
        membarrier::light_membarrier();
        if check_stop(src) {
            return Err(ProtectError::Stopped);
        }
        let ptr_new = untagged(src_link(src).load(Ordering::Acquire));
        if ptr == ptr_new {
            return Ok(());
        }
        Err(ProtectError::Changed(ptr_new))
    }

    /// hp++ protection
    pub fn protect_pp<T, S, F1, F2>(
        &mut self,
        src: &S,
        src_link: &F1,
        check_stop: &F2,
    ) -> Result<*mut T, ()>
    where
        F1: Fn(&S) -> &AtomicPtr<T>,
        F2: Fn(&S) -> bool,
    {
        let mut ptr = src_link(src).load(Ordering::Relaxed);
        loop {
            match self.try_protect_pp(ptr, src, src_link, check_stop) {
                Err(ProtectError::Stopped) => return Err(()),
                Err(ProtectError::Changed(ptr_new)) => ptr = ptr_new,
                Ok(_) => return Ok(ptr),
            }
        }
    }

    #[inline]
    pub fn swap(x: &mut HazardPointer, y: &mut HazardPointer) {
        mem::swap(&mut x.idx, &mut y.idx);
    }

    /// Copy protection to another hp. Previous protection of `to` is reset.
    /// This is only possible when `to` is scanned after `self`.
    /// Correctness of this is quite subtle, so avoid using it.
    /// There are usually better alternative approaches.
    pub fn copy_to(&mut self, to: &mut HazardPointer) -> Result<(), ()> {
        if to.idx <= self.idx {
            return Err(());
        }
        to.protect_raw(self.slot().load(Ordering::Relaxed));
        Ok(())
    }
}

impl Drop for HazardPointer<'_> {
    fn drop(&mut self) {
        self.reset_protection();
        unsafe { (*(self.thread as *mut Thread)).release(self.idx) };
    }
}

/// Push-only list of recyclable thread records
#[derive(Debug)]
pub(crate) struct ThreadRecords {
    head: AtomicPtr<ThreadRecord>,
}

/// Single-writer growable hazard pointer array.
/// Does not shrink. (Use single-writer doubly linked list? see HP04)
#[derive(Debug)]
pub struct ThreadRecord {
    pub(crate) next: *mut ThreadRecord,
    pub(crate) available: AtomicBool,
    pub(crate) hazptrs: CachePadded<AtomicPtr<HazardArray>>,
}

type HazardArray = Vec<AtomicPtr<u8>>;

impl ThreadRecords {
    pub(crate) const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    pub(crate) fn acquire(&self) -> (&ThreadRecord, Vec<usize>) {
        if let Some(avail) = self.try_acquire_available() {
            return avail;
        }
        self.acquire_new()
    }

    fn try_acquire_available(&self) -> Option<(&ThreadRecord, Vec<usize>)> {
        let mut cur = self.head.load(Ordering::Acquire);
        while let Some(cur_ref) = unsafe { cur.as_ref() } {
            if cur_ref.available.load(Ordering::Relaxed)
                && cur_ref
                    .available
                    .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                let len = unsafe { &*cur_ref.hazptrs.load(Ordering::Relaxed) }.len();
                return Some((cur_ref, (0..len).collect()));
            }
            cur = cur_ref.next;
        }
        None
    }

    fn acquire_new(&self) -> (&ThreadRecord, Vec<usize>) {
        const HAZARD_ARRAY_INIT_SIZE: usize = 64;
        let array = Vec::from(unsafe { mem::zeroed::<[AtomicPtr<u8>; HAZARD_ARRAY_INIT_SIZE]>() });
        let new = Box::leak(Box::new(ThreadRecord {
            hazptrs: CachePadded::new(AtomicPtr::new(Box::into_raw(Box::new(array)))),
            next: ptr::null_mut(),
            available: AtomicBool::new(false),
        }));

        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            new.next = head;
            match self
                .head
                .compare_exchange(head, new, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return (new, (0..HAZARD_ARRAY_INIT_SIZE).collect()),
                Err(head_new) => head = head_new,
            }
        }
    }

    pub(crate) fn release(&self, rec: &ThreadRecord) {
        rec.available.store(true, Ordering::Release);
    }

    pub(crate) fn iter(&self) -> ThreadRecordsIter<'_> {
        ThreadRecordsIter {
            cur: self.head.load(Ordering::Acquire).cast_const(),
            _marker: PhantomData,
        }
    }
}

pub(crate) struct ThreadRecordsIter<'domain> {
    cur: *const ThreadRecord,
    _marker: PhantomData<&'domain ThreadRecord>,
}

impl<'domain> Iterator for ThreadRecordsIter<'domain> {
    type Item = &'domain ThreadRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cur_ref) = unsafe { self.cur.as_ref() } {
            self.cur = cur_ref.next;
            Some(cur_ref)
        } else {
            None
        }
    }
}

impl ThreadRecord {
    pub(crate) fn iter<'domain>(
        &self,
        reader: &mut Thread<'domain>,
    ) -> ThreadHazardArrayIter<'domain> {
        let mut hp = HazardPointer::new(reader);
        let array = hp.protect(&self.hazptrs);
        ThreadHazardArrayIter {
            array,
            idx: 0,
            _hp: hp,
        }
    }
}

pub(crate) struct ThreadHazardArrayIter<'domain> {
    array: *const HazardArray,
    idx: usize,
    _hp: HazardPointer<'domain>,
}

impl<'domain> Iterator for ThreadHazardArrayIter<'domain> {
    type Item = *mut u8;

    fn next(&mut self) -> Option<Self::Item> {
        let array = unsafe { &*self.array };
        array.get(self.idx).map(|slot| {
            self.idx += 1;
            slot.load(Ordering::Acquire)
        })
    }
}
