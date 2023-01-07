use core::cell::Cell;
use core::marker::PhantomData;
use core::ptr;
use core::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

use crate::untagged;
use crate::DEFAULT_THREAD;

pub struct HazardPointer<'domain> {
    hazard: &'domain ThreadHazPtrRecord,
    thread: &'domain ThreadRecord,
    _marker: PhantomData<*mut ()>, // !Send + !Sync
}

pub enum ProtectError<T> {
    Stopped,
    Changed(*mut T),
}

impl Default for HazardPointer<'static> {
    fn default() -> Self {
        DEFAULT_THREAD.with(|t| HazardPointer::new(t.hazards))
    }
}

impl<'domain> HazardPointer<'domain> {
    /// Creat a hazard pointer in the given thread
    pub fn new(thread: &'domain ThreadRecord) -> Self {
        Self {
            hazard: thread.hazptrs.acquire(),
            thread,
            _marker: PhantomData,
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
        self.hazard.protect(ptr as *mut u8);
    }

    /// Release the protection awarded by this hazard pointer, if any.
    ///
    /// If the hazard pointer was protecting an object, that object may now be reclaimed when
    /// retired (assuming it isn't protected by any _other_ hazard pointers).
    pub fn reset_protection(&mut self) {
        self.hazard.reset();
    }

    /// Check if `src` still points to `pointer`. If not, returns the current value.
    ///
    /// For a pointer `p`, if "`src` still pointing to `pointer`" implies that `p` is not retired,
    /// then `Ok(())` means that shields set to `p` are validated.
    pub fn validate<T>(pointer: *mut T, src: &AtomicPtr<T>) -> Result<(), *mut T> {
        membarrier::light_membarrier();
        // relaxed is ok thanks to the previous load (that created `pointer`) + the fence above
        let new = src.load(Ordering::Relaxed);
        if pointer as usize == new as usize {
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
        Self::validate(pointer, src).map_err(|new| {
            self.reset_protection();
            new
        })
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
    ) -> Result<*mut T, ProtectError<T>>
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
            return Ok(ptr);
        }
        Err(ProtectError::Changed(ptr_new))
    }
}

impl Drop for HazardPointer<'_> {
    fn drop(&mut self) {
        self.hazard.reset();
        self.thread.hazptrs.release(self.hazard);
    }
}

/// Push-only list of thread records
pub(crate) struct ThreadRecords {
    head: AtomicPtr<ThreadRecord>,
}

pub struct ThreadRecord {
    pub(crate) hazptrs: ThreadHazPtrRecords,
    pub(crate) next: *mut ThreadRecord,
    pub(crate) available: AtomicBool,
}

/// Single-writer hazard pointer bag.
/// - push only
/// - efficient recycling
/// - No need to use CAS.
// TODO: This can be array, like Chase-Lev deque.
pub(crate) struct ThreadHazPtrRecords {
    head: AtomicPtr<ThreadHazPtrRecord>,
    // this is cell because it's only used by the owning thread
    head_available: Cell<*mut ThreadHazPtrRecord>,
}

pub(crate) struct ThreadHazPtrRecord {
    pub(crate) ptr: AtomicPtr<u8>,
    pub(crate) next: *mut ThreadHazPtrRecord,
    // this is cell because it's only used by the owning thread
    pub(crate) available_next: Cell<*mut ThreadHazPtrRecord>,
}

impl ThreadRecords {
    pub(crate) const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    pub(crate) fn acquire(&self) -> &ThreadRecord {
        if let Some(avail) = self.try_acquire_available() {
            return avail;
        }
        self.acquire_new()
    }

    fn try_acquire_available(&self) -> Option<&ThreadRecord> {
        let mut cur = self.head.load(Ordering::Acquire);
        while let Some(cur_ref) = unsafe { cur.as_ref() } {
            if cur_ref.available.load(Ordering::Relaxed)
                && cur_ref
                    .available
                    .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                return Some(cur_ref);
            }
            cur = cur_ref.next;
        }
        None
    }

    fn acquire_new(&self) -> &ThreadRecord {
        let new = Box::leak(Box::new(ThreadRecord {
            hazptrs: ThreadHazPtrRecords {
                head: AtomicPtr::new(ptr::null_mut()),
                head_available: Cell::new(ptr::null_mut()),
            },
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
                Ok(_) => return new,
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

impl ThreadHazPtrRecords {
    pub(crate) fn acquire(&self) -> &ThreadHazPtrRecord {
        if let Some(avail) = self.try_acquire_available() {
            return avail;
        }
        self.acquire_new()
    }

    fn try_acquire_available(&self) -> Option<&ThreadHazPtrRecord> {
        let head = self.head_available.get();
        let head_ref = unsafe { head.as_ref()? };
        let next = head_ref.available_next.get();
        self.head_available.set(next);
        Some(head_ref)
    }

    fn acquire_new(&self) -> &ThreadHazPtrRecord {
        let head = self.head.load(Ordering::Relaxed);
        let hazptr = Box::leak(Box::new(ThreadHazPtrRecord {
            ptr: AtomicPtr::new(ptr::null_mut()),
            next: head,
            available_next: Cell::new(ptr::null_mut()),
        }));
        self.head.store(hazptr, Ordering::Release);
        hazptr
    }

    pub(crate) fn release(&self, rec: &ThreadHazPtrRecord) {
        let avail = self.head_available.get();
        rec.available_next.set(avail);
        self.head_available.set(rec as *const _ as *mut _);
    }

    pub(crate) fn iter(&self) -> ThreadHazPtrRecordsIter {
        ThreadHazPtrRecordsIter {
            cur: self.head.load(Ordering::Acquire),
        }
    }
}

pub(crate) struct ThreadHazPtrRecordsIter {
    cur: *mut ThreadHazPtrRecord,
}

impl Iterator for ThreadHazPtrRecordsIter {
    type Item = *mut u8;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cur_ref) = unsafe { self.cur.as_ref() } {
            self.cur = cur_ref.next;
            Some(cur_ref.ptr.load(Ordering::Acquire))
        } else {
            None
        }
    }
}

impl ThreadHazPtrRecord {
    fn reset(&self) {
        self.ptr.store(ptr::null_mut(), Ordering::Release);
    }

    fn protect(&self, ptr: *mut u8) {
        self.ptr.store(ptr, Ordering::Release);
    }
}
