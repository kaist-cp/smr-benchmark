use std::{
    marker::PhantomData,
    mem::{swap, zeroed},
    ptr::null_mut,
    sync::atomic::{AtomicBool, AtomicPtr, Ordering},
};

use crate::thread::Thread;

/// A low-level owner of hazard pointer slot.
///
/// A `Shield` owns a `HazardPointer` as its field.
pub(crate) struct HazardPointer {
    thread: *const Thread,
    idx: usize,
}

impl HazardPointer {
    /// Create a hazard pointer in the given thread
    pub fn new(thread: &mut Thread) -> Self {
        let idx = thread.acquire();
        Self { thread, idx }
    }

    #[inline]
    fn slot(&self) -> &AtomicPtr<u8> {
        unsafe {
            let array = &*(*(*self.thread).hazards).hazptrs.load(Ordering::Relaxed);
            array.get_unchecked(self.idx)
        }
    }

    /// Protect the given address.
    pub fn protect_raw<T>(&mut self, ptr: *mut T) {
        self.slot().store(ptr as *mut u8, Ordering::Release);
    }

    /// Release the protection awarded by this hazard pointer, if any.
    pub fn reset_protection(&mut self) {
        self.slot().store(null_mut(), Ordering::Release);
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

    #[inline]
    pub fn swap(x: &mut HazardPointer, y: &mut HazardPointer) {
        swap(&mut x.idx, &mut y.idx);
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

impl Drop for HazardPointer {
    fn drop(&mut self) {
        self.reset_protection();
        unsafe { (*(self.thread as *mut Thread)).release(self.idx) };
    }
}

/// Push-only list of recyclable thread records.
#[derive(Debug)]
pub(crate) struct ThreadRecords {
    head: AtomicPtr<ThreadRecord>,
}

/// Single-writer growable hazard pointer array.
pub struct ThreadRecord {
    pub(crate) next: *mut ThreadRecord,
    pub(crate) available: AtomicBool,
    pub(crate) hazptrs: AtomicPtr<HazardArray>,
}

type HazardArray = Vec<AtomicPtr<u8>>;

impl ThreadRecords {
    pub(crate) const fn new() -> Self {
        Self {
            head: AtomicPtr::new(null_mut()),
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
        let array = Vec::from(unsafe { zeroed::<[AtomicPtr<u8>; HAZARD_ARRAY_INIT_SIZE]>() });
        let new = Box::leak(Box::new(ThreadRecord {
            hazptrs: AtomicPtr::new(Box::into_raw(Box::new(array))),
            next: null_mut(),
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

    pub(crate) fn iter<'g>(&'g self) -> ThreadRecordsIter<'g> {
        ThreadRecordsIter {
            cur: self.head.load(Ordering::Acquire).cast_const(),
            _marker: PhantomData,
        }
    }
}

pub(crate) struct ThreadRecordsIter<'g> {
    cur: *const ThreadRecord,
    _marker: PhantomData<&'g ()>,
}

impl<'g> Iterator for ThreadRecordsIter<'g> {
    type Item = &'g ThreadRecord;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cur_ref = unsafe { self.cur.as_ref()? };
            self.cur = cur_ref.next;
            if !cur_ref.available.load(Ordering::Acquire) {
                return Some(cur_ref);
            }
        }
    }
}

impl ThreadRecord {
    pub(crate) fn iter(&self, reader: &mut Thread) -> ThreadHazardArrayIter {
        let mut hp = HazardPointer::new(reader);
        let array = hp.protect(&self.hazptrs);
        ThreadHazardArrayIter {
            array: unsafe { &*array }.as_slice(),
            idx: 0,
            _hp: hp,
        }
    }
}

pub(crate) struct ThreadHazardArrayIter {
    array: *const [AtomicPtr<u8>],
    idx: usize,
    _hp: HazardPointer,
}

impl Iterator for ThreadHazardArrayIter {
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
