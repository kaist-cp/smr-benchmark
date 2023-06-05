//! Lock-free intrusive linked list.
//!
//! Ideas from Michael.  High Performance Dynamic Lock-Free Hash Tables and List-Based Sets.  SPAA
//! 2002.  http://dl.acm.org/citation.cfm?id=564870.564881

use core::marker::PhantomData;
use core::mem;
use core::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

use crate::pebr_backend::{unprotected, Atomic, EpochGuard, Shared, Shield, ShieldError};

/// An entry in a linked list.
///
/// An Entry is accessed from multiple threads, so it would be beneficial to put it in a different
/// cache-line than thread-local data in terms of performance.
#[derive(Debug, Default)]
pub struct Entry {
    /// The next entry in the linked list.
    /// If the tag is 1, this entry is marked as deleted.
    next: Atomic<Entry>,
}

/// Implementing this trait asserts that the type `T` can be used as an element in the intrusive
/// linked list defined in this module. `T` has to contain (or otherwise be linked to) an instance
/// of `Entry`.
///
/// # Example
///
/// ```ignore
/// struct A {
///     entry: Entry,
///     data: usize,
/// }
///
/// impl IsElement<A> for A {
///     fn entry_of(a: &A) -> &Entry {
///         let entry_ptr = ((a as usize) + offset_of!(A, entry)) as *const Entry;
///         unsafe { &*entry_ptr }
///     }
///
///     unsafe fn element_of(entry: &Entry) -> &T {
///         let elem_ptr = ((entry as usize) - offset_of!(A, entry)) as *const T;
///         &*elem_ptr
///     }
///
///     unsafe fn finalize(entry: &Entry, guard: &Guard) {
///         guard.defer_destroy(Shared::from(Self::element_of(entry) as *const _));
///     }
/// }
/// ```
///
/// This trait is implemented on a type separate from `T` (although it can be just `T`), because
/// one type might be placeable into multiple lists, in which case it would require multiple
/// implementations of `IsElement`. In such cases, each struct implementing `IsElement<T>`
/// represents a distinct `Entry` in `T`.
///
/// For example, we can insert the following struct into two lists using `entry1` for one
/// and `entry2` for the other:
///
/// ```ignore
/// struct B {
///     entry1: Entry,
///     entry2: Entry,
///     data: usize,
/// }
/// ```
///
pub trait IsElement<T> {
    /// Returns a reference to this element's `Entry`.
    fn entry_of(_: &T) -> &Entry;

    /// Given a reference to an element's entry, returns that element.
    ///
    /// ```ignore
    /// let elem = ListElement::new();
    /// assert_eq!(elem.entry_of(),
    ///            unsafe { ListElement::element_of(elem.entry_of()) } );
    /// ```
    ///
    /// # Safety
    ///
    /// The caller has to guarantee that the `Entry` is called with was retrieved from an instance
    /// of the element type (`T`).
    unsafe fn element_of(_: &Entry) -> &T;

    /// The function that is called when an entry is unlinked from list.
    ///
    /// # Safety
    ///
    /// The caller has to guarantee that the `Entry` is called with was retrieved from an instance
    /// of the element type (`T`).
    unsafe fn finalize(_: &Entry, _: &EpochGuard);
}

unsafe fn entry_of_shared<'g, T, C: IsElement<T>>(element: Shared<'g, T>) -> Shared<'g, Entry> {
    Shared::from(C::entry_of(&*element.as_raw()) as *const _).with_tag(element.tag())
}

unsafe fn element_of_shared<'g, T, C: IsElement<T>>(entry: Shared<'g, Entry>) -> Shared<'g, T> {
    Shared::from(C::element_of(&*entry.as_raw()) as *const _).with_tag(entry.tag())
}

/// A lock-free, intrusive linked list of type `T`.
#[derive(Debug)]
pub struct List<T, C: IsElement<T> = T> {
    /// The head of the linked list.
    head: Entry,

    /// The phantom data for using `T` and `C`.
    _marker: PhantomData<(T, C)>,
}

/// An iterator used for retrieving values from the list.
#[derive(Debug)]
pub struct Iter<'g, T: 'g, C: IsElement<T>> {
    /// The guard that protects the iteration.
    guard: &'g EpochGuard,

    /// Pointer from the predecessor to the current entry.
    pred: &'g mut Shield<T>,

    /// The current entry.
    curr: &'g mut Shield<T>,

    succ: Shared<'g, Entry>,

    /// Whether to detach and `defer_destroy` those nodes marked as deleted.
    is_detaching: bool,

    /// Whether the iteration was stalled due to lost race.
    is_stalled: bool,

    /// Logically, we store a borrow of an instance of `T` and
    /// use the type information from `C`.
    _marker: PhantomData<(&'g T, C)>,
}

/// An error that occurs during iteration over the list.
#[derive(PartialEq, Debug)]
pub enum IterError {
    /// A concurrent thread modified the state of the list at the same place that this iterator
    /// was inspecting. Subsequent iteration will restart from the beginning of the list.
    Stalled,

    /// Shield error during iteration.
    ShieldError(ShieldError),
}

impl Entry {
    /// Marks this entry as deleted, deferring the actual deallocation to a later iteration.
    ///
    /// # Safety
    ///
    /// The entry should be a member of a linked list, and it should not have been deleted.
    /// It should be safe to call `C::finalize` on the entry after the `guard` is dropped, where `C`
    /// is the associated helper for the linked list.
    pub unsafe fn delete(&self) {
        self.next.fetch_or(1, Release, unprotected());
    }

    /// Returns an iterator from the entry.
    ///
    /// # Caveat
    ///
    /// Every object that is inserted at the moment this function is called and persists at least
    /// until the end of iteration will be returned. Since this iterator traverses a lock-free
    /// linked list that may be concurrently modified, some additional caveats apply:
    ///
    /// 1. If a new object is inserted during iteration, it may or may not be returned.
    /// 2. If an object is deleted during iteration, it may or may not be returned.
    /// 3. The iteration may be aborted when it lost in a race condition. In this case, the winning
    ///    thread will continue to iterate over the same list.
    ///
    /// # Safety
    ///
    /// PR(@jeehoonkang): document it
    #[must_use]
    #[inline]
    pub unsafe fn iter<'g, T, C: IsElement<T>>(
        &'g self,
        pred: &'g mut Shield<T>,
        curr: &'g mut Shield<T>,
        is_detaching: bool,
        guard: &'g EpochGuard,
    ) -> Result<Iter<'g, T, C>, ShieldError> {
        curr.defend_fake(Shared::from(C::element_of(self) as *const _));

        Ok(Iter {
            guard,
            pred,
            curr,
            succ: self.next.load(Acquire, guard),
            is_detaching,
            is_stalled: false,
            _marker: PhantomData,
        })
    }
}

impl<T, C: IsElement<T>> List<T, C> {
    /// Returns a new, empty linked list.
    pub fn new() -> Self {
        Self {
            head: Entry::default(),
            _marker: PhantomData,
        }
    }

    /// Inserts `entry` into the head of the list.
    ///
    /// # Safety
    ///
    /// You should guarantee that:
    ///
    /// - `container` is not null
    /// - `container` is immovable, e.g. inside an `Owned`
    /// - the same `Entry` is not inserted more than once
    /// - the inserted object will be removed before the list is dropped
    pub unsafe fn insert<'g>(&'g self, container: Shared<'g, T>) {
        // Use of `unprotected()` is safe here because we are not dereferencing shared memory
        // locations.
        let guard = unprotected();

        // Insert right after head, i.e. at the beginning of the list.
        let to = &self.head.next;
        // Get the intrusively stored Entry of the new element to insert.
        let entry: &Entry = C::entry_of(container.deref());
        // Make a Shared ptr to that Entry.
        let entry_ptr = Shared::from(entry as *const _);
        // Read the current successor of where we want to insert.
        let mut next = to.load(Relaxed, guard);

        loop {
            // Set the Entry of the to-be-inserted element to point to the previous successor of
            // `to`.
            entry.next.store(next, Relaxed);
            match to.compare_and_set_weak(next, entry_ptr, Release, guard) {
                Ok(_) => break,
                // We lost the race or weak CAS failed spuriously. Update the successor and try
                // again.
                Err(err) => next = err.current,
            }
        }
    }

    /// Returns an iterator over all objects.
    ///
    /// # Caveat
    ///
    /// Every object that is inserted at the moment this function is called and persists at least
    /// until the end of iteration will be returned. Since this iterator traverses a lock-free
    /// linked list that may be concurrently modified, some additional caveats apply:
    ///
    /// 1. If a new object is inserted during iteration, it may or may not be returned.
    /// 2. If an object is deleted during iteration, it may or may not be returned.
    /// 3. The iteration may be aborted when it lost in a race condition. In this case, the winning
    ///    thread will continue to iterate over the same list.
    #[must_use]
    pub fn iter<'g>(
        &'g self,
        pred: &'g mut Shield<T>,
        curr: &'g mut Shield<T>,
        is_detaching: bool,
        guard: &'g EpochGuard,
    ) -> Result<Iter<'g, T, C>, ShieldError> {
        unsafe {
            // @PR(jeehoonkang): document why it's safe.
            self.head.iter(pred, curr, is_detaching, guard)
        }
    }
}

impl<T, C: IsElement<T>> Drop for List<T, C> {
    fn drop(&mut self) {
        unsafe {
            let guard = unprotected();
            let mut curr = self.head.next.load(Relaxed, guard);
            while let Some(c) = curr.as_ref() {
                let succ = c.next.load(Relaxed, guard);
                // Verify that all elements have been removed from the list.
                assert_eq!(succ.tag(), 1);

                C::finalize(curr.deref(), guard);
                curr = succ;
            }
        }
    }
}

impl<'g, T: 'g, C: IsElement<T>> Iterator for Iter<'g, T, C> {
    type Item = Result<*const T, IterError>;

    fn next(&mut self) -> Option<Self::Item> {
        // If the iteration was already stalled, return `IterError::Stalled`.
        if self.is_stalled {
            return Some(Err(IterError::Stalled));
        }

        while !self.succ.is_null() {
            debug_assert_eq!(self.succ.tag(), 0);

            // Move one step forward.
            mem::swap(&mut self.pred, &mut self.curr);
            if let Err(e) = self
                .curr
                .defend(unsafe { element_of_shared::<T, C>(self.succ) }, self.guard)
            {
                return Some(Err(IterError::ShieldError(e)));
            }

            let curr_ref = unsafe { self.curr.deref() };
            self.succ = C::entry_of(curr_ref).next.load(Acquire, self.guard);

            // Ignores a removed node if this iterator is not detaching it.
            if self.succ.tag() & 1 != 0 && !self.is_detaching {
                continue;
            }

            // Detaches a removed node.
            if self.succ.tag() & 1 != 0 && self.is_detaching {
                let succ = self.succ.with_tag(0);
                match C::entry_of(unsafe { self.pred.deref() })
                    .next
                    .compare_and_set(
                        unsafe { entry_of_shared::<T, C>(self.curr.shared()) },
                        succ,
                        AcqRel,
                        self.guard,
                    ) {
                    Ok(_) => {
                        // We succeeded in unlinking this element from the list, so we have to
                        // schedule deallocation. Deferred drop is okay, because `list.delete()` can
                        // only be called if `T: 'static`.
                        unsafe {
                            C::finalize(C::entry_of(&*self.curr.as_raw()), self.guard);
                        }

                        mem::swap(&mut self.pred, &mut self.curr);
                        self.succ = succ;
                    }
                    Err(e) => {
                        // A concurrent thread modified the predecessor node. If it is deleted, we need
                        // to restart from `head`.
                        if e.current.tag() != 0 {
                            self.pred.release();
                            self.curr.release();
                            self.is_stalled = true;
                            return Some(Err(IterError::Stalled));
                        }

                        mem::swap(&mut self.pred, &mut self.curr);
                        self.succ = e.current;
                    }
                }
                continue;
            }

            self.pred.release();
            return Some(Ok(unsafe { self.curr.deref() }));
        }

        // We reached the end of the list.
        self.pred.release();
        self.curr.release();
        None
    }
}

impl<'g, T: 'g, C: IsElement<T>> Drop for Iter<'g, T, C> {
    fn drop(&mut self) {
        self.pred.release();
        self.curr.release();
    }
}

/// Repeats executing the given function until getting an `Ok`.
#[inline]
#[must_use]
pub fn repeat_iter<F, R>(mut f: F) -> Result<R, ShieldError>
where
    F: FnMut() -> Result<R, IterError>,
{
    loop {
        match f() {
            Ok(r) => return Ok(r),
            Err(IterError::Stalled) => continue,
            Err(IterError::ShieldError(e)) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pebr_backend::{Collector, Owned};
    use crossbeam_utils::thread;
    use std::sync::Barrier;

    impl IsElement<Entry> for Entry {
        fn entry_of(entry: &Entry) -> &Entry {
            entry
        }

        unsafe fn element_of(entry: &Entry) -> &Entry {
            entry
        }

        unsafe fn finalize(entry: &Entry, guard: &EpochGuard) {
            guard.defer_destroy(Shared::from(Self::element_of(entry) as *const _));
        }
    }

    /// Checks whether the list retains inserted elements
    /// and returns them in the correct order.
    #[test]
    fn insert() {
        let collector = Collector::new();
        let handle = collector.register();
        let guard = handle.pin();

        let l: List<Entry> = List::new();

        let e1 = Owned::new(Entry::default()).into_shared(&guard);
        let e2 = Owned::new(Entry::default()).into_shared(&guard);
        let e3 = Owned::new(Entry::default()).into_shared(&guard);

        unsafe {
            l.insert(e1);
            l.insert(e2);
            l.insert(e3);
        }

        let mut pred = Shield::null(&guard);
        let mut curr = Shield::null(&guard);

        let mut iter = l.iter(&mut pred, &mut curr, true, &guard).unwrap();
        let maybe_e3 = iter.next();
        assert!(maybe_e3.is_some());
        assert!(maybe_e3.unwrap().unwrap() as *const Entry == e3.as_raw());
        let maybe_e2 = iter.next();
        assert!(maybe_e2.is_some());
        assert!(maybe_e2.unwrap().unwrap() as *const Entry == e2.as_raw());
        let maybe_e1 = iter.next();
        assert!(maybe_e1.is_some());
        assert!(maybe_e1.unwrap().unwrap() as *const Entry == e1.as_raw());
        assert!(iter.next().is_none());

        unsafe {
            e1.as_ref().unwrap().delete();
            e2.as_ref().unwrap().delete();
            e3.as_ref().unwrap().delete();
        }
    }

    /// Checks whether elements can be removed from the list and whether
    /// the correct elements are removed.
    #[test]
    fn delete() {
        let collector = Collector::new();
        let handle = collector.register();
        let guard = handle.pin();

        let l: List<Entry> = List::new();

        let e1 = Owned::new(Entry::default()).into_shared(&guard);
        let e2 = Owned::new(Entry::default()).into_shared(&guard);
        let e3 = Owned::new(Entry::default()).into_shared(&guard);
        unsafe {
            l.insert(e1);
            l.insert(e2);
            l.insert(e3);
            e2.as_ref().unwrap().delete();
        }

        let mut pred = Shield::null(&guard);
        let mut curr = Shield::null(&guard);

        {
            let mut iter = l.iter(&mut pred, &mut curr, true, &guard).unwrap();
            let maybe_e3 = iter.next();
            assert!(maybe_e3.is_some());
            assert!(maybe_e3.unwrap().unwrap() as *const Entry == e3.as_raw());
            let maybe_e1 = iter.next();
            assert!(maybe_e1.is_some());
            assert!(maybe_e1.unwrap().unwrap() as *const Entry == e1.as_raw());
            assert!(iter.next().is_none());
        }

        unsafe {
            e1.as_ref().unwrap().delete();
            e3.as_ref().unwrap().delete();
        }

        let mut iter = l.iter(&mut pred, &mut curr, true, &guard).unwrap();
        assert!(iter.next().is_none());
    }

    const THREADS: usize = 8;
    const ITERS: usize = 512;

    /// Contends the list on insert and delete operations to make sure they can run concurrently.
    #[test]
    fn insert_delete_multi() {
        let collector = Collector::new();

        let l: List<Entry> = List::new();
        let b = Barrier::new(THREADS);

        thread::scope(|s| {
            for _ in 0..THREADS {
                s.spawn(|_| {
                    b.wait();

                    let handle = collector.register();
                    let guard: EpochGuard = handle.pin();
                    let mut v = Vec::with_capacity(ITERS);

                    for _ in 0..ITERS {
                        let e = Owned::new(Entry::default()).into_shared(&guard);
                        v.push(e);
                        unsafe {
                            l.insert(e);
                        }
                    }

                    for e in v {
                        unsafe {
                            e.as_ref().unwrap().delete();
                        }
                    }
                });
            }
        })
        .unwrap();

        let handle = collector.register();
        let guard = handle.pin();

        let mut pred = Shield::null(&guard);
        let mut curr = Shield::null(&guard);

        let mut iter = l.iter(&mut pred, &mut curr, true, &guard).unwrap();
        assert!(iter.next().is_none());
    }

    /// Contends the list on iteration to make sure that it can be iterated over concurrently.
    #[test]
    fn iter_multi() {
        let collector = Collector::new();

        let l: List<Entry> = List::new();
        let b = Barrier::new(THREADS);

        thread::scope(|s| {
            for _ in 0..THREADS {
                s.spawn(|_| {
                    b.wait();

                    let handle = collector.register();
                    let mut guard: EpochGuard = handle.pin();
                    let mut v = Vec::with_capacity(ITERS);

                    for _ in 0..ITERS {
                        let e = Owned::new(Entry::default()).into_shared(unsafe { unprotected() });
                        v.push(e);
                        unsafe {
                            l.insert(e);
                        }
                    }

                    fn f(
                        l: &List<Entry>,
                        pred: &mut Shield<Entry>,
                        curr: &mut Shield<Entry>,
                        guard: &EpochGuard,
                    ) -> Result<(), ShieldError> {
                        let mut iter = l.iter(pred, curr, true, &guard)?;

                        for _ in 0..ITERS {
                            match iter.next() {
                                Some(Ok(_)) => {}
                                Some(Err(IterError::Stalled)) => break,
                                Some(Err(IterError::ShieldError(e))) => return Err(e),
                                None => panic!("Not enough elements"),
                            }
                        }

                        Ok(())
                    }

                    let mut pred = Shield::null(&guard);
                    let mut curr = Shield::null(&guard);
                    while let Err(_) = f(&l, &mut pred, &mut curr, &guard) {
                        guard.repin();
                    }

                    for e in v {
                        unsafe {
                            e.as_ref().unwrap().delete();
                        }
                    }
                });
            }
        })
        .unwrap();

        let handle = collector.register();
        let guard = handle.pin();

        let mut pred = Shield::null(&guard);
        let mut curr = Shield::null(&guard);
        let mut iter = l.iter(&mut pred, &mut curr, true, &guard).unwrap();
        assert!(iter.next().is_none());
    }
}
