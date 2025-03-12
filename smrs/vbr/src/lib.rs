use std::{
    cell::RefCell, collections::VecDeque, fmt::Display, marker::PhantomData, mem::align_of,
    ops::Deref, ptr::null_mut, sync::atomic::AtomicU64,
};

use atomic::{Atomic, Ordering};
use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;
use portable_atomic::{compiler_fence, AtomicU128, AtomicUsize};

static ENTRIES_PER_BAG: AtomicUsize = AtomicUsize::new(128);
pub const INIT_BAGS_PER_LOCAL: usize = 1;
pub const NOT_RETIRED: u64 = u64::MAX;

/// Sets the capacity of thread-local garbage bag.
///
/// This value applies to all threads.
#[inline]
pub fn set_bag_capacity(cap: usize) {
    assert!(cap > 1, "capacity must be greater than 1.");
    ENTRIES_PER_BAG.store(cap, Ordering::Relaxed);
}

/// Returns the current capacity of thread-local garbage bag.
#[inline]
pub fn bag_capacity() -> usize {
    ENTRIES_PER_BAG.load(Ordering::Relaxed)
}

#[derive(Default)]
pub struct Inner<T> {
    birth: AtomicU64,
    retire: AtomicU64,
    data: T,
}

impl<T> Deref for Inner<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

pub struct Global<T: Default> {
    epoch: CachePadded<AtomicU64>,
    avail: SegQueue<*mut Bag<Inner<T>>>,
}

unsafe impl<T: Default> Sync for Global<T> {}
unsafe impl<T: Default> Send for Global<T> {}

impl<T: Default> Global<T> {
    pub fn new(capacity: usize) -> Self {
        let avail = SegQueue::new();
        let count = capacity / bag_capacity() + if capacity % bag_capacity() > 0 { 1 } else { 0 };
        for _ in 0..count {
            avail.push(Box::into_raw(Box::new(Bag::new_with_alloc())));
        }
        Self {
            epoch: CachePadded::new(AtomicU64::new(1)),
            avail,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    pub fn advance(&self, expected: u64) -> Result<u64, u64> {
        match self.epoch.compare_exchange(
            expected,
            expected + 1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => Ok(expected + 1),
            Err(_) => Err(expected),
        }
    }

    pub fn acquire(&self) -> *mut Bag<Inner<T>> {
        loop {
            if let Some(bag) = self.avail.pop() {
                return bag;
            } else {
                self.avail
                    .push(Box::into_raw(Box::new(Bag::new_with_alloc())));
            }
        }
    }

    pub fn retire(&self, bag: *mut Bag<Inner<T>>) {
        self.avail.push(bag);
    }
}

pub struct Bag<T> {
    entries: VecDeque<*mut T>,
}

impl<T: Default> Bag<T> {
    fn new() -> Self {
        Self {
            entries: VecDeque::with_capacity(bag_capacity()),
        }
    }

    fn new_with_alloc() -> Self {
        let mut entries = vec![null_mut(); bag_capacity()];
        for ptr in &mut entries {
            *ptr = Box::into_raw(Box::new(T::default()));
        }
        Self {
            entries: entries.into(),
        }
    }

    fn push(&mut self, obj: *mut T) -> bool {
        if self.entries.len() < bag_capacity() {
            self.entries.push_back(obj);
            return true;
        }
        false
    }

    fn pop(&mut self) -> Option<*mut T> {
        self.entries.pop_front()
    }
}

pub struct Local<T: Default> {
    global: *const Global<T>,
    avail: RefCell<VecDeque<*mut Bag<Inner<T>>>>,
    retired: RefCell<VecDeque<*mut Bag<Inner<T>>>>,
}

impl<T: Default> Local<T> {
    fn global(&self) -> &Global<T> {
        unsafe { &*self.global }
    }

    pub fn new(global: &Global<T>) -> Self {
        let mut avail = VecDeque::with_capacity(INIT_BAGS_PER_LOCAL);
        avail.resize_with(INIT_BAGS_PER_LOCAL, || {
            Box::into_raw(Box::new(Bag::new_with_alloc()))
        });
        let mut retired = VecDeque::new();
        retired.push_back(Box::into_raw(Box::new(Bag::new())));
        Self {
            global,
            avail: RefCell::new(avail),
            retired: RefCell::new(retired),
        }
    }

    fn pop_avail(&self) -> *mut Inner<T> {
        loop {
            // Try acquiring an available slot from a thread-local bag.
            loop {
                let bag = match self.avail.borrow().front() {
                    Some(bag) => *bag,
                    None => break,
                };
                let bag_ref = unsafe { &mut *bag };
                if let Some(item) = bag_ref.pop() {
                    return item;
                } else {
                    self.avail.borrow_mut().pop_front();
                    unsafe { drop(Box::from_raw(bag)) };
                }
            }

            // Acquire some fresh bags from the global and try again.
            self.avail
                .borrow_mut()
                .resize_with(INIT_BAGS_PER_LOCAL, || self.global().acquire());
        }
    }

    fn return_avail(&self, inner: *mut Inner<T>) {
        let bag = *self.avail.borrow().front().unwrap();
        let bag_ref = unsafe { &mut *bag };
        bag_ref.push(inner);
    }

    fn push_retired(&self, inner: *mut Inner<T>) {
        // Try find an available slot from a thread-local bag.
        loop {
            let bag = match self.retired.borrow().front() {
                Some(bag) => *bag,
                None => break,
            };
            let bag_ref = unsafe { &mut *bag };
            if bag_ref.push(inner) {
                return;
            } else {
                self.retired.borrow_mut().pop_front();
                self.global().retire(bag);
            }
        }

        // Create a fresh bag to store a node.
        let mut bag = Box::new(Bag::new());
        bag.push(inner);
        self.retired.borrow_mut().push_back(Box::into_raw(bag));
    }

    pub fn guard(&self) -> Guard<T> {
        Guard {
            local: self,
            epoch: self.global().epoch(),
        }
    }
}

impl<T: Default> Drop for Local<T> {
    fn drop(&mut self) {
        for bag in self
            .retired
            .borrow_mut()
            .drain(..)
            .chain(self.avail.borrow_mut().drain(..))
        {
            if unsafe { &*bag }.entries.len() == 0 {
                unsafe { drop(Box::from_raw(bag)) };
            } else {
                self.global().retire(bag);
            }
        }
    }
}

pub struct Guard<T: Default> {
    local: *const Local<T>,
    epoch: u64,
}

impl<T: Default> Guard<T> {
    fn global(&self) -> &Global<T> {
        unsafe { &*self.local().global }
    }

    fn local(&self) -> &Local<T> {
        unsafe { &*self.local }
    }

    pub fn refresh(&mut self) {
        self.epoch = self.global().epoch();
    }

    pub fn allocate<'g, F>(&'g self, mut init: F) -> Result<Shared<'g, T>, ()>
    where
        F: FnMut(Shared<'g, T>),
    {
        let ptr = self.local().pop_avail();
        debug_assert!(!ptr.is_null());
        let slot_ref = unsafe { &*ptr };
        // If the retire epoch is (greater than or) equal to the current epoch,
        // try advance the global epoch.
        if self.epoch <= slot_ref.retire.load(Ordering::SeqCst) {
            self.local().return_avail(ptr);
            let _ = self.global().advance(self.epoch);
            return Err(());
        }

        debug_assert!(self.epoch > slot_ref.birth.load(Ordering::SeqCst));

        slot_ref.birth.store(self.epoch, Ordering::SeqCst);
        slot_ref.retire.store(NOT_RETIRED, Ordering::SeqCst);
        let result = Shared {
            ptr,
            birth: self.epoch,
            _marker: PhantomData,
        };
        init(result);
        Ok(result)
    }

    /// # Safety
    ///
    /// The current thread must conceptually have exclusive permission to retire this pointer
    /// (e.g., after successful physical deletion in a lock-free data structure).
    pub unsafe fn retire(&self, ptr: Shared<T>) {
        let inner = ptr.as_inner().expect("Attempted to retire a null pointer.");
        if inner.birth.load(Ordering::SeqCst) > ptr.birth {
            // It is already retired and reclaimed.
            return;
        }
        self.retire_raw(inner as *const _ as *mut _);
    }

    /// # Safety
    ///
    /// The pointee must not be relcaimed yet, and the current thread must conceptually have exclusive
    /// permission to retire this pointer (e.g., after successful physical deletion in a lock-free
    /// data structure).
    pub unsafe fn retire_raw(&self, ptr: *mut Inner<T>) {
        let inner = unsafe { &*ptr };
        if inner.retire.load(Ordering::SeqCst) != NOT_RETIRED {
            return;
        }

        let curr_epoch = self.global().epoch();
        inner.retire.store(curr_epoch, Ordering::SeqCst);
        self.local()
            .push_retired((inner as *const Inner<T>).cast_mut());
    }

    pub fn validate_epoch(&self) -> Result<(), ()> {
        if self.epoch == self.global().epoch() {
            Ok(())
        } else {
            Err(())
        }
    }
}

pub struct Shared<'g, T> {
    ptr: *mut Inner<T>,
    birth: u64,
    _marker: PhantomData<&'g T>,
}

impl<'g, T> Shared<'g, T> {
    pub unsafe fn deref(&self) -> &'g T {
        &unsafe { &*ptr_with_tag(self.ptr, 0) }.data
    }

    pub fn as_ref(&self) -> Option<&'g T> {
        self.as_inner().map(|inner| &inner.data)
    }

    fn as_inner(&self) -> Option<&'g Inner<T>> {
        unsafe { ptr_with_tag(self.ptr, 0).as_ref() }
    }

    pub fn validate_birth_epoch(&self) -> bool {
        let Some(inner) = self.as_inner() else {
            return true;
        };
        inner.birth.load(Ordering::SeqCst) == self.birth
    }

    pub fn birth(&self) -> u64 {
        self.birth
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn null() -> Self {
        Self {
            ptr: null_mut(),
            birth: 0,
            _marker: PhantomData,
        }
    }

    pub fn tag(&self) -> usize {
        decompose_ptr(self.ptr).1
    }

    pub fn with_tag(&self, tag: usize) -> Self {
        Self {
            ptr: ptr_with_tag(self.ptr, tag),
            birth: self.birth,
            _marker: PhantomData,
        }
    }

    pub fn as_raw(&self) -> *mut Inner<T> {
        self.ptr
    }
}

impl<'g, T> Clone for Shared<'g, T> {
    fn clone(&self) -> Self {
        Self { ..*self }
    }
}

impl<'g, T> Copy for Shared<'g, T> {}

impl<'g, T> PartialEq for Shared<'g, T> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr && self.birth == other.birth
    }
}

impl<'g, T> Display for Shared<'g, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{:p}, {}>", self.ptr, self.birth)
    }
}

pub struct MutAtomic<T> {
    link: AtomicU128,
    _marker: PhantomData<T>,
}

impl<T> Default for MutAtomic<T> {
    fn default() -> Self {
        Self {
            link: AtomicU128::new(0),
            _marker: PhantomData,
        }
    }
}

unsafe impl<T> Sync for MutAtomic<T> {}
unsafe impl<T> Send for MutAtomic<T> {}

impl<T: Default> MutAtomic<T> {
    pub fn null() -> Self {
        Self {
            link: AtomicU128::new(0),
            _marker: PhantomData,
        }
    }

    pub fn load<'g>(&self, order: Ordering, guard: &'g Guard<T>) -> Result<Shared<'g, T>, ()> {
        let result = unsafe { self.load_unchecked(order) };
        compiler_fence(Ordering::SeqCst);
        guard.validate_epoch()?;
        Ok(result)
    }

    pub unsafe fn load_unchecked<'g>(&self, order: Ordering) -> Shared<'g, T> {
        let (_, ptr) = decompose_u128::<Inner<T>>(self.link.load(order));
        let birth = if let Some(ver) = unsafe { ptr_with_tag(ptr, 0).as_ref() } {
            ver.birth.load(Ordering::SeqCst)
        } else {
            0
        };
        Shared {
            ptr,
            birth,
            _marker: PhantomData,
        }
    }

    pub fn compare_exchange(
        &self,
        owner: Shared<T>,
        current: Shared<T>,
        new: Shared<T>,
        success: Ordering,
        failure: Ordering,
        _: &Guard<T>,
    ) -> CompareExchangeError<T> {
        if !owner.validate_birth_epoch() {
            return CompareExchangeError::Reallocated;
        }
        let curr = compose_u128(owner.birth.max(current.birth), current.as_raw());
        let next = compose_u128(owner.birth.max(new.birth), new.as_raw());
        match self.link.compare_exchange(curr, next, success, failure) {
            Ok(comp) => CompareExchangeError::Success(decompose_u128(comp)),
            Err(comp) => CompareExchangeError::Failure(decompose_u128(comp)),
        }
    }

    /// # Safety
    ///
    /// It is allowed only in an `allocate` of an owning object.
    pub unsafe fn store(&self, owner: Shared<T>, new: Shared<T>) {
        let curr = self.link.load(Ordering::SeqCst);
        let next = compose_u128(owner.birth.max(new.birth), new.as_raw());
        self.link
            .compare_exchange(curr, next, Ordering::SeqCst, Ordering::SeqCst)
            .unwrap();
    }
}

pub enum CompareExchangeError<T> {
    Success(VerPtr<Inner<T>>),
    Failure(VerPtr<Inner<T>>),
    Reallocated,
}

impl<T> CompareExchangeError<T> {
    pub fn success(&self) -> Result<VerPtr<Inner<T>>, ()> {
        if let Self::Success(vp) = self {
            Ok(*vp)
        } else {
            Err(())
        }
    }
}

pub struct Entry<T> {
    link: *mut Inner<T>,
}

unsafe impl<T> Sync for Entry<T> {}
unsafe impl<T> Send for Entry<T> {}

impl<T: Default> Entry<T> {
    pub fn new(init: Shared<T>) -> Self {
        Self {
            link: init.as_raw(),
        }
    }

    pub fn load(&self, guard: &Guard<T>) -> Result<Shared<T>, ()> {
        let ptr = self.link;
        if let Some(ver) = unsafe { ptr_with_tag(ptr, 0).as_ref() } {
            let birth = ver.birth.load(Ordering::SeqCst);
            compiler_fence(Ordering::SeqCst);
            guard.validate_epoch()?;
            Ok(Shared {
                ptr,
                birth,
                _marker: PhantomData,
            })
        } else {
            Ok(Shared {
                ptr,
                birth: 0,
                _marker: PhantomData,
            })
        }
    }

    pub fn load_raw(&self) -> *mut Inner<T> {
        self.link
    }
}

pub struct ImmAtomic<T: Copy> {
    data: Atomic<T>,
}

unsafe impl<T: Copy> Sync for ImmAtomic<T> {}
unsafe impl<T: Copy> Send for ImmAtomic<T> {}

impl<T: Copy + Default> ImmAtomic<T> {
    pub fn new(v: T) -> Self {
        Self {
            data: Atomic::new(v),
        }
    }

    pub fn get<G: Default>(&self, guard: &Guard<G>) -> Result<T, ()> {
        let value = unsafe { self.get_unchecked() };
        compiler_fence(Ordering::SeqCst);
        guard.validate_epoch()?;
        Ok(value)
    }

    pub unsafe fn get_unchecked(&self) -> T {
        self.data.load(Ordering::SeqCst)
    }

    /// # Safety
    ///
    /// It is allowed only in an `allocate` of an owning object.
    pub unsafe fn set(&self, v: T) {
        self.data.store(v, Ordering::SeqCst);
    }
}

fn compose_u128<T>(meta: u64, ptr: *mut T) -> u128 {
    ((meta as u128) << 64) | (ptr as usize as u128)
}

fn decompose_u128<T>(value: u128) -> VerPtr<T> {
    let meta = (value >> 64) as u64;
    let ptr = (value & (u64::MAX as u128)) as usize as *mut T;
    (meta, ptr)
}

pub type VerPtr<T> = (u64, *mut T);

/// Returns a bitmask containing the unused least significant bits of an aligned pointer to `T`.
#[inline]
pub fn low_bits<T>() -> usize {
    (1 << align_of::<T>().trailing_zeros()) - 1
}

/// Given a tagged pointer `data`, returns the same pointer, but tagged with `tag`.
///
/// `tag` is truncated to fit into the unused bits of the pointer to `T`.
#[inline]
pub fn ptr_with_tag<T>(ptr: *mut T, tag: usize) -> *mut T {
    ((ptr as usize & !low_bits::<T>()) | (tag & low_bits::<T>())) as _
}

/// Decomposes a tagged pointer `data` into the pointer and the tag.
#[inline]
pub fn decompose_ptr<T>(ptr: *mut T) -> (*mut T, usize) {
    let raw = ((ptr as usize) & !low_bits::<T>()) as *mut T;
    let tag = (ptr as usize) & low_bits::<T>();
    (raw, tag)
}
