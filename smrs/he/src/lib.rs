use std::cell::{Cell, RefCell, RefMut};
use std::mem::{align_of, replace, size_of, swap};
use std::ptr::null_mut;
use std::sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Mutex;

use crossbeam_utils::CachePadded;
use static_assertions::const_assert_eq;

const SLOTS_CAP: usize = 16;
static BATCH_CAP: AtomicUsize = AtomicUsize::new(128);

pub static GLOBAL_GARBAGE_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn set_batch_capacity(cap: usize) {
    BATCH_CAP.store(cap, Ordering::SeqCst);
}

fn batch_capacity() -> usize {
    BATCH_CAP.load(Ordering::Relaxed)
}

fn collect_period() -> usize {
    BATCH_CAP.load(Ordering::Relaxed) * 2
}

struct Node<T> {
    item: T,
    birth: usize,
    retire: Cell<usize>,
}

impl<T> Node<T> {
    fn new(item: T, birth: usize) -> Self {
        Self {
            item,
            birth,
            retire: Cell::new(0),
        }
    }
}

struct Retired {
    ptr: *mut Node<()>,
    lifespan: unsafe fn(ptr: *mut Node<()>) -> (usize, usize),
    deleter: unsafe fn(ptr: *mut Node<()>),
}

unsafe impl Send for Retired {}

impl Retired {
    fn new<T>(ptr: *mut Node<T>) -> Self {
        Self {
            ptr: ptr as *mut Node<()>,
            lifespan: lifespan::<T>,
            deleter: free::<T>,
        }
    }

    unsafe fn execute(self) {
        (self.deleter)(self.ptr)
    }

    unsafe fn lifespan(&self) -> (usize, usize) {
        (self.lifespan)(self.ptr)
    }
}

unsafe fn lifespan<T>(ptr: *mut Node<()>) -> (usize, usize) {
    let node = &*(ptr as *mut Node<T>);
    (node.birth, node.retire.get())
}

unsafe fn free<T>(ptr: *mut Node<()>) {
    drop(Box::from_raw(ptr as *mut Node<T>))
}

pub(crate) struct RetiredList {
    head: AtomicPtr<RetiredListNode>,
}

struct RetiredListNode {
    retireds: Vec<Retired>,
    next: *const RetiredListNode,
}

impl RetiredList {
    pub(crate) const fn new() -> Self {
        Self {
            head: AtomicPtr::new(null_mut()),
        }
    }

    pub(crate) fn push(&self, retireds: Vec<Retired>) {
        let new = Box::leak(Box::new(RetiredListNode {
            retireds,
            next: null_mut(),
        }));

        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            new.next = head;
            match self
                .head
                .compare_exchange(head, new, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(head_new) => head = head_new,
            }
        }
    }

    pub(crate) fn pop_all(&self) -> Vec<Retired> {
        let mut cur = self.head.swap(null_mut(), Ordering::AcqRel);
        let mut retireds = Vec::new();
        while !cur.is_null() {
            let mut cur_box = unsafe { Box::from_raw(cur) };
            retireds.append(&mut cur_box.retireds);
            cur = cur_box.next.cast_mut();
        }
        retireds
    }
}

pub struct Domain {
    slots: Vec<CachePadded<[AtomicUsize; SLOTS_CAP]>>,
    batches: Vec<CachePadded<RefCell<Vec<Retired>>>>,
    global_batch: CachePadded<RetiredList>,
    era: CachePadded<AtomicUsize>,
    avail_hidx: Mutex<Vec<usize>>,
}

unsafe impl Sync for Domain {}
unsafe impl Send for Domain {}

impl Drop for Domain {
    fn drop(&mut self) {
        debug_assert_eq!(self.avail_hidx.lock().unwrap().len(), self.slots.len());
        let retireds = self
            .batches
            .iter_mut()
            .flat_map(|batch| batch.get_mut().drain(..))
            .chain(self.global_batch.pop_all().into_iter());
        for retired in retireds {
            unsafe { retired.execute() };
        }
    }
}

impl Domain {
    pub fn new(handles: usize) -> Self {
        Self {
            slots: (0..handles).map(|_| Default::default()).collect(),
            batches: (0..handles).map(|_| Default::default()).collect(),
            global_batch: CachePadded::new(RetiredList::new()),
            era: CachePadded::new(AtomicUsize::new(1)),
            avail_hidx: Mutex::new((0..handles).collect()),
        }
    }

    fn load_era(&self, order: Ordering) -> usize {
        self.era.load(order)
    }

    pub fn register(&self) -> Handle<'_> {
        Handle {
            domain: self,
            hidx: self.avail_hidx.lock().unwrap().pop().unwrap(),
            alloc_count: Cell::new(0),
            retire_count: Cell::new(0),
            avail_sidx: RefCell::new((0..SLOTS_CAP).collect()),
        }
    }

    fn iter_eras(&self) -> impl Iterator<Item = usize> + '_ {
        self.slots
            .iter()
            .flat_map(|slots| slots.iter().map(|slot| slot.load(Ordering::Acquire)))
    }
}

pub struct Handle<'d> {
    domain: &'d Domain,
    hidx: usize,
    alloc_count: Cell<usize>,
    retire_count: Cell<usize>,
    avail_sidx: RefCell<Vec<usize>>,
}

impl<'d> Handle<'d> {
    fn incr_alloc(&self) {
        let next_count = self.alloc_count.get() + 1;
        self.alloc_count.set(next_count);
        if next_count % batch_capacity() == 0 {
            self.domain.era.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub fn global_era(&self, order: Ordering) -> usize {
        self.domain.load_era(order)
    }

    fn slot(&self, idx: usize) -> &AtomicUsize {
        &self.domain.slots[self.hidx][idx]
    }

    fn batch_mut(&self) -> RefMut<'_, Vec<Retired>> {
        self.domain.batches[self.hidx].borrow_mut()
    }

    pub unsafe fn retire<T>(&self, ptr: Shared<T>) {
        let curr_era = self.global_era(Ordering::SeqCst);
        ptr.ptr.deref().retire.set(curr_era);
        let mut batch = self.batch_mut();
        batch.push(Retired::new(ptr.ptr.as_raw()));
        let count = self.retire_count.get() + 1;
        self.retire_count.set(count);

        if batch.len() >= batch_capacity() {
            GLOBAL_GARBAGE_COUNT.fetch_add(batch.len(), Ordering::AcqRel);
            self.domain
                .global_batch
                .push(replace(&mut *batch, Vec::with_capacity(batch_capacity())));
        }
        if count % collect_period() == 0 {
            self.collect();
        }
    }

    fn collect(&self) {
        let retireds = self.domain.global_batch.pop_all();
        let retireds_len = retireds.len();
        fence(Ordering::SeqCst);

        let mut eras = self.domain.iter_eras().collect::<Vec<_>>();
        eras.sort_unstable();

        let not_freed: Vec<_> = retireds
            .into_iter()
            .filter_map(|ret| {
                let (birth, retire) = unsafe { ret.lifespan() };
                let ge_idx = eras.partition_point(|&era| era < birth);
                if eras.len() != ge_idx && eras[ge_idx] <= retire {
                    Some(ret)
                } else {
                    unsafe { ret.execute() };
                    None
                }
            })
            .collect();

        let freed_count = retireds_len - not_freed.len();
        GLOBAL_GARBAGE_COUNT.fetch_sub(freed_count, Ordering::AcqRel);

        self.domain.global_batch.push(not_freed);
    }
}

impl<'d> Drop for Handle<'d> {
    fn drop(&mut self) {
        self.domain.avail_hidx.lock().unwrap().push(self.hidx);
    }
}

pub struct HazardEra<'d, 'h> {
    handle: &'h Handle<'d>,
    sidx: usize,
}

impl<'d, 'h> HazardEra<'d, 'h> {
    pub fn new(handle: &'h Handle<'d>) -> Self {
        let sidx = handle.avail_sidx.borrow_mut().pop().unwrap();
        Self { handle, sidx }
    }

    pub fn era(&self) -> &AtomicUsize {
        &self.handle.slot(self.sidx)
    }

    pub fn swap(h1: &mut Self, h2: &mut Self) {
        swap(&mut h1.sidx, &mut h2.sidx);
    }

    pub fn clear(&mut self) {
        self.handle.slot(self.sidx).store(0, Ordering::Release);
    }
}

impl<'d, 'h> Drop for HazardEra<'d, 'h> {
    fn drop(&mut self) {
        self.handle.avail_sidx.borrow_mut().push(self.sidx);
    }
}

pub struct Shared<T> {
    ptr: TaggedPtr<Node<T>>,
}

impl<'d, T> From<usize> for Shared<T> {
    fn from(value: usize) -> Self {
        Self {
            ptr: TaggedPtr::from(value as *const _),
        }
    }
}

impl<'d, T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self { ptr: self.ptr }
    }
}

impl<'d, T> Copy for Shared<T> {}

impl<'d, T> Shared<T> {
    pub fn new(item: T, handle: &Handle<'d>) -> Self {
        handle.incr_alloc();
        let era = handle.global_era(Ordering::SeqCst);
        Self {
            ptr: TaggedPtr::from(Box::into_raw(Box::new(Node::new(item, era)))),
        }
    }

    pub fn null() -> Self {
        Self {
            ptr: TaggedPtr::null(),
        }
    }

    pub fn tag(&self) -> usize {
        self.ptr.tag()
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn with_tag(&self, tag: usize) -> Self {
        Self::from_raw(self.ptr.with_tag(tag))
    }

    pub unsafe fn as_ref<'g>(&self) -> Option<&'g T> {
        self.ptr.as_ref().map(|node| &node.item)
    }

    pub unsafe fn deref<'g>(&self) -> &'g T {
        &self.ptr.deref().item
    }

    pub unsafe fn deref_mut<'g>(&mut self) -> &'g mut T {
        &mut self.ptr.deref_mut().item
    }

    fn from_raw(ptr: TaggedPtr<Node<T>>) -> Self {
        Self { ptr }
    }

    pub unsafe fn into_owned(self) -> T {
        Box::from_raw(self.ptr.as_raw()).item
    }

    /// Returns `true` if the two pointer values, including the tag values set by `with_tag`,
    /// are identical.
    pub fn ptr_eq(self, other: Self) -> bool {
        self.ptr.ptr_eq(other.ptr)
    }
}

pub struct Atomic<T> {
    link: atomic::Atomic<TaggedPtr<Node<T>>>,
}

const_assert_eq!(
    size_of::<atomic::Atomic<TaggedPtr<Node<u8>>>>(),
    size_of::<usize>()
);

unsafe impl<'d, T: Sync> Sync for Atomic<T> {}
unsafe impl<'d, T: Send> Send for Atomic<T> {}

impl<'d, T> Default for Atomic<T> {
    fn default() -> Self {
        Self::null()
    }
}

impl<'d, T> From<Shared<T>> for Atomic<T> {
    fn from(value: Shared<T>) -> Self {
        Self {
            link: atomic::Atomic::new(value.ptr),
        }
    }
}

impl<'d, T> Atomic<T> {
    pub fn new(init: T, handle: &Handle<'d>) -> Self {
        Self {
            link: atomic::Atomic::new(Shared::new(init, handle).ptr),
        }
    }

    pub fn null() -> Self {
        Self {
            link: atomic::Atomic::new(TaggedPtr::null()),
        }
    }

    pub fn load(&self, order: Ordering) -> Shared<T> {
        Shared::from_raw(self.link.load(order))
    }

    pub fn store(&self, ptr: Shared<T>, order: Ordering) {
        self.link.store(ptr.ptr, order);
    }

    pub fn fetch_or(&self, val: usize, order: Ordering) -> Shared<T> {
        let prev = unsafe { &*(&self.link as *const _ as *const AtomicUsize) }.fetch_or(val, order);
        Shared::from_raw(TaggedPtr::from(prev as *const _))
    }

    /// Loads and protects the pointer by the original CrystallineL's way.
    ///
    /// Hint: For traversal based structures where need to check tag, the guarnatee is similar to
    /// `load` + `HazardPointer::set`. For Tstack, the guarnatee is similar to
    /// `HazardPointer::protect`.
    pub fn protect<'h>(&self, he: &mut HazardEra<'d, 'h>) -> Shared<T> {
        let mut prev_era = he.era().load(Ordering::Relaxed);
        loop {
            // Somewhat similar to atomically load and protect in Hazard pointers.
            let ptr = self.link.load(Ordering::Acquire);
            let curr_era = he.handle.global_era(Ordering::Acquire);
            if curr_era == prev_era {
                return Shared::from_raw(ptr);
            }
            he.era().store(curr_era, Ordering::SeqCst);
            prev_era = curr_era;
        }
    }

    pub fn compare_exchange(
        &self,
        current: Shared<T>,
        new: Shared<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Shared<T>, CompareExchangeError<T>> {
        match self
            .link
            .compare_exchange(current.ptr, new.ptr, success, failure)
        {
            Ok(current) => Ok(Shared::from_raw(current)),
            Err(current) => Err(CompareExchangeError {
                new,
                current: Shared::from_raw(current),
            }),
        }
    }

    pub unsafe fn into_owned(self) -> T {
        Box::from_raw(self.link.into_inner().as_raw()).item
    }
}

pub struct CompareExchangeError<T> {
    pub new: Shared<T>,
    pub current: Shared<T>,
}

struct TaggedPtr<T> {
    ptr: *mut T,
}

impl<T> Default for TaggedPtr<T> {
    fn default() -> Self {
        Self { ptr: null_mut() }
    }
}

impl<T> Clone for TaggedPtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for TaggedPtr<T> {}

impl<T> From<*const T> for TaggedPtr<T> {
    fn from(value: *const T) -> Self {
        Self {
            ptr: value.cast_mut(),
        }
    }
}

impl<T> From<*mut T> for TaggedPtr<T> {
    fn from(value: *mut T) -> Self {
        Self { ptr: value }
    }
}

impl<T> TaggedPtr<T> {
    pub fn null() -> Self {
        Self { ptr: null_mut() }
    }

    pub fn is_null(&self) -> bool {
        self.as_raw().is_null()
    }

    pub fn tag(&self) -> usize {
        let ptr = self.ptr as usize;
        ptr & low_bits::<T>()
    }

    /// Converts this pointer to a raw pointer (without the tag).
    pub fn as_raw(&self) -> *mut T {
        let ptr = self.ptr as usize;
        (ptr & !low_bits::<T>()) as *mut T
    }

    pub fn with_tag(&self, tag: usize) -> Self {
        Self::from(with_tag(self.ptr, tag))
    }

    /// # Safety
    ///
    /// The pointer (without high and low tag bits) must be a valid location to dereference.
    pub unsafe fn deref<'g>(&self) -> &'g T {
        &*self.as_raw()
    }

    /// # Safety
    ///
    /// The pointer (without high and low tag bits) must be a valid location to dereference.
    pub unsafe fn deref_mut<'g>(&mut self) -> &'g mut T {
        &mut *self.as_raw()
    }

    /// # Safety
    ///
    /// The pointer (without high and low tag bits) must be a valid location to dereference.
    pub unsafe fn as_ref<'g>(&self) -> Option<&'g T> {
        if self.is_null() {
            None
        } else {
            Some(self.deref())
        }
    }

    /// Returns `true` if the two pointer values, including the tag values set by `with_tag`,
    /// are identical.
    pub fn ptr_eq(self, other: Self) -> bool {
        self.ptr == other.ptr
    }
}

/// Returns a bitmask containing the unused least significant bits of an aligned pointer to `T`.
const fn low_bits<T>() -> usize {
    (1 << align_of::<T>().trailing_zeros()) - 1
}

/// Returns the pointer with the given tag
fn with_tag<T>(ptr: *mut T, tag: usize) -> *mut T {
    ((ptr as usize & !low_bits::<T>()) | (tag & low_bits::<T>())) as *mut T
}
