use std::cell::{Cell, RefCell, RefMut, UnsafeCell};
use std::mem::{align_of, size_of, swap, ManuallyDrop};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicIsize, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Mutex;

use crossbeam_utils::CachePadded;
use static_assertions::const_assert_eq;

const SLOTS_CAP: usize = 16;
const CACHE_CAP: usize = 12;
static COLLECT_FREQ: AtomicUsize = AtomicUsize::new(110);

pub static GLOBAL_GARBAGE_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn set_collect_frequency(freq: usize) {
    COLLECT_FREQ.store(freq, Ordering::Relaxed);
}

const fn invalid_ptr<T>() -> *mut T {
    usize::MAX as *mut T
}

#[repr(C)]
struct Node<T> {
    item: T,
    info: UnsafeCell<NodeInfo<T>>,
}

impl<T> Node<T> {
    fn new(item: T, birth_era: usize) -> Self {
        Self {
            item,
            info: UnsafeCell::new(NodeInfo::alive(birth_era)),
        }
    }

    unsafe fn birth_era(&self) -> usize {
        (*self.info.get()).birth_era
    }

    unsafe fn next(&self) -> &AtomicPtr<Node<T>> {
        &(*self.info.get()).ret_info.ns.next
    }

    unsafe fn set_next(&self, ptr: *mut Node<T>, order: Ordering) {
        (*self.info.get()).ret_info.ns.next.store(ptr, order);
    }

    unsafe fn slot(&self) -> *mut AtomicPtr<Node<T>> {
        (*self.info.get()).ret_info.ns.slot
    }

    unsafe fn set_slot(&self, ptr: *mut AtomicPtr<Node<T>>) {
        (*(*self.info.get()).ret_info).ns.slot = ptr
    }

    unsafe fn batch_link(&self) -> *mut Node<T> {
        (*self.info.get()).ret_info.batch_link
    }

    unsafe fn set_batch_link(&self, ptr: *mut Node<T>) {
        (*(*self.info.get()).ret_info).batch_link = ptr;
    }

    unsafe fn refs(&self) -> &AtomicIsize {
        &(*self.info.get()).ret_info.rb.refs
    }

    unsafe fn batch_next(&self) -> *mut Node<T> {
        (*self.info.get()).ret_info.rb.batch_next
    }

    unsafe fn set_batch_next(&self, ptr: *mut Node<T>) {
        (*(*self.info.get()).ret_info).rb.batch_next = ptr;
    }

    #[inline]
    fn free_list(&self) {
        let mut curr = (self as *const Node<T>).cast_mut();
        unsafe {
            while let Some(curr_node) = curr.as_ref() {
                let mut start = curr_node.batch_link();
                curr = curr_node.next().load(Ordering::SeqCst);

                let mut count = 0;
                loop {
                    let obj = start;
                    start = (*obj).batch_next();
                    count += 1;
                    drop(Box::from_raw(obj));
                    if start.is_null() {
                        break;
                    }
                }
                GLOBAL_GARBAGE_COUNT.fetch_sub(count, Ordering::AcqRel);
            }
        }
    }
}

#[repr(C)]
union NodeInfo<T> {
    birth_era: usize,
    ret_info: ManuallyDrop<RetiredInfo<T>>,
}

#[repr(C)]
struct RetiredInfo<T> {
    ns: NextOrSlot<T>,
    batch_link: *mut Node<T>,
    rb: RefsOrBatNxt<T>,
}

#[repr(C)]
union NextOrSlot<T> {
    next: ManuallyDrop<AtomicPtr<Node<T>>>,
    slot: *mut AtomicPtr<Node<T>>,
}

#[repr(C)]
union RefsOrBatNxt<T> {
    refs: ManuallyDrop<AtomicIsize>,
    batch_next: *mut Node<T>,
}

impl<T> NodeInfo<T> {
    fn alive(birth_era: usize) -> Self {
        Self { birth_era }
    }
}

struct Batch<T> {
    min_era: usize,
    first: *mut Node<T>,
    last: *mut Node<T>,
    count: usize,
    list_count: usize,
    list: *mut Node<T>,
}

impl<T> Default for Batch<T> {
    fn default() -> Self {
        Self {
            min_era: 0,
            first: null_mut(),
            last: null_mut(),
            count: 0,
            list_count: 0,
            list: null_mut(),
        }
    }
}

impl<T> Batch<T> {
    fn traverse(&mut self, next: *mut Node<T>) {
        let mut curr = next;
        unsafe {
            while let Some(curr_node) = curr.as_ref() {
                curr = curr_node.next().load(Ordering::Acquire);
                let refs = &*curr_node.batch_link();
                if refs.refs().fetch_sub(1, Ordering::AcqRel) == 1 {
                    // Here, we have exclusive ownership.
                    refs.set_next(self.list, Ordering::SeqCst);
                    self.list = refs as *const _ as *mut _;
                }
            }
        }
    }

    fn traverse_cache(&mut self, next: *mut Node<T>) {
        if next.is_null() {
            return;
        }
        if self.list_count == CACHE_CAP {
            if let Some(list) = unsafe { self.list.as_ref() } {
                list.free_list();
            }
            self.list = null_mut();
            self.list_count = 0;
        }
        self.list_count += 1;
        self.traverse(next);
    }
}

#[repr(C)]
struct EraSlots<T> {
    first: [AtomicPtr<Node<T>>; SLOTS_CAP],
    era: [AtomicUsize; SLOTS_CAP],
}

// Some static checks for a hack in `try_retire`.
const_assert_eq!(size_of::<AtomicPtr<Node<u8>>>(), size_of::<AtomicUsize>());
const_assert_eq!(
    size_of::<EraSlots<u8>>(),
    size_of::<AtomicUsize>() * SLOTS_CAP * 2
);

impl<T> Default for EraSlots<T> {
    fn default() -> Self {
        // Derive macro requires `T: Default`, which is unnecessary.
        Self {
            first: [(); SLOTS_CAP].map(|_| AtomicPtr::new(invalid_ptr())),
            era: Default::default(),
        }
    }
}

pub struct Domain<T> {
    slots: Vec<CachePadded<EraSlots<T>>>,
    batches: Vec<CachePadded<RefCell<Batch<T>>>>,
    era: CachePadded<AtomicUsize>,
    avail_hidx: Mutex<Vec<usize>>,
}

unsafe impl<T> Sync for Domain<T> {}
unsafe impl<T> Send for Domain<T> {}

impl<T> Drop for Domain<T> {
    fn drop(&mut self) {
        debug_assert_eq!(self.avail_hidx.lock().unwrap().len(), self.slots.len());
        let handles = (0..self.slots.len())
            .map(|_| self.register())
            .collect::<Vec<_>>();
        for handle in handles {
            unsafe { handle.clear_all() };
            let batch = handle.batch_mut();
            if !batch.first.is_null() {
                let mut curr = batch.first;
                while curr != batch.last {
                    let next = unsafe { (*curr).batch_next() };
                    unsafe { drop(Box::from_raw(curr)) };
                    curr = next;
                }
                unsafe { drop(Box::from_raw(batch.last)) };
            }
        }
    }
}

impl<T> Domain<T> {
    pub fn new(handles: usize) -> Self {
        Self {
            slots: (0..handles).map(|_| Default::default()).collect(),
            batches: (0..handles).map(|_| Default::default()).collect(),
            era: CachePadded::new(AtomicUsize::new(1)),
            avail_hidx: Mutex::new((0..handles).collect()),
        }
    }

    fn load_era(&self) -> usize {
        self.era.load(Ordering::Acquire)
    }

    pub fn register(&self) -> Handle<'_, T> {
        Handle {
            domain: self,
            hidx: self.avail_hidx.lock().unwrap().pop().unwrap(),
            alloc_count: Cell::new(0),
            avail_sidx: RefCell::new((0..SLOTS_CAP).collect()),
        }
    }
}

pub struct Handle<'d, T> {
    domain: &'d Domain<T>,
    hidx: usize,
    alloc_count: Cell<usize>,
    avail_sidx: RefCell<Vec<usize>>,
}

impl<'d, T> Handle<'d, T> {
    fn incr_alloc(&self) {
        let next_count = self.alloc_count.get() + 1;
        self.alloc_count.set(next_count);
        if next_count % COLLECT_FREQ.load(Ordering::Relaxed) == 0 {
            self.domain.era.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub fn global_era(&self) -> usize {
        self.domain.load_era()
    }

    fn slots(&self) -> &EraSlots<T> {
        &self.domain.slots[self.hidx]
    }

    fn batch_mut(&self) -> RefMut<'_, Batch<T>> {
        self.domain.batches[self.hidx].borrow_mut()
    }

    pub unsafe fn retire(&self, ptr: Shared<T>) {
        debug_assert!(!ptr.is_null());
        let node = unsafe { ptr.ptr.deref() };
        let mut batch = self.batch_mut();
        if batch.first.is_null() {
            batch.min_era = unsafe { node.birth_era() };
            batch.last = node as *const _ as *mut _;
        } else {
            let birth_era = unsafe { node.birth_era() };
            if batch.min_era > birth_era {
                batch.min_era = birth_era;
            }
            unsafe { node.set_batch_link(batch.last) };
        }

        // Implicitly initialize refs to 0 for the last node.
        unsafe { node.set_batch_next(batch.first) };

        let node_ptr = node as *const _ as *mut Node<T>;
        batch.first = node_ptr;
        batch.count += 1;
        if batch.count % COLLECT_FREQ.load(Ordering::Relaxed) == 0 {
            unsafe { (*batch.last).set_batch_link(node_ptr) };
            self.try_retire(batch);
        }
    }

    fn try_retire(&self, mut batch: RefMut<'_, Batch<T>>) {
        let mut curr = batch.first;
        let refs = batch.last;
        let min_era = batch.min_era;

        // Find available slots.
        let mut last = curr;
        for slot in &self.domain.slots {
            for i in 0..SLOTS_CAP {
                let first = slot.first[i].load(Ordering::Acquire);
                if first == invalid_ptr() {
                    continue;
                }
                let era = slot.era[i].load(Ordering::Acquire);
                if era < min_era {
                    continue;
                }
                if last == refs {
                    return;
                }
                unsafe { (*last).set_slot(&slot.first[i] as *const _ as *mut _) };
                last = unsafe { (*last).batch_next() };
            }
        }

        // Retire if successful.
        GLOBAL_GARBAGE_COUNT.fetch_add(batch.count, Ordering::AcqRel);
        let mut adjs = 0;
        while curr != last {
            'body: {
                unsafe {
                    let slot_first = (*curr).slot();
                    // HACK: Get a corresponding era. Hint: See the layout of `EraSlots<T>`...
                    let slot_era = slot_first.offset(SLOTS_CAP as isize) as *mut AtomicUsize;
                    let mut prev = (*slot_first).load(Ordering::Acquire);

                    loop {
                        if prev == invalid_ptr() {
                            break 'body;
                        }
                        let era = (*slot_era).load(Ordering::Acquire);
                        if era < min_era {
                            break 'body;
                        }
                        (*curr).set_next(prev, Ordering::Relaxed);
                        match (*slot_first).compare_exchange_weak(
                            prev,
                            curr,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => break,
                            Err(new) => prev = new,
                        }
                    }
                    adjs += 1;
                }
            }
            curr = unsafe { (*curr).batch_next() };
        }

        // Adjust the reference count.
        unsafe {
            if (*refs).refs().fetch_add(adjs, Ordering::AcqRel) == -adjs {
                (*refs).set_next(null_mut(), Ordering::SeqCst);
                (*refs).free_list();
            }
        }
        batch.first = null_mut();
        batch.count = 0;
    }

    pub unsafe fn clear_all(&self) {
        let mut first = [null_mut(); SLOTS_CAP];
        for i in 0..SLOTS_CAP {
            first[i] = self.slots().first[i].swap(invalid_ptr(), Ordering::AcqRel);
        }
        for i in 0..SLOTS_CAP {
            if first[i] != invalid_ptr() {
                self.batch_mut().traverse(first[i]);
            }
        }
        let mut batch = self.batch_mut();
        if let Some(list) = unsafe { batch.list.as_ref() } {
            list.free_list();
        }
        batch.list = null_mut();
        batch.list_count = 0;
    }
}

impl<'d, T> Drop for Handle<'d, T> {
    fn drop(&mut self) {
        self.domain.avail_hidx.lock().unwrap().push(self.hidx);
    }
}

pub struct HazardEra<'d, 'h, T> {
    handle: &'h Handle<'d, T>,
    sidx: usize,
}

impl<'d, 'h, T> HazardEra<'d, 'h, T> {
    pub fn new(handle: &'h Handle<'d, T>) -> Self {
        let sidx = handle.avail_sidx.borrow_mut().pop().unwrap();
        Self { handle, sidx }
    }

    pub fn era(&self) -> usize {
        self.handle.slots().era[self.sidx].load(Ordering::Acquire)
    }

    pub fn update_era(&mut self, mut curr_era: usize) -> usize {
        let first_link = &self.handle.slots().first[self.sidx];
        if !first_link.load(Ordering::Acquire).is_null() {
            let first = first_link.swap(invalid_ptr(), Ordering::AcqRel);
            if first != invalid_ptr() {
                self.handle.batch_mut().traverse_cache(first);
            }
            first_link.store(null_mut(), Ordering::SeqCst);
            curr_era = self.handle.global_era();
        }
        self.handle.slots().era[self.sidx].store(curr_era, Ordering::SeqCst);
        return curr_era;
    }

    pub fn swap(h1: &mut Self, h2: &mut Self) {
        swap(&mut h1.sidx, &mut h2.sidx);
    }

    pub fn clear(&mut self) {
        self.handle.slots().era[self.sidx].store(0, Ordering::Release);
    }
}

impl<'d, 'h, T> Drop for HazardEra<'d, 'h, T> {
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
    pub fn new(item: T, handle: &Handle<'d, T>) -> Self {
        handle.incr_alloc();
        let era = handle.global_era();
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

// Technically, `Atomic` and `Shared` should depend on the lifetime of the domain `'d`,
// but it can create a circular lifetime dependency because it will make `T` in `Domain<T>`
// also depend on `'d`.
pub struct Atomic<T> {
    link: atomic::Atomic<TaggedPtr<Node<T>>>,
}

const_assert_eq!(
    size_of::<atomic::Atomic<TaggedPtr<Node<u8>>>>(),
    size_of::<usize>()
);

unsafe impl<T: Sync> Sync for Atomic<T> {}
unsafe impl<T: Send> Send for Atomic<T> {}

impl<T> Default for Atomic<T> {
    fn default() -> Self {
        Self::null()
    }
}

impl<T> From<Shared<T>> for Atomic<T> {
    fn from(value: Shared<T>) -> Self {
        Self {
            link: atomic::Atomic::new(value.ptr),
        }
    }
}

impl<T> Atomic<T> {
    pub fn new(init: T, handle: &Handle<T>) -> Self {
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
    pub fn protect<'d, 'h>(&self, he: &mut HazardEra<'d, 'h, T>) -> Shared<T> {
        let mut prev_era = he.era();
        loop {
            // Somewhat similar to atomically load and protect in Hazard pointers.
            let ptr = self.link.load(Ordering::Acquire);
            let curr_era = he.handle.global_era();
            if curr_era == prev_era {
                return Shared::from_raw(ptr);
            }
            prev_era = he.update_era(curr_era);
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
