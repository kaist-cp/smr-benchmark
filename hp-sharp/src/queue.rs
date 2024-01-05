use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem::MaybeUninit,
    ptr::null_mut,
    sync::atomic::{fence, AtomicBool, AtomicPtr, Ordering},
};

use crossbeam_utils::{Backoff, CachePadded};

type Item = crate::deferred::SealedBag;

pub struct Node {
    prev: *mut Node,
    next: AtomicPtr<Node>,
    item: MaybeUninit<Item>,
}

impl Node {
    fn sentinel() -> Self {
        Self {
            item: MaybeUninit::uninit(),
            prev: null_mut(),
            next: AtomicPtr::new(null_mut()),
        }
    }

    fn new(item: Item) -> Self {
        Self {
            item: MaybeUninit::new(item),
            prev: null_mut(),
            next: AtomicPtr::new(null_mut()),
        }
    }
}

pub struct DoubleLink {
    head: CachePadded<AtomicPtr<Node>>,
    tail: CachePadded<AtomicPtr<Node>>,
}

impl DoubleLink {
    pub fn new() -> Self {
        let sentinel = Box::into_raw(Box::new(Node::sentinel()));
        unsafe { (*sentinel).prev = sentinel };
        Self {
            head: CachePadded::new(AtomicPtr::new(sentinel)),
            tail: CachePadded::new(AtomicPtr::new(sentinel)),
        }
    }

    pub fn push(&self, item: Item) {
        HANDLE.with(|handle| self.push_internal(item, handle))
    }

    pub fn pop_if<F>(&self, pred: F) -> Option<Item>
    where
        F: Fn(&Item) -> bool,
    {
        HANDLE.with(|handle| self.pop_internal(pred, handle))
    }

    fn push_internal(&self, item: Item, handle: &LocalHandle) {
        let node = Box::into_raw(Box::new(Node::new(item)));
        let node_mut = unsafe { &mut *node };
        let backoff = Backoff::new();
        loop {
            let ltail = protect_link(&self.tail, handle);
            // A protection of `lprev` is not required, as a hazard pointer of `ltail`
            // protects adjacent nodes as well.
            let lprev = unsafe { &*(*ltail).prev };

            node_mut.prev = ltail;
            // Try to help the previous enqueue to complete.
            if lprev.next.load(Ordering::SeqCst).is_null() {
                lprev.next.store(ltail, Ordering::Relaxed);
            }
            if self
                .tail
                .compare_exchange(ltail, node, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                unsafe { &*ltail }.next.store(node, Ordering::Release);
                handle.reset_protection();
                return;
            }
            backoff.spin();
        }
    }

    fn pop_internal<F>(&self, pred: F, handle: &LocalHandle) -> Option<Item>
    where
        F: Fn(&Item) -> bool,
    {
        let backoff = Backoff::new();
        loop {
            let lhead = protect_link(&self.head, handle);
            let lnext = unsafe { &*lhead }.next.load(Ordering::Acquire);
            // Check if this queue is empty or the given predicate fails.
            if lnext.is_null() || !pred(unsafe { MaybeUninit::assume_init_ref(&(*lnext).item) }) {
                handle.reset_protection();
                return None;
            }

            if self
                .head
                .compare_exchange(lhead, lnext, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let item = unsafe { MaybeUninit::assume_init_read(&(*lnext).item) };
                handle.reset_protection();
                handle.retire(lhead, self.tail.load(Ordering::SeqCst));
                return Some(item);
            }
            backoff.spin();
        }
    }
}

fn protect_link(link: &AtomicPtr<Node>, handle: &LocalHandle) -> *mut Node {
    let mut ptr = link.load(Ordering::Relaxed);
    loop {
        handle.protect(ptr);
        fence(Ordering::SeqCst);
        let new_ptr = link.load(Ordering::Acquire);
        if ptr == new_ptr {
            return ptr;
        }
        ptr = new_ptr;
    }
}

/// The global data for a garbage collector.
struct Global {
    /// The intrusive linked list of `Local`s.
    locals: LocalList,
}

impl Global {
    const fn new() -> Self {
        Self {
            locals: LocalList::new(),
        }
    }

    fn register(&self) -> LocalHandle {
        LocalHandle::new(self.locals.acquire(self))
    }
}

struct Local {
    using: AtomicBool,
    next: AtomicPtr<Local>,
    bag: UnsafeCell<Vec<*mut Node>>,
    hazptr: AtomicPtr<Node>,
    global: *const Global,
}

impl Local {
    fn new(global: &Global) -> Self {
        Self {
            using: AtomicBool::new(true),
            next: AtomicPtr::new(null_mut()),
            bag: UnsafeCell::new(Vec::new()),
            hazptr: AtomicPtr::new(null_mut()),
            global,
        }
    }

    fn release(&self) {
        // Sync with `LocalList::acquire`.
        fence(Ordering::Release);
        self.using.store(false, Ordering::Relaxed);
    }
}

struct LocalHandle {
    local: *const Local,
}

impl LocalHandle {
    const RECL_PERIOD: usize = 8;

    fn new(local: &Local) -> Self {
        Self { local }
    }

    fn local(&self) -> &Local {
        unsafe { &*self.local }
    }

    fn global(&self) -> &Global {
        unsafe { &*self.local().global }
    }

    fn protect(&self, ptr: *mut Node) {
        self.local().hazptr.store(ptr, Ordering::Release)
    }

    fn reset_protection(&self) {
        self.local().hazptr.store(null_mut(), Ordering::Release)
    }

    fn retire(&self, ptr: *mut Node, ltail: *mut Node) {
        let bag = unsafe { &mut *self.local().bag.get() };
        bag.push(ptr);
        if bag.len() % Self::RECL_PERIOD == 0 {
            self.try_reclaim(bag, ltail);
        }
    }

    fn try_reclaim(&self, bag: &mut Vec<*mut Node>, ltail: *mut Node) {
        fence(Ordering::SeqCst);
        let mut guarded = self
            .global()
            .locals
            .iter_using()
            .map(|local| local.hazptr.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        guarded.sort();

        let is_guarded = |ptr: *mut Node| {
            let node = unsafe { &*ptr };
            guarded.binary_search(&ptr).is_ok()
                || guarded.binary_search(&node.prev).is_ok()
                || guarded
                    .binary_search(&node.next.load(Ordering::SeqCst))
                    .is_ok()
        };

        *bag = bag
            .drain(..)
            .filter_map(|d| {
                let tail_adj = unsafe { &*d }.next.load(Ordering::SeqCst) == ltail;
                if is_guarded(d) || tail_adj {
                    Some(d)
                } else {
                    unsafe { drop(Box::from_raw(d)) };
                    None
                }
            })
            .collect();
    }
}

impl Drop for LocalHandle {
    fn drop(&mut self) {
        unsafe {
            if let Some(local) = self.local.as_ref() {
                local.release();
            }
        }
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
    fn acquire<'c>(&'c self, global: &Global) -> &'c Local {
        let mut prev_link = &self.head;

        // Sync with `Local::release`.
        fence(Ordering::Acquire);
        let local = loop {
            match unsafe { prev_link.load(Ordering::Acquire).as_ref() } {
                Some(curr) => {
                    if curr
                        .using
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        break curr;
                    }
                    prev_link = &curr.next;
                }
                None => {
                    let new_local = Box::into_raw(Box::new(Local::new(global)));
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

static GLOBAL: Global = Global::new();

thread_local! {
    static HANDLE: LocalHandle = GLOBAL.register();
}
