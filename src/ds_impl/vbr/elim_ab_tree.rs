use super::concurrent_map::ConcurrentMap;
use arrayvec::ArrayVec;
use vbr::{Entry, Global, Guard, Inner, Local};

use std::cell::{Cell, UnsafeCell};
use std::hint::spin_loop;
use std::iter::once;
use std::ptr::{eq, null, null_mut};
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};

// Copied from the original author's code:
// https://gitlab.com/trbot86/setbench/-/blob/f4711af3ace28d8b4fa871559db74fb4e0e62cc0/ds/srivastava_abtree_mcs/adapter.h#L17
const DEGREE: usize = 11;

macro_rules! try_acq_val_or {
    ($node:ident, $lock:ident, $op:expr, $key:expr, $guard:ident, $acq_val_err:expr, $epoch_val_err:expr) => {
        let __slot = UnsafeCell::new(MCSLockSlot::new());
        let $lock = match (
            $node.acquire($op, $key, &__slot),
            $node.marked.load(Ordering::Acquire),
        ) {
            (AcqResult::Acquired(lock), false) => lock,
            _ => $acq_val_err,
        };
        if $guard.validate_epoch().is_err() {
            $epoch_val_err;
        }
    };
}

struct MCSLockSlot<K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    node: *const Node<K, V>,
    op: Operation,
    key: Option<K>,
    next: AtomicPtr<Self>,
    owned: AtomicBool,
    short_circuit: AtomicBool,
    ret: Cell<Option<V>>,
}

impl<K, V> MCSLockSlot<K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    fn new() -> Self {
        Self {
            node: null(),
            op: Operation::Insert,
            key: Default::default(),
            next: Default::default(),
            owned: AtomicBool::new(false),
            short_circuit: AtomicBool::new(false),
            ret: Cell::new(None),
        }
    }

    fn init(&mut self, node: &Node<K, V>, op: Operation, key: Option<K>) {
        self.node = node;
        self.op = op;
        self.key = key;
    }
}

struct MCSLockGuard<'l, K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    slot: &'l UnsafeCell<MCSLockSlot<K, V>>,
}

impl<'l, K, V> MCSLockGuard<'l, K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    fn new(slot: &'l UnsafeCell<MCSLockSlot<K, V>>) -> Self {
        Self { slot }
    }

    unsafe fn owner_node(&self) -> &Node<K, V> {
        &*(&*self.slot.get()).node
    }
}

impl<'l, K, V> Drop for MCSLockGuard<'l, K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    fn drop(&mut self) {
        let slot = unsafe { &*self.slot.get() };
        let node = unsafe { &*slot.node };
        debug_assert!(slot.owned.load(Ordering::Acquire));

        if let Some(next) = unsafe { slot.next.load(Ordering::Acquire).as_ref() } {
            next.owned.store(true, Ordering::Release);
            slot.owned.store(false, Ordering::Release);
            return;
        }

        if node
            .lock
            .compare_exchange(
                self.slot.get(),
                null_mut(),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            slot.owned.store(false, Ordering::Release);
            return;
        }
        loop {
            if let Some(next) = unsafe { slot.next.load(Ordering::Relaxed).as_ref() } {
                next.owned.store(true, Ordering::Release);
                slot.owned.store(false, Ordering::Release);
                return;
            }
            spin_loop();
        }
    }
}

enum AcqResult<'l, K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    Acquired(MCSLockGuard<'l, K, V>),
    Eliminated(V),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Insert,
    Delete,
    Balance,
}

#[derive(Default)]
pub struct Node<K, V>
where
    K: Copy + Default,
    V: Copy + Default,
{
    _keys: [Cell<Option<K>>; DEGREE],
    search_key: Cell<K>,
    lock: AtomicPtr<MCSLockSlot<K, V>>,
    /// The number of next pointers (for an internal node) or values (for a leaf node).
    /// Note that it may not be equal to the number of keys, because the last next pointer
    /// is mapped by a bottom key (i.e., `None`).
    size: AtomicUsize,
    weight: Cell<bool>,
    marked: AtomicBool,
    // Leaf node data
    values: [Cell<Option<V>>; DEGREE],
    write_version: AtomicUsize,
    // Internal node data
    next: [AtomicPtr<Inner<Node<K, V>>>; DEGREE],
    is_leaf: Cell<bool>,
}

impl<K, V> Node<K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    fn weight(&self, guard: &Guard<Self>) -> Result<bool, ()> {
        let result = self.weight.get();
        guard.validate_epoch().map(|_| result)
    }

    fn search_key(&self, guard: &Guard<Self>) -> Result<K, ()> {
        let result = self.search_key.get();
        guard.validate_epoch().map(|_| result)
    }

    fn is_leaf(&self, guard: &Guard<Self>) -> Result<bool, ()> {
        let result = self.is_leaf.get();
        guard.validate_epoch().map(|_| result)
    }

    fn load_next(&self, index: usize, guard: &Guard<Self>) -> Result<*mut Inner<Self>, ()> {
        let result = self.next[index].load(Ordering::Acquire);
        guard.validate_epoch().map(|_| result)
    }

    fn load_next_locked<'l>(
        &'l self,
        index: usize,
        _: &MCSLockGuard<'l, K, V>,
    ) -> *mut Inner<Self> {
        self.next[index].load(Ordering::Acquire)
    }

    fn store_next<'l>(&self, index: usize, ptr: *mut Inner<Self>, _: &MCSLockGuard<'l, K, V>) {
        self.next[index].store(ptr, Ordering::Release);
    }

    /// # Safety
    ///
    /// The current thread has an exclusive ownership and it is not exposed to the shared memory.
    /// (e.g., in the `init` closure of `allocate` function)
    unsafe fn init_next(&self, index: usize, ptr: *mut Inner<Self>) {
        self.next[index].store(ptr, Ordering::Release);
    }

    fn start_write<'g>(&'g self, lock: &MCSLockGuard<'g, K, V>) -> WriteGuard<'g, K, V> {
        debug_assert!(eq(unsafe { lock.owner_node() }, self));
        let init_version = self.write_version.load(Ordering::Acquire);
        debug_assert!(init_version % 2 == 0);
        // It is safe to skip the epoch validation, as we are grabbing the lock.
        self.write_version
            .store(init_version + 1, Ordering::Release);
        compiler_fence(Ordering::SeqCst);

        return WriteGuard {
            init_version,
            node: self,
        };
    }

    fn key_count(&self, guard: &Guard<Self>) -> Result<usize, ()> {
        let result = if self.is_leaf.get() {
            self.size.load(Ordering::Acquire)
        } else {
            self.size.load(Ordering::Acquire) - 1
        };
        guard.validate_epoch().map(|_| result)
    }

    fn key_count_locked<'l>(&'l self, _: &MCSLockGuard<'l, K, V>) -> usize {
        if self.is_leaf.get() {
            self.size.load(Ordering::Acquire)
        } else {
            self.size.load(Ordering::Acquire) - 1
        }
    }

    fn get_value(&self, index: usize, guard: &Guard<Self>) -> Result<Option<V>, ()> {
        let value = self.values[index].get();
        guard.validate_epoch().map(|_| value)
    }

    fn get_value_locked<'l>(&'l self, index: usize, _: &MCSLockGuard<'l, K, V>) -> Option<V> {
        self.values[index].get()
    }

    /// # Safety
    ///
    /// The current thread has an exclusive ownership and it is not exposed to the shared memory.
    /// (e.g., in the `init` closure of `allocate` function)
    unsafe fn init_value(&self, index: usize, val: V) {
        self.values[index].set(Some(val));
    }

    fn set_value<'g>(&'g self, index: usize, val: V, _: &WriteGuard<'g, K, V>) {
        self.values[index].set(Some(val));
    }

    /// # Safety
    ///
    /// The current thread has an exclusive ownership and it is not exposed to the shared memory.
    /// (e.g., in the `init` closure of `allocate` function)
    unsafe fn get_key_unchecked(&self, index: usize) -> Option<K> {
        self._keys[index].get()
    }

    fn get_key(&self, index: usize, guard: &Guard<Self>) -> Result<Option<K>, ()> {
        let result = self._keys[index].get();
        guard.validate_epoch().map(|_| result)
    }

    fn get_key_locked<'l>(&self, index: usize, _: &MCSLockGuard<'l, K, V>) -> Option<K> {
        self._keys[index].get()
    }

    fn key_slice<'l>(&self, _: &MCSLockGuard<'l, K, V>) -> &[Cell<Option<K>>] {
        &self._keys
    }

    /// # Safety
    ///
    /// The current thread has an exclusive ownership and it is not exposed to the shared memory.
    /// (e.g., in the `init` closure of `allocate` function)
    unsafe fn key_slice_unchecked<'l>(&self) -> &[Cell<Option<K>>] {
        &self._keys
    }

    /// # Safety
    ///
    /// The current thread has an exclusive ownership and it is not exposed to the shared memory.
    /// (e.g., in the `init` closure of `allocate` function)
    unsafe fn init_key(&self, index: usize, key: Option<K>) {
        self._keys[index].set(key);
    }

    fn set_key<'g>(&'g self, index: usize, key: Option<K>, _: &WriteGuard<'g, K, V>) {
        self._keys[index].set(key);
    }

    unsafe fn init_on_allocate(&self, weight: bool, size: usize, search_key: K) {
        for key in &self._keys {
            key.set(Default::default());
        }
        self.search_key.set(search_key);
        self.size.store(size, Ordering::Release);
        self.weight.set(weight);
        self.marked.store(false, Ordering::Release);
        for next in &self.next {
            next.store(null_mut(), Ordering::Release);
        }
        for value in &self.values {
            value.set(Default::default());
        }
        self.write_version.store(0, Ordering::Release);
    }

    unsafe fn init_for_internal(&self, weight: bool, size: usize, search_key: K) {
        self.init_on_allocate(weight, size, search_key);
        self.is_leaf.set(false);
    }

    unsafe fn init_for_leaf(&self, weight: bool, size: usize, search_key: K) {
        self.init_on_allocate(weight, size, search_key);
        self.is_leaf.set(true);
    }
}

impl<K, V> Node<K, V>
where
    K: PartialOrd + Eq + Default + Copy,
    V: Default + Copy,
{
    fn child_index(&self, key: &K, guard: &Guard<Self>) -> Result<usize, ()> {
        let key_count = self.key_count(guard)?;
        let mut index = 0;
        while index < key_count && !(key < &self.get_key(index, guard)?.unwrap()) {
            index += 1;
        }
        guard.validate_epoch().map(|_| index)
    }

    // Search a node for a key repeatedly until we successfully read a consistent version.
    fn read_consistent(&self, key: &K, guard: &Guard<Self>) -> Result<(usize, Option<V>), ()> {
        loop {
            guard.validate_epoch()?;
            let mut version = self.write_version.load(Ordering::Acquire);
            while version & 1 > 0 {
                version = self.write_version.load(Ordering::Acquire);
            }
            let mut key_index = 0;
            while key_index < DEGREE && self.get_key(key_index, guard)? != Some(*key) {
                key_index += 1;
            }
            let value = self.values.get(key_index).and_then(|value| value.get());
            compiler_fence(Ordering::SeqCst);

            if version == self.write_version.load(Ordering::Acquire) {
                return guard.validate_epoch().map(|_| (key_index, value));
            }
        }
    }

    fn acquire<'l>(
        &'l self,
        op: Operation,
        key: Option<K>,
        slot: &'l UnsafeCell<MCSLockSlot<K, V>>,
    ) -> AcqResult<'l, K, V> {
        unsafe { &mut *slot.get() }.init(self, op, key);
        let old_tail = self.lock.swap(slot.get(), Ordering::AcqRel);
        let curr = unsafe { &*slot.get() };

        if let Some(old_tail) = unsafe { old_tail.as_ref() } {
            old_tail.next.store(slot.get(), Ordering::Release);
            while !curr.owned.load(Ordering::Acquire) && !curr.short_circuit.load(Ordering::Acquire)
            {
                spin_loop();
            }
            debug_assert!(
                !curr.owned.load(Ordering::Relaxed) || !curr.short_circuit.load(Ordering::Relaxed)
            );
            if curr.short_circuit.load(Ordering::Relaxed) {
                return AcqResult::Eliminated(curr.ret.get().unwrap());
            }
            debug_assert!(curr.owned.load(Ordering::Relaxed));
        } else {
            curr.owned.store(true, Ordering::Release);
        }
        return AcqResult::Acquired(MCSLockGuard::new(slot));
    }

    fn elim_key_ops<'l>(
        &'l self,
        value: V,
        wguard: WriteGuard<'l, K, V>,
        guard: &MCSLockGuard<'l, K, V>,
    ) {
        let slot = unsafe { &*guard.slot.get() };
        debug_assert!(slot.owned.load(Ordering::Relaxed));
        debug_assert!(slot.op != Operation::Balance);

        let stop_node = self.lock.load(Ordering::Acquire);
        drop(wguard);

        if eq(stop_node.cast(), slot) {
            return;
        }

        let mut prev_alive = guard.slot.get();
        let mut curr = slot.next.load(Ordering::Acquire);
        while curr.is_null() {
            curr = slot.next.load(Ordering::Acquire);
        }

        while curr != stop_node {
            let curr_node = unsafe { &*curr };
            let mut next = curr_node.next.load(Ordering::Acquire);
            while next.is_null() {
                next = curr_node.next.load(Ordering::Acquire);
            }

            if curr_node.key != slot.key || curr_node.op == Operation::Balance {
                unsafe { &*prev_alive }.next.store(curr, Ordering::Release);
                prev_alive = curr;
            } else {
                // Shortcircuit curr.
                curr_node.ret.set(Some(value));
                curr_node.short_circuit.store(true, Ordering::Release);
            }
            curr = next;
        }

        unsafe { &*prev_alive }
            .next
            .store(stop_node, Ordering::Release);
    }

    /// Merge keys of p and l into one big array (and similarly for nexts).
    /// We essentially replace the pointer to l with the contents of l.
    fn absorb_child<'l1, 'l2>(
        &'l1 self,
        child: &'l2 Self,
        child_idx: usize,
        self_lock: &MCSLockGuard<'l1, K, V>,
        child_lock: &MCSLockGuard<'l2, K, V>,
    ) -> (
        [AtomicPtr<Inner<Node<K, V>>>; DEGREE * 2],
        [Cell<Option<K>>; DEGREE * 2],
    ) {
        let next: [AtomicPtr<Inner<Node<K, V>>>; DEGREE * 2] = Default::default();
        let keys: [Cell<Option<K>>; DEGREE * 2] = Default::default();
        let psize = self.size.load(Ordering::Relaxed);
        let nsize = child.size.load(Ordering::Relaxed);

        atomic_clone(&self.next[0..], &next[0..], child_idx);
        atomic_clone(&child.next[0..], &next[child_idx..], nsize);
        atomic_clone(
            &self.next[child_idx + 1..],
            &next[child_idx + nsize..],
            psize - (child_idx + 1),
        );

        cell_clone(&self.key_slice(self_lock)[0..], &keys[0..], child_idx);
        cell_clone(
            &child.key_slice(child_lock)[0..],
            &keys[child_idx..],
            child.key_count_locked(child_lock),
        );
        // Safety: Both `parent` and `child` is locked, so they cannot be reclaimed.
        cell_clone(
            &self.key_slice(self_lock)[child_idx..],
            &keys[child_idx + child.key_count_locked(child_lock)..],
            self.key_count_locked(self_lock) - child_idx,
        );

        (next, keys)
    }

    /// It requires a lock to guarantee the consistency.
    /// Its length is equal to `key_count`.
    fn enumerate_key<'g>(
        &'g self,
        lock: &MCSLockGuard<'g, K, V>,
    ) -> impl Iterator<Item = (usize, K)> + 'g {
        self.key_slice(lock)
            .iter()
            .enumerate()
            .filter_map(|(i, k)| k.get().map(|k| (i, k)))
    }

    /// Iterates key-value pairs in this **leaf** node.
    /// It requires a lock to guarantee the consistency.
    /// Its length is equal to the size of this node.
    fn iter_key_value<'g>(
        &'g self,
        lock: &'g MCSLockGuard<'g, K, V>,
    ) -> impl Iterator<Item = (K, V)> + 'g {
        self.enumerate_key(lock)
            .map(|(i, k)| (k, self.get_value_locked(i, lock).unwrap()))
    }

    /// Iterates key-next pairs in this **internal** node.
    /// It requires a lock to guarantee the consistency.
    /// Its length is equal to the size of this node, and only the last key is `None`.
    fn iter_key_next<'g>(
        &'g self,
        lock: &'g MCSLockGuard<'g, K, V>,
    ) -> impl Iterator<Item = (Option<K>, *mut Inner<Self>)> + 'g {
        self.enumerate_key(lock)
            .map(|(i, k)| (Some(k), self.load_next_locked(i, lock)))
            .chain(once((
                None,
                self.load_next_locked(self.key_count_locked(lock), lock),
            )))
    }
}

struct WriteGuard<'g, K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    init_version: usize,
    node: &'g Node<K, V>,
}

impl<'g, K, V> Drop for WriteGuard<'g, K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    fn drop(&mut self) {
        self.node
            .write_version
            .store(self.init_version + 2, Ordering::Release);
    }
}

struct Cursor<K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    l: *mut Inner<Node<K, V>>,
    p: *mut Inner<Node<K, V>>,
    gp: *mut Inner<Node<K, V>>,
    /// Index of `p` in `gp`.
    gp_p_idx: usize,
    /// Index of `l` in `p`.
    p_l_idx: usize,
    /// Index of the key in `l`.
    l_key_idx: usize,
    val: Option<V>,
}

pub struct ElimABTree<K, V>
where
    K: Default + Copy,
    V: Default + Copy,
{
    entry: Entry<Node<K, V>>,
}

unsafe impl<K, V> Sync for ElimABTree<K, V>
where
    K: Sync + Default + Copy,
    V: Sync + Default + Copy,
{
}
unsafe impl<K, V> Send for ElimABTree<K, V>
where
    K: Send + Default + Copy,
    V: Send + Default + Copy,
{
}

impl<K, V> ElimABTree<K, V>
where
    K: Ord + Eq + Default + Copy,
    V: Default + Copy,
{
    const ABSORB_THRESHOLD: usize = DEGREE;
    const UNDERFULL_THRESHOLD: usize = if DEGREE / 4 < 2 { 2 } else { DEGREE / 4 };

    pub fn new(local: &Local<Node<K, V>>) -> Self {
        loop {
            let guard = &local.guard();
            let left = ok_or!(
                guard.allocate(|node| unsafe { node.deref().init_for_leaf(true, 0, K::default()) }),
                continue
            );
            let entry = ok_or!(
                guard.allocate(|node| unsafe {
                    node.deref().init_for_internal(true, 1, K::default());
                    node.deref().init_next(0, left.as_raw());
                }),
                unsafe {
                    guard.retire(left);
                    continue;
                }
            );
            return Self {
                entry: Entry::new(entry),
            };
        }
    }

    /// Performs a basic search and returns the value associated with the key,
    /// or `None` if nothing is found. Unlike other search methods, it does not return
    /// any path information, making it slightly faster.
    pub fn search_basic(&self, key: &K, local: &Local<Node<K, V>>) -> Option<V> {
        loop {
            let guard = &local.guard();
            return ok_or!(self.search_basic_inner(key, guard), continue);
        }
    }

    fn search_basic_inner(&self, key: &K, guard: &Guard<Node<K, V>>) -> Result<Option<V>, ()> {
        let entry = unsafe { self.entry.load(guard)?.deref() };
        let mut node = unsafe { &*entry.load_next(0, guard)? };
        while !node.is_leaf(guard)? {
            let next = node.load_next(node.child_index(key, guard)?, guard)?;
            node = unsafe { &*next };
        }
        Ok(node.read_consistent(key, guard)?.1)
    }

    fn search(
        &self,
        key: &K,
        target: Option<*mut Inner<Node<K, V>>>,
        guard: &mut Guard<Node<K, V>>,
    ) -> (bool, Cursor<K, V>) {
        loop {
            if let Ok(result) = self.search_inner(key, target, guard) {
                return result;
            }
            guard.refresh();
        }
    }

    fn search_inner(
        &self,
        key: &K,
        target: Option<*mut Inner<Node<K, V>>>,
        guard: &Guard<Node<K, V>>,
    ) -> Result<(bool, Cursor<K, V>), ()> {
        let entry_sh = self.entry.load(guard)?;
        let entry = unsafe { entry_sh.deref() };
        let mut cursor = Cursor {
            l: entry.load_next(0, guard)?,
            p: entry_sh.as_raw(),
            gp: null_mut(),
            gp_p_idx: 0,
            p_l_idx: 0,
            l_key_idx: 0,
            val: None,
        };

        while !unsafe { &*cursor.l }.is_leaf(guard)?
            && target.map(|target| target != cursor.l).unwrap_or(true)
        {
            let l_node = unsafe { &*cursor.l };
            cursor.gp = cursor.p;
            cursor.p = cursor.l;
            cursor.gp_p_idx = cursor.p_l_idx;
            cursor.p_l_idx = l_node.child_index(key, guard)?;
            cursor.l = l_node.load_next(cursor.p_l_idx, guard)?;
        }

        if let Some(target) = target {
            Ok((cursor.l == target, cursor))
        } else {
            let (index, value) = unsafe { &*cursor.l }.read_consistent(key, guard)?;
            cursor.val = value;
            cursor.l_key_idx = index;
            Ok((value.is_some(), cursor))
        }
    }

    pub fn insert(&self, key: &K, value: &V, local: &Local<Node<K, V>>) -> Option<V> {
        loop {
            let guard = &mut local.guard();
            let (_, cursor) = self.search(key, None, guard);
            if let Some(value) = cursor.val {
                return Some(value);
            }
            match self.insert_inner(key, value, &cursor, guard) {
                Ok(result) => return result,
                Err(_) => continue,
            }
        }
    }

    fn insert_inner<'g>(
        &self,
        key: &K,
        value: &V,
        cursor: &Cursor<K, V>,
        guard: &mut Guard<Node<K, V>>,
    ) -> Result<Option<V>, ()> {
        let node = unsafe { &*cursor.l };
        let parent = unsafe { &*cursor.p };

        debug_assert!(node.is_leaf(guard)?);
        debug_assert!(!parent.is_leaf(guard)?);

        let node_lock_slot = UnsafeCell::new(MCSLockSlot::new());
        let node_lock = match node.acquire(Operation::Insert, Some(*key), &node_lock_slot) {
            AcqResult::Acquired(lock) => lock,
            AcqResult::Eliminated(value) => return Ok(Some(value)),
        };
        if node.marked.load(Ordering::SeqCst) {
            return Err(());
        }
        for i in 0..DEGREE {
            if node.get_key_locked(i, &node_lock) == Some(*key) {
                return Ok(Some(node.get_value(i, guard)?.unwrap()));
            }
        }
        // At this point, we are guaranteed key is not in the node.

        if node.size.load(Ordering::Acquire) < Self::ABSORB_THRESHOLD {
            // We have the capacity to fit this new key. So let's just find an empty slot.
            for i in 0..DEGREE {
                if node.get_key_locked(i, &node_lock).is_some() {
                    continue;
                }
                let wguard = node.start_write(&node_lock);
                node.set_key(i, Some(*key), &wguard);
                node.set_value(i, *value, &wguard);
                node.size
                    .store(node.size.load(Ordering::Relaxed) + 1, Ordering::Relaxed);

                node.elim_key_ops(*value, wguard, &node_lock);

                drop(node_lock);
                return Ok(None);
            }
            unreachable!("Should never happen");
        } else {
            // We do not have a room for this key. We need to make new nodes.
            try_acq_val_or!(
                parent,
                parent_lock,
                Operation::Insert,
                None,
                guard,
                return Err(()),
                return Err(())
            );

            let mut kv_pairs = node
                .iter_key_value(&node_lock)
                .chain(once((*key, *value)))
                .collect::<ArrayVec<(K, V), { DEGREE + 1 }>>();
            kv_pairs.sort_by_key(|(k, _)| *k);

            // Create new node(s).
            // Since the new arrays are too big to fit in a single node,
            // we replace `l` by a new subtree containing three new nodes: a parent, and two leaves.
            // The array contents are then split between the two new leaves.

            let left_size = kv_pairs.len() / 2;
            let right_size = DEGREE + 1 - left_size;

            let left = guard.allocate(|left| unsafe {
                left.deref().init_for_leaf(true, left_size, kv_pairs[0].0);
                for i in 0..left_size {
                    left.deref().init_key(i, Some(kv_pairs[i].0));
                    left.deref().init_value(i, kv_pairs[i].1);
                }
            })?;

            let Ok(right) = guard.allocate(|right| unsafe {
                right
                    .deref()
                    .init_for_leaf(true, right_size, kv_pairs[left_size].0);
                for i in 0..right_size {
                    right.deref().init_key(i, Some(kv_pairs[i + left_size].0));
                    right.deref().init_value(i, kv_pairs[i + left_size].1);
                }
            }) else {
                unsafe { guard.retire(left) };
                return Err(());
            };

            // The weight of new internal node `n` will be zero, unless it is the root.
            // This is because we test `p == entry`, above; in doing this, we are actually
            // performing Root-Zero at the same time as this Overflow if `n` will become the root.
            let Ok(internal) = guard.allocate(|internal| unsafe {
                internal.deref().init_for_internal(
                    eq(parent, self.entry.load_raw()),
                    2,
                    kv_pairs[left_size].0,
                );
                internal.deref().init_key(0, Some(kv_pairs[left_size].0));
                internal.deref().init_next(0, left.as_raw());
                internal.deref().init_next(1, right.as_raw());
            }) else {
                unsafe { guard.retire(left) };
                unsafe { guard.retire(right) };
                return Err(());
            };

            // If the parent is not marked, `parent.next[cursor.p_l_idx]` is guaranteed to contain
            // a node since any update to parent would have deleted node (and hence we would have
            // returned at the `node.marked` check).
            parent.store_next(cursor.p_l_idx, internal.as_raw(), &parent_lock);
            node.marked.store(true, Ordering::Release);

            // Manually unlock and fix the tag.
            drop((parent_lock, node_lock));
            unsafe { guard.retire_raw(cursor.l) };
            self.fix_tag_violation(kv_pairs[left_size].0, internal.as_raw(), guard);

            Ok(None)
        }
    }

    fn fix_tag_violation(
        &self,
        search_key: K,
        viol: *mut Inner<Node<K, V>>,
        guard: &mut Guard<Node<K, V>>,
    ) {
        let mut stack = vec![(search_key, viol)];
        while let Some((search_key, viol)) = stack.pop() {
            guard.refresh();
            let (found, cursor) = self.search(&search_key, Some(viol), guard);
            if !found || cursor.l != viol {
                // `viol` was replaced by another update.
                // We hand over responsibility for `viol` to that update.
                continue;
            }
            let Ok((success, recur)) = self.fix_tag_violation_inner(&cursor, guard) else {
                stack.push((search_key, viol));
                continue;
            };
            if !success {
                stack.push((search_key, viol));
            }
            stack.extend(recur);
        }
    }

    fn fix_tag_violation_inner(
        &self,
        cursor: &Cursor<K, V>,
        guard: &mut Guard<Node<K, V>>,
    ) -> Result<(bool, Option<(K, *mut Inner<Node<K, V>>)>), ()> {
        let viol = cursor.l;
        let viol_node = unsafe { &*viol };
        if viol_node.weight(guard)? {
            return Ok((true, None));
        }

        // `viol` should be internal because leaves always have weight = 1.
        debug_assert!(!viol_node.is_leaf(guard)?);
        // `viol` is not the entry or root node because both should always have weight = 1.
        debug_assert!(
            !eq(viol_node, self.entry.load_raw())
                && unsafe { self.entry.load(guard)?.deref() }.load_next(0, guard)? != viol
        );

        debug_assert!(!cursor.gp.is_null());
        let node = unsafe { &*cursor.l };
        let parent = unsafe { &*cursor.p };
        let gparent = unsafe { &*cursor.gp };
        debug_assert!(!node.is_leaf(guard)?);
        debug_assert!(!parent.is_leaf(guard)?);
        debug_assert!(!gparent.is_leaf(guard)?);

        // We cannot apply this update if p has a weight violation.
        // So, we check if this is the case, and, if so, try to fix it.
        if !parent.weight(guard)? {
            return Ok((false, Some((parent.search_key(guard)?, cursor.p))));
        }

        try_acq_val_or!(
            node,
            node_lock,
            Operation::Balance,
            None,
            guard,
            return Ok((false, None)),
            return Err(())
        );
        try_acq_val_or!(
            parent,
            parent_lock,
            Operation::Balance,
            None,
            guard,
            return Ok((false, None)),
            return Err(())
        );
        try_acq_val_or!(
            gparent,
            gparent_lock,
            Operation::Balance,
            None,
            guard,
            return Ok((false, None)),
            return Err(())
        );

        let psize = parent.size.load(Ordering::Relaxed);
        let nsize = viol_node.size.load(Ordering::Relaxed);
        // We don't ever change the size of a tag node, so its size should always be 2.
        debug_assert_eq!(nsize, 2);
        let c = psize + nsize;
        let size = c - 1;
        let (next, keys) = parent.absorb_child(node, cursor.p_l_idx, &parent_lock, &node_lock);

        if size <= Self::ABSORB_THRESHOLD {
            // Absorb case.

            // Create new node(s).
            // The new arrays are small enough to fit in a single node,
            // so we replace p by a new internal node.
            let absorber = guard.allocate(|absorber| unsafe {
                absorber.deref().init_for_internal(
                    true,
                    size,
                    parent.get_key_locked(0, &parent_lock).unwrap(),
                );
                atomic_clone(&next, &absorber.deref().next, DEGREE);
                cell_clone(&keys, &absorber.deref().key_slice_unchecked(), DEGREE);
            })?;

            gparent.store_next(cursor.gp_p_idx, absorber.as_raw(), &gparent_lock);
            node.marked.store(true, Ordering::Relaxed);
            parent.marked.store(true, Ordering::Relaxed);

            unsafe { guard.retire_raw(cursor.l) };
            unsafe { guard.retire_raw(cursor.p) };
            return Ok((true, None));
        } else {
            // Split case.

            // The new arrays are too big to fit in a single node,
            // so we replace p by a new internal node and two new children.
            //
            // We take the big merged array and split it into two arrays,
            // which are used to create two new children u and v.
            // we then create a new internal node (whose weight will be zero
            // if it is not the root), with u and v as its children.

            // Create new node(s).
            let left_size = size / 2;
            let left = guard.allocate(|left| unsafe {
                left.deref()
                    .init_for_internal(true, left_size, keys[0].get().unwrap());
                cell_clone(&keys, &left.deref().key_slice_unchecked(), left_size - 1);
                atomic_clone(&next, &left.deref().next, left_size);
            })?;

            let right_size = size - left_size;
            let Ok(right) = guard.allocate(|right| unsafe {
                right
                    .deref()
                    .init_for_internal(true, right_size, keys[left_size].get().unwrap());
                cell_clone(
                    &keys[left_size..],
                    &right.deref().key_slice_unchecked()[0..],
                    right_size - 1,
                );
                atomic_clone(&next[left_size..], &right.deref().next[0..], right_size);
            }) else {
                unsafe { guard.retire(left) };
                return Err(());
            };

            // Note: keys[left_size - 1] should be the same as new_internal.keys[0].
            let Ok(new_internal) = guard.allocate(|new_internal| unsafe {
                new_internal.deref().init_for_internal(
                    eq(gparent, self.entry.load_raw()),
                    2,
                    keys[left_size - 1].get().unwrap(),
                );
                new_internal.deref().init_key(0, keys[left_size - 1].get());
                new_internal.deref().init_next(0, left.as_raw());
                new_internal.deref().init_next(1, right.as_raw());
            }) else {
                unsafe { guard.retire(left) };
                unsafe { guard.retire(right) };
                return Err(());
            };

            // The weight of new internal node `n` will be zero, unless it is the root.
            // This is because we test `p == entry`, above; in doing this, we are actually
            // performing Root-Zero at the same time
            // as this Overflow if `n` will become the root.

            gparent.store_next(cursor.gp_p_idx, new_internal.as_raw(), &gparent_lock);
            node.marked.store(true, Ordering::Relaxed);
            parent.marked.store(true, Ordering::Relaxed);

            unsafe { guard.retire_raw(cursor.l) };
            unsafe { guard.retire_raw(cursor.p) };

            drop((node_lock, parent_lock, gparent_lock));
            return Ok((
                true,
                Some((keys[left_size - 1].get().unwrap(), new_internal.as_raw())),
            ));
        }
    }

    pub fn remove(&self, key: &K, local: &Local<Node<K, V>>) -> Option<V> {
        loop {
            let guard = &mut local.guard();
            let (_, cursor) = self.search(key, None, guard);
            if cursor.val.is_none() {
                return None;
            }
            match self.remove_inner(key, &cursor, guard) {
                Ok(result) => return result,
                Err(()) => continue,
            }
        }
    }

    fn remove_inner(
        &self,
        key: &K,
        cursor: &Cursor<K, V>,
        guard: &mut Guard<Node<K, V>>,
    ) -> Result<Option<V>, ()> {
        let node = unsafe { &*cursor.l };
        let parent = unsafe { &*cursor.p };
        let gparent = unsafe { cursor.gp.as_ref() };

        debug_assert!(node.is_leaf(guard)?);
        debug_assert!(!parent.is_leaf(guard)?);
        debug_assert!(if let Some(gp) = gparent {
            !gp.is_leaf(guard)?
        } else {
            true
        });

        try_acq_val_or!(
            node,
            node_lock,
            Operation::Delete,
            Some(*key),
            guard,
            return Err(()),
            return Err(())
        );
        // Bug Fix: Added a check to ensure the node size is greater than 0.
        // This prevents underflows caused by decrementing the size value.
        // This check is not present in the original code.
        if node.size.load(Ordering::Acquire) == 0 {
            return Err(());
        }

        let new_size = node.size.load(Ordering::Relaxed) - 1;
        for i in 0..DEGREE {
            if node.get_key_locked(i, &node_lock) == Some(*key) {
                // Safety of raw `get`: `node` is locked.
                let val = node.values[i].get().unwrap();
                let wguard = node.start_write(&node_lock);
                node.set_key(i, None, &wguard);
                node.size.store(new_size, Ordering::Relaxed);

                node.elim_key_ops(val, wguard, &node_lock);

                if new_size == Self::UNDERFULL_THRESHOLD - 1 {
                    let search_key = node.search_key.get();
                    drop(node_lock);
                    self.fix_underfull_violation(search_key, cursor.l, guard);
                }
                return Ok(Some(val));
            }
        }
        Err(())
    }

    fn fix_underfull_violation(
        &self,
        search_key: K,
        viol: *mut Inner<Node<K, V>>,
        guard: &mut Guard<Node<K, V>>,
    ) {
        let mut stack = vec![(search_key, viol)];
        while let Some((search_key, viol)) = stack.pop() {
            // We search for `viol` and try to fix any violation we find there.
            // This entails performing AbsorbSibling or Distribute.
            guard.refresh();
            let (_, cursor) = self.search(&search_key, Some(viol), guard);
            if cursor.l != viol {
                // `viol` was replaced by another update.
                // We hand over responsibility for `viol` to that update.
                continue;
            }
            let Ok((success, recur)) = self.fix_underfull_violation_inner(&cursor, guard) else {
                stack.push((search_key, viol));
                continue;
            };
            if !success {
                stack.push((search_key, viol));
            }
            stack.extend(recur);
        }
    }

    fn fix_underfull_violation_inner(
        &self,
        cursor: &Cursor<K, V>,
        guard: &mut Guard<Node<K, V>>,
    ) -> Result<(bool, ArrayVec<(K, *mut Inner<Node<K, V>>), 2>), ()> {
        let viol = cursor.l;
        let viol_node = unsafe { &*viol };

        // We do not need a lock for the `viol == entry.ptrs[0]` check since since we cannot
        // "be turned into" the root. The root is only created by the root absorb
        // operation below, so a node that is not the root will never become the root.
        if viol_node.size.load(Ordering::Relaxed) >= Self::UNDERFULL_THRESHOLD
            || eq(viol_node, self.entry.load_raw())
            || viol == unsafe { self.entry.load(guard)?.deref() }.load_next(0, guard)?
        {
            // No degree violation at `viol`.
            return Ok((true, ArrayVec::<_, 2>::new()));
        }

        let node = unsafe { &*cursor.l };
        let parent = unsafe { &*cursor.p };
        // `gp` cannot be null, because if AbsorbSibling or Distribute can be applied,
        // then `p` is not the root.
        debug_assert!(!cursor.gp.is_null());
        let gparent = unsafe { &*cursor.gp };

        if parent.size.load(Ordering::Relaxed) < Self::UNDERFULL_THRESHOLD
            && !eq(parent, self.entry.load_raw())
            && cursor.p != unsafe { self.entry.load(guard)?.deref() }.load_next(0, guard)?
        {
            return Ok((
                false,
                ArrayVec::from_iter(once((parent.search_key(guard)?, cursor.p))),
            ));
        }

        let sibling_idx = if cursor.p_l_idx > 0 {
            cursor.p_l_idx - 1
        } else {
            1
        };
        // Don't need a lock on parent here because if the pointer to sibling changes
        // to a different node after this, sibling will be marked
        // (Invariant: when a pointer switches away from a node, the node is marked)
        let sibling_sh = parent.load_next(sibling_idx, guard)?;
        let sibling = unsafe { &*sibling_sh };

        // Prevent deadlocks by acquiring left node first.
        let ((left, left_idx), (right, right_idx)) = if sibling_idx < cursor.p_l_idx {
            ((sibling, sibling_idx), (node, cursor.p_l_idx))
        } else {
            ((node, cursor.p_l_idx), (sibling, sibling_idx))
        };

        try_acq_val_or!(
            left,
            left_lock,
            Operation::Balance,
            None,
            guard,
            return Ok((false, ArrayVec::new())),
            return Err(())
        );
        try_acq_val_or!(
            right,
            right_lock,
            Operation::Balance,
            None,
            guard,
            return Ok((false, ArrayVec::new())),
            return Err(())
        );

        // Repeat this check, this might have changed while we locked `viol`.
        if viol_node.size.load(Ordering::Relaxed) >= Self::UNDERFULL_THRESHOLD {
            // No degree violation at `viol`.
            return Ok((true, ArrayVec::new()));
        }

        try_acq_val_or!(
            parent,
            parent_lock,
            Operation::Balance,
            None,
            guard,
            return Ok((false, ArrayVec::new())),
            return Err(())
        );
        try_acq_val_or!(
            gparent,
            gparent_lock,
            Operation::Balance,
            None,
            guard,
            return Ok((false, ArrayVec::new())),
            return Err(())
        );

        // We can only apply AbsorbSibling or Distribute if there are no
        // weight violations at `parent`, `node`, or `sibling`.
        // So, we first check for any weight violations and fix any that we see.
        if !parent.weight.get() {
            drop((left_lock, right_lock, parent_lock, gparent_lock));
            self.fix_tag_violation(parent.search_key.get(), cursor.p, guard);
            return Ok((false, ArrayVec::new()));
        }
        if !node.weight.get() {
            drop((left_lock, right_lock, parent_lock, gparent_lock));
            self.fix_tag_violation(node.search_key.get(), cursor.l, guard);
            return Ok((false, ArrayVec::new()));
        }
        if !sibling.weight.get() {
            drop((left_lock, right_lock, parent_lock, gparent_lock));
            self.fix_tag_violation(sibling.search_key.get(), sibling_sh, guard);
            return Ok((false, ArrayVec::new()));
        }

        // There are no weight violations at `parent`, `node` or `sibling`.
        debug_assert!(parent.weight.get() && node.weight.get() && sibling.weight.get());
        // l and s are either both leaves or both internal nodes,
        // because there are no weight violations at these nodes.
        debug_assert!(
            (node.is_leaf.get() && sibling.is_leaf.get())
                || (!node.is_leaf.get() && !sibling.is_leaf.get())
        );

        let lsize = left.size.load(Ordering::Relaxed);
        let rsize = right.size.load(Ordering::Relaxed);
        let psize = parent.size.load(Ordering::Relaxed);
        let size = lsize + rsize;

        if size < 2 * Self::UNDERFULL_THRESHOLD {
            // AbsorbSibling
            let new_node = guard.allocate(|new_node| unsafe {
                if left.is_leaf.get() {
                    debug_assert!(right.is_leaf.get());
                    let new_leaf = new_node.deref();
                    new_leaf.init_for_leaf(true, size, node.search_key.get());
                    let kv_iter = left
                        .iter_key_value(&left_lock)
                        .chain(right.iter_key_value(&right_lock))
                        .enumerate();
                    for (i, (key, value)) in kv_iter {
                        new_leaf.init_key(i, Some(key));
                        new_leaf.init_value(i, value);
                    }
                } else {
                    debug_assert!(!right.is_leaf.get());
                    let new_internal = new_node.deref();
                    new_internal.init_for_internal(true, size, node.search_key.get());
                    let key_btw = parent.get_key_locked(left_idx, &parent_lock).unwrap();
                    let kn_iter = left
                        .iter_key_next(&left_lock)
                        .map(|(k, n)| (Some(k.unwrap_or(key_btw)), n))
                        .chain(right.iter_key_next(&right_lock))
                        .enumerate();
                    for (i, (key, next)) in kn_iter {
                        new_internal.init_key(i, key);
                        new_internal.init_next(i, next);
                    }
                }
            })?;

            // Now, we atomically replace `p` and its children with the new nodes.
            // If appropriate, we perform RootAbsorb at the same time.
            if eq(gparent, self.entry.load_raw()) && psize == 2 {
                debug_assert!(cursor.gp_p_idx == 0);
                gparent.store_next(cursor.gp_p_idx, new_node.as_raw(), &gparent_lock);
                node.marked.store(true, Ordering::Relaxed);
                parent.marked.store(true, Ordering::Relaxed);
                sibling.marked.store(true, Ordering::Relaxed);

                unsafe {
                    guard.retire_raw(cursor.l);
                    guard.retire_raw(cursor.p);
                    guard.retire_raw(sibling_sh);
                }

                let search_key = node.search_key.get();
                drop((left_lock, right_lock, parent_lock, gparent_lock));
                return Ok((
                    true,
                    ArrayVec::from_iter(once((search_key, new_node.as_raw()))),
                ));
            } else {
                debug_assert!(!eq(gparent, self.entry.load_raw()) || psize > 2);
                let Ok(new_parent) = guard.allocate(|new_parent| unsafe {
                    let new_parent = new_parent.deref();
                    new_parent.init_for_internal(true, psize - 1, parent.search_key.get());

                    for i in 0..left_idx {
                        new_parent.init_key(i, parent.get_key_locked(i, &parent_lock));
                    }
                    for i in 0..sibling_idx {
                        new_parent.init_next(i, parent.load_next_locked(i, &parent_lock));
                    }
                    for i in left_idx + 1..parent.key_count_locked(&parent_lock) {
                        new_parent.init_key(i - 1, parent.get_key_locked(i, &parent_lock));
                    }
                    for i in cursor.p_l_idx + 1..psize {
                        new_parent.init_next(i - 1, parent.load_next_locked(i, &parent_lock));
                    }

                    new_parent.init_next(
                        cursor.p_l_idx - (if cursor.p_l_idx > sibling_idx { 1 } else { 0 }),
                        new_node.as_raw(),
                    );
                }) else {
                    unsafe { guard.retire(new_node) };
                    return Err(());
                };

                gparent.store_next(cursor.gp_p_idx, new_parent.as_raw(), &gparent_lock);
                node.marked.store(true, Ordering::Relaxed);
                parent.marked.store(true, Ordering::Relaxed);
                sibling.marked.store(true, Ordering::Relaxed);

                unsafe {
                    guard.retire_raw(cursor.l);
                    guard.retire_raw(cursor.p);
                    guard.retire_raw(sibling_sh);
                }

                let node_key = node.search_key.get();
                let parent_key = parent.search_key.get();
                drop((left_lock, right_lock, parent_lock, gparent_lock));
                return Ok((
                    true,
                    ArrayVec::from_iter(
                        [
                            (node_key, new_node.as_raw()),
                            (parent_key, new_parent.as_raw()),
                        ]
                        .into_iter(),
                    ),
                ));
            }
        } else {
            // Distribute
            let left_size = size / 2;
            let right_size = size - left_size;

            assert!(left.is_leaf.get() == right.is_leaf.get());

            // `pivot`: Reserve one key for the parent
            //          (to go between `new_left` and `new_right`).
            let (new_left, new_right, pivot) = if left.is_leaf.get() {
                // Combine the contents of `l` and `s`.
                let mut kv_pairs = left
                    .iter_key_value(&left_lock)
                    .chain(right.iter_key_value(&right_lock))
                    .collect::<ArrayVec<(K, V), { 2 * DEGREE }>>();
                kv_pairs.sort_by_key(|(k, _)| *k);
                let mut kv_iter = kv_pairs.iter().copied();

                let new_left = guard.allocate(|new_leaf| unsafe {
                    let new_leaf = new_leaf.deref();
                    new_leaf.init_for_leaf(true, left_size, Default::default());
                    for i in 0..left_size {
                        let (k, v) = kv_iter.next().unwrap();
                        new_leaf.init_key(i, Some(k));
                        new_leaf.init_value(i, v);
                    }
                    new_leaf
                        .search_key
                        .set(new_leaf.get_key_unchecked(0).unwrap());
                })?;

                let Ok(new_right) = guard.allocate(|new_leaf| unsafe {
                    debug_assert!(left.is_leaf.get());
                    let new_leaf = new_leaf.deref();
                    new_leaf.init_for_leaf(true, right_size, Default::default());
                    for i in 0..right_size {
                        let (k, v) = kv_iter.next().unwrap();
                        new_leaf.init_key(i, Some(k));
                        new_leaf.init_value(i, v);
                    }
                    new_leaf
                        .search_key
                        .set(new_leaf.get_key_unchecked(0).unwrap());
                }) else {
                    unsafe { guard.retire(new_left) };
                    return Err(());
                };
                let pivot = unsafe { new_right.deref() }.search_key.get();

                debug_assert!(kv_iter.next().is_none());
                (new_left, new_right, pivot)
            } else {
                // Combine the contents of `l` and `s`
                // (and one key from `p` if `l` and `s` are internal).
                let key_btw = parent.get_key_locked(left_idx, &parent_lock).unwrap();
                let mut kn_iter = left
                    .iter_key_next(&left_lock)
                    .map(|(k, n)| (Some(k.unwrap_or(key_btw)), n))
                    .chain(right.iter_key_next(&right_lock));

                let new_left = guard.allocate(|new_internal| unsafe {
                    let new_internal = new_internal.deref();
                    new_internal.init_for_internal(true, left_size, Default::default());
                    for i in 0..left_size {
                        let (k, n) = kn_iter.next().unwrap();
                        new_internal.init_key(i, k);
                        new_internal.init_next(i, n);
                    }
                    new_internal
                        .search_key
                        .set(new_internal.get_key_unchecked(0).unwrap());
                })?;
                let pivot = unsafe { new_left.deref().get_key_unchecked(left_size - 1) }
                    .take()
                    .unwrap();

                let Ok(new_right) = guard.allocate(|new_internal| unsafe {
                    let new_internal = new_internal.deref();
                    new_internal.init_for_internal(true, right_size, Default::default());
                    for i in 0..right_size {
                        let (k, n) = kn_iter.next().unwrap();
                        new_internal.init_key(i, k);
                        new_internal.init_next(i, n);
                    }
                    new_internal
                        .search_key
                        .set(new_internal.get_key_unchecked(0).unwrap());
                }) else {
                    unsafe { guard.retire(new_left) };
                    return Err(());
                };

                debug_assert!(kn_iter.next().is_none());
                (new_left, new_right, pivot)
            };

            let Ok(new_parent) = guard.allocate(|new_parent| unsafe {
                let new_parent = new_parent.deref();
                new_parent.init_for_internal(parent.weight.get(), psize, parent.search_key.get());
                cell_clone(
                    &parent.key_slice(&parent_lock)[0..],
                    &new_parent.key_slice_unchecked()[0..],
                    parent.key_count_locked(&parent_lock),
                );
                atomic_clone(&parent.next[0..], &new_parent.next[0..], psize);
                new_parent.init_next(left_idx, new_left.as_raw());
                new_parent.init_next(right_idx, new_right.as_raw());
                new_parent.init_key(left_idx, Some(pivot));
            }) else {
                unsafe { guard.retire(new_left) };
                unsafe { guard.retire(new_right) };
                return Err(());
            };

            gparent.store_next(cursor.gp_p_idx, new_parent.as_raw(), &gparent_lock);
            node.marked.store(true, Ordering::Relaxed);
            parent.marked.store(true, Ordering::Relaxed);
            sibling.marked.store(true, Ordering::Relaxed);

            unsafe {
                guard.retire_raw(cursor.l);
                guard.retire_raw(cursor.p);
                guard.retire_raw(sibling_sh);
            }

            return Ok((true, ArrayVec::new()));
        }
    }
}

/// Similar to `memcpy`, but for `Cell` types.
#[inline]
fn cell_clone<T: Copy>(src: &[Cell<T>], dst: &[Cell<T>], len: usize) {
    for i in 0..len {
        dst[i].set(src[i].get());
    }
}

/// Similar to `memcpy`, but for `AtomicPtr` types.
#[inline]
fn atomic_clone<T>(src: &[AtomicPtr<T>], dst: &[AtomicPtr<T>], len: usize) {
    for i in 0..len {
        dst[i].store(src[i].load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

impl<K, V> ConcurrentMap<K, V> for ElimABTree<K, V>
where
    K: 'static + Ord + Copy + Default,
    V: 'static + Copy + Default,
{
    type Global = Global<Node<K, V>>;
    type Local = Local<Node<K, V>>;

    fn global(key_range_hint: usize) -> Self::Global {
        Global::new(key_range_hint)
    }

    fn local(global: &Self::Global) -> Self::Local {
        Local::new(global)
    }

    fn new(local: &Self::Local) -> Self {
        ElimABTree::new(local)
    }

    #[inline(always)]
    fn get(&self, key: &K, local: &Self::Local) -> Option<V> {
        self.search_basic(key, local)
    }

    #[inline(always)]
    fn insert(&self, key: K, value: V, local: &Self::Local) -> bool {
        self.insert(&key, &value, local).is_none()
    }

    #[inline(always)]
    fn remove(&self, key: &K, local: &Self::Local) -> Option<V> {
        self.remove(key, local)
    }
}

#[cfg(test)]
mod tests {
    use super::ElimABTree;
    use crate::ds_impl::vbr::concurrent_map;

    #[test]
    fn smoke_elim_ab_tree() {
        concurrent_map::tests::smoke::<ElimABTree<i32, i32>>();
    }
}
