use super::concurrent_map::{ConcurrentMap, OutputHolder};
use arrayvec::ArrayVec;
use crossbeam_ebr::{unprotected, Atomic, Guard, Owned, Pointer, Shared};

use std::cell::{Cell, UnsafeCell};
use std::hint::spin_loop;
use std::iter::once;
use std::ptr::{eq, null, null_mut};
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};

// Copied from the original author's code:
// https://gitlab.com/trbot86/setbench/-/blob/f4711af3ace28d8b4fa871559db74fb4e0e62cc0/ds/srivastava_abtree_mcs/adapter.h#L17
const DEGREE: usize = 11;

macro_rules! try_acq_val_or {
    ($node:ident, $lock:ident, $op:expr, $key:expr, $acq_val_err:expr) => {
        let __slot = UnsafeCell::new(MCSLockSlot::new());
        let $lock = match (
            $node.acquire($op, $key, &__slot),
            $node.marked.load(Ordering::Acquire),
        ) {
            (AcqResult::Acquired(lock), false) => lock,
            _ => $acq_val_err,
        };
    };
}

struct MCSLockSlot<K, V> {
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

struct MCSLockGuard<'l, K, V> {
    slot: &'l UnsafeCell<MCSLockSlot<K, V>>,
}

impl<'l, K, V> MCSLockGuard<'l, K, V> {
    fn new(slot: &'l UnsafeCell<MCSLockSlot<K, V>>) -> Self {
        Self { slot }
    }

    unsafe fn owner_node(&self) -> &Node<K, V> {
        &*(&*self.slot.get()).node
    }
}

impl<'l, K, V> Drop for MCSLockGuard<'l, K, V> {
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

enum AcqResult<'l, K, V> {
    Acquired(MCSLockGuard<'l, K, V>),
    Eliminated(V),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Insert,
    Delete,
    Balance,
}

struct Node<K, V> {
    keys: [Cell<Option<K>>; DEGREE],
    search_key: K,
    lock: AtomicPtr<MCSLockSlot<K, V>>,
    /// The number of next pointers (for an internal node) or values (for a leaf node).
    /// Note that it may not be equal to the number of keys, because the last next pointer
    /// is mapped by a bottom key (i.e., `None`).
    size: AtomicUsize,
    weight: bool,
    marked: AtomicBool,
    kind: NodeKind<K, V>,
}

// Leaf or Internal node specific data.
enum NodeKind<K, V> {
    Leaf {
        values: [Cell<Option<V>>; DEGREE],
        write_version: AtomicUsize,
    },
    Internal {
        next: [Atomic<Node<K, V>>; DEGREE],
    },
}

impl<K, V> Node<K, V> {
    fn is_leaf(&self) -> bool {
        match &self.kind {
            NodeKind::Leaf { .. } => true,
            NodeKind::Internal { .. } => false,
        }
    }

    fn next(&self) -> &[Atomic<Self>; DEGREE] {
        match &self.kind {
            NodeKind::Internal { next } => next,
            _ => panic!("No next pointers for a leaf node."),
        }
    }

    fn next_mut(&mut self) -> &mut [Atomic<Self>; DEGREE] {
        match &mut self.kind {
            NodeKind::Internal { next } => next,
            _ => panic!("No next pointers for a leaf node."),
        }
    }

    fn load_next<'g>(&self, index: usize, guard: &'g Guard) -> Shared<'g, Self> {
        self.next()[index].load(Ordering::Acquire, guard)
    }

    fn store_next<'g>(&'g self, index: usize, ptr: impl Pointer<Self>, _: &MCSLockGuard<'g, K, V>) {
        self.next()[index].store(ptr, Ordering::Release);
    }

    fn init_next<'g>(&mut self, index: usize, ptr: impl Pointer<Self>) {
        self.next_mut()[index] = Atomic::from(ptr.into_usize() as *const Self);
    }

    /// # Safety
    ///
    /// The write version record must be accessed by `start_write` and `WriteGuard`.
    unsafe fn write_version(&self) -> &AtomicUsize {
        match &self.kind {
            NodeKind::Leaf { write_version, .. } => write_version,
            _ => panic!("No write version for an internal node."),
        }
    }

    fn start_write<'g>(&'g self, lock: &MCSLockGuard<'g, K, V>) -> WriteGuard<'g, K, V> {
        debug_assert!(eq(unsafe { lock.owner_node() }, self));
        let version = unsafe { self.write_version() };
        let init_version = version.load(Ordering::Acquire);
        debug_assert!(init_version % 2 == 0);
        version.store(init_version + 1, Ordering::Release);
        compiler_fence(Ordering::SeqCst);

        return WriteGuard {
            init_version,
            node: self,
        };
    }

    fn key_count(&self) -> usize {
        match &self.kind {
            NodeKind::Leaf { .. } => self.size.load(Ordering::Acquire),
            NodeKind::Internal { .. } => self.size.load(Ordering::Acquire) - 1,
        }
    }
}

impl<K, V> Node<K, V>
where
    K: PartialOrd + Eq + Default + Copy,
    V: Default + Copy,
{
    fn get_value(&self, index: usize) -> Option<V> {
        match &self.kind {
            NodeKind::Leaf { values, .. } => values[index].get(),
            _ => panic!("No values for an internal node."),
        }
    }

    fn set_value<'g>(&'g self, index: usize, val: V, _: &WriteGuard<'g, K, V>) {
        match &self.kind {
            NodeKind::Leaf { values, .. } => values[index].set(Some(val)),
            _ => panic!("No values for an internal node."),
        }
    }

    fn init_value(&mut self, index: usize, val: V) {
        match &mut self.kind {
            NodeKind::Leaf { values, .. } => *values[index].get_mut() = Some(val),
            _ => panic!("No values for an internal node."),
        }
    }

    fn get_key(&self, index: usize) -> Option<K> {
        self.keys[index].get()
    }

    fn set_key<'g>(&'g self, index: usize, key: Option<K>, _: &WriteGuard<'g, K, V>) {
        self.keys[index].set(key);
    }

    fn init_key(&mut self, index: usize, key: Option<K>) {
        *self.keys[index].get_mut() = key;
    }

    fn internal(weight: bool, size: usize, search_key: K) -> Self {
        Self {
            keys: Default::default(),
            search_key,
            lock: Default::default(),
            size: AtomicUsize::new(size),
            weight,
            marked: AtomicBool::new(false),
            kind: NodeKind::Internal {
                next: Default::default(),
            },
        }
    }

    fn leaf(weight: bool, size: usize, search_key: K) -> Self {
        Self {
            keys: Default::default(),
            search_key,
            lock: Default::default(),
            size: AtomicUsize::new(size),
            weight,
            marked: AtomicBool::new(false),
            kind: NodeKind::Leaf {
                values: Default::default(),
                write_version: AtomicUsize::new(0),
            },
        }
    }

    fn child_index(&self, key: &K) -> usize {
        let key_count = self.key_count();
        let mut index = 0;
        while index < key_count && !(key < &self.keys[index].get().unwrap()) {
            index += 1;
        }
        index
    }

    // Search a node for a key repeatedly until we successfully read a consistent version.
    fn read_consistent(&self, key: &K) -> (usize, Option<V>) {
        let NodeKind::Leaf {
            values,
            write_version,
        } = &self.kind
        else {
            panic!("Attempted to read value from an internal node.");
        };
        loop {
            let mut version = write_version.load(Ordering::Acquire);
            while version & 1 > 0 {
                version = write_version.load(Ordering::Acquire);
            }
            let mut key_index = 0;
            while key_index < DEGREE && self.keys[key_index].get() != Some(*key) {
                key_index += 1;
            }
            let value = values.get(key_index).and_then(|value| value.get());
            compiler_fence(Ordering::SeqCst);

            if version == write_version.load(Ordering::Acquire) {
                return (key_index, value);
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
        debug_assert!(self.is_leaf());
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
    fn absorb_child(
        &self,
        child: &Self,
        child_idx: usize,
    ) -> (
        [Atomic<Node<K, V>>; DEGREE * 2],
        [Cell<Option<K>>; DEGREE * 2],
    ) {
        let mut next: [Atomic<Node<K, V>>; DEGREE * 2] = Default::default();
        let mut keys: [Cell<Option<K>>; DEGREE * 2] = Default::default();
        let psize = self.size.load(Ordering::Relaxed);
        let nsize = child.size.load(Ordering::Relaxed);

        slice_clone(&self.next()[0..], &mut next[0..], child_idx);
        slice_clone(&child.next()[0..], &mut next[child_idx..], nsize);
        slice_clone(
            &self.next()[child_idx + 1..],
            &mut next[child_idx + nsize..],
            psize - (child_idx + 1),
        );

        slice_clone(&self.keys[0..], &mut keys[0..], child_idx);
        slice_clone(&child.keys[0..], &mut keys[child_idx..], child.key_count());
        slice_clone(
            &self.keys[child_idx..],
            &mut keys[child_idx + child.key_count()..],
            self.key_count() - child_idx,
        );

        (next, keys)
    }

    /// It requires a lock to guarantee the consistency.
    /// Its length is equal to `key_count`.
    fn enumerate_key<'g>(
        &'g self,
        _: &MCSLockGuard<'g, K, V>,
    ) -> impl Iterator<Item = (usize, K)> + 'g {
        self.keys
            .iter()
            .enumerate()
            .filter_map(|(i, k)| k.get().map(|k| (i, k)))
    }

    /// Iterates key-value pairs in this **leaf** node.
    /// It requires a lock to guarantee the consistency.
    /// Its length is equal to the size of this node.
    fn iter_key_value<'g>(
        &'g self,
        lock: &MCSLockGuard<'g, K, V>,
    ) -> impl Iterator<Item = (K, V)> + 'g {
        self.enumerate_key(lock)
            .map(|(i, k)| (k, self.get_value(i).unwrap()))
    }

    /// Iterates key-next pairs in this **internal** node.
    /// It requires a lock to guarantee the consistency.
    /// Its length is equal to the size of this node, and only the last key is `None`.
    fn iter_key_next<'g>(
        &'g self,
        lock: &MCSLockGuard<'g, K, V>,
        guard: &'g Guard,
    ) -> impl Iterator<Item = (Option<K>, Shared<'g, Self>)> + 'g {
        self.enumerate_key(lock)
            .map(|(i, k)| (Some(k), self.load_next(i, guard)))
            .chain(once((None, self.load_next(self.key_count(), guard))))
    }
}

struct WriteGuard<'g, K, V> {
    init_version: usize,
    node: &'g Node<K, V>,
}

impl<'g, K, V> Drop for WriteGuard<'g, K, V> {
    fn drop(&mut self) {
        unsafe { self.node.write_version() }.store(self.init_version + 2, Ordering::Release);
    }
}

struct Cursor<'g, K, V> {
    l: Shared<'g, Node<K, V>>,
    p: Shared<'g, Node<K, V>>,
    gp: Shared<'g, Node<K, V>>,
    /// Index of `p` in `gp`.
    gp_p_idx: usize,
    /// Index of `l` in `p`.
    p_l_idx: usize,
    /// Index of the key in `l`.
    l_key_idx: usize,
    val: Option<V>,
}

pub struct ElimABTree<K, V> {
    entry: Node<K, V>,
}

unsafe impl<K: Sync, V: Sync> Sync for ElimABTree<K, V> {}
unsafe impl<K: Send, V: Send> Send for ElimABTree<K, V> {}

impl<K, V> ElimABTree<K, V>
where
    K: Ord + Eq + Default + Copy,
    V: Default + Copy,
{
    const ABSORB_THRESHOLD: usize = DEGREE;
    const UNDERFULL_THRESHOLD: usize = if DEGREE / 4 < 2 { 2 } else { DEGREE / 4 };

    pub fn new() -> Self {
        let left = Node::leaf(true, 0, K::default());
        let mut entry = Node::internal(true, 1, K::default());
        entry.init_next(0, Owned::new(left));
        Self { entry }
    }

    /// Performs a basic search and returns the value associated with the key,
    /// or `None` if nothing is found. Unlike other search methods, it does not return
    /// any path information, making it slightly faster.
    pub fn search_basic(&self, key: &K, guard: &Guard) -> Option<V> {
        let mut node = unsafe { self.entry.load_next(0, guard).deref() };
        while let NodeKind::Internal { next } = &node.kind {
            let next = next[node.child_index(key)].load(Ordering::Acquire, guard);
            node = unsafe { next.deref() };
        }
        node.read_consistent(key).1
    }

    fn search<'g>(
        &self,
        key: &K,
        target: Option<Shared<'g, Node<K, V>>>,
        guard: &'g Guard,
    ) -> (bool, Cursor<'g, K, V>) {
        let mut cursor = Cursor {
            l: self.entry.load_next(0, guard),
            p: unsafe { Shared::from_usize(&self.entry as *const _ as usize) },
            gp: Shared::null(),
            gp_p_idx: 0,
            p_l_idx: 0,
            l_key_idx: 0,
            val: None,
        };

        while !unsafe { cursor.l.deref() }.is_leaf()
            && target.map(|target| target != cursor.l).unwrap_or(true)
        {
            let l_node = unsafe { cursor.l.deref() };
            cursor.gp = cursor.p;
            cursor.p = cursor.l;
            cursor.gp_p_idx = cursor.p_l_idx;
            cursor.p_l_idx = l_node.child_index(key);
            cursor.l = l_node.load_next(cursor.p_l_idx, guard);
        }

        if let Some(target) = target {
            (cursor.l == target, cursor)
        } else {
            let (index, value) = unsafe { cursor.l.deref() }.read_consistent(key);
            cursor.val = value;
            cursor.l_key_idx = index;
            (value.is_some(), cursor)
        }
    }

    pub fn insert(&self, key: &K, value: &V, guard: &Guard) -> Option<V> {
        loop {
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
        cursor: &Cursor<'g, K, V>,
        guard: &'g Guard,
    ) -> Result<Option<V>, ()> {
        let node = unsafe { cursor.l.deref() };
        let parent = unsafe { cursor.p.deref() };

        debug_assert!(node.is_leaf());
        debug_assert!(!parent.is_leaf());

        let node_lock_slot = UnsafeCell::new(MCSLockSlot::new());
        let node_lock = match node.acquire(Operation::Insert, Some(*key), &node_lock_slot) {
            AcqResult::Acquired(lock) => lock,
            AcqResult::Eliminated(value) => return Ok(Some(value)),
        };
        if node.marked.load(Ordering::SeqCst) {
            return Err(());
        }
        for i in 0..DEGREE {
            if node.get_key(i) == Some(*key) {
                return Ok(Some(node.get_value(i).unwrap()));
            }
        }
        // At this point, we are guaranteed key is not in the node.

        if node.size.load(Ordering::Acquire) < Self::ABSORB_THRESHOLD {
            // We have the capacity to fit this new key. So let's just find an empty slot.
            for i in 0..DEGREE {
                if node.get_key(i).is_some() {
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
            try_acq_val_or!(parent, parent_lock, Operation::Insert, None, return Err(()));

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

            let mut left = Node::leaf(true, left_size, kv_pairs[0].0);
            for i in 0..left_size {
                left.init_key(i, Some(kv_pairs[i].0));
                left.init_value(i, kv_pairs[i].1);
            }

            let mut right = Node::leaf(true, right_size, kv_pairs[left_size].0);
            for i in 0..right_size {
                right.init_key(i, Some(kv_pairs[i + left_size].0));
                right.init_value(i, kv_pairs[i + left_size].1);
            }

            // The weight of new internal node `n` will be zero, unless it is the root.
            // This is because we test `p == entry`, above; in doing this, we are actually
            // performing Root-Zero at the same time as this Overflow if `n` will become the root.
            let mut internal = Node::internal(eq(parent, &self.entry), 2, kv_pairs[left_size].0);
            internal.init_key(0, Some(kv_pairs[left_size].0));
            internal.init_next(0, Owned::new(left));
            internal.init_next(1, Owned::new(right));

            // If the parent is not marked, `parent.next[cursor.p_l_idx]` is guaranteed to contain
            // a node since any update to parent would have deleted node (and hence we would have
            // returned at the `node.marked` check).
            let new_internal = Owned::new(internal).into_shared(guard);
            parent.store_next(cursor.p_l_idx, new_internal, &parent_lock);
            node.marked.store(true, Ordering::Release);

            // Manually unlock and fix the tag.
            drop((parent_lock, node_lock));
            unsafe { guard.defer_destroy(cursor.l) };
            self.fix_tag_violation(new_internal, guard);

            Ok(None)
        }
    }

    fn fix_tag_violation<'g>(&self, viol: Shared<'g, Node<K, V>>, guard: &'g Guard) {
        loop {
            let viol_node = unsafe { viol.deref() };
            if viol_node.weight {
                return;
            }

            // `viol` should be internal because leaves always have weight = 1.
            debug_assert!(!viol_node.is_leaf());
            // `viol` is not the entry or root node because both should always have weight = 1.
            debug_assert!(!eq(viol_node, &self.entry) && self.entry.load_next(0, guard) != viol);

            let (found, cursor) = self.search(&viol_node.search_key, Some(viol), guard);
            if !found {
                return;
            }

            debug_assert!(!cursor.gp.is_null());
            let node = unsafe { cursor.l.deref() };
            let parent = unsafe { cursor.p.deref() };
            let gparent = unsafe { cursor.gp.deref() };
            debug_assert!(!node.is_leaf());
            debug_assert!(!parent.is_leaf());
            debug_assert!(!gparent.is_leaf());

            if !eq(node, viol_node) {
                // `viol` was replaced by another update.
                // We hand over responsibility for `viol` to that update.
                return;
            }

            // We cannot apply this update if p has a weight violation.
            // So, we check if this is the case, and, if so, try to fix it.
            if !parent.weight {
                self.fix_tag_violation(cursor.p, guard);
                continue;
            }

            try_acq_val_or!(node, node_lock, Operation::Balance, None, continue);
            try_acq_val_or!(parent, parent_lock, Operation::Balance, None, continue);
            try_acq_val_or!(gparent, gparent_lock, Operation::Balance, None, continue);

            let psize = parent.size.load(Ordering::Relaxed);
            let nsize = viol_node.size.load(Ordering::Relaxed);
            // We don't ever change the size of a tag node, so its size should always be 2.
            debug_assert_eq!(nsize, 2);
            let c = psize + nsize;
            let size = c - 1;
            let (next, keys) = parent.absorb_child(node, cursor.p_l_idx);

            if size <= Self::ABSORB_THRESHOLD {
                // Absorb case.

                // Create new node(s).
                // The new arrays are small enough to fit in a single node,
                // so we replace p by a new internal node.
                let mut absorber = Node::internal(true, size, parent.get_key(0).unwrap());
                slice_clone(&next, absorber.next_mut(), DEGREE);
                slice_clone(&keys, &mut absorber.keys, DEGREE);

                gparent.store_next(cursor.gp_p_idx, Owned::new(absorber), &gparent_lock);
                node.marked.store(true, Ordering::Relaxed);
                parent.marked.store(true, Ordering::Relaxed);

                unsafe { guard.defer_destroy(cursor.l) };
                unsafe { guard.defer_destroy(cursor.p) };
                return;
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
                let mut left = Node::internal(true, left_size, keys[0].get().unwrap());
                slice_clone(&keys[0..], &mut left.keys[0..], left_size - 1);
                slice_clone(&next[0..], &mut left.next_mut()[0..], left_size);

                let right_size = size - left_size;
                let mut right = Node::internal(true, right_size, keys[left_size].get().unwrap());
                slice_clone(&keys[left_size..], &mut right.keys[0..], right_size - 1);
                slice_clone(&next[left_size..], &mut right.next_mut()[0..], right_size);

                // Note: keys[left_size - 1] should be the same as new_internal.keys[0].
                let mut new_internal = Node::internal(
                    eq(gparent, &self.entry),
                    2,
                    keys[left_size - 1].get().unwrap(),
                );
                new_internal.init_key(0, keys[left_size - 1].get());
                new_internal.init_next(0, Owned::new(left));
                new_internal.init_next(1, Owned::new(right));

                // The weight of new internal node `n` will be zero, unless it is the root.
                // This is because we test `p == entry`, above; in doing this, we are actually
                // performing Root-Zero at the same time
                // as this Overflow if `n` will become the root.

                let new_internal = Owned::new(new_internal).into_shared(guard);
                gparent.store_next(cursor.gp_p_idx, new_internal, &gparent_lock);
                node.marked.store(true, Ordering::Relaxed);
                parent.marked.store(true, Ordering::Relaxed);

                unsafe { guard.defer_destroy(cursor.l) };
                unsafe { guard.defer_destroy(cursor.p) };

                drop((node_lock, parent_lock, gparent_lock));
                self.fix_tag_violation(new_internal, guard);
                return;
            }
        }
    }

    pub fn remove(&self, key: &K, guard: &Guard) -> Option<V> {
        loop {
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

    fn remove_inner<'g>(
        &self,
        key: &K,
        cursor: &Cursor<'g, K, V>,
        guard: &'g Guard,
    ) -> Result<Option<V>, ()> {
        let node = unsafe { cursor.l.deref() };
        let parent = unsafe { cursor.p.deref() };
        let gparent = unsafe { cursor.gp.as_ref() };

        debug_assert!(node.is_leaf());
        debug_assert!(!parent.is_leaf());
        debug_assert!(gparent.map(|gp| !gp.is_leaf()).unwrap_or(true));

        try_acq_val_or!(
            node,
            node_lock,
            Operation::Delete,
            Some(*key),
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
            if node.get_key(i) == Some(*key) {
                let val = node.get_value(i).unwrap();
                let wguard = node.start_write(&node_lock);
                node.set_key(i, None, &wguard);
                node.size.store(new_size, Ordering::Relaxed);

                node.elim_key_ops(val, wguard, &node_lock);

                if new_size == Self::UNDERFULL_THRESHOLD - 1 {
                    drop(node_lock);
                    self.fix_underfull_violation(cursor.l, guard);
                }
                return Ok(Some(val));
            }
        }
        Err(())
    }

    fn fix_underfull_violation<'g>(&self, viol: Shared<'g, Node<K, V>>, guard: &'g Guard) {
        // We search for `viol` and try to fix any violation we find there.
        // This entails performing AbsorbSibling or Distribute.
        let viol_node = unsafe { viol.deref() };
        loop {
            // We do not need a lock for the `viol == entry.ptrs[0]` check since since we cannot
            // "be turned into" the root. The root is only created by the root absorb
            // operation below, so a node that is not the root will never become the root.
            if viol_node.size.load(Ordering::Relaxed) >= Self::UNDERFULL_THRESHOLD
                || eq(viol_node, &self.entry)
                || viol == self.entry.load_next(0, guard)
            {
                // No degree violation at `viol`.
                return;
            }

            // Search for `viol`.
            let (_, cursor) = self.search(&viol_node.search_key, Some(viol), guard);
            let node = unsafe { cursor.l.deref() };
            let parent = unsafe { cursor.p.deref() };
            // `gp` cannot be null, because if AbsorbSibling or Distribute can be applied,
            // then `p` is not the root.
            debug_assert!(!cursor.gp.is_null());
            let gparent = unsafe { cursor.gp.deref() };

            if parent.size.load(Ordering::Relaxed) < Self::UNDERFULL_THRESHOLD
                && !eq(parent, &self.entry)
                && cursor.p != self.entry.load_next(0, guard)
            {
                self.fix_underfull_violation(cursor.p, guard);
                continue;
            }

            if !eq(node, viol_node) {
                // `viol` was replaced by another update.
                // We hand over responsibility for `viol` to that update.
                return;
            }

            let sibling_idx = if cursor.p_l_idx > 0 {
                cursor.p_l_idx - 1
            } else {
                1
            };
            // Don't need a lock on parent here because if the pointer to sibling changes
            // to a different node after this, sibling will be marked
            // (Invariant: when a pointer switches away from a node, the node is marked)
            let sibling_sh = parent.load_next(sibling_idx, guard);
            let sibling = unsafe { sibling_sh.deref() };

            // Prevent deadlocks by acquiring left node first.
            let ((left, left_idx), (right, right_idx)) = if sibling_idx < cursor.p_l_idx {
                ((sibling, sibling_idx), (node, cursor.p_l_idx))
            } else {
                ((node, cursor.p_l_idx), (sibling, sibling_idx))
            };

            try_acq_val_or!(left, left_lock, Operation::Balance, None, continue);
            try_acq_val_or!(right, right_lock, Operation::Balance, None, continue);

            // Repeat this check, this might have changed while we locked `viol`.
            if viol_node.size.load(Ordering::Relaxed) >= Self::UNDERFULL_THRESHOLD {
                // No degree violation at `viol`.
                return;
            }

            try_acq_val_or!(parent, parent_lock, Operation::Balance, None, continue);
            try_acq_val_or!(gparent, gparent_lock, Operation::Balance, None, continue);

            // We can only apply AbsorbSibling or Distribute if there are no
            // weight violations at `parent`, `node`, or `sibling`.
            // So, we first check for any weight violations and fix any that we see.
            if !parent.weight || !node.weight || !sibling.weight {
                drop((left_lock, right_lock, parent_lock, gparent_lock));
                self.fix_tag_violation(cursor.p, guard);
                self.fix_tag_violation(cursor.l, guard);
                self.fix_tag_violation(sibling_sh, guard);
                continue;
            }

            // There are no weight violations at `parent`, `node` or `sibling`.
            debug_assert!(parent.weight && node.weight && sibling.weight);
            // l and s are either both leaves or both internal nodes,
            // because there are no weight violations at these nodes.
            debug_assert!(
                (node.is_leaf() && sibling.is_leaf()) || (!node.is_leaf() && !sibling.is_leaf())
            );

            let lsize = left.size.load(Ordering::Relaxed);
            let rsize = right.size.load(Ordering::Relaxed);
            let psize = parent.size.load(Ordering::Relaxed);
            let size = lsize + rsize;

            if size < 2 * Self::UNDERFULL_THRESHOLD {
                // AbsorbSibling
                let new_node = if left.is_leaf() {
                    debug_assert!(right.is_leaf());
                    let mut new_leaf = Owned::new(Node::leaf(true, size, node.search_key));
                    let kv_iter = left
                        .iter_key_value(&left_lock)
                        .chain(right.iter_key_value(&right_lock))
                        .enumerate();
                    for (i, (key, value)) in kv_iter {
                        new_leaf.init_key(i, Some(key));
                        new_leaf.init_value(i, value);
                    }
                    new_leaf
                } else {
                    debug_assert!(!right.is_leaf());
                    let mut new_internal = Owned::new(Node::internal(true, size, node.search_key));
                    let key_btw = parent.get_key(left_idx).unwrap();
                    let kn_iter = left
                        .iter_key_next(&left_lock, guard)
                        .map(|(k, n)| (Some(k.unwrap_or(key_btw)), n))
                        .chain(right.iter_key_next(&right_lock, guard))
                        .enumerate();
                    for (i, (key, next)) in kn_iter {
                        new_internal.init_key(i, key);
                        new_internal.init_next(i, next);
                    }
                    new_internal
                }
                .into_shared(guard);

                // Now, we atomically replace `p` and its children with the new nodes.
                // If appropriate, we perform RootAbsorb at the same time.
                if eq(gparent, &self.entry) && psize == 2 {
                    debug_assert!(cursor.gp_p_idx == 0);
                    gparent.store_next(cursor.gp_p_idx, new_node, &gparent_lock);
                    node.marked.store(true, Ordering::Relaxed);
                    parent.marked.store(true, Ordering::Relaxed);
                    sibling.marked.store(true, Ordering::Relaxed);

                    unsafe {
                        guard.defer_destroy(cursor.l);
                        guard.defer_destroy(cursor.p);
                        guard.defer_destroy(sibling_sh);
                    }

                    drop((left_lock, right_lock, parent_lock, gparent_lock));
                    self.fix_underfull_violation(new_node, guard);
                    return;
                } else {
                    debug_assert!(!eq(gparent, &self.entry) || psize > 2);
                    let mut new_parent = Node::internal(true, psize - 1, parent.search_key);
                    for i in 0..left_idx {
                        new_parent.init_key(i, parent.get_key(i));
                    }
                    for i in 0..sibling_idx {
                        new_parent.init_next(i, parent.load_next(i, guard));
                    }
                    for i in left_idx + 1..parent.key_count() {
                        new_parent.init_key(i - 1, parent.get_key(i));
                    }
                    for i in cursor.p_l_idx + 1..psize {
                        new_parent.init_next(i - 1, parent.load_next(i, guard));
                    }

                    new_parent.init_next(
                        cursor.p_l_idx - (if cursor.p_l_idx > sibling_idx { 1 } else { 0 }),
                        new_node,
                    );
                    let new_parent = Owned::new(new_parent).into_shared(guard);

                    gparent.store_next(cursor.gp_p_idx, new_parent, &gparent_lock);
                    node.marked.store(true, Ordering::Relaxed);
                    parent.marked.store(true, Ordering::Relaxed);
                    sibling.marked.store(true, Ordering::Relaxed);

                    unsafe {
                        guard.defer_destroy(cursor.l);
                        guard.defer_destroy(cursor.p);
                        guard.defer_destroy(sibling_sh);
                    }

                    drop((left_lock, right_lock, parent_lock, gparent_lock));
                    self.fix_underfull_violation(new_node, guard);
                    self.fix_underfull_violation(new_parent, guard);
                    return;
                }
            } else {
                // Distribute
                let left_size = size / 2;
                let right_size = size - left_size;

                assert!(left.is_leaf() == right.is_leaf());

                // `pivot`: Reserve one key for the parent
                //          (to go between `new_left` and `new_right`).
                let (new_left, new_right, pivot) = if left.is_leaf() {
                    // Combine the contents of `l` and `s`.
                    let mut kv_pairs = left
                        .iter_key_value(&left_lock)
                        .chain(right.iter_key_value(&right_lock))
                        .collect::<ArrayVec<(K, V), { 2 * DEGREE }>>();
                    kv_pairs.sort_by_key(|(k, _)| *k);
                    let mut kv_iter = kv_pairs.iter().copied();

                    let new_left = {
                        let mut new_leaf =
                            Owned::new(Node::leaf(true, left_size, Default::default()));
                        for i in 0..left_size {
                            let (k, v) = kv_iter.next().unwrap();
                            new_leaf.init_key(i, Some(k));
                            new_leaf.init_value(i, v);
                        }
                        new_leaf.search_key = new_leaf.get_key(0).unwrap();
                        new_leaf
                    };

                    let (new_right, pivot) = {
                        debug_assert!(left.is_leaf());
                        let mut new_leaf =
                            Owned::new(Node::leaf(true, right_size, Default::default()));
                        for i in 0..right_size {
                            let (k, v) = kv_iter.next().unwrap();
                            new_leaf.init_key(i, Some(k));
                            new_leaf.init_value(i, v);
                        }
                        let pivot = new_leaf.get_key(0).unwrap();
                        new_leaf.search_key = pivot;
                        (new_leaf, pivot)
                    };

                    debug_assert!(kv_iter.next().is_none());
                    (new_left, new_right, pivot)
                } else {
                    // Combine the contents of `l` and `s`
                    // (and one key from `p` if `l` and `s` are internal).
                    let key_btw = parent.get_key(left_idx).unwrap();
                    let mut kn_iter = left
                        .iter_key_next(&left_lock, guard)
                        .map(|(k, n)| (Some(k.unwrap_or(key_btw)), n))
                        .chain(right.iter_key_next(&right_lock, guard));

                    let (new_left, pivot) = {
                        let mut new_internal =
                            Owned::new(Node::internal(true, left_size, Default::default()));
                        for i in 0..left_size {
                            let (k, n) = kn_iter.next().unwrap();
                            new_internal.init_key(i, k);
                            new_internal.init_next(i, n);
                        }
                        let pivot = new_internal.keys[left_size - 1].take().unwrap();
                        new_internal.search_key = new_internal.get_key(0).unwrap();
                        (new_internal, pivot)
                    };

                    let new_right = {
                        let mut new_internal =
                            Owned::new(Node::internal(true, right_size, Default::default()));
                        for i in 0..right_size {
                            let (k, n) = kn_iter.next().unwrap();
                            new_internal.init_key(i, k);
                            new_internal.init_next(i, n);
                        }
                        new_internal.search_key = new_internal.get_key(0).unwrap();
                        new_internal
                    };

                    debug_assert!(kn_iter.next().is_none());
                    (new_left, new_right, pivot)
                };

                let mut new_parent =
                    Owned::new(Node::internal(parent.weight, psize, parent.search_key));
                slice_clone(
                    &parent.keys[0..],
                    &mut new_parent.keys[0..],
                    parent.key_count(),
                );
                slice_clone(&parent.next()[0..], &mut new_parent.next_mut()[0..], psize);
                new_parent.init_next(left_idx, new_left);
                new_parent.init_next(right_idx, new_right);
                new_parent.init_key(left_idx, Some(pivot));

                gparent.store_next(cursor.gp_p_idx, new_parent, &gparent_lock);
                node.marked.store(true, Ordering::Relaxed);
                parent.marked.store(true, Ordering::Relaxed);
                sibling.marked.store(true, Ordering::Relaxed);

                unsafe {
                    guard.defer_destroy(cursor.l);
                    guard.defer_destroy(cursor.p);
                    guard.defer_destroy(sibling_sh);
                }

                return;
            }
        }
    }
}

impl<K, V> Drop for ElimABTree<K, V> {
    fn drop(&mut self) {
        let mut stack = vec![];
        let guard = unsafe { unprotected() };
        for next in &self.entry.next()[0..self.entry.size.load(Ordering::Relaxed)] {
            stack.push(next.load(Ordering::Relaxed, guard));
        }

        while let Some(node) = stack.pop() {
            let node_ref = unsafe { node.deref() };
            if !node_ref.is_leaf() {
                for next in &node_ref.next()[0..node_ref.size.load(Ordering::Relaxed)] {
                    stack.push(next.load(Ordering::Relaxed, guard));
                }
            }
            drop(unsafe { node.into_owned() });
        }
    }
}

/// Similar to `memcpy`, but for `Clone` types.
#[inline]
fn slice_clone<T: Clone>(src: &[T], dst: &mut [T], len: usize) {
    dst[0..len].clone_from_slice(&src[0..len]);
}

impl<K, V> ConcurrentMap<K, V> for ElimABTree<K, V>
where
    K: Ord + Eq + Default + Copy,
    V: Default + Copy,
{
    fn new() -> Self {
        Self::new()
    }

    #[inline(always)]
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<impl OutputHolder<V>> {
        self.search_basic(key, guard)
    }

    #[inline(always)]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.insert(&key, &value, guard).is_none()
    }

    #[inline(always)]
    fn remove<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<impl OutputHolder<V>> {
        self.remove(key, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::ElimABTree;
    use crate::ds_impl::ebr::concurrent_map;

    #[test]
    fn smoke_elim_ab_tree() {
        concurrent_map::tests::smoke::<_, ElimABTree<i32, i32>, _>(&|a| *a);
    }
}
