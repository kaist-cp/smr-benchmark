use super::concurrent_map::{ConcurrentMap, OutputHolder};
use crossbeam_ebr::{unprotected, Atomic, Guard, Owned, Pointer, Shared};

use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::mem::{transmute, MaybeUninit};
use std::ptr::{null, null_mut};
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};

// Copied from the original author's code:
// https://gitlab.com/trbot86/setbench/-/blob/f4711af3ace28d8b4fa871559db74fb4e0e62cc0/ds/srivastava_abtree_mcs/adapter.h#L17
const DEGREE: usize = 11;

struct MCSLockSlot<K, V> {
    node: *const Node<K, V>,
    op: Operation,
    key: Option<K>,
    next: AtomicPtr<Self>,
    owned: AtomicBool,
    short_circuit: AtomicBool,
    ret: UnsafeCell<Option<V>>,
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
            ret: UnsafeCell::new(None),
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
}

impl<'l, K, V> Drop for MCSLockGuard<'l, K, V> {
    fn drop(&mut self) {
        let slot = unsafe { &*self.slot.get() };
        let node = unsafe { &*slot.node };
        let next = if let Some(next) = unsafe { slot.next.load(Ordering::Relaxed).as_ref() } {
            next
        } else {
            if node
                .lock
                .compare_exchange(
                    self.slot.get(),
                    null_mut(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                slot.owned.store(false, Ordering::Release);
                return;
            }
            loop {
                if let Some(next) = unsafe { slot.next.load(Ordering::Relaxed).as_ref() } {
                    break next;
                }
                spin_loop();
            }
        };
        next.owned.store(true, Ordering::Release);
        slot.owned.store(false, Ordering::Release);
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
    keys: [UnsafeCell<Option<K>>; DEGREE],
    search_key: K,
    lock: AtomicPtr<MCSLockSlot<K, V>>,
    size: AtomicUsize,
    weight: AtomicBool,
    marked: AtomicBool,
    kind: NodeSpecific<K, V>,
}

// Leaf or Internal node specific data.
enum NodeSpecific<K, V> {
    Leaf {
        values: [UnsafeCell<V>; DEGREE],
        write_version: AtomicUsize,
    },
    Internal {
        next: [Atomic<Node<K, V>>; DEGREE],
    },
}

impl<K, V> Node<K, V> {
    fn is_leaf(&self) -> bool {
        match &self.kind {
            NodeSpecific::Leaf { .. } => true,
            NodeSpecific::Internal { .. } => false,
        }
    }

    fn next(&self) -> &[Atomic<Self>; DEGREE] {
        match &self.kind {
            NodeSpecific::Internal { next } => next,
            _ => panic!("No next pointers for a leaf node."),
        }
    }

    fn next_mut(&mut self) -> &mut [Atomic<Self>; DEGREE] {
        match &mut self.kind {
            NodeSpecific::Internal { next } => next,
            _ => panic!("No next pointers for a leaf node."),
        }
    }

    fn values(&self) -> &[UnsafeCell<V>; DEGREE] {
        match &self.kind {
            NodeSpecific::Leaf { values, .. } => values,
            _ => panic!("No values for an internal node."),
        }
    }

    fn values_mut(&mut self) -> &mut [UnsafeCell<V>; DEGREE] {
        match &mut self.kind {
            NodeSpecific::Leaf { values, .. } => values,
            _ => panic!("No values for an internal node."),
        }
    }

    fn write_version(&self) -> &AtomicUsize {
        match &self.kind {
            NodeSpecific::Leaf { write_version, .. } => write_version,
            _ => panic!("No write version for an internal node."),
        }
    }

    fn key_count(&self) -> usize {
        match &self.kind {
            NodeSpecific::Leaf { .. } => self.size.load(Ordering::Acquire),
            NodeSpecific::Internal { .. } => self.size.load(Ordering::Acquire) - 1,
        }
    }
}

impl<K, V> Node<K, V>
where
    K: PartialOrd + Eq + Default + Copy,
    V: Default + Copy,
{
    fn internal(weight: bool, size: usize, search_key: K) -> Self {
        Self {
            keys: Default::default(),
            search_key,
            lock: Default::default(),
            size: AtomicUsize::new(size),
            weight: AtomicBool::new(weight),
            marked: AtomicBool::new(false),
            kind: NodeSpecific::Internal {
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
            weight: AtomicBool::new(weight),
            marked: AtomicBool::new(false),
            kind: NodeSpecific::Leaf {
                values: Default::default(),
                write_version: AtomicUsize::new(0),
            },
        }
    }

    fn child_index(&self, key: &K) -> usize {
        let mut index = 0;
        while index < self.key_count()
            && !(key < unsafe { &*self.keys[index].get() }.as_ref().unwrap())
        {
            index += 1;
        }
        index
    }

    // Search a node for a key repeatedly until we successfully read a consistent version.
    fn read_value_version(&self, key: &K) -> (usize, Option<V>, usize) {
        let NodeSpecific::Leaf {
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
            while key_index < DEGREE && unsafe { *self.keys[key_index].get() } != Some(*key) {
                key_index += 1;
            }
            let value = if key_index < DEGREE {
                Some(unsafe { *values[key_index].get() })
            } else {
                None
            };
            compiler_fence(Ordering::SeqCst);

            if version == write_version.load(Ordering::Acquire) {
                return (key_index, value, version);
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
        let old_tail = self.lock.swap(slot.get(), Ordering::Relaxed);
        let curr = unsafe { &*slot.get() };

        if let Some(old_tail) = unsafe { old_tail.as_ref() } {
            old_tail.next.store(slot.get(), Ordering::Relaxed);
            while !curr.owned.load(Ordering::Acquire) && !curr.short_circuit.load(Ordering::Acquire)
            {
                spin_loop();
            }
            if curr.short_circuit.load(Ordering::Relaxed) {
                return AcqResult::Eliminated(unsafe { *curr.ret.get() }.unwrap());
            }
            debug_assert!(curr.owned.load(Ordering::Relaxed));
        } else {
            curr.owned.store(true, Ordering::Release);
        }
        return AcqResult::Acquired(MCSLockGuard::new(slot));
    }

    fn elim_key_ops<'l>(&'l self, value: V, old_version: usize, guard: &MCSLockGuard<'l, K, V>) {
        let slot = unsafe { &*guard.slot.get() };
        debug_assert!(slot.owned.load(Ordering::Relaxed));
        debug_assert!(self.is_leaf());
        debug_assert!(slot.op != Operation::Balance);

        let stop_node = self.lock.load(Ordering::Relaxed);
        self.write_version()
            .store(old_version + 2, Ordering::Release);

        if std::ptr::eq(stop_node.cast(), slot) {
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
                unsafe { (*curr_node.ret.get()) = Some(value) };
                curr_node.short_circuit.store(true, Ordering::Release);
            }
            curr = next;
        }

        unsafe { &*prev_alive }
            .next
            .store(stop_node, Ordering::Release);
    }
}

enum InsertResult<V> {
    Ok,
    Retry,
    Exists(V),
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
    l_version: usize,
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
        let entry = Node::internal(true, 1, K::default());
        entry.next()[0].store(Owned::new(left), Ordering::Relaxed);
        Self { entry }
    }

    /// Performs a basic search and returns the value associated with the key,
    /// or `None` if nothing is found. Unlike other search methods, it does not return
    /// any path information, making it slightly faster.
    pub fn search_basic(&self, key: &K, guard: &Guard) -> Option<V> {
        let mut node = unsafe { self.entry.next()[0].load(Ordering::Acquire, guard).deref() };
        while let NodeSpecific::Internal { next } = &node.kind {
            let next = next[node.child_index(key)].load(Ordering::Acquire, guard);
            node = unsafe { next.deref() };
        }
        node.read_value_version(key).1
    }

    fn search<'g>(
        &self,
        key: &K,
        target: Option<Shared<'g, Node<K, V>>>,
        guard: &'g Guard,
    ) -> (bool, Cursor<'g, K, V>) {
        let mut cursor = Cursor {
            l: self.entry.next()[0].load(Ordering::Relaxed, guard),
            p: unsafe { Shared::from_usize(&self.entry as *const _ as usize) },
            gp: Shared::null(),
            gp_p_idx: 0,
            p_l_idx: 0,
            l_key_idx: 0,
            val: None,
            l_version: 0,
        };

        while !unsafe { cursor.l.deref() }.is_leaf()
            && target.map(|target| target != cursor.l).unwrap_or(true)
        {
            let l_node = unsafe { cursor.l.deref() };
            cursor.gp = cursor.p;
            cursor.p = cursor.l;
            cursor.gp_p_idx = cursor.p_l_idx;
            cursor.p_l_idx = l_node.child_index(key);
            cursor.l = l_node.next()[cursor.p_l_idx].load(Ordering::Acquire, guard);
        }

        if let Some(target) = target {
            (cursor.l == target, cursor)
        } else {
            let (index, value, version) = unsafe { cursor.l.deref() }.read_value_version(key);
            cursor.val = value;
            cursor.l_key_idx = index;
            cursor.l_version = version;
            (value.is_some(), cursor)
        }
    }

    pub fn insert(&self, key: &K, value: &V, guard: &Guard) -> Result<(), V> {
        loop {
            let (_, cursor) = self.search(key, None, guard);
            if let Some(value) = cursor.val {
                return Err(value);
            }
            match self.insert_inner(key, value, &cursor, guard) {
                InsertResult::Ok => return Ok(()),
                InsertResult::Exists(value) => return Err(value),
                InsertResult::Retry => continue,
            }
        }
    }

    fn insert_inner<'g>(
        &self,
        key: &K,
        value: &V,
        cursor: &Cursor<'g, K, V>,
        guard: &'g Guard,
    ) -> InsertResult<V> {
        let node = unsafe { cursor.l.deref() };
        let parent = unsafe { cursor.p.deref() };

        debug_assert!(node.is_leaf());
        debug_assert!(!parent.is_leaf());

        let node_lock_slot = UnsafeCell::new(MCSLockSlot::new());
        let node_lock = match node.acquire(Operation::Insert, Some(*key), &node_lock_slot) {
            AcqResult::Acquired(lock) => lock,
            AcqResult::Eliminated(value) => return InsertResult::Exists(value),
        };
        if node.marked.load(Ordering::SeqCst) {
            return InsertResult::Retry;
        }
        for i in 0..DEGREE {
            if unsafe { *node.keys[i].get() } == Some(*key) {
                return InsertResult::Exists(unsafe { *node.values()[i].get() });
            }
        }
        // At this point, we are guaranteed key is not in the node.

        if node.size.load(Ordering::Acquire) < Self::ABSORB_THRESHOLD {
            // We have the capacity to fit this new key. So let's just find an empty slot.
            for i in 0..DEGREE {
                if unsafe { *node.keys[i].get() }.is_some() {
                    continue;
                }
                let old_version = node.write_version().load(Ordering::Relaxed);
                node.write_version()
                    .store(old_version + 1, Ordering::Relaxed);
                debug_assert!(old_version % 2 == 0);
                compiler_fence(Ordering::SeqCst);
                unsafe {
                    *node.keys[i].get() = Some(*key);
                    *node.values()[i].get() = *value;
                }
                node.size
                    .store(node.size.load(Ordering::Relaxed) + 1, Ordering::Relaxed);

                node.elim_key_ops(*value, old_version, &node_lock);

                // TODO: do smarter (lifetime)
                drop(node_lock);
                return InsertResult::Ok;
            }
            unreachable!("Should never happen");
        } else {
            // We do not have a room for this key. We need to make new nodes.
            let parent_lock_slot = UnsafeCell::new(MCSLockSlot::new());
            let parent_lock = match (
                parent.acquire(Operation::Insert, None, &parent_lock_slot),
                parent.marked.load(Ordering::SeqCst),
            ) {
                (AcqResult::Acquired(lock), false) => lock,
                _ => return InsertResult::Retry,
            };

            let mut kv_pairs: [MaybeUninit<(K, V)>; DEGREE + 1] =
                [MaybeUninit::uninit(); DEGREE + 1];
            let mut count = 0;
            for i in 0..DEGREE {
                if let Some(key) = unsafe { *node.keys[i].get() } {
                    let value = unsafe { *node.values()[i].get() };
                    kv_pairs[count].write((key, value));
                    count += 1;
                }
            }
            kv_pairs[count].write((*key, *value));
            count += 1;
            let kv_pairs = unsafe { transmute::<_, &mut [(K, V)]>(&mut kv_pairs[0..count]) };
            kv_pairs.sort_by_key(|(k, _)| *k);

            // Create new node(s).
            // Since the new arrays are too big to fit in a single node,
            // we replace `l` by a new subtree containing three new nodes: a parent, and two leaves.
            // The array contents are then split between the two new leaves.

            let left_size = count / 2;
            let right_size = DEGREE + 1 - left_size;

            let mut left = Node::leaf(true, left_size, kv_pairs[0].0);
            for i in 0..left_size {
                *left.keys[i].get_mut() = Some(kv_pairs[i].0);
                *left.values_mut()[i].get_mut() = kv_pairs[i].1;
            }

            let mut right = Node::leaf(true, right_size, kv_pairs[left_size].0);
            for i in 0..right_size {
                *right.keys[i].get_mut() = Some(kv_pairs[i + left_size].0);
                *right.values_mut()[i].get_mut() = kv_pairs[i + left_size].1;
            }

            // TODO: understand this comment...
            // The weight of new internal node `n` will be zero, unless it is the root.
            // This is because we test `p == entry`, above; in doing this, we are actually
            // performing Root-Zero at the same time as this Overflow if `n` will become the root.
            let mut internal =
                Node::internal(std::ptr::eq(parent, &self.entry), 2, kv_pairs[left_size].0);
            *internal.keys[0].get_mut() = Some(kv_pairs[left_size].0);
            internal.next()[0].store(Owned::new(left), Ordering::Relaxed);
            internal.next()[1].store(Owned::new(right), Ordering::Relaxed);

            // If the parent is not marked, `parent.next[cursor.p_l_idx]` is guaranteed to contain
            // a node since any update to parent would have deleted node (and hence we would have
            // returned at the `node.marked` check).
            let new_internal = Owned::new(internal).into_shared(guard);
            parent.next()[cursor.p_l_idx].store(new_internal, Ordering::Relaxed);
            node.marked.store(true, Ordering::Release);
            drop(node_lock);

            // Manually unlock and fix the tag.
            drop(parent_lock);
            unsafe { guard.defer_destroy(cursor.l) };
            self.fix_tag_violation(new_internal, guard);

            InsertResult::Ok
        }
    }

    fn fix_tag_violation<'g>(&self, viol: Shared<'g, Node<K, V>>, guard: &'g Guard) {
        loop {
            let viol_node = unsafe { viol.deref() };
            if viol_node.weight.load(Ordering::Relaxed) {
                return;
            }

            // `viol` should be internal because leaves always have weight = 1.
            debug_assert!(!viol_node.is_leaf());
            // `viol` is not the entry or root node because both should always have weight = 1.
            debug_assert!(
                !std::ptr::eq(viol_node, &self.entry)
                    && self.entry.next()[0].load(Ordering::Relaxed, guard) != viol
            );

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

            if !std::ptr::eq(node, viol_node) {
                // `viol` was replaced by another update.
                // We hand over responsibility for `viol` to that update.
                return;
            }

            // We cannot apply this update if p has a weight violation.
            // So, we check if this is the case, and, if so, try to fix it.
            if !parent.weight.load(Ordering::Relaxed) {
                self.fix_tag_violation(cursor.p, guard);
                continue;
            }

            let node_lock_slot = UnsafeCell::new(MCSLockSlot::new());
            let node_lock = match (
                node.acquire(Operation::Balance, None, &node_lock_slot),
                node.marked.load(Ordering::Relaxed),
            ) {
                (AcqResult::Acquired(lock), false) => lock,
                _ => continue,
            };
            let parent_lock_slot = UnsafeCell::new(MCSLockSlot::new());
            let parent_lock = match (
                parent.acquire(Operation::Balance, None, &parent_lock_slot),
                parent.marked.load(Ordering::Relaxed),
            ) {
                (AcqResult::Acquired(lock), false) => lock,
                _ => continue,
            };
            let gparent_lock_slot = UnsafeCell::new(MCSLockSlot::new());
            let gparent_lock = match (
                gparent.acquire(Operation::Balance, None, &gparent_lock_slot),
                gparent.marked.load(Ordering::Relaxed),
            ) {
                (AcqResult::Acquired(lock), false) => lock,
                _ => continue,
            };

            let psize = parent.size.load(Ordering::Relaxed);
            let nsize = viol_node.size.load(Ordering::Relaxed);
            // We don't ever change the size of a tag node, so its size should always be 2.
            debug_assert_eq!(nsize, 2);
            let c = psize + nsize;
            let size = c - 1;

            if size <= Self::ABSORB_THRESHOLD {
                // Absorb case.

                // Create new node(s).
                // The new arrays are small enough to fit in a single node,
                // so we replace p by a new internal node.
                let mut absorber =
                    Node::internal(true, size, unsafe { &*parent.keys[0].get() }.unwrap());

                ptrs_clone(
                    &parent.next()[0..],
                    &mut absorber.next_mut()[0..],
                    cursor.p_l_idx,
                );
                ptrs_clone(
                    &node.next()[0..],
                    &mut absorber.next_mut()[cursor.p_l_idx..],
                    nsize,
                );
                ptrs_clone(
                    &parent.next()[cursor.p_l_idx + 1..],
                    &mut absorber.next_mut()[cursor.p_l_idx + nsize..],
                    psize - (cursor.p_l_idx + 1),
                );

                ufcells_clone(&parent.keys[0..], &mut absorber.keys[0..], cursor.p_l_idx);
                ufcells_clone(
                    &node.keys[0..],
                    &mut absorber.keys[cursor.p_l_idx..],
                    node.key_count(),
                );
                ufcells_clone(
                    &parent.keys[cursor.p_l_idx..],
                    &mut absorber.keys[cursor.p_l_idx + node.key_count()..],
                    parent.key_count() - cursor.p_l_idx,
                );

                gparent.next()[cursor.gp_p_idx].store(Owned::new(absorber), Ordering::Relaxed);
                node.marked.store(true, Ordering::Relaxed);
                parent.marked.store(true, Ordering::Relaxed);

                unsafe { guard.defer_destroy(cursor.l) };
                unsafe { guard.defer_destroy(cursor.p) };
                return;
            } else {
                // Split case.

                // Merge keys of p and l into one big array (and similarly for children).
                // We essentially replace the pointer to l with the contents of l.
                let mut next: [Atomic<Node<K, V>>; 2 * DEGREE] = Default::default();
                let mut keys: [UnsafeCell<Option<K>>; 2 * DEGREE] = Default::default();

                ptrs_clone(&parent.next()[0..], &mut next[0..], cursor.p_l_idx);
                ptrs_clone(&node.next()[0..], &mut next[cursor.p_l_idx..], nsize);
                ptrs_clone(
                    &parent.next()[cursor.p_l_idx + 1..],
                    &mut next[cursor.p_l_idx + nsize..],
                    psize - (cursor.p_l_idx + 1),
                );

                ufcells_clone(&parent.keys[0..], &mut keys[0..], cursor.p_l_idx);
                ufcells_clone(
                    &node.keys[0..],
                    &mut keys[cursor.p_l_idx..],
                    node.key_count(),
                );
                ufcells_clone(
                    &parent.keys[cursor.p_l_idx..],
                    &mut keys[cursor.p_l_idx + node.key_count()..],
                    parent.key_count() - cursor.p_l_idx,
                );

                // The new arrays are too big to fit in a single node,
                // so we replace p by a new internal node and two new children.
                //
                // We take the big merged array and split it into two arrays,
                // which are used to create two new children u and v.
                // we then create a new internal node (whose weight will be zero
                // if it is not the root), with u and v as its children.

                // Create new node(s).
                let left_size = size / 2;
                let mut left = Node::internal(true, left_size, unsafe { *keys[0].get() }.unwrap());
                ufcells_clone(&keys[0..], &mut left.keys[0..], left_size - 1);
                ptrs_clone(&next[0..], &mut left.next_mut()[0..], left_size);

                let right_size = size - left_size;
                let mut right =
                    Node::internal(true, right_size, unsafe { *keys[left_size].get() }.unwrap());
                ufcells_clone(&keys[left_size..], &mut right.keys[0..], right_size - 1);
                ptrs_clone(&next[left_size..], &mut right.next_mut()[0..], right_size);

                // Note: keys[left_size - 1] should be the same as new_internal.keys[0].
                let mut new_internal = Node::internal(
                    std::ptr::eq(gparent, &self.entry),
                    2,
                    unsafe { *keys[left_size - 1].get() }.unwrap(),
                );
                *new_internal.keys[0].get_mut() = unsafe { *keys[left_size - 1].get() };
                new_internal.next()[0].store(Owned::new(left), Ordering::Relaxed);
                new_internal.next()[1].store(Owned::new(right), Ordering::Relaxed);

                // TODO: understand this comment...
                // The weight of new internal node `n` will be zero, unless it is the root.
                // This is because we test `p == entry`, above; in doing this, we are actually
                // performing Root-Zero at the same time
                // as this Overflow if `n` will become the root.

                let new_internal = Owned::new(new_internal).into_shared(guard);
                gparent.next()[cursor.gp_p_idx].store(new_internal, Ordering::Relaxed);
                node.marked.store(true, Ordering::Relaxed);
                parent.marked.store(true, Ordering::Relaxed);

                unsafe { guard.defer_destroy(cursor.l) };
                unsafe { guard.defer_destroy(cursor.p) };

                drop(node_lock);
                drop(parent_lock);
                drop(gparent_lock);
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

        let node_lock_slot = UnsafeCell::new(MCSLockSlot::new());
        let node_lock = match node.acquire(Operation::Delete, Some(*key), &node_lock_slot) {
            AcqResult::Acquired(lock) => lock,
            AcqResult::Eliminated(_) => return Err(()),
        };
        if node.marked.load(Ordering::SeqCst) {
            return Err(());
        }

        let new_size = node.size.load(Ordering::Relaxed) - 1;
        for i in 0..DEGREE {
            if unsafe { *node.keys[i].get() } == Some(*key) {
                let val = unsafe { *node.values()[i].get() };
                let old_version = node.write_version().load(Ordering::Relaxed);
                node.write_version()
                    .store(old_version + 1, Ordering::Relaxed);
                compiler_fence(Ordering::SeqCst);
                unsafe { *node.keys[i].get() = None };
                node.size.store(new_size, Ordering::Relaxed);

                node.elim_key_ops(val, old_version, &node_lock);

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
                || std::ptr::eq(viol_node, &self.entry)
                || viol == self.entry.next()[0].load(Ordering::Relaxed, guard)
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
                && !std::ptr::eq(parent, &self.entry)
                && cursor.p != self.entry.next()[0].load(Ordering::Relaxed, guard)
            {
                self.fix_underfull_violation(cursor.p, guard);
                continue;
            }

            if !std::ptr::eq(node, viol_node) {
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
            let sibling_sh = parent.next()[sibling_idx].load(Ordering::Relaxed, guard);
            let sibling = unsafe { sibling_sh.deref() };

            // Prevent deadlocks by acquiring left node first.
            let node_lock_slot = UnsafeCell::new(MCSLockSlot::new());
            let sibling_lock_slot = UnsafeCell::new(MCSLockSlot::new());
            let ((left, left_idx, left_slot), (right, right_idx, right_slot)) =
                if sibling_idx < cursor.p_l_idx {
                    (
                        (sibling, sibling_idx, sibling_lock_slot),
                        (node, cursor.p_l_idx, node_lock_slot),
                    )
                } else {
                    (
                        (node, cursor.p_l_idx, node_lock_slot),
                        (sibling, sibling_idx, sibling_lock_slot),
                    )
                };

            // TODO: maybe we should give a reference to a Pin?
            let left_lock = match left.acquire(Operation::Balance, None, &left_slot) {
                AcqResult::Acquired(lock) => lock,
                AcqResult::Eliminated(_) => continue,
            };
            if left.marked.load(Ordering::Relaxed) {
                continue;
            }
            let right_lock = match right.acquire(Operation::Balance, None, &right_slot) {
                AcqResult::Acquired(lock) => lock,
                AcqResult::Eliminated(_) => continue,
            };
            if right.marked.load(Ordering::Relaxed) {
                continue;
            }

            // Repeat this check, this might have changed while we locked `viol`.
            if viol_node.size.load(Ordering::Relaxed) >= Self::UNDERFULL_THRESHOLD {
                // No degree violation at `viol`.
                return;
            }

            let parent_lock_slot = UnsafeCell::new(MCSLockSlot::new());
            let parent_lock = match parent.acquire(Operation::Balance, None, &parent_lock_slot) {
                AcqResult::Acquired(lock) => lock,
                AcqResult::Eliminated(_) => continue,
            };
            if parent.marked.load(Ordering::Relaxed) {
                continue;
            }

            let gparent_lock_slot = UnsafeCell::new(MCSLockSlot::new());
            let gparent_lock = match gparent.acquire(Operation::Balance, None, &gparent_lock_slot) {
                AcqResult::Acquired(lock) => lock,
                AcqResult::Eliminated(_) => continue,
            };
            if gparent.marked.load(Ordering::Relaxed) {
                continue;
            }

            // We can only apply AbsorbSibling or Distribute if there are no
            // weight violations at `parent`, `node`, or `sibling`.
            // So, we first check for any weight violations and fix any that we see.
            if !parent.weight.load(Ordering::Relaxed)
                || !node.weight.load(Ordering::Relaxed)
                || !sibling.weight.load(Ordering::Relaxed)
            {
                drop(left_lock);
                drop(right_lock);
                drop(parent_lock);
                drop(gparent_lock);
                self.fix_tag_violation(cursor.p, guard);
                self.fix_tag_violation(cursor.l, guard);
                self.fix_tag_violation(sibling_sh, guard);
                continue;
            }

            // There are no weight violations at `parent`, `node` or `sibling`.
            debug_assert!(
                parent.weight.load(Ordering::Relaxed)
                    && node.weight.load(Ordering::Relaxed)
                    && sibling.weight.load(Ordering::Relaxed)
            );
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
                let (mut key_count, mut next_count) = (0, 0);
                let new_node = if left.is_leaf() {
                    let mut new_leaf = Owned::new(Node::leaf(true, size, node.search_key));
                    for i in 0..DEGREE {
                        let key = some_or!(unsafe { *left.keys[i].get() }, continue);
                        let value = unsafe { *left.values()[i].get() };
                        *new_leaf.keys[key_count].get_mut() = Some(key);
                        *new_leaf.values_mut()[next_count].get_mut() = value;
                        key_count += 1;
                        next_count += 1;
                    }
                    debug_assert!(right.is_leaf());
                    for i in 0..DEGREE {
                        let key = some_or!(unsafe { *right.keys[i].get() }, continue);
                        let value = unsafe { *right.values()[i].get() };
                        *new_leaf.keys[key_count].get_mut() = Some(key);
                        *new_leaf.values_mut()[next_count].get_mut() = value;
                        key_count += 1;
                        next_count += 1;
                    }
                    new_leaf
                } else {
                    let mut new_internal = Owned::new(Node::internal(true, size, node.search_key));
                    for i in 0..left.key_count() {
                        *new_internal.keys[key_count].get_mut() = unsafe { *left.keys[i].get() };
                        key_count += 1;
                    }
                    *new_internal.keys[key_count].get_mut() =
                        unsafe { *parent.keys[left_idx].get() };
                    key_count += 1;
                    for i in 0..lsize {
                        new_internal.next_mut()[next_count] =
                            Atomic::from(left.next()[i].load(Ordering::Relaxed, guard));
                        next_count += 1;
                    }
                    debug_assert!(!right.is_leaf());
                    for i in 0..right.key_count() {
                        *new_internal.keys[key_count].get_mut() = unsafe { *right.keys[i].get() };
                        key_count += 1;
                    }
                    for i in 0..rsize {
                        new_internal.next_mut()[next_count] =
                            Atomic::from(right.next()[i].load(Ordering::Relaxed, guard));
                        next_count += 1;
                    }
                    new_internal
                }
                .into_shared(guard);

                // Now, we atomically replace `p` and its children with the new nodes.
                // If appropriate, we perform RootAbsorb at the same time.
                if std::ptr::eq(gparent, &self.entry) && psize == 2 {
                    debug_assert!(cursor.gp_p_idx == 0);
                    gparent.next()[cursor.gp_p_idx].store(new_node, Ordering::Relaxed);
                    node.marked.store(true, Ordering::Relaxed);
                    parent.marked.store(true, Ordering::Relaxed);
                    sibling.marked.store(true, Ordering::Relaxed);

                    unsafe {
                        guard.defer_destroy(cursor.l);
                        guard.defer_destroy(cursor.p);
                        guard.defer_destroy(sibling_sh);
                    }

                    drop(left_lock);
                    drop(right_lock);
                    drop(parent_lock);
                    drop(gparent_lock);
                    self.fix_underfull_violation(new_node, guard);
                    return;
                } else {
                    debug_assert!(!std::ptr::eq(gparent, &self.entry) || psize > 2);
                    let mut new_parent = Node::internal(true, psize - 1, parent.search_key);
                    for i in 0..left_idx {
                        *new_parent.keys[i].get_mut() = unsafe { *parent.keys[i].get() };
                    }
                    for i in 0..sibling_idx {
                        new_parent.next_mut()[i] =
                            Atomic::from(parent.next()[i].load(Ordering::Relaxed, guard));
                    }
                    for i in left_idx + 1..parent.key_count() {
                        *new_parent.keys[i - 1].get_mut() = unsafe { *parent.keys[i].get() };
                    }
                    for i in cursor.p_l_idx + 1..psize {
                        new_parent.next_mut()[i - 1] =
                            Atomic::from(parent.next()[i].load(Ordering::Relaxed, guard));
                    }

                    new_parent.next_mut()
                        [cursor.p_l_idx - (if cursor.p_l_idx > sibling_idx { 1 } else { 0 })] =
                        Atomic::from(new_node);
                    let new_parent = Owned::new(new_parent).into_shared(guard);

                    gparent.next()[cursor.gp_p_idx].store(new_parent, Ordering::Relaxed);
                    node.marked.store(true, Ordering::Relaxed);
                    parent.marked.store(true, Ordering::Relaxed);
                    sibling.marked.store(true, Ordering::Relaxed);

                    unsafe {
                        guard.defer_destroy(cursor.l);
                        guard.defer_destroy(cursor.p);
                        guard.defer_destroy(sibling_sh);
                    }

                    drop(left_lock);
                    drop(right_lock);
                    drop(parent_lock);
                    drop(gparent_lock);

                    self.fix_underfull_violation(new_node, guard);
                    self.fix_underfull_violation(new_parent, guard);
                    return;
                }
            } else {
                // Distribute
                let left_size = size / 2;
                let right_size = size - left_size;

                let mut kv_pairs: [(MaybeUninit<K>, MaybeUninit<V>); 2 * DEGREE] =
                    [(MaybeUninit::uninit(), MaybeUninit::uninit()); 2 * DEGREE];

                // Combine the contents of `l` and `s`
                // (and one key from `p` if `l` and `s` are internal).
                let (mut key_count, mut val_count) = (0, 0);
                if left.is_leaf() {
                    debug_assert!(right.is_leaf());
                    for i in 0..DEGREE {
                        let key = some_or!(unsafe { *left.keys[i].get() }, continue);
                        let val = unsafe { *left.values()[i].get() };
                        kv_pairs[key_count].0.write(key);
                        kv_pairs[val_count].1.write(val);
                        key_count += 1;
                        val_count += 1;
                    }
                } else {
                    for i in 0..left.key_count() {
                        kv_pairs[key_count]
                            .0
                            .write(unsafe { *left.keys[i].get() }.unwrap());
                        key_count += 1;
                    }
                    for i in 0..lsize {
                        kv_pairs[val_count]
                            .1
                            .write(unsafe { *left.values()[i].get() });
                        val_count += 1;
                    }
                }

                if !left.is_leaf() {
                    kv_pairs[key_count]
                        .0
                        .write(unsafe { *parent.keys[left_idx].get() }.unwrap());
                    key_count += 1;
                }

                if right.is_leaf() {
                    debug_assert!(left.is_leaf());
                    for i in 0..DEGREE {
                        let key = some_or!(unsafe { *right.keys[i].get() }, continue);
                        let val = unsafe { *right.values()[i].get() };
                        kv_pairs[key_count].0.write(key);
                        kv_pairs[val_count].1.write(val);
                        key_count += 1;
                        val_count += 1;
                    }
                } else {
                    for i in 0..right.key_count() {
                        kv_pairs[key_count]
                            .0
                            .write(unsafe { *right.keys[i].get() }.unwrap());
                        key_count += 1;
                    }
                    for i in 0..rsize {
                        kv_pairs[val_count]
                            .1
                            .write(unsafe { *right.values()[i].get() });
                        val_count += 1;
                    }
                }

                let kv_pairs =
                    unsafe { transmute::<_, &mut [(K, V)]>(&mut kv_pairs[0..key_count]) };
                if left.is_leaf() {
                    kv_pairs.sort_by_key(|(k, _)| *k);
                }

                (key_count, val_count) = (0, 0);

                let (new_left, pivot) = if left.is_leaf() {
                    let mut new_leaf =
                        Owned::new(Node::leaf(true, left_size, kv_pairs[key_count].0));
                    for i in 0..left_size {
                        *new_leaf.keys[i].get_mut() = Some(kv_pairs[key_count].0);
                        *new_leaf.values_mut()[i].get_mut() = kv_pairs[val_count].1;
                        key_count += 1;
                        val_count += 1;
                    }
                    (new_leaf, kv_pairs[key_count].0)
                } else {
                    let mut new_internal =
                        Owned::new(Node::internal(true, left_size, kv_pairs[key_count].0));
                    for i in 0..left_size - 1 {
                        *new_internal.keys[i].get_mut() = Some(kv_pairs[key_count].0);
                        key_count += 1;
                    }
                    for i in 0..left_size {
                        *new_internal.values_mut()[i].get_mut() = kv_pairs[val_count].1;
                        val_count += 1;
                    }
                    let pivot = kv_pairs[key_count].0;
                    key_count += 1;
                    (new_internal, pivot)
                };

                // Reserve one key for the parent (to go between `new_left` and `new_right`).

                let new_right = if right.is_leaf() {
                    debug_assert!(left.is_leaf());
                    let mut new_leaf =
                        Owned::new(Node::leaf(true, right_size, kv_pairs[key_count].0));
                    for i in 0..right_size - (if left.is_leaf() { 0 } else { 1 }) {
                        *new_leaf.keys[i].get_mut() = Some(kv_pairs[key_count].0);
                        key_count += 1;
                    }
                    for i in 0..right_size {
                        *new_leaf.values_mut()[i].get_mut() = kv_pairs[val_count].1;
                        val_count += 1;
                    }
                    new_leaf
                } else {
                    let mut new_internal =
                        Owned::new(Node::internal(true, right_size, kv_pairs[key_count].0));
                    for i in 0..right_size - (if left.is_leaf() { 0 } else { 1 }) {
                        *new_internal.keys[i].get_mut() = Some(kv_pairs[key_count].0);
                        key_count += 1;
                    }
                    for i in 0..right_size {
                        *new_internal.values_mut()[i].get_mut() = kv_pairs[val_count].1;
                        val_count += 1;
                    }
                    new_internal
                };

                let mut new_parent = Owned::new(Node::internal(
                    parent.weight.load(Ordering::Relaxed),
                    psize,
                    parent.search_key,
                ));
                ufcells_clone(
                    &parent.keys[0..],
                    &mut new_parent.keys[0..],
                    parent.key_count(),
                );
                ptrs_clone(&parent.next()[0..], &mut new_parent.next_mut()[0..], psize);
                new_parent.next_mut()[left_idx] = Atomic::from(new_left);
                new_parent.next_mut()[right_idx] = Atomic::from(new_right);
                *new_parent.keys[left_idx].get_mut() = Some(pivot);

                gparent.next()[cursor.gp_p_idx].store(new_parent, Ordering::SeqCst);
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

#[inline]
fn ptrs_clone<T>(src: &[Atomic<T>], dst: &mut [Atomic<T>], len: usize) {
    dst[0..len].clone_from_slice(&src[0..len]);
}

/// TODO: unsafe?
#[inline]
fn ufcells_clone<T: Copy>(src: &[UnsafeCell<T>], dst: &mut [UnsafeCell<T>], len: usize) {
    for i in 0..len {
        unsafe { *dst[i].get_mut() = *src[i].get() };
    }
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
        self.insert(&key, &value, guard).is_ok()
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
    fn smoke_nm_tree() {
        concurrent_map::tests::smoke::<_, ElimABTree<i32, i32>, _>(&|a| *a);
    }
}
