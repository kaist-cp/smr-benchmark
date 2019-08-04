use crossbeam_pebr::{unprotected, Atomic, Guard, Owned, Shared, Shield, ShieldError};

use super::concurrent_map::ConcurrentMap;
use std::cmp;
use std::mem;
use std::sync::atomic::Ordering;

bitflags! {
    /// TODO
    /// A remove operation is registered by marking the corresponding edges: the (parent, target)
    /// edge is _flagged_ and the (parent, sibling) edge is _tagged_.
    struct Marks: usize {
        const FLAG = 1usize.wrapping_shl(1);
        const TAG  = 1usize.wrapping_shl(0);
    }
}

impl Marks {
    fn new(flag: bool, tag: bool) -> Self {
        (if flag { Marks::FLAG } else { Marks::empty() })
            | (if tag { Marks::TAG } else { Marks::empty() })
    }

    fn flag(self) -> bool {
        !(self & Marks::FLAG).is_empty()
    }

    fn tag(self) -> bool {
        !(self & Marks::TAG).is_empty()
    }
}

#[derive(Clone, PartialEq, Eq, Ord, Debug)]
enum Key<K> {
    Fin(K),
    Inf,
}

impl<K> PartialOrd for Key<K>
where
    K: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Key::Fin(k1), Key::Fin(k2)) => k1.partial_cmp(k2),
            (Key::Fin(_), Key::Inf) => Some(std::cmp::Ordering::Less),
            (Key::Inf, Key::Fin(_)) => Some(std::cmp::Ordering::Greater),
            (Key::Inf, Key::Inf) => Some(std::cmp::Ordering::Equal),
        }
    }
}

impl<K> PartialEq<K> for Key<K>
where
    K: PartialEq,
{
    fn eq(&self, rhs: &K) -> bool {
        match self {
            Key::Fin(k) => k == rhs,
            _ => false,
        }
    }
}

impl<K> PartialOrd<K> for Key<K>
where
    K: PartialOrd,
{
    fn partial_cmp(&self, rhs: &K) -> Option<std::cmp::Ordering> {
        match self {
            Key::Fin(k) => k.partial_cmp(rhs),
            _ => Some(std::cmp::Ordering::Greater),
        }
    }
}

impl<K> Key<K>
where
    K: Ord,
{
    fn cmp(&self, rhs: &K) -> std::cmp::Ordering {
        match self {
            Key::Fin(k) => k.cmp(rhs),
            _ => std::cmp::Ordering::Greater,
        }
    }
}

#[derive(Debug)]
struct Node<K, V> {
    key: Key<K>,
    // TODO(@jeehoonkang): how about having another type that is either (1) value, or (2) left and
    // right.
    value: Option<V>,
    left: Atomic<Node<K, V>>,
    right: Atomic<Node<K, V>>,
}

impl<K, V> Node<K, V>
where
    K: Clone,
    V: Clone,
{
    fn new_leaf(key: Key<K>, value: Option<V>) -> Node<K, V> {
        Node {
            key,
            value,
            left: Atomic::null(),
            right: Atomic::null(),
        }
    }

    /// Make a new internal node, consuming the given left and right nodes,
    /// using the right node's key.
    fn new_internal(left: Node<K, V>, right: Node<K, V>) -> Node<K, V> {
        Node {
            key: right.key.clone(),
            value: None,
            left: Atomic::from(left),
            right: Atomic::from(right),
        }
    }
}

#[derive(Clone, Copy)]
enum Direction {
    L,
    R,
}

/// All Shared<_> are unmarked.
///
/// All of the edges of path from `successor` to `parent` are in the process of removal.
pub struct SeekRecord<K, V> {
    /// Parent of `successor`
    ancestor: Shield<Node<K, V>>,
    /// The first internal node with a marked outgoing edge
    successor: Shield<Node<K, V>>,
    /// The direction of successor from ancestor.
    successor_dir: Direction,
    /// Parent of `leaf`
    parent: Shield<Node<K, V>>,
    /// The end of the access path.
    leaf: Shield<Node<K, V>>,
    /// The direction of leaf from parent.
    leaf_dir: Direction,
}

// TODO(@jeehoonkang): code duplication...
impl<K, V> SeekRecord<K, V> {
    fn new(guard: &Guard) -> Self {
        Self {
            ancestor: Shield::null(guard),
            successor: Shield::null(guard),
            successor_dir: Direction::L,
            parent: Shield::null(guard),
            leaf: Shield::null(guard),
            leaf_dir: Direction::L,
        }
    }

    fn release(&mut self) {
        self.ancestor.release();
        self.successor.release();
        self.parent.release();
        self.leaf.release();
    }

    fn successor_addr(&self) -> &Atomic<Node<K, V>> {
        match self.successor_dir {
            Direction::L => &unsafe { self.ancestor.deref() }.left,
            Direction::R => &unsafe { self.ancestor.deref() }.right,
        }
    }

    fn leaf_addr(&self) -> &Atomic<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => &unsafe { self.parent.deref() }.left,
            Direction::R => &unsafe { self.parent.deref() }.right,
        }
    }

    fn leaf_sibling_addr(&self) -> &Atomic<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => &unsafe { self.parent.deref() }.right,
            Direction::R => &unsafe { self.parent.deref() }.left,
        }
    }
}

// COMMENT(@jeehoonkang): write down the invariant of the tree
pub struct NMTreeMap<K, V> {
    r: Node<K, V>,
}

impl<K, V> Default for NMTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for NMTreeMap<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut stack = vec![
                self.r.left.load(Ordering::Relaxed, unprotected()),
                self.r.right.load(Ordering::Relaxed, unprotected()),
            ];
            assert!(self.r.value.is_none());

            while let Some(mut node) = stack.pop() {
                if node.is_null() {
                    continue;
                }

                let node_ref = node.deref_mut();

                stack.push(node_ref.left.load(Ordering::Relaxed, unprotected()));
                stack.push(node_ref.right.load(Ordering::Relaxed, unprotected()));
                drop(node.into_owned());
            }
        }
    }
}

impl<K, V> NMTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        // An empty tree has 5 default nodes with infinite keys so that the SeekRecord is allways
        // well-defined.
        //          r
        //         / \
        //        s  inf2
        //       / \
        //   inf0   inf1
        let inf0 = Node::new_leaf(Key::Inf, None);
        let inf1 = Node::new_leaf(Key::Inf, None);
        let inf2 = Node::new_leaf(Key::Inf, None);
        let s = Node::new_internal(inf0, inf1);
        let r = Node::new_internal(s, inf2);
        NMTreeMap { r }
    }

    // All `Shared<_>` fields are unmarked.
    fn seek<'g>(
        &'g self,
        key: &K,
        record: &mut SeekRecord<K, V>,
        guard: &'g Guard,
    ) -> Result<(), ShieldError> {
        let s = self.r.left.load(Ordering::Relaxed, guard);
        record
            .ancestor
            .defend(Shared::from(&self.r as *const _), guard)?;
        record.successor.defend(s, guard)?;
        record.successor_dir = Direction::L;

        let leaf = unsafe { record.successor.deref() }
            .left
            .load(Ordering::Relaxed, guard)
            .with_tag(Marks::empty().bits());
        record.parent.defend(s, guard)?;
        record.leaf.defend(leaf, guard)?;
        record.leaf_dir = Direction::L;

        let mut prev_tag = Marks::from_bits_truncate(leaf.tag()).tag();
        let mut curr_dir = Direction::L;
        let mut curr = unsafe { record.leaf.deref() }
            .left
            .load(Ordering::Relaxed, guard);

        while !curr.is_null() {
            if !prev_tag {
                // untagged edge: advance ancestor and successor pointers
                record.ancestor.defend(record.parent.shared(), guard)?;
                record.successor.defend(record.leaf.shared(), guard)?;
                record.successor_dir = record.leaf_dir;
            }

            // advance parent and leaf pointers
            mem::swap(&mut record.parent, &mut record.leaf);
            record
                .leaf
                .defend(curr.with_tag(Marks::empty().bits()), guard)?;
            record.leaf_dir = curr_dir;

            // update other variables
            prev_tag = Marks::from_bits_truncate(curr.tag()).tag();
            let curr_node = unsafe { record.leaf.deref() };
            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                curr_dir = Direction::L;
                curr = curr_node.left.load(Ordering::Acquire, guard);
            } else {
                curr_dir = Direction::R;
                curr = curr_node.right.load(Ordering::Acquire, guard);
            }
        }

        Ok(())
    }

    /// Physically removes node.
    ///
    /// Returns true if it successfully unlinks the flagged node in `record`.
    fn cleanup(&self, record: &SeekRecord<K, V>, guard: &Guard) -> bool {
        // Identify the node(subtree) that will replace `successor`.
        let leaf_marked = record.leaf_addr().load(Ordering::Acquire, guard);
        let leaf_flag = Marks::from_bits_truncate(leaf_marked.tag()).flag();
        let target_sibling_addr = if leaf_flag {
            record.leaf_sibling_addr()
        } else {
            record.leaf_addr()
        };

        // NOTE: the ibr implementation uses CAS
        // tag (parent, sibling) edge -> all of the parent's edges can't change now
        // TODO: Is Release enough?
        target_sibling_addr.fetch_or(Marks::TAG.bits(), Ordering::AcqRel, guard);

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        let target_sibling = target_sibling_addr.load(Ordering::Acquire, guard);
        let flag = Marks::from_bits_truncate(target_sibling.tag()).flag();
        let is_unlinked = record
            .successor_addr()
            .compare_and_set(
                record.successor.shared(),
                target_sibling.with_tag(Marks::new(flag, false).bits()),
                Ordering::AcqRel,
                &guard,
            )
            .is_ok();

        if is_unlinked {
            unsafe {
                // destroy the subtree of successor except target_sibling
                let mut stack = vec![record.successor.shared()];

                while let Some(mut node) = stack.pop() {
                    if node.is_null()
                        || (node.with_tag(Marks::empty().bits())
                            == target_sibling.with_tag(Marks::empty().bits()))
                    {
                        continue;
                    }

                    let node_ref = node.deref_mut();

                    stack.push(node_ref.left.load(Ordering::Relaxed, guard));
                    stack.push(node_ref.right.load(Ordering::Relaxed, guard));
                    guard.defer_destroy(node);
                }
            }
        }

        is_unlinked
    }

    #[inline]
    pub fn get_inner<'g>(
        &'g self,
        key: &'g K,
        record: &'g mut SeekRecord<K, V>,
        guard: &'g Guard,
    ) -> Result<Option<&'g V>, ShieldError> {
        self.seek(key, record, guard)?;
        let leaf_node = unsafe { record.leaf.deref() };

        if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
            return Ok(None);
        }

        Ok(Some(leaf_node.value.as_ref().unwrap()))
    }

    pub fn get<'g>(
        &'g self,
        key: &'g K,
        record: &'g mut SeekRecord<K, V>,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        // TODO(@jeehoonkang): we want to use `FindError::retry`, but it requires higher-kinded
        // things...
        loop {
            match self.get_inner(
                key,
                unsafe { &mut *(record as &_ as *const _ as *mut SeekRecord<K, V>) },
                guard,
            ) {
                Ok(r) => return r,
                Err(ShieldError::Ejected) => {
                    unsafe {
                        // HACK(@jeehoonkang): We wanted to say `guard.repin()`, which is totally
                        // fine, but the current Rust's type checker cannot verify it.
                        (&mut *(guard as &_ as *const _ as *mut Guard)).repin();
                    }
                }
            }
        }
    }

    #[inline]
    pub fn insert_inner(
        &self,
        key: &K,
        value: V,
        record: &mut SeekRecord<K, V>,
        guard: &Guard,
    ) -> Result<(), (V, Option<ShieldError>)> {
        let mut new_leaf = Owned::new(Node::new_leaf(Key::Fin(key.clone()), Some(value)))
            .into_shared(unsafe { unprotected() });

        let mut new_internal = Owned::new(Node {
            key: Key::Inf, // temporary placeholder
            value: None,
            left: Atomic::null(),
            right: Atomic::null(),
        })
        .into_shared(unsafe { unprotected() });

        loop {
            self.seek(key, record, guard).map_err(|e| unsafe {
                let value = new_leaf.deref_mut().value.take().unwrap();
                drop(new_leaf.into_owned());
                drop(new_internal.into_owned());
                (value, Some(e))
            })?;
            let leaf = record.leaf.shared();

            let (new_left, new_right) = match unsafe { leaf.deref() }.key.cmp(key) {
                cmp::Ordering::Equal => unsafe {
                    // Newly created nodes that failed to be inserted are free'd here.
                    let value = new_leaf.deref_mut().value.take().unwrap();
                    drop(new_leaf.into_owned());
                    drop(new_internal.into_owned());
                    return Err((value, None));
                },
                cmp::Ordering::Greater => (new_leaf, leaf),
                cmp::Ordering::Less => (leaf, new_leaf),
            };

            let new_internal_node = unsafe { new_internal.deref_mut() };
            new_internal_node.key = unsafe { new_right.deref().key.clone() };
            new_internal_node.left.store(new_left, Ordering::Relaxed);
            new_internal_node.right.store(new_right, Ordering::Relaxed);

            // NOTE: record.leaf_addr is called childAddr in the paper.
            match record
                .leaf_addr()
                .compare_and_set(leaf, new_internal, Ordering::AcqRel, &guard)
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    // Insertion failed. Help the conflicting remove operation if needed.
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if e.current.with_tag(Marks::empty().bits()) == leaf {
                        self.cleanup(&record, guard);
                    }
                }
            }
        }
    }

    pub fn insert(
        &self,
        key: K,
        mut value: V,
        record: &mut SeekRecord<K, V>,
        guard: &mut Guard,
    ) -> Result<(), (K, V)> {
        // TODO(@jeehoonkang): we want to use `FindError::retry`, but it requires higher-kinded
        // things...
        loop {
            match self.insert_inner(&key, value, record, guard) {
                Ok(()) => return Ok(()),
                Err((v, None)) => return Err((key, v)),
                Err((v, Some(ShieldError::Ejected))) => {
                    value = v;
                    unsafe {
                        // HACK(@jeehoonkang): We wanted to say `guard.repin()`, which is totally
                        // fine, but the current Rust's type checker cannot verify it.
                        (&mut *(guard as &_ as *const _ as *mut Guard)).repin();
                    }
                }
            }
        }
    }

    #[inline]
    fn remove_inner(
        &self,
        key: &K,
        record: &mut SeekRecord<K, V>,
        guard: &Guard,
    ) -> Result<Option<V>, ShieldError> {
        // NOTE: The paper version uses one big loop for both phases.
        // injection phase
        //
        // `leaf` and `value` are the snapshot of the node to be deleted.
        let (leaf, value) = loop {
            self.seek(key, record, guard)?;

            // candidates
            let leaf = record.leaf.shared();
            let leaf_node = unsafe { record.leaf.as_ref().unwrap() };

            if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
                return Ok(None);
            }

            // Copy the value before the physical deletion.
            let value = leaf_node.value.as_ref().unwrap().clone();

            // Try injecting the deletion flag.
            match record.leaf_addr().compare_and_set(
                leaf,
                leaf.with_tag(Marks::new(true, false).bits()),
                Ordering::AcqRel,
                &guard,
            ) {
                Ok(_) => {
                    // Finalize the node to be removed
                    if self.cleanup(&record, guard) {
                        return Ok(Some(value));
                    }

                    // In-place cleanup failed. Enter the cleanup phase.
                    break (leaf, value);
                }
                Err(e) => {
                    // Flagging failed.
                    // case 1. record.leaf_addr(e.current) points to another node: restart.
                    // case 2. Another thread flagged/tagged the edge to leaf: help and restart
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if leaf == e.current.with_tag(Marks::empty().bits()) {
                        self.cleanup(&record, guard);
                    }
                }
            }
        };

        let leaf = Shared::from(leaf.as_raw());

        // cleanup phase
        loop {
            self.seek(key, record, guard)?;
            if record.leaf.shared() != leaf {
                // The edge to leaf flagged for deletion was removed by a helping thread
                return Ok(Some(value));
            }

            // leaf is still present in the tree.
            if self.cleanup(&record, guard) {
                return Ok(Some(value));
            }
        }
    }

    pub fn remove(&self, key: &K, record: &mut SeekRecord<K, V>, guard: &mut Guard) -> Option<V> {
        // TODO(@jeehoonkang): we want to use `FindError::retry`, but it requires higher-kinded
        // things...
        loop {
            match self.remove_inner(key, record, guard) {
                Ok(r) => return r,
                Err(ShieldError::Ejected) => {
                    unsafe {
                        // HACK(@jeehoonkang): We wanted to say `guard.repin()`, which is totally
                        // fine, but the current Rust's type checker cannot verify it.
                        (&mut *(guard as &_ as *const _ as *mut Guard)).repin();
                    }
                }
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for NMTreeMap<K, V>
where
    K: Ord + Clone + 'static,
    V: Clone + 'static,
{
    type Handle = SeekRecord<K, V>;

    fn new() -> Self {
        Self::new()
    }

    fn handle(guard: &Guard) -> Self::Handle {
        SeekRecord::new(guard)
    }

    fn clear(handle: &mut Self::Handle) {
        handle.release();
    }

    #[inline]
    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &'g K,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        self.get(key, handle, guard)
    }

    #[inline]
    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool {
        self.insert(key, value, handle, guard).is_ok()
    }

    #[inline]
    fn remove(&self, handle: &mut Self::Handle, key: &K, guard: &mut Guard) -> Option<V> {
        self.remove(key, handle, guard)
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    use super::{NMTreeMap, SeekRecord};
    use crossbeam_utils::thread;
    use rand::prelude::*;

    #[test]
    fn smoke_nm_tree() {
        let nm_tree_map = &NMTreeMap::new();

        // insert
        thread::scope(|s| {
            for t in 0..10 {
                s.spawn(move |_| {
                    let mut record = SeekRecord::new(&crossbeam_pebr::pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(nm_tree_map
                            .insert(i, i.to_string(), &mut record, &mut crossbeam_pebr::pin())
                            .is_ok());
                    }
                });
            }
        })
        .unwrap();

        // remove
        thread::scope(|s| {
            for t in 0..5 {
                s.spawn(move |_| {
                    let mut record = SeekRecord::new(&crossbeam_pebr::pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            nm_tree_map
                                .remove(&i, &mut record, &mut crossbeam_pebr::pin())
                                .unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();

        // get
        thread::scope(|s| {
            for t in 5..10 {
                s.spawn(move |_| {
                    let mut record = SeekRecord::new(&crossbeam_pebr::pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            *nm_tree_map
                                .get(&i, &mut record, &mut crossbeam_pebr::pin())
                                .unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();
    }
}
