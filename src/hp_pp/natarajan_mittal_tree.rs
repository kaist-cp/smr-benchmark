use hp_pp::{tag, tagged, untagged, HazardPointer};
use hp_pp::{try_unlink, ProtectError};

use crate::hp::concurrent_map::ConcurrentMap;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::{cmp, slice};

bitflags! {
    /// TODO
    /// A remove operation is registered by marking the corresponding edges: the (parent, target)
    /// edge is _flagged_ and the (parent, sibling) edge is _tagged_.
    struct Marks: usize {
        const STOP = 1usize.wrapping_shl(2);
        const FLAG = 1usize.wrapping_shl(1);
        const TAG  = 1usize.wrapping_shl(0);
    }
}

impl Marks {
    fn new(stop: bool, flag: bool, tag: bool) -> Self {
        (if stop { Marks::STOP } else { Marks::empty() })
            | (if flag { Marks::FLAG } else { Marks::empty() })
            | (if tag { Marks::TAG } else { Marks::empty() })
    }

    fn stop(self) -> bool {
        !(self & Marks::STOP).is_empty()
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
    left: AtomicPtr<Node<K, V>>,
    right: AtomicPtr<Node<K, V>>,
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
            left: AtomicPtr::new(ptr::null_mut()),
            right: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Make a new internal node, consuming the given left and right nodes,
    /// using the right node's key.
    fn new_internal(left: Node<K, V>, right: Node<K, V>) -> Node<K, V> {
        let right_key = right.key.clone();
        let left = Box::into_raw(Box::new(left));
        let right = Box::into_raw(Box::new(right));
        Node {
            key: right_key,
            value: None,
            left: AtomicPtr::new(left),
            right: AtomicPtr::new(right),
        }
    }
}

#[derive(Clone, Copy)]
enum Direction {
    L,
    R,
}

pub struct Handle<'domain> {
    ancestor_h: HazardPointer<'domain>,
    successor_h: HazardPointer<'domain>,
    parent_h: HazardPointer<'domain>,
    leaf_h: HazardPointer<'domain>,
}

impl Default for Handle<'static> {
    fn default() -> Self {
        Self {
            ancestor_h: HazardPointer::default(),
            successor_h: HazardPointer::default(),
            parent_h: HazardPointer::default(),
            leaf_h: HazardPointer::default(),
        }
    }
}

impl<'domain> Handle<'domain> {
    // bypass E0499-E0503, etc that are supposed to be fixed by polonius
    #[inline]
    fn launder<'hp1, 'hp2>(&'hp1 mut self) -> &'hp2 mut Self {
        unsafe { core::mem::transmute(self) }
    }
}

/// All Shared<_> are unmarked.
///
/// All of the edges of path from `successor` to `parent` are in the process of removal.
pub struct SeekRecord<'domain, 'hp, K, V> {
    /// Parent of `successor`
    ancestor: *mut Node<K, V>,
    /// The first internal node with a marked outgoing edge
    successor: *mut Node<K, V>,
    /// The direction of successor from ancestor.
    successor_dir: Direction,
    /// Parent of `leaf`
    parent: *mut Node<K, V>,
    /// The end of the access path.
    leaf: *mut Node<K, V>,
    /// The direction of leaf from parent.
    leaf_dir: Direction,

    handle: &'hp mut Handle<'domain>,
}

// TODO(@jeehoonkang): code duplication...
impl<'domain, 'hp, K, V> SeekRecord<'domain, 'hp, K, V> {
    fn new(handle: &'hp mut Handle<'domain>) -> Self {
        Self {
            ancestor: ptr::null_mut(),
            successor: ptr::null_mut(),
            successor_dir: Direction::L,
            parent: ptr::null_mut(),
            leaf: ptr::null_mut(),
            leaf_dir: Direction::L,
            handle,
        }
    }

    fn successor_addr(&self) -> &AtomicPtr<Node<K, V>> {
        match self.successor_dir {
            Direction::L => unsafe { &(*untagged(self.ancestor)).left },
            Direction::R => unsafe { &(*untagged(self.ancestor)).right },
        }
    }

    fn leaf_addr(&self) -> &AtomicPtr<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => unsafe { &(*untagged(self.parent)).left },
            Direction::R => unsafe { &(*untagged(self.parent)).right },
        }
    }

    fn leaf_sibling_addr(&self) -> &AtomicPtr<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => unsafe { &(*untagged(self.parent)).right },
            Direction::R => unsafe { &(*untagged(self.parent)).left },
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
                self.r.left.load(Ordering::Relaxed),
                self.r.right.load(Ordering::Relaxed),
            ];
            assert!(self.r.value.is_none());

            while let Some(node) = stack.pop() {
                if untagged(node).is_null() {
                    continue;
                }

                let node_addr = untagged(node);
                let node_ref = &*node_addr;

                stack.push(node_ref.left.load(Ordering::Relaxed));
                stack.push(node_ref.right.load(Ordering::Relaxed));
                drop(Box::from_raw(node_addr));
            }
        }
    }
}

impl<K, V> hp_pp::Invalidate for Node<K, V> {
    fn invalidate(&self) {
        let left = self.left.load(Ordering::Acquire);
        let right = self.right.load(Ordering::Acquire);
        self.left.store(
            tagged(left, Marks::new(true, true, true).bits),
            Ordering::Release,
        );
        self.right.store(
            tagged(right, Marks::new(true, true, true).bits),
            Ordering::Release,
        );
    }
}

struct Unlink<'r, 'domain, 'hp, K, V> {
    record: &'r SeekRecord<'domain, 'hp, K, V>,
    target_sibling: *mut Node<K, V>,
    flag: bool,
}

impl<'r, 'domain, 'hp, K, V> hp_pp::Unlink<Node<K, V>> for Unlink<'r, 'domain, 'hp, K, V> {
    fn do_unlink(&self) -> Result<Vec<*mut Node<K, V>>, ()> {
        let link = tagged(
            self.target_sibling,
            Marks::new(false, self.flag, false).bits(),
        );
        if self
            .record
            .successor_addr()
            .compare_exchange(
                self.record.successor,
                link,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            // destroy the subtree of successor except target_sibling
            let mut stack = vec![self.record.successor];
            let mut collected = Vec::with_capacity(32);

            while let Some(node) = stack.pop() {
                let node_addr = untagged(node);
                if node_addr.is_null() || (node_addr == untagged(self.target_sibling)) {
                    continue;
                }

                let node_ref = unsafe { &*node_addr };

                stack.push(node_ref.left.load(Ordering::Relaxed));
                stack.push(node_ref.right.load(Ordering::Relaxed));
                collected.push(node_addr);
            }
            Ok(collected)
        } else {
            Err(())
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
    fn seek<'domain, 'hp>(
        &self,
        key: &K,
        record: &mut SeekRecord<'domain, 'hp, K, V>,
    ) -> Result<(), ()> {
        let s = untagged(self.r.left.load(Ordering::Relaxed));
        let s_node = unsafe { &*s };

        // We doesn't have to defend with hazard pointers here
        record.ancestor = &self.r as *const _ as *mut _;
        record.successor = s; // TODO: should preserve tag?

        record.successor_dir = Direction::L;

        let leaf = untagged(s_node.left.load(Ordering::Relaxed));

        // We doesn't have to defend with hazard pointers here
        record.parent = s;

        record
            .handle
            .leaf_h
            .try_protect_pp(leaf, s_node, &s_node.left, &|s_node| {
                tag(s_node.left.load(Ordering::Acquire)) & 2 == 2
            })
            .map_err(|_| ())?;
        record.leaf = leaf;
        record.leaf_dir = Direction::L;

        let mut prev_tag = Marks::from_bits_truncate(tag(leaf)).tag();
        let mut curr_dir = Direction::L;
        let mut curr = unsafe { &*record.leaf }.left.load(Ordering::Relaxed);

        while !untagged(curr).is_null() {
            if !prev_tag {
                // untagged edge: advance ancestor and successor pointers
                record
                    .handle
                    .ancestor_h
                    .protect_raw(untagged(record.parent));
                record.ancestor = record.parent;
                record.handle.successor_h.protect_raw(untagged(record.leaf));
                record.successor = record.leaf;
                record.successor_dir = record.leaf_dir;
            }

            // advance parent and leaf pointers
            mem::swap(&mut record.parent, &mut record.leaf);
            mem::swap(&mut record.handle.parent_h, &mut record.handle.leaf_h);
            let mut curr_base = untagged(curr);
            let parent_ref = unsafe { &*record.parent };
            loop {
                match record.handle.leaf_h.try_protect_pp(
                    curr_base,
                    parent_ref,
                    match curr_dir {
                        Direction::L => &parent_ref.left,
                        Direction::R => &parent_ref.right,
                    },
                    &|src| {
                        Marks::from_bits_truncate(tag(match curr_dir {
                            Direction::L => src.left.load(Ordering::Acquire),
                            Direction::R => src.right.load(Ordering::Acquire),
                        }))
                        .stop()
                    },
                ) {
                    Ok(_) => break,
                    Err(ProtectError::Changed(curr_base_new)) => curr_base = curr_base_new,
                    Err(ProtectError::Stopped) => return Err(()),
                }
            }

            record.leaf = curr_base;
            record.leaf_dir = curr_dir;

            // update other variables
            prev_tag = Marks::from_bits_truncate(tag(curr)).tag();
            let curr_node = unsafe { &*curr_base };
            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                curr_dir = Direction::L;
                curr = curr_node.left.load(Ordering::Acquire);
            } else {
                curr_dir = Direction::R;
                curr = curr_node.right.load(Ordering::Acquire);
            }
        }
        return Ok(());
    }

    /// Similar to `seek`, but traverse the tree with only two pointers
    fn seek_leaf<'domain, 'hp>(
        &self,
        key: &K,
        record: &mut SeekRecord<'domain, 'hp, K, V>,
    ) -> Result<(), ()> {
        let s = untagged(self.r.left.load(Ordering::Relaxed));
        let s_node = unsafe { &*s };
        let leaf = untagged(s_node.left.load(Ordering::Relaxed));

        record.parent = s;
        record
            .handle
            .leaf_h
            .try_protect_pp(leaf, s_node, &s_node.left, &|s_node| {
                tag(s_node.left.load(Ordering::Acquire)) & 2 == 2
            })
            .map_err(|_| ())?;
        record.leaf = leaf;

        let mut curr_dir = Direction::L;
        let mut curr = unsafe { &*record.leaf }.left.load(Ordering::Relaxed);

        while !untagged(curr).is_null() {
            HazardPointer::swap(&mut record.handle.leaf_h, &mut record.handle.parent_h);
            mem::swap(&mut record.leaf, &mut record.parent);
            let mut curr_base = untagged(curr);

            loop {
                match record.handle.leaf_h.try_protect_pp(
                    curr_base,
                    unsafe { &*record.parent },
                    match curr_dir {
                        Direction::L => &unsafe { &*record.parent }.left,
                        Direction::R => &unsafe { &*record.parent }.right,
                    },
                    &|src| {
                        Marks::from_bits_truncate(tag(match curr_dir {
                            Direction::L => src.left.load(Ordering::Acquire),
                            Direction::R => src.right.load(Ordering::Acquire),
                        }))
                        .stop()
                    },
                ) {
                    Ok(_) => break,
                    Err(ProtectError::Changed(curr_base_new)) => curr_base = curr_base_new,
                    Err(ProtectError::Stopped) => return Err(()),
                }
            }

            record.leaf = curr_base;
            record.leaf_dir = curr_dir;

            let curr_node = unsafe { &*curr_base };
            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                curr_dir = Direction::L;
                curr = curr_node.left.load(Ordering::Acquire);
            } else {
                curr_dir = Direction::R;
                curr = curr_node.right.load(Ordering::Acquire);
            }
        }

        Ok(())
    }

    /// Physically removes node.
    ///
    /// Returns true if it successfully unlinks the flagged node in `record`.
    fn cleanup(&self, record: &SeekRecord<K, V>) -> bool {
        // Identify the node(subtree) that will replace `successor`.
        let leaf_marked = record.leaf_addr().load(Ordering::Acquire);
        let leaf_flag = Marks::from_bits_truncate(tag(leaf_marked)).flag();
        let target_sibling_addr = if leaf_flag {
            record.leaf_sibling_addr()
        } else {
            record.leaf_addr()
        };

        // NOTE: the ibr implementation uses CAS
        // tag (parent, sibling) edge -> all of the parent's edges can't change now
        // TODO: Is Release enough?
        target_sibling_addr.fetch_or(Marks::TAG.bits(), Ordering::AcqRel);

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        let target_sibling = target_sibling_addr.load(Ordering::Acquire);
        let flag = Marks::from_bits_truncate(tag(target_sibling)).flag();
        let link = tagged(target_sibling, Marks::new(false, flag, false).bits());
        let unlink = Unlink {
            record,
            target_sibling,
            flag,
        };

        unsafe { try_unlink(unlink, slice::from_ref(&link)) }
    }

    fn get_inner<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Result<Option<&'hp V>, ()> {
        let mut record = SeekRecord::new(handle);

        self.seek_leaf(key, &mut record)?;
        let leaf_node = unsafe { &*untagged(record.leaf) };

        if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
            return Ok(None);
        }

        Ok(Some(leaf_node.value.as_ref().unwrap()))
    }

    pub fn get<'domain, 'hp>(&self, key: &K, handle: &'hp mut Handle<'domain>) -> Option<&'hp V> {
        loop {
            if let Ok(r) = self.get_inner(key, handle.launder()) {
                return r;
            }
        }
    }

    fn insert_inner(
        &self,
        key: &K,
        value: V,
        record: &mut SeekRecord<K, V>,
    ) -> Result<(), Result<V, V>> {
        let new_leaf = Box::into_raw(Box::new(Node::new_leaf(Key::Fin(key.clone()), Some(value))));

        let new_internal = Box::into_raw(Box::new(Node::<K, V> {
            key: Key::Inf, // temporary placeholder
            value: None,
            left: AtomicPtr::new(ptr::null_mut()),
            right: AtomicPtr::new(ptr::null_mut()),
        }));

        loop {
            self.seek(&key, record).map_err(|_| unsafe {
                let value = (*new_leaf).value.take().unwrap();
                drop(Box::from_raw(new_leaf));
                drop(Box::from_raw(new_internal));
                Err(value)
            })?;
            let leaf = record.leaf;

            let (new_left, new_right) = match unsafe { &*untagged(leaf) }.key.cmp(&key) {
                cmp::Ordering::Equal => {
                    // Newly created nodes that failed to be inserted are free'd here.
                    let value = unsafe { &mut *new_leaf }.value.take().unwrap();
                    unsafe {
                        drop(Box::from_raw(new_leaf));
                        drop(Box::from_raw(new_internal));
                    }
                    return Err(Ok(value));
                }
                cmp::Ordering::Greater => (new_leaf, leaf),
                cmp::Ordering::Less => (leaf, new_leaf),
            };

            let new_internal_node = unsafe { &mut *new_internal };
            new_internal_node.key = unsafe { (*untagged(new_right)).key.clone() };
            new_internal_node.left.store(new_left, Ordering::Relaxed);
            new_internal_node.right.store(new_right, Ordering::Relaxed);

            // NOTE: record.leaf_addr is called childAddr in the paper.
            match record.leaf_addr().compare_exchange(
                leaf,
                new_internal,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => {
                    // Insertion failed. Help the conflicting remove operation if needed.
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if untagged(current) == leaf {
                        self.cleanup(&record);
                    }
                }
            }
        }
    }

    pub fn insert<'domain, 'hp>(
        &self,
        key: K,
        mut value: V,
        handle: &'hp mut Handle<'domain>,
    ) -> Result<(), (K, V)> {
        loop {
            let mut record = SeekRecord::new(handle);
            match self.insert_inner(&key, value, &mut record) {
                Ok(()) => return Ok(()),
                Err(Ok(v)) => return Err((key, v)),
                Err(Err(v)) => value = v,
            }
        }
    }

    fn remove_inner<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Result<Option<&'hp V>, ()> {
        // `leaf` and `value` are the snapshot of the node to be deleted.
        // NOTE: The paper version uses one big loop for both phases.
        // injection phase
        let mut record = SeekRecord::new(handle);
        let (leaf, value) = loop {
            self.seek(key, &mut record)?;

            // candidates
            let leaf = record.leaf;
            let leaf_node = unsafe { &*untagged(record.leaf) };

            if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
                return Ok(None);
            }

            let value = leaf_node.value.as_ref().unwrap();

            // Try injecting the deletion flag.
            match record.leaf_addr().compare_exchange(
                leaf,
                tagged(leaf, Marks::new(false, true, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Finalize the node to be removed
                    if self.cleanup(&record) {
                        return Ok(Some(value));
                    }
                    // In-place cleanup failed. Enter the cleanup phase.
                    break (leaf, value);
                }
                Err(current) => {
                    // Flagging failed.
                    // case 1. record.leaf_addr(e.current) points to another node: restart.
                    // case 2. Another thread flagged/tagged the edge to leaf: help and restart
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if leaf == tagged(current, Marks::empty().bits()) {
                        self.cleanup(&record);
                    }
                }
            }
        };

        let leaf = untagged(leaf);

        // cleanup phase
        loop {
            self.seek(key, &mut record)?;
            if record.leaf != leaf {
                // The edge to leaf flagged for deletion was removed by a helping thread
                return Ok(Some(value));
            }

            // leaf is still present in the tree.
            if self.cleanup(&record) {
                return Ok(Some(value));
            }
        }
    }

    pub fn remove<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V> {
        loop {
            if let Ok(r) = self.remove_inner(key, handle.launder()) {
                return r;
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for NMTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Handle<'domain> = Handle<'domain>;

    fn new() -> Self {
        Self::new()
    }

    fn handle() -> Self::Handle<'static> {
        Handle::default()
    }

    #[inline]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        self.get(key, handle)
    }

    #[inline]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.insert(key, value, handle).is_ok()
    }

    #[inline]
    fn remove<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: &K,
    ) -> Option<&'hp V> {
        self.remove(key, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::NMTreeMap;
    use crate::hp::concurrent_map;

    #[test]
    fn smoke_nm_tree() {
        concurrent_map::tests::smoke::<NMTreeMap<i32, String>>();
    }
}
