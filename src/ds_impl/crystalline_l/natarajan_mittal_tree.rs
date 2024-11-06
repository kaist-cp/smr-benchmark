use super::concurrent_map::ConcurrentMap;

use std::cmp;
use std::sync::atomic::Ordering;

use crystalline_l::{Atomic, Handle, HazardEra, Shared};

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

    fn marked(self) -> bool {
        !(self & (Marks::TAG | Marks::FLAG)).is_empty()
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
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

pub struct Node<K, V> {
    key: Key<K>,
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
    fn new_internal(
        left: Node<K, V>,
        right: Node<K, V>,
        handle: &Handle<'_, Node<K, V>>,
    ) -> Node<K, V> {
        Node {
            key: right.key.clone(),
            value: None,
            left: Atomic::new(left, handle),
            right: Atomic::new(right, handle),
        }
    }
}

#[derive(Clone, Copy)]
enum Direction {
    L,
    R,
}

pub struct EraShields<'d, 'h, K, V> {
    ancestor_h: HazardEra<'d, 'h, Node<K, V>>,
    successor_h: HazardEra<'d, 'h, Node<K, V>>,
    parent_h: HazardEra<'d, 'h, Node<K, V>>,
    leaf_h: HazardEra<'d, 'h, Node<K, V>>,
    curr_h: HazardEra<'d, 'h, Node<K, V>>,
}

impl<'d, 'h, K, V> EraShields<'d, 'h, K, V> {
    pub fn new(handle: &'h Handle<'d, Node<K, V>>) -> Self {
        Self {
            ancestor_h: HazardEra::new(handle),
            successor_h: HazardEra::new(handle),
            parent_h: HazardEra::new(handle),
            leaf_h: HazardEra::new(handle),
            curr_h: HazardEra::new(handle),
        }
    }

    // bypass E0499-E0503, etc that are supposed to be fixed by polonius
    #[inline]
    fn launder<'he2>(&mut self) -> &'he2 mut Self {
        unsafe { core::mem::transmute(self) }
    }
}

/// All Shared<_> are unmarked.
///
/// All of the edges of path from `successor` to `parent` are in the process of removal.
pub struct SeekRecord<K, V> {
    /// Parent of `successor`
    ancestor: Shared<Node<K, V>>,
    /// The first internal node with a marked outgoing edge
    successor: Shared<Node<K, V>>,
    /// The direction of successor from ancestor.
    successor_dir: Direction,
    /// Parent of `leaf`
    parent: Shared<Node<K, V>>,
    /// The end of the access path.
    leaf: Shared<Node<K, V>>,
    /// The direction of leaf from parent.
    leaf_dir: Direction,
}

impl<K, V> SeekRecord<K, V> {
    fn new() -> Self {
        Self {
            ancestor: Shared::null(),
            successor: Shared::null(),
            successor_dir: Direction::L,
            parent: Shared::null(),
            leaf: Shared::null(),
            leaf_dir: Direction::L,
        }
    }

    fn successor_addr(&self) -> &Atomic<Node<K, V>> {
        match self.successor_dir {
            Direction::L => unsafe { &self.ancestor.deref().left },
            Direction::R => unsafe { &self.ancestor.deref().right },
        }
    }

    fn leaf_addr(&self) -> &Atomic<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => unsafe { &self.parent.deref().left },
            Direction::R => unsafe { &self.parent.deref().right },
        }
    }

    fn leaf_sibling_addr(&self) -> &Atomic<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => unsafe { &self.parent.deref().right },
            Direction::R => unsafe { &self.parent.deref().left },
        }
    }
}

pub struct NMTreeMap<K, V> {
    r: Node<K, V>,
}

impl<K, V> Default for NMTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn default() -> Self {
        todo!("new")
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
                if node.is_null() {
                    continue;
                }

                let node_ref = node.deref();

                stack.push(node_ref.left.load(Ordering::Relaxed));
                stack.push(node_ref.right.load(Ordering::Relaxed));
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
    pub fn new<'d>(handle: &Handle<'d, Node<K, V>>) -> Self {
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
        let s = Node::new_internal(inf0, inf1, handle);
        let r = Node::new_internal(s, inf2, handle);
        NMTreeMap { r }
    }

    fn seek<'d, 'h>(
        &self,
        key: &K,
        record: &mut SeekRecord<K, V>,
        shields: &mut EraShields<'d, 'h, K, V>,
    ) -> Result<(), ()> {
        let s = self.r.left.load(Ordering::Relaxed).with_tag(0);
        let s_node = unsafe { s.deref() };

        // The root node is always alive; we do not have to protect it.
        record.ancestor = Shared::from(&self.r as *const _ as usize);
        record.successor = s; // TODO: should preserve tag?

        record.successor_dir = Direction::L;
        // The `s` node is always alive; we do not have to protect it.
        record.parent = s;

        record.leaf = s_node.left.protect(&mut shields.leaf_h);
        record.leaf_dir = Direction::L;

        let mut prev_tag = Marks::from_bits_truncate(record.leaf.tag()).tag();
        let mut curr_dir = Direction::L;
        let mut curr = unsafe { record.leaf.deref() }
            .left
            .protect(&mut shields.curr_h);

        // `ancestor` always points untagged node.
        while !curr.is_null() {
            if !prev_tag {
                // untagged edge: advance ancestor and successor pointers
                record.ancestor = record.parent;
                record.successor = record.leaf;
                record.successor_dir = record.leaf_dir;
                // `ancestor` and `successor` are already protected by
                // hazard pointers of `parent` and `leaf`.

                // Advance the parent and leaf pointers when the cursor looks like the following:
                // (): protected by its dedicated shield.
                //
                //  (parent), ancestor -> O                   (ancestor) -> O
                //                       / \                               / \
                // (leaf), successor -> O   O   => (parent), successor -> O   O
                //                     / \                               / \
                //                    O   O                   (leaf) -> O   O
                record.parent = record.leaf;
                HazardEra::swap(&mut shields.ancestor_h, &mut shields.parent_h);
                HazardEra::swap(&mut shields.parent_h, &mut shields.leaf_h);
            } else if record.successor.ptr_eq(record.parent) {
                // Advance the parent and leaf pointer when the cursor looks like the following:
                // (): protected by its dedicated shield.
                //
                //            (ancestor) -> O             (ancestor) -> O
                //                         / \                         / \
                // (parent), successor -> O   O        (successor) -> O   O
                //                       / \      =>                 / \
                //            (leaf) -> O   O           (parent) -> O   O
                //                     / \                         / \
                //                    O   O             (leaf) -> O   O
                record.parent = record.leaf;
                HazardEra::swap(&mut shields.successor_h, &mut shields.parent_h);
                HazardEra::swap(&mut shields.parent_h, &mut shields.leaf_h);
            } else {
                // Advance the parent and leaf pointer when the cursor looks like the following:
                // (): protected by its dedicated shield.
                //
                //    (ancestor) -> O
                //                 / \
                // (successor) -> O   O
                //             ... ...
                //  (parent) -> O
                //             / \
                //  (leaf) -> O   O
                record.parent = record.leaf;
                HazardEra::swap(&mut shields.parent_h, &mut shields.leaf_h);
            }
            debug_assert_eq!(record.successor.tag(), 0);

            if Marks::from_bits_truncate(curr.tag()).marked() {
                // `curr` is marked. Validate by `ancestor`.
                let succ_new = record.successor_addr().load(Ordering::Acquire);
                if Marks::from_bits_truncate(succ_new.tag()).marked()
                    || !record.successor.ptr_eq(succ_new)
                {
                    // Validation is failed. Let's restart from the root.
                    // TODO: Maybe it can be optimized (by restarting from the anchor), but
                    //       it would require a serious reasoning (including shield swapping, etc).
                    return Err(());
                }
            }

            record.leaf = curr;
            record.leaf_dir = curr_dir;

            // update other variables
            prev_tag = Marks::from_bits_truncate(curr.tag()).tag();
            let curr_node = unsafe { curr.deref() };
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
    fn cleanup<'d>(&self, record: &mut SeekRecord<K, V>, handle: &Handle<'d, Node<K, V>>) -> bool {
        // Identify the node(subtree) that will replace `successor`.
        let leaf_marked = record.leaf_addr().load(Ordering::Acquire);
        let leaf_flag = Marks::from_bits_truncate(leaf_marked.tag()).flag();
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
        let flag = Marks::from_bits_truncate(target_sibling.tag()).flag();
        let is_unlinked = record
            .successor_addr()
            .compare_exchange(
                record.successor.with_tag(0),
                target_sibling.with_tag(Marks::new(flag, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();

        if is_unlinked {
            unsafe {
                // destroy the subtree of successor except target_sibling
                let mut stack = vec![record.successor];

                while let Some(node) = stack.pop() {
                    if node.is_null() || node.with_tag(0).ptr_eq(target_sibling.with_tag(0)) {
                        continue;
                    }

                    let node_ref = node.deref();

                    stack.push(node_ref.left.load(Ordering::Relaxed));
                    stack.push(node_ref.right.load(Ordering::Relaxed));
                    handle.retire(node);
                }
            }
        }

        is_unlinked
    }

    fn get_inner<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut EraShields<'d, 'h, K, V>,
    ) -> Result<Option<&'he V>, ()> {
        let mut record = SeekRecord::new();

        self.seek(key, &mut record, shields)?;
        let leaf_node = unsafe { record.leaf.deref() };

        if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
            return Ok(None);
        }

        Ok(Some(leaf_node.value.as_ref().unwrap()))
    }

    pub fn get<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut EraShields<'d, 'h, K, V>,
    ) -> Option<&'he V> {
        loop {
            if let Ok(r) = self.get_inner(key, shields.launder()) {
                return r;
            }
        }
    }

    fn insert_inner<'d, 'h, 'he>(
        &self,
        key: &K,
        value: V,
        record: &mut SeekRecord<K, V>,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &Handle<'d, Node<K, V>>,
    ) -> Result<(), Result<V, V>> {
        let mut new_leaf = Shared::new(Node::new_leaf(Key::Fin(key.clone()), Some(value)), handle);

        let mut new_internal = Shared::new(
            Node::<K, V> {
                key: Key::Inf, // temporary placeholder
                value: None,
                left: Atomic::null(),
                right: Atomic::null(),
            },
            handle,
        );

        loop {
            self.seek(key, record, shields).map_err(|_| unsafe {
                let value = new_leaf.deref_mut().value.take().unwrap();
                drop(new_leaf.into_owned());
                drop(new_internal.into_owned());
                Err(value)
            })?;
            let leaf = record.leaf.with_tag(0);

            let (new_left, new_right) = match unsafe { leaf.deref() }.key.cmp(key) {
                cmp::Ordering::Equal => {
                    // Newly created nodes that failed to be inserted are free'd here.
                    let value = unsafe { new_leaf.deref_mut() }.value.take().unwrap();
                    unsafe {
                        drop(new_leaf.into_owned());
                        drop(new_internal.into_owned());
                    }
                    return Err(Ok(value));
                }
                cmp::Ordering::Greater => (new_leaf, leaf),
                cmp::Ordering::Less => (leaf, new_leaf),
            };

            let new_internal_node = unsafe { new_internal.deref_mut() };
            new_internal_node.key = unsafe { new_right.deref().key.clone() };
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
                Err(e) => {
                    // Insertion failed. Help the conflicting remove operation if needed.
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if e.current.with_tag(0).ptr_eq(leaf) {
                        self.cleanup(record, handle);
                    }
                }
            }
        }
    }

    pub fn insert<'d, 'h, 'he>(
        &self,
        key: K,
        mut value: V,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &Handle<'d, Node<K, V>>,
    ) -> Result<(), (K, V)> {
        loop {
            let mut record = SeekRecord::new();
            match self.insert_inner(&key, value, &mut record, shields, handle) {
                Ok(()) => return Ok(()),
                Err(Ok(v)) => return Err((key, v)),
                Err(Err(v)) => value = v,
            }
        }
    }

    fn remove_inner<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &Handle<'d, Node<K, V>>,
    ) -> Result<Option<&'he V>, ()> {
        // `leaf` and `value` are the snapshot of the node to be deleted.
        // NOTE: The paper version uses one big loop for both phases.
        // injection phase
        let mut record = SeekRecord::new();
        let (leaf, value) = loop {
            self.seek(key, &mut record, shields)?;

            // candidates
            let leaf = record.leaf.with_tag(0);
            let leaf_node = unsafe { record.leaf.deref() };

            if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
                return Ok(None);
            }

            let value = leaf_node.value.as_ref().unwrap();

            // Try injecting the deletion flag.
            match record.leaf_addr().compare_exchange(
                leaf,
                leaf.with_tag(Marks::new(true, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Finalize the node to be removed
                    if self.cleanup(&mut record, handle) {
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
                    if leaf.ptr_eq(e.current.with_tag(Marks::empty().bits())) {
                        self.cleanup(&mut record, handle);
                    }
                }
            }
        };

        let leaf = leaf.with_tag(0);

        // cleanup phase
        loop {
            self.seek(key, &mut record, shields)?;
            if !record.leaf.with_tag(0).ptr_eq(leaf) {
                // The edge to leaf flagged for deletion was removed by a helping thread
                return Ok(Some(value));
            }

            // leaf is still present in the tree.
            if self.cleanup(&mut record, handle) {
                return Ok(Some(value));
            }
        }
    }

    pub fn remove<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut EraShields<'d, 'h, K, V>,
        handle: &Handle<'d, Node<K, V>>,
    ) -> Option<&'he V> {
        loop {
            if let Ok(r) = self.remove_inner(key, shields.launder(), handle) {
                return r;
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for NMTreeMap<K, V>
where
    K: Ord + Clone + 'static,
    V: Clone + 'static,
{
    type Node = Node<K, V>;

    type Shields<'d, 'h> = EraShields<'d, 'h, K, V>
    where
        'd: 'h;

    fn shields<'d, 'h>(handle: &'h Handle<'d, Self::Node>) -> Self::Shields<'d, 'h> {
        EraShields::new(handle)
    }

    fn new<'d, 'h>(handle: &'h Handle<'d, Self::Node>) -> Self {
        Self::new(handle)
    }

    #[inline(always)]
    fn get<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        _handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V> {
        self.get(key, shields)
    }

    #[inline(always)]
    fn insert<'d, 'h>(
        &self,
        key: K,
        value: V,
        shields: &mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> bool {
        self.insert(key, value, shields, handle).is_ok()
    }

    #[inline(always)]
    fn remove<'d, 'h, 'he>(
        &self,
        key: &K,
        shields: &'he mut Self::Shields<'d, 'h>,
        handle: &'h Handle<'d, Self::Node>,
    ) -> Option<&'he V> {
        self.remove(key, shields, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::NMTreeMap;
    use crate::ds_impl::crystalline_l::concurrent_map;

    #[test]
    fn smoke_nm_tree() {
        concurrent_map::tests::smoke::<NMTreeMap<i32, String>>();
    }
}
