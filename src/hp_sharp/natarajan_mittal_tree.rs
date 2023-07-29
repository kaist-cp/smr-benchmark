use std::{cmp, sync::atomic::Ordering};

use hp_sharp::{
    Atomic, CsGuard, Invalidate, Owned, Pointer, Protector, Retire, Shared, Shield, Thread,
};

use super::{concurrent_map::OutputHolder, ConcurrentMap};

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

struct Node<K, V> {
    key: Key<K>,
    // TODO(@jeehoonkang): how about having another type that is either (1) value, or (2) left and
    // right.
    value: Option<V>,
    left: Atomic<Node<K, V>>,
    right: Atomic<Node<K, V>>,
}

// TODO(@jeonghyeon): automate
impl<K, V> Invalidate for Node<K, V> {
    #[inline]
    fn invalidate(&self) {
        // We do not use `traverse_loop` for this data structure.
    }

    #[inline]
    fn is_invalidated(&self, _: &CsGuard) -> bool {
        false
        // We do not use `traverse_loop` for this data structure.
    }
}

impl<K, V> Node<K, V>
where
    K: Clone,
    V: Clone,
{
    #[inline]
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
    #[inline]
    fn new_internal(left: Node<K, V>, right: Node<K, V>) -> Node<K, V> {
        Node {
            key: right.key.clone(),
            value: None,
            left: Atomic::new(left),
            right: Atomic::new(right),
        }
    }
}

#[derive(Clone, Copy)]
enum Direction {
    L,
    R,
}

pub struct Output<K, V>(SeekRecord<K, V>, SeekRecord<K, V>);

impl<K, V> OutputHolder<V> for Output<K, V> {
    fn default(handle: &mut Thread) -> Self {
        Self(SeekRecord::empty(handle), SeekRecord::empty(handle))
    }

    fn output(&self) -> &V {
        self.0.leaf.as_ref().unwrap().value.as_ref().unwrap()
    }
}

/// All Shared<_> are unmarked.
///
/// All of the edges of path from `successor` to `parent` are in the process of removal.
// TODO(@jeonghyeon): automate
struct SeekRecord<K, V> {
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

// TODO(@jeonghyeon): automate
struct SharedSeekRecord<'r, K, V> {
    /// Parent of `successor`
    ancestor: Shared<'r, Node<K, V>>,
    /// The first internal node with a marked outgoing edge
    successor: Shared<'r, Node<K, V>>,
    /// The direction of successor from ancestor.
    successor_dir: Direction,
    /// Parent of `leaf`
    parent: Shared<'r, Node<K, V>>,
    /// The end of the access path.
    leaf: Shared<'r, Node<K, V>>,
    /// The direction of leaf from parent.
    leaf_dir: Direction,
}

// TODO(@jeonghyeon): automate
impl<'r, K, V> Clone for SharedSeekRecord<'r, K, V> {
    fn clone(&self) -> Self {
        Self { ..*self }
    }
}

// TODO(@jeonghyeon): automate
impl<'r, K, V> Copy for SharedSeekRecord<'r, K, V> {}

impl<K, V> Protector for SeekRecord<K, V> {
    type Target<'r> = SharedSeekRecord<'r, K, V>;

    #[inline]
    fn empty(handle: &mut Thread) -> Self {
        Self {
            ancestor: Shield::null(handle),
            successor: Shield::null(handle),
            successor_dir: Direction::L,
            parent: Shield::null(handle),
            leaf: Shield::null(handle),
            leaf_dir: Direction::L,
        }
    }

    #[inline]
    fn protect_unchecked(&mut self, read: &Self::Target<'_>) {
        self.ancestor.protect_unchecked(&read.ancestor);
        self.successor.protect_unchecked(&read.successor);
        self.successor_dir = read.successor_dir;
        self.parent.protect_unchecked(&read.parent);
        self.leaf.protect_unchecked(&read.leaf);
        self.leaf_dir = read.leaf_dir;
    }

    #[inline]
    fn as_target<'r>(&self, guard: &'r CsGuard) -> Option<Self::Target<'r>> {
        Some(SharedSeekRecord {
            ancestor: self.ancestor.as_target(guard)?,
            successor: self.successor.as_target(guard)?,
            successor_dir: self.successor_dir,
            parent: self.parent.as_target(guard)?,
            leaf: self.leaf.as_target(guard)?,
            leaf_dir: self.leaf_dir,
        })
    }

    #[inline]
    fn release(&mut self) {
        self.ancestor.release();
        self.successor.release();
        self.parent.release();
        self.leaf.release();
    }
}

// TODO(@jeehoonkang): code duplication...
impl<K, V> SeekRecord<K, V> {
    fn successor_addr<'g>(&'g self) -> &'g Atomic<Node<K, V>> {
        match self.successor_dir {
            Direction::L => unsafe { &self.ancestor.deref_unchecked().left },
            Direction::R => unsafe { &self.ancestor.deref_unchecked().right },
        }
    }

    fn leaf_addr<'g>(&'g self) -> &'g Atomic<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => unsafe { &self.parent.deref_unchecked().left },
            Direction::R => unsafe { &self.parent.deref_unchecked().right },
        }
    }

    fn leaf_sibling_addr<'g>(&'g self) -> &'g Atomic<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => unsafe { &self.parent.deref_unchecked().right },
            Direction::R => unsafe { &self.parent.deref_unchecked().left },
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
            let guard = &CsGuard::unprotected();
            let mut stack = vec![
                self.r.left.load(Ordering::Relaxed, guard),
                self.r.right.load(Ordering::Relaxed, guard),
            ];
            assert!(self.r.value.is_none());

            while let Some(node) = stack.pop() {
                if node.is_null() {
                    continue;
                }

                let node_ref = node.deref_unchecked();

                stack.push(node_ref.left.load(Ordering::Relaxed, guard));
                stack.push(node_ref.right.load(Ordering::Relaxed, guard));
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

    /// All `Shared<_>` fields are unmarked.
    fn seek(&self, key: &K, output: &mut Output<K, V>, handle: &mut Thread) {
        let result = &mut output.0;
        unsafe {
            result.traverse(handle, |guard| {
                let s = self.r.left.load(Ordering::Relaxed, guard);
                let s_node = s.deref_unchecked();
                let mut leaf = s_node
                    .left
                    .load(Ordering::Relaxed, guard)
                    .with_tag(Marks::empty().bits());
                let leaf_node = leaf.deref_unchecked();

                let mut ancestor = Shared::from_usize(&self.r as *const _ as usize);
                let mut successor = s;
                let mut successor_dir = Direction::L;
                let mut parent = s;
                let mut leaf_dir = Direction::L;
                let mut curr = leaf_node.left.load(Ordering::Relaxed, guard);
                let mut curr_dir = Direction::L;
                let mut prev_tag = Marks::from_bits_truncate(leaf.tag()).tag();

                while let Some(curr_node) = curr.as_ref(guard) {
                    if !prev_tag {
                        // untagged edge: advance ancestor and successor pointers
                        ancestor = parent;
                        successor = leaf;
                        successor_dir = leaf_dir;
                    }

                    // advance parent and leaf pointers
                    parent = leaf;
                    leaf = curr.with_tag(Marks::empty().bits());
                    leaf_dir = curr_dir;

                    // update other variables
                    prev_tag = Marks::from_bits_truncate(curr.tag()).tag();
                    if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                        curr_dir = Direction::L;
                        curr = curr_node.left.load(Ordering::Acquire, guard);
                    } else {
                        curr_dir = Direction::R;
                        curr = curr_node.right.load(Ordering::Acquire, guard);
                    }
                }

                SharedSeekRecord {
                    ancestor,
                    successor,
                    successor_dir,
                    parent,
                    leaf,
                    leaf_dir,
                }
            });
        }
    }

    /// Similar to `seek`, but traverse the tree with only two pointers
    fn seek_leaf(&self, key: &K, output: &mut Output<K, V>, handle: &mut Thread) {
        let result = &mut output.0.leaf;
        unsafe {
            result.traverse(handle, |guard| {
                let s = self.r.left.load(Ordering::Relaxed, guard);
                let s_node = s.deref_unchecked();
                let mut leaf = s_node.left.load(Ordering::Acquire, guard).with_tag(0);
                let leaf_node = leaf.deref_unchecked();
                let mut curr = leaf_node.left.load(Ordering::Acquire, guard).with_tag(0);

                while let Some(curr_node) = curr.as_ref(guard) {
                    leaf = curr;
                    if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                        curr = curr_node.left.load(Ordering::Acquire, guard);
                    } else {
                        curr = curr_node.right.load(Ordering::Acquire, guard);
                    }
                    curr = curr.with_tag(0);
                }
                leaf
            });
        }
    }

    /// Physically removes node.
    ///
    /// Returns true if it successfully unlinks the flagged node in `record`.
    fn cleanup(&self, record: &SeekRecord<K, V>, handle: &mut Thread) -> bool {
        // Identify the node(subtree) that will replace `successor`.
        let leaf_marked = record.leaf_addr().load(Ordering::Acquire, handle);
        let leaf_flag = Marks::from_bits_truncate(leaf_marked.tag()).flag();
        let target_sibling_addr = if leaf_flag {
            record.leaf_sibling_addr()
        } else {
            record.leaf_addr()
        };

        // NOTE: the ibr implementation uses CAS
        // tag (parent, sibling) edge -> all of the parent's edges can't change now
        // TODO: Is Release enough?
        target_sibling_addr.fetch_or(Marks::TAG.bits(), Ordering::AcqRel, handle);

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        let target_sibling = target_sibling_addr.load(Ordering::Acquire, handle);
        let flag = Marks::from_bits_truncate(target_sibling.tag()).flag();
        let is_unlinked = record
            .successor_addr()
            .compare_exchange(
                record.successor.shared(),
                target_sibling.with_tag(Marks::new(flag, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
                handle,
            )
            .is_ok();

        if is_unlinked {
            unsafe {
                // Safety: As this thread is a winner of the physical CAS, it is the only one who
                // retires this chain.
                let guard = &CsGuard::unprotected();

                // destroy the subtree of successor except target_sibling
                let mut stack = vec![record.successor.shared()];

                while let Some(node) = stack.pop() {
                    if node.is_null()
                        || (node.with_tag(Marks::empty().bits())
                            == target_sibling.with_tag(Marks::empty().bits()))
                    {
                        continue;
                    }

                    let node_ref = node.deref_unchecked();

                    stack.push(node_ref.left.load(Ordering::Relaxed, guard));
                    stack.push(node_ref.right.load(Ordering::Relaxed, guard));
                    handle.retire(node);
                }
            }
        }

        is_unlinked
    }

    pub fn get(&self, key: &K, output: &mut Output<K, V>, handle: &mut Thread) -> bool {
        self.seek_leaf(key, output, handle);
        let leaf_node = unsafe { output.0.leaf.deref_unchecked() };

        leaf_node.key.cmp(key) == cmp::Ordering::Equal
    }

    pub fn insert(&self, key: K, value: V, output: &mut Output<K, V>, handle: &mut Thread) -> bool {
        let new_leaf = Owned::new(Node::new_leaf(Key::Fin(key.clone()), Some(value))).into_shared();

        let mut new_internal = Owned::new(Node {
            key: Key::Inf, // temporary placeholder
            value: None,
            left: Atomic::null(),
            right: Atomic::null(),
        })
        .into_shared();

        loop {
            self.seek(&key, output, handle);
            let record = &output.0;

            let (new_left, new_right) = match unsafe { record.leaf.deref_unchecked() }.key.cmp(&key)
            {
                cmp::Ordering::Equal => unsafe {
                    // Newly created nodes that failed to be inserted are free'd here.
                    drop(new_leaf.into_owned());
                    drop(new_internal.into_owned());
                    return false;
                },
                cmp::Ordering::Greater => (new_leaf, record.leaf.shared()),
                cmp::Ordering::Less => (record.leaf.shared(), new_leaf),
            };

            let new_internal_node = unsafe { new_internal.deref_mut_unchecked() };
            new_internal_node.key = unsafe { new_right.deref_unchecked() }.key.clone();
            new_internal_node
                .left
                .store(new_left, Ordering::Relaxed, handle);
            new_internal_node
                .right
                .store(new_right, Ordering::Relaxed, handle);

            // NOTE: record.leaf_addr is called childAddr in the paper.
            match record.leaf_addr().compare_exchange(
                record.leaf.shared(),
                new_internal,
                Ordering::AcqRel,
                Ordering::Acquire,
                handle,
            ) {
                Ok(_) => return true,
                Err(e) => {
                    // Insertion failed. Help the conflicting remove operation if needed.
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if e.actual.with_tag(Marks::empty().bits()) == record.leaf.shared() {
                        self.cleanup(record, handle);
                    }
                }
            }
        }
    }

    pub fn remove<'g>(&self, key: &K, output: &mut Output<K, V>, handle: &mut Thread) -> bool {
        // `leaf` and `value` are the snapshot of the node to be deleted.
        // NOTE: The paper version uses one big loop for both phases.
        // injection phase
        loop {
            self.seek(key, output, handle);
            let record = &output.0;

            let leaf_node = unsafe { record.leaf.deref_unchecked() };

            if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
                return false;
            }

            // Try injecting the deletion flag.
            match record.leaf_addr().compare_exchange(
                record.leaf.shared(),
                record
                    .leaf
                    .shared()
                    .with_tag(Marks::new(true, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
                handle,
            ) {
                Ok(_) => {
                    // Finalize the node to be removed
                    if self.cleanup(record, handle) {
                        return true;
                    }
                    // In-place cleanup failed. Enter the cleanup phase.
                    break;
                }
                Err(e) => {
                    // Flagging failed.
                    // case 1. record.leaf_addr(e.current) points to another node: restart.
                    // case 2. Another thread flagged/tagged the edge to leaf: help and restart
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if record.leaf.shared() == e.actual.with_tag(Marks::empty().bits()) {
                        self.cleanup(record, handle);
                    }
                }
            }
        }

        let leaf = output.0.leaf.shared().into_usize();

        // cleanup phase
        loop {
            self.seek(key, output, handle);
            if output.0.leaf.as_raw() != leaf {
                // The edge to leaf flagged for deletion was removed by a helping thread
                return true;
            }

            // leaf is still present in the tree.
            if self.cleanup(&output.0, handle) {
                return true;
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for NMTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Output = Output<K, V>;

    fn new() -> Self {
        Self::new()
    }

    #[inline(never)]
    fn get(&self, key: &K, output: &mut Self::Output, handle: &mut Thread) -> bool {
        self.get(key, output, handle)
    }

    #[inline(never)]
    fn insert(&self, key: K, value: V, output: &mut Self::Output, handle: &mut Thread) -> bool {
        self.insert(key, value, output, handle)
    }

    #[inline(never)]
    fn remove<'domain, 'hp>(
        &self,
        key: &K,
        output: &mut Self::Output,
        handle: &mut Thread,
    ) -> bool {
        self.remove(key, output, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::NMTreeMap;
    use crate::hp_sharp::concurrent_map;

    #[test]
    fn smoke_nm_tree() {
        concurrent_map::tests::smoke::<NMTreeMap<i32, String>>();
    }
}
