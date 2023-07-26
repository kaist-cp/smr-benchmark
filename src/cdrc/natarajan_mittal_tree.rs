use cdrc_rs::{AcquireRetire, AtomicRcPtr, RcPtr, SnapshotPtr};

use super::concurrent_map::ConcurrentMap;
use std::cmp;
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

struct Node<K, V, Guard>
where
    Guard: AcquireRetire,
{
    key: Key<K>,
    // TODO(@jeehoonkang): how about having another type that is either (1) value, or (2) left and
    // right.
    value: Option<V>,
    left: AtomicRcPtr<Node<K, V, Guard>, Guard>,
    right: AtomicRcPtr<Node<K, V, Guard>, Guard>,
}

impl<K, V, Guard> Node<K, V, Guard>
where
    K: Clone,
    V: Clone,
    Guard: AcquireRetire,
{
    fn new_leaf(key: Key<K>, value: Option<V>) -> Node<K, V, Guard> {
        Node {
            key,
            value,
            left: AtomicRcPtr::null(),
            right: AtomicRcPtr::null(),
        }
    }

    /// Make a new internal node, consuming the given left and right nodes,
    /// using the right node's key.
    fn new_internal(left: Node<K, V, Guard>, right: Node<K, V, Guard>) -> Node<K, V, Guard> {
        Node {
            key: right.key.clone(),
            value: None,
            left: AtomicRcPtr::new(left, unsafe { &Guard::unprotected() }),
            right: AtomicRcPtr::new(right, unsafe { &Guard::unprotected() }),
        }
    }
}

enum Direction {
    L,
    R,
}

/// All Shared<_> are unmarked.
///
/// All of the edges of path from `successor` to `parent` are in the process of removal.
struct SeekRecord<'g, K, V, Guard>
where
    Guard: AcquireRetire,
{
    /// Parent of `successor`
    ancestor: SnapshotPtr<'g, Node<K, V, Guard>, Guard>,
    /// The first internal node with a marked outgoing edge
    successor: SnapshotPtr<'g, Node<K, V, Guard>, Guard>,
    /// The direction of successor from ancestor.
    successor_dir: Direction,
    /// Parent of `leaf`
    parent: SnapshotPtr<'g, Node<K, V, Guard>, Guard>,
    /// The end of the access path.
    leaf: SnapshotPtr<'g, Node<K, V, Guard>, Guard>,
    /// The direction of leaf from parent.
    leaf_dir: Direction,
}

// TODO(@jeehoonkang): code duplication...
impl<'g, K, V, Guard> SeekRecord<'g, K, V, Guard>
where
    Guard: AcquireRetire,
{
    fn successor_addr(&'g self) -> &'g AtomicRcPtr<Node<K, V, Guard>, Guard> {
        match self.successor_dir {
            Direction::L => &unsafe { self.ancestor.deref() }.left,
            Direction::R => &unsafe { self.ancestor.deref() }.right,
        }
    }

    fn leaf_addr(&'g self) -> &'g AtomicRcPtr<Node<K, V, Guard>, Guard> {
        match self.leaf_dir {
            Direction::L => &unsafe { self.parent.deref() }.left,
            Direction::R => &unsafe { self.parent.deref() }.right,
        }
    }

    fn leaf_sibling_addr(&'g self) -> &'g AtomicRcPtr<Node<K, V, Guard>, Guard> {
        match self.leaf_dir {
            Direction::L => &unsafe { self.parent.deref() }.right,
            Direction::R => &unsafe { self.parent.deref() }.left,
        }
    }
}

// COMMENT(@jeehoonkang): write down the invariant of the tree
pub struct NMTreeMap<K, V, Guard>
where
    Guard: AcquireRetire,
{
    // It was `Node<K, V>` in EBR implementation,
    // but it will be difficult to initialize fake ancestor
    // with the previous definition.
    r: AtomicRcPtr<Node<K, V, Guard>, Guard>,
}

impl<K, V, Guard> Default for NMTreeMap<K, V, Guard>
where
    K: Ord + Clone,
    V: Clone,
    Guard: AcquireRetire,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, Guard> NMTreeMap<K, V, Guard>
where
    K: Ord + Clone,
    V: Clone,
    Guard: AcquireRetire,
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
        NMTreeMap {
            r: AtomicRcPtr::new(r, unsafe { &Guard::unprotected() }),
        }
    }

    // All `Shared<_>` fields are unmarked.
    fn seek<'g>(&'g self, key: &K, guard: &'g Guard) -> SeekRecord<'g, K, V, Guard> {
        let r = self.r.load_snapshot(guard);
        let s = unsafe { r.deref() }.left.load_snapshot(guard);
        let s_node = unsafe { s.deref() };
        let leaf = s_node
            .left
            .load_snapshot(guard)
            .with_mark(Marks::empty().bits());
        let leaf_node = unsafe { leaf.deref() };

        let mut record = SeekRecord {
            ancestor: r,
            successor: s.clone(guard),
            successor_dir: Direction::L,
            parent: s,
            leaf,
            leaf_dir: Direction::L,
        };

        let mut prev_tag = Marks::from_bits_truncate(record.leaf.mark()).tag();
        let mut curr_dir = Direction::L;
        let mut curr = leaf_node.left.load_snapshot(guard);

        while let Some(curr_node) = unsafe { curr.as_ref() } {
            if !prev_tag {
                // untagged edge: advance ancestor and successor pointers
                record.ancestor = record.parent;
                record.successor = record.leaf.clone(guard);
                record.successor_dir = record.leaf_dir;
            }

            let curr_mark = curr.mark();

            // advance parent and leaf pointers
            record.parent = record.leaf;
            record.leaf = curr.with_mark(Marks::empty().bits());
            record.leaf_dir = curr_dir;

            // update other variables
            prev_tag = Marks::from_bits_truncate(curr_mark).tag();
            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                curr_dir = Direction::L;
                curr = curr_node.left.load_snapshot(guard);
            } else {
                curr_dir = Direction::R;
                curr = curr_node.right.load_snapshot(guard);
            }
        }

        record
    }

    /// Similar to `seek`, but traverse the tree with only two pointers
    fn seek_leaf<'g>(&'g self, key: &K, guard: &'g Guard) -> SeekRecord<'g, K, V, Guard> {
        let r = self.r.load_snapshot(guard);
        let s = unsafe { r.deref() }.left.load_snapshot(guard);
        let s_node = unsafe { s.deref() };
        let leaf = s_node
            .left
            .load_snapshot(guard)
            .with_mark(Marks::empty().bits());

        let mut record = SeekRecord {
            ancestor: SnapshotPtr::null(guard),
            successor: SnapshotPtr::null(guard),
            successor_dir: Direction::L,
            parent: s,
            leaf,
            leaf_dir: Direction::L,
        };

        let mut curr = unsafe { record.leaf.deref() }
            .left
            .load_snapshot(guard)
            .with_mark(Marks::empty().bits());

        while let Some(curr_node) = unsafe { curr.as_ref() } {
            record.leaf = curr;

            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                curr = curr_node.left.load_snapshot(guard);
            } else {
                curr = curr_node.right.load_snapshot(guard);
            }
            curr = curr.with_mark(Marks::empty().bits());
        }

        record
    }

    /// Physically removes node.
    ///
    /// Returns true if it successfully unlinks the flagged node in `record`.
    fn cleanup(&self, record: &SeekRecord<'_, K, V, Guard>, guard: &Guard) -> bool {
        // Identify the node(subtree) that will replace `successor`.
        let leaf_marked = record.leaf_addr().load_snapshot(guard);
        let leaf_flag = Marks::from_bits_truncate(leaf_marked.mark()).flag();
        let target_sibling_addr = if leaf_flag {
            record.leaf_sibling_addr()
        } else {
            record.leaf_addr()
        };

        // NOTE: the ibr implementation uses CAS
        // tag (parent, sibling) edge -> all of the parent's edges can't change now
        target_sibling_addr.fetch_or(Marks::TAG.bits(), guard);

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        let target_sibling = target_sibling_addr.load_snapshot(guard);
        let flag = Marks::from_bits_truncate(target_sibling.mark()).flag();
        record
            .successor_addr()
            .compare_exchange_ss_ss(
                &record.successor,
                &target_sibling.with_mark(Marks::new(flag, false).bits()),
                guard,
            )
            .is_ok()
    }

    pub fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        let record = self.seek_leaf(key, guard);
        let leaf_node = unsafe { record.leaf.deref() };

        if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
            return None;
        }

        Some(leaf_node.value.as_ref().unwrap())
    }

    pub fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), (K, V)> {
        let mut new_leaf =
            RcPtr::make_shared(Node::new_leaf(Key::Fin(key.clone()), Some(value)), guard);

        let mut new_internal = RcPtr::make_shared(
            Node {
                key: Key::Inf, // temporary placeholder
                value: None,
                left: AtomicRcPtr::null(),
                right: AtomicRcPtr::null(),
            },
            guard,
        );

        loop {
            let record = self.seek(&key, guard);
            let leaf = record.leaf.clone(guard);

            let new_internal_node = unsafe { new_internal.deref_mut() };

            match unsafe { record.leaf.deref() }.key.cmp(&key) {
                cmp::Ordering::Equal => unsafe {
                    // Newly created nodes that failed to be inserted are free'd here.
                    let value = new_leaf.deref_mut().value.take().unwrap();
                    return Err((key, value));
                },
                cmp::Ordering::Greater => {
                    new_internal_node.key = unsafe { leaf.deref().key.clone() };
                    new_internal_node
                        .left
                        .store(new_leaf.clone(guard), Ordering::Relaxed, guard);
                    new_internal_node
                        .right
                        .store_snapshot(leaf, Ordering::Relaxed, guard);
                }
                cmp::Ordering::Less => {
                    new_internal_node.key = unsafe { new_leaf.deref().key.clone() };
                    new_internal_node
                        .left
                        .store_snapshot(leaf, Ordering::Relaxed, guard);
                    new_internal_node
                        .right
                        .store(new_leaf.clone(guard), Ordering::Relaxed, guard);
                }
            }

            // NOTE: record.leaf_addr is called childAddr in the paper.
            match record
                .leaf_addr()
                .compare_exchange_ss_rc(&record.leaf, &new_internal, guard)
            {
                Ok(()) => return Ok(()),
                Err(current) => {
                    // Insertion failed. Help the conflicting remove operation if needed.
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if current.with_mark(Marks::empty().bits()).as_usize() == record.leaf.as_usize()
                    {
                        self.cleanup(&record, guard);
                    }
                }
            }
        }
    }

    pub fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let mut record;
        // `leaf` and `value` are the snapshot of the node to be deleted.
        // NOTE: The paper version uses one big loop for both phases.
        // injection phase
        let (leaf, value) = loop {
            record = self.seek(key, guard);

            // candidates
            let leaf = record.leaf.clone(guard);
            let leaf_node = unsafe { record.leaf.as_ref().unwrap() };

            if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
                return None;
            }

            let value = leaf_node.value.as_ref().unwrap();

            // Try injecting the deletion flag.
            match record.leaf_addr().compare_exchange_mark(
                &record.leaf,
                Marks::new(true, false).bits(),
                guard,
            ) {
                Ok(()) => {
                    // Finalize the node to be removed
                    if self.cleanup(&record, guard) {
                        return Some(value);
                    }
                    // In-place cleanup failed. Enter the cleanup phase.
                    break (leaf, value);
                }
                Err(current) => {
                    // Flagging failed.
                    // case 1. record.leaf_addr(e.current) points to another node: restart.
                    // case 2. Another thread flagged/tagged the edge to leaf: help and restart
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if record.leaf.as_usize() == current.with_mark(Marks::empty().bits()).as_usize()
                    {
                        self.cleanup(&record, guard);
                    }
                }
            }
        };

        // cleanup phase
        loop {
            record = self.seek(key, guard);
            if record.leaf != leaf {
                // The edge to leaf flagged for deletion was removed by a helping thread
                return Some(value);
            }

            // leaf is still present in the tree.
            if self.cleanup(&record, guard) {
                return Some(value);
            }
        }
    }
}

impl<K, V, Guard> ConcurrentMap<K, V, Guard> for NMTreeMap<K, V, Guard>
where
    K: Ord + Clone,
    V: Clone,
    Guard: AcquireRetire,
{
    fn new() -> Self {
        Self::new()
    }

    #[inline]
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, guard)
    }
    #[inline]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.insert(key, value, guard).is_ok()
    }
    #[inline]
    fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.remove(key, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::NMTreeMap;
    use crate::cdrc::concurrent_map;
    use cdrc_rs::GuardEBR;

    #[test]
    fn smoke_nm_tree() {
        concurrent_map::tests::smoke::<GuardEBR, NMTreeMap<i32, String, GuardEBR>>();
    }
}
