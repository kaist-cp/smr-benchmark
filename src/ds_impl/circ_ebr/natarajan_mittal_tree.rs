use circ::{AtomicRc, CsEBR, GraphNode, Pointer, Rc, Snapshot, StrongPtr};

use super::concurrent_map::{ConcurrentMap, OutputHolder};
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

pub struct Node<K, V> {
    key: Key<K>,
    // TODO(@jeehoonkang): how about having another type that is either (1) value, or (2) left and
    // right.
    value: Option<V>,
    left: AtomicRc<Node<K, V>, CsEBR>,
    right: AtomicRc<Node<K, V>, CsEBR>,
}

impl<K, V> GraphNode<CsEBR> for Node<K, V> {
    const UNIQUE_OUTDEGREE: bool = false;

    #[inline]
    fn pop_outgoings(&self, result: &mut Vec<Rc<Self, CsEBR>>)
    where
        Self: Sized,
    {
        result.push(self.left.swap(Rc::null(), Ordering::Relaxed));
        result.push(self.right.swap(Rc::null(), Ordering::Relaxed));
    }

    #[inline]
    fn pop_unique(&self) -> Rc<Self, CsEBR>
    where
        Self: Sized,
    {
        unimplemented!()
    }
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
            left: AtomicRc::null(),
            right: AtomicRc::null(),
        }
    }

    /// Make a new internal node, consuming the given left and right nodes,
    /// using the right node's key.
    fn new_internal(left: Node<K, V>, right: Node<K, V>) -> Node<K, V> {
        Node {
            key: right.key.clone(),
            value: None,
            left: AtomicRc::new(left),
            right: AtomicRc::new(right),
        }
    }
}

impl<K, V> OutputHolder<V> for Snapshot<Node<K, V>, CsEBR> {
    fn output(&self) -> &V {
        self.as_ref()
            .map(|node| node.value.as_ref().unwrap())
            .unwrap()
    }
}

#[derive(Default, Clone, Copy)]
enum Direction {
    #[default]
    L,
    R,
}

/// All Shared<_> are unmarked.
///
/// All of the edges of path from `successor` to `parent` are in the process of removal.
pub struct SeekRecord<K, V> {
    /// Parent of `successor`
    ancestor: Snapshot<Node<K, V>, CsEBR>,
    /// The first internal node with a marked outgoing edge.
    successor: Snapshot<Node<K, V>, CsEBR>,
    /// The direction of successor from ancestor.
    successor_dir: Direction,
    /// Parent of `leaf`
    parent: Snapshot<Node<K, V>, CsEBR>,
    /// The end of the access path.
    leaf: Snapshot<Node<K, V>, CsEBR>,
    /// The direction of leaf from parent.
    leaf_dir: Direction,
}

// TODO(@jeehoonkang): code duplication...
impl<K, V> SeekRecord<K, V> {
    fn successor_addr(&self) -> &AtomicRc<Node<K, V>, CsEBR> {
        match self.successor_dir {
            Direction::L => &unsafe { self.ancestor.deref() }.left,
            Direction::R => &unsafe { self.ancestor.deref() }.right,
        }
    }

    fn leaf_addr(&self) -> &AtomicRc<Node<K, V>, CsEBR> {
        match self.leaf_dir {
            Direction::L => &unsafe { self.parent.deref() }.left,
            Direction::R => &unsafe { self.parent.deref() }.right,
        }
    }

    fn leaf_sibling_addr(&self) -> &AtomicRc<Node<K, V>, CsEBR> {
        match self.leaf_dir {
            Direction::L => &unsafe { self.parent.deref() }.right,
            Direction::R => &unsafe { self.parent.deref() }.left,
        }
    }
}

// COMMENT(@jeehoonkang): write down the invariant of the tree
pub struct NMTreeMap<K, V> {
    r: AtomicRc<Node<K, V>, CsEBR>,
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
        NMTreeMap {
            r: AtomicRc::new(r),
        }
    }

    // All `Shared<_>` fields are unmarked.
    fn seek(&self, key: &K, cs: &CsEBR) -> SeekRecord<K, V> {
        let r = self.r.load_ss(cs);
        let s = unsafe { r.deref() }.left.load_ss(cs);
        let mut leaf = unsafe { s.deref() }.left.load_ss(cs);
        leaf.set_tag(Marks::empty().bits());

        let mut record = SeekRecord {
            ancestor: r,
            successor: s,
            successor_dir: Direction::L,
            parent: s,
            leaf,
            leaf_dir: Direction::L,
        };

        let mut prev_tag = Marks::from_bits_truncate(leaf.tag()).tag();
        let mut curr_dir = Direction::L;
        let mut curr = unsafe { leaf.deref() }.left.load_ss(cs);

        while let Some(curr_node) = curr.as_ref() {
            if !prev_tag {
                // untagged edge: advance ancestor and successor pointers
                record.ancestor = record.parent;
                record.successor = record.leaf;
                record.successor_dir = record.leaf_dir;
            }

            // advance parent and leaf pointers
            record.parent = record.leaf;
            record.leaf = curr;
            record.leaf.set_tag(Marks::empty().bits());
            record.leaf_dir = curr_dir;

            // update other variables
            prev_tag = Marks::from_bits_truncate(curr.tag()).tag();
            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                curr_dir = Direction::L;
                curr = curr_node.left.load_ss(cs);
            } else {
                curr_dir = Direction::R;
                curr = curr_node.right.load_ss(cs);
            }
        }

        record
    }

    /// Similar to `seek`, but traverse the tree with only two pointers
    fn seek_leaf(&self, key: &K, cs: &CsEBR) -> SeekRecord<K, V> {
        let r = self.r.load_ss(cs);
        let s = unsafe { r.deref() }.left.load_ss(cs);
        let mut leaf = unsafe { s.deref() }.left.load_ss(cs);
        leaf.set_tag(0);

        let mut record = SeekRecord {
            ancestor: Snapshot::new(),
            successor: Snapshot::new(),
            successor_dir: Direction::L,
            parent: s,
            leaf,
            leaf_dir: Direction::L,
        };

        let mut curr = unsafe { record.leaf.deref() }.left.load_ss(cs);
        curr.set_tag(0);

        while let Some(curr_node) = curr.as_ref() {
            record.leaf = curr;

            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                curr = curr_node.left.load_ss(cs);
            } else {
                curr = curr_node.right.load_ss(cs);
            }
            curr.set_tag(0);
        }

        record
    }

    /// Physically removes node.
    ///
    /// Returns true if it successfully unlinks the flagged node in `record`.
    fn cleanup(&self, record: &SeekRecord<K, V>, cs: &CsEBR) -> bool {
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
        loop {
            let target_sibling = target_sibling_addr.load_ss(cs);
            if target_sibling_addr
                .compare_exchange_tag(
                    target_sibling,
                    target_sibling.tag() | Marks::TAG.bits(),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    cs,
                )
                .is_ok()
            {
                break;
            }
        }

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        let target_sibling = target_sibling_addr.load_ss(cs);
        let flag = Marks::from_bits_truncate(target_sibling.tag()).flag();
        record
            .successor_addr()
            .compare_exchange(
                record.successor.as_ptr(),
                target_sibling
                    .upgrade()
                    .with_tag(Marks::new(flag, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
                cs,
            )
            .is_ok()
    }

    pub fn get(&self, key: &K, cs: &CsEBR) -> Option<Snapshot<Node<K, V>, CsEBR>> {
        let record = self.seek_leaf(key, cs);
        let leaf_node = unsafe { record.leaf.deref() };
        if leaf_node.key.cmp(key) == cmp::Ordering::Equal {
            Some(record.leaf)
        } else {
            None
        }
    }

    pub fn insert(&self, key: K, value: V, cs: &CsEBR) -> bool {
        let mut new_leaf = Rc::new(Node::new_leaf(Key::Fin(key.clone()), Some(value)));

        let mut new_internal = Rc::new(Node {
            key: Key::Inf, // temporary placeholder
            value: None,
            left: AtomicRc::null(),
            right: AtomicRc::null(),
        });

        loop {
            let record = self.seek(&key, cs);
            let new_internal_node = unsafe { new_internal.deref_mut() };

            let leaf_pos = match unsafe { record.leaf.deref() }.key.cmp(&key) {
                cmp::Ordering::Equal => {
                    unsafe { new_internal.into_inner() }.unwrap();
                    unsafe { new_leaf.into_inner() }.unwrap();
                    return false;
                }
                cmp::Ordering::Greater => {
                    new_internal_node.key = unsafe { record.leaf.deref().key.clone() };
                    new_internal_node
                        .left
                        .store(new_leaf, Ordering::Relaxed, cs);
                    new_internal_node
                        .right
                        .store(record.leaf.upgrade(), Ordering::Relaxed, cs);
                    Direction::R
                }
                cmp::Ordering::Less => {
                    new_internal_node.key = unsafe { new_leaf.deref().key.clone() };
                    new_internal_node
                        .left
                        .store(record.leaf.upgrade(), Ordering::Relaxed, cs);
                    new_internal_node
                        .right
                        .store(new_leaf, Ordering::Relaxed, cs);
                    Direction::L
                }
            };

            // NOTE: record.leaf_addr is called childAddr in the paper.
            match record.leaf_addr().compare_exchange(
                record.leaf.as_ptr(),
                new_internal,
                Ordering::AcqRel,
                Ordering::Acquire,
                cs,
            ) {
                Ok(_) => return true,
                Err(e) => {
                    // Insertion failed. Help the conflicting remove operation if needed.
                    // NOTE: The paper version checks if any of the mark is set, which is
                    // redundant.
                    new_internal = e.desired;
                    let new_internal_ref = unsafe { new_internal.deref() };

                    let new_leaf_link = match leaf_pos {
                        Direction::L => &new_internal_ref.right,
                        Direction::R => &new_internal_ref.left,
                    };

                    new_leaf = new_leaf_link.swap(Rc::null(), Ordering::Relaxed);

                    if e.current.with_tag(Marks::empty().bits()) == record.leaf.as_ptr() {
                        self.cleanup(&record, cs);
                    }
                }
            }
        }
    }

    pub fn remove(&self, key: &K, cs: &CsEBR) -> Option<Snapshot<Node<K, V>, CsEBR>> {
        // `leaf` and `value` are the snapshot of the node to be deleted.
        // NOTE: The paper version uses one big loop for both phases.
        // injection phase
        let leaf = loop {
            let record = self.seek(key, cs);

            // candidates
            let leaf_node = record.leaf.as_ref().unwrap();

            if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
                return None;
            }

            // Try injecting the deletion flag.
            match record.leaf_addr().compare_exchange_tag(
                &record.leaf,
                Marks::new(true, false).bits(),
                Ordering::AcqRel,
                Ordering::Acquire,
                cs,
            ) {
                Ok(_) => {
                    // Finalize the node to be removed
                    if self.cleanup(&record, cs) {
                        return Some(record.leaf);
                    }
                    // In-place cleanup failed. Enter the cleanup phase.
                    break record.leaf;
                }
                Err(e) => {
                    // Flagging failed.
                    // case 1. record.leaf_addr(e.current) points to another node: restart.
                    // case 2. Another thread flagged/tagged the edge to leaf: help and restart
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if record.leaf.as_ptr() == e.current.with_tag(Marks::empty().bits()) {
                        self.cleanup(&record, cs);
                    }
                }
            }
        };

        // cleanup phase
        loop {
            let next_record = self.seek(key, cs);
            if next_record.leaf != leaf {
                // The edge to leaf flagged for deletion was removed by a helping thread
                return Some(leaf);
            }

            // leaf is still present in the tree.
            if self.cleanup(&next_record, cs) {
                return Some(leaf);
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for NMTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Output = Snapshot<Node<K, V>, CsEBR>;

    fn new() -> Self {
        Self::new()
    }

    #[inline(always)]
    fn get(&self, key: &K, cs: &CsEBR) -> Option<Self::Output> {
        self.get(key, cs)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V, cs: &CsEBR) -> bool {
        self.insert(key, value, cs)
    }
    #[inline(always)]
    fn remove<'g>(&'g self, key: &K, cs: &'g CsEBR) -> Option<Self::Output> {
        self.remove(key, cs)
    }
}

#[cfg(test)]
mod tests {
    use super::NMTreeMap;
    use crate::ds_impl::circ_ebr::concurrent_map;

    #[test]
    fn smoke_nm_tree() {
        concurrent_map::tests::smoke::<NMTreeMap<i32, String>>();
    }
}
