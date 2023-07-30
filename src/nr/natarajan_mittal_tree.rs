use super::compose_tag;
use super::{concurrent_map::ConcurrentMap, tag, untagged};
use std::cmp;
use std::mem::transmute;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, Ordering};

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
            left: AtomicPtr::new(null_mut()),
            right: AtomicPtr::new(null_mut()),
        }
    }

    /// Make a new internal node, consuming the given left and right nodes,
    /// using the right node's key.
    fn new_internal(left: Node<K, V>, right: Node<K, V>) -> Node<K, V> {
        Node {
            key: right.key.clone(),
            value: None,
            left: AtomicPtr::new(Box::into_raw(Box::new(left))),
            right: AtomicPtr::new(Box::into_raw(Box::new(right))),
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
struct SeekRecord<K, V> {
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
}

// TODO(@jeehoonkang): code duplication...
impl<K, V> SeekRecord<K, V> {
    fn successor_addr(&self) -> &AtomicPtr<Node<K, V>> {
        match self.successor_dir {
            Direction::L => &unsafe { &*untagged(self.ancestor) }.left,
            Direction::R => &unsafe { &*untagged(self.ancestor) }.right,
        }
    }

    fn leaf_addr(&self) -> &AtomicPtr<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => &unsafe { &*untagged(self.parent) }.left,
            Direction::R => &unsafe { &*untagged(self.parent) }.right,
        }
    }

    fn leaf_sibling_addr(&self) -> &AtomicPtr<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => &unsafe { &*untagged(self.parent) }.right,
            Direction::R => &unsafe { &*untagged(self.parent) }.left,
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
    fn seek(&self, key: &K) -> SeekRecord<K, V> {
        let s = self.r.left.load(Ordering::Relaxed);
        let s_node = unsafe { &*untagged(s) };
        let leaf = untagged(s_node.left.load(Ordering::Relaxed));
        let leaf_node = unsafe { &*untagged(leaf) };

        let mut record = SeekRecord {
            ancestor: &self.r as *const _ as *mut Node<K, V>,
            successor: s,
            successor_dir: Direction::L,
            parent: s,
            leaf,
            leaf_dir: Direction::L,
        };

        let mut prev_tag = Marks::from_bits_truncate(tag(leaf)).tag();
        let mut curr_dir = Direction::L;
        let mut curr = leaf_node.left.load(Ordering::Relaxed);

        while let Some(curr_node) = unsafe { untagged(curr).as_ref() } {
            if !prev_tag {
                // untagged edge: advance ancestor and successor pointers
                record.ancestor = record.parent;
                record.successor = record.leaf;
                record.successor_dir = record.leaf_dir;
            }

            // advance parent and leaf pointers
            record.parent = record.leaf;
            record.leaf = untagged(curr);
            record.leaf_dir = curr_dir;

            // update other variables
            prev_tag = Marks::from_bits_truncate(tag(curr)).tag();
            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                curr_dir = Direction::L;
                curr = curr_node.left.load(Ordering::Acquire);
            } else {
                curr_dir = Direction::R;
                curr = curr_node.right.load(Ordering::Acquire);
            }
        }

        record
    }

    /// Similar to `seek`, but traverse the tree with only two pointers
    fn seek_leaf(&self, key: &K) -> SeekRecord<K, V> {
        let s = self.r.left.load(Ordering::Relaxed);
        let s_node = unsafe { &*untagged(s) };
        let leaf = untagged(s_node.left.load(Ordering::Acquire));

        let mut record = SeekRecord {
            ancestor: null_mut(),
            successor: null_mut(),
            successor_dir: Direction::L,
            parent: s,
            leaf,
            leaf_dir: Direction::L,
        };

        let mut curr = untagged(
            unsafe { &*untagged(record.leaf) }
                .left
                .load(Ordering::Acquire),
        );

        while let Some(curr_node) = unsafe { curr.as_ref() } {
            record.leaf = curr;

            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                curr = curr_node.left.load(Ordering::Acquire);
            } else {
                curr = curr_node.right.load(Ordering::Acquire);
            }
            curr = untagged(curr);
        }

        record
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
        record
            .successor_addr()
            .compare_exchange(
                record.successor,
                compose_tag(target_sibling, Marks::new(flag, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn get(&self, key: &K) -> Option<&'static V> {
        let record = self.seek_leaf(key);
        let leaf_node = unsafe { &*untagged(record.leaf) };

        if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
            return None;
        }

        Some(leaf_node.value.as_ref().unwrap())
    }

    pub fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        let new_leaf = Box::into_raw(Box::new(Node::new_leaf(Key::Fin(key.clone()), Some(value))));

        let new_internal = Box::into_raw(Box::new(Node {
            key: Key::Inf, // temporary placeholder
            value: None,
            left: AtomicPtr::new(null_mut()),
            right: AtomicPtr::new(null_mut()),
        }));

        loop {
            let record = self.seek(&key);
            let leaf = record.leaf;

            let (new_left, new_right) = match unsafe { &*leaf }.key.cmp(&key) {
                cmp::Ordering::Equal => unsafe {
                    // Newly created nodes that failed to be inserted are free'd here.
                    let value = (&mut *new_leaf).value.take().unwrap();
                    drop(Box::from_raw(new_leaf));
                    drop(Box::from_raw(new_internal));
                    return Err((key, value));
                },
                cmp::Ordering::Greater => (new_leaf, leaf),
                cmp::Ordering::Less => (leaf, new_leaf),
            };

            let new_internal_node = unsafe { &mut *new_internal };
            new_internal_node.key = unsafe { (&*new_right).key.clone() };
            new_internal_node.left.store(new_left, Ordering::Relaxed);
            new_internal_node.right.store(new_right, Ordering::Relaxed);

            // NOTE: record.leaf_addr is called childAddr in the paper.
            match record.leaf_addr().compare_exchange(
                record.leaf,
                new_internal,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    // Insertion failed. Help the conflicting remove operation if needed.
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if untagged(e) == record.leaf {
                        self.cleanup(&record);
                    }
                }
            }
        }
    }

    pub fn remove(&self, key: &K) -> Option<&'static V> {
        let mut record;
        // `leaf` and `value` are the snapshot of the node to be deleted.
        // NOTE: The paper version uses one big loop for both phases.
        // injection phase
        let (leaf, value) = loop {
            record = self.seek(key);

            // candidates
            let leaf = record.leaf;
            let leaf_node = unsafe { record.leaf.as_ref().unwrap() };

            if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
                return None;
            }

            let value = unsafe { transmute(leaf_node.value.as_ref().unwrap()) };

            // Try injecting the deletion flag.
            match record.leaf_addr().compare_exchange(
                record.leaf,
                compose_tag(record.leaf, Marks::new(true, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Finalize the node to be removed
                    if self.cleanup(&record) {
                        return Some(value);
                    }
                    // In-place cleanup failed. Enter the cleanup phase.
                    break (leaf, value);
                }
                Err(e) => {
                    // Flagging failed.
                    // case 1. record.leaf_addr(e.current) points to another node: restart.
                    // case 2. Another thread flagged/tagged the edge to leaf: help and restart
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if record.leaf == untagged(e) {
                        self.cleanup(&record);
                    }
                }
            }
        };

        // cleanup phase
        loop {
            record = self.seek(key);
            if record.leaf != leaf {
                // The edge to leaf flagged for deletion was removed by a helping thread
                return Some(value);
            }

            // leaf is still present in the tree.
            if self.cleanup(&record) {
                return Some(value);
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for NMTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new() -> Self {
        Self::new()
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<&'static V> {
        self.get(key)
    }
    #[inline(always)]
    fn insert(&self, key: K, value: V) -> bool {
        self.insert(key, value).is_ok()
    }
    #[inline(always)]
    fn remove(&self, key: &K) -> Option<&'static V> {
        self.remove(key)
    }
}

#[cfg(test)]
mod tests {
    use super::NMTreeMap;
    use crate::nr::concurrent_map;

    #[test]
    fn smoke_nm_tree() {
        concurrent_map::tests::smoke::<NMTreeMap<i32, String>>();
    }
}
