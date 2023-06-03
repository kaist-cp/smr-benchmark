use crossbeam_cbr::{
    rc::{AcquiredPtr, Atomic, Defender, Rc, Shared, Shield},
    EpochGuard, ReadGuard, ReadStatus, WriteResult,
};

use std::sync::atomic::Ordering;
use std::{cmp, mem::swap};

use super::concurrent_map::Shields;

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

pub struct Handle<K, V>(SeekRecord<K, V>, SeekRecord<K, V>);

impl<K, V> Shields<V> for Handle<K, V>
where
    K: 'static,
    V: 'static,
{
    #[inline]
    fn default(guard: &EpochGuard) -> Self {
        Self(SeekRecord::default(guard), SeekRecord::default(guard))
    }

    #[inline]
    fn result_value(&self) -> &V {
        todo!()
    }

    #[inline]
    fn release(&mut self) {
        self.0.release();
        self.1.release();
    }
}

/// All Shared<_> are unmarked.
///
/// All of the edges of path from `successor` to `parent` are in the process of removal.
///
/// TODO(@jeonghyeon): implement `#[derive(Defender)]`,
/// so that `ReadCursor` and the trait implementation
/// is generated automatically.
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
    /// The temporary protector of the next node of `leaf`.
    next: Shield<Node<K, V>>,
}

/// This struct definition must be generated automatically by a `derive` macro.
struct ReadSeekRecord<'r, K, V> {
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

impl<'r, K, V> Clone for ReadSeekRecord<'r, K, V> {
    fn clone(&self) -> Self {
        Self { ..*self }
    }
}

impl<'r, K, V> Copy for ReadSeekRecord<'r, K, V> {}

/// This trait implementation must be generated automatically by a `derive` macro.
impl<K: 'static, V: 'static> Defender for SeekRecord<K, V> {
    type Read<'r> = ReadSeekRecord<'r, K, V>;

    #[inline]
    fn default(guard: &EpochGuard) -> Self {
        Self {
            ancestor: Shield::null(guard),
            successor: Shield::null(guard),
            successor_dir: Direction::L,
            parent: Shield::null(guard),
            leaf: Shield::null(guard),
            leaf_dir: Direction::L,
            next: Shield::null(guard),
        }
    }

    #[inline]
    unsafe fn defend_unchecked(&mut self, read: &Self::Read<'_>) {
        self.ancestor.defend_unchecked(&read.ancestor);
        self.successor.defend_unchecked(&read.successor);
        self.successor_dir = read.successor_dir;
        self.parent.defend_unchecked(&read.parent);
        self.leaf.defend_unchecked(&read.leaf);
        self.leaf_dir = read.leaf_dir;
    }

    #[inline]
    unsafe fn as_read<'r>(&mut self) -> Self::Read<'r> {
        ReadSeekRecord {
            ancestor: self.ancestor.as_read(),
            successor: self.successor.as_read(),
            successor_dir: self.successor_dir,
            parent: self.parent.as_read(),
            leaf: self.leaf.as_read(),
            leaf_dir: self.leaf_dir,
        }
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
    fn successor_addr(&self) -> &Atomic<Node<K, V>> {
        match self.successor_dir {
            Direction::L => &self.ancestor.as_ref().unwrap().left,
            Direction::R => &self.ancestor.as_ref().unwrap().right,
        }
    }

    fn leaf_addr(&self) -> &Atomic<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => &self.parent.as_ref().unwrap().left,
            Direction::R => &self.parent.as_ref().unwrap().right,
        }
    }

    fn leaf_sibling_addr(&self) -> &Atomic<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => &self.parent.as_ref().unwrap().right,
            Direction::R => &self.parent.as_ref().unwrap().left,
        }
    }
}

// COMMENT(@jeehoonkang): write down the invariant of the tree
pub struct NMTreeMap<K, V> {
    // It was `Node<K, V>` in EBR implementation,
    // but it will be difficult to initialize fake ancestor
    // with the previous definition.
    r: Atomic<Node<K, V>>,
}

impl<K, V> Default for NMTreeMap<K, V>
where
    K: Ord + Clone + 'static,
    V: Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> NMTreeMap<K, V>
where
    K: Ord + Clone + 'static,
    V: Clone + 'static,
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
        NMTreeMap { r: Atomic::new(r) }
    }

    // All `Shared<_>` fields are unmarked.
    fn seek_naive(&self, key: &K, handle: &mut Handle<K, V>, guard: &mut EpochGuard) {
        let record = &mut handle.0;

        self.r.defend_with(&mut record.ancestor, guard);
        record
            .ancestor
            .as_ref()
            .unwrap()
            .left
            .defend_with(&mut record.successor, guard);
        record
            .successor
            .as_ref()
            .unwrap()
            .left
            .defend_with(&mut record.leaf, guard);

        let mut prev_tag = Marks::from_bits_truncate(record.leaf.tag()).tag();
        let mut curr_dir = Direction::L;
        record
            .leaf
            .as_ref()
            .unwrap()
            .left
            .defend_with(&mut record.next, guard);
        record.leaf.set_tag(0);

        while !record.next.is_null() {
            if !prev_tag {
                // untagged edge: advance ancestor and successor pointers
                record.parent.copy_to(&mut record.ancestor, guard);
                record.leaf.copy_to(&mut record.successor, guard);
                record.successor_dir = record.leaf_dir;
            }

            // advance parent and leaf pointers
            let curr_tag = record.next.tag();
            record.next.set_tag(0);
            swap(&mut record.parent, &mut record.leaf);
            swap(&mut record.leaf, &mut record.next);
            record.leaf_dir = curr_dir;

            // update other variables
            prev_tag = Marks::from_bits_truncate(curr_tag).tag();
            let curr_node = record.leaf.as_ref().unwrap();
            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                curr_dir = Direction::L;
                curr_node.left.defend_with(&mut record.next, guard);
            } else {
                curr_dir = Direction::R;
                curr_node.right.defend_with(&mut record.next, guard);
            }
        }
    }

    /// Similar to `seek`, but traverse the tree with only two pointers
    fn seek_leaf_naive(&self, key: &K, handle: &mut Handle<K, V>, guard: &mut EpochGuard) {
        todo!();
    }
}
