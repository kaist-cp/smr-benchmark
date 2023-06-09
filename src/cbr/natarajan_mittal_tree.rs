use crossbeam_cbr::{
    AcquiredPtr, Atomic, Defender, EpochGuard, GeneralPtr, Rc, ReadStatus, Shared, Shield,
};

use std::fmt::{Debug, Display};
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
        self.0.leaf.as_ref().unwrap().value.as_ref().unwrap()
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
    curr: Shield<Node<K, V>>,
    /// The direction of curr from leaf.
    curr_dir: Direction,
    prev_tag: bool,
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
    /// The temporary protector of the next node of `leaf`.
    curr: Shared<'r, Node<K, V>>,
    /// The direction of curr from leaf.
    curr_dir: Direction,
    prev_tag: bool,
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
            curr: Shield::null(guard),
            curr_dir: Direction::L,
            prev_tag: false,
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
        self.curr.defend_unchecked(&read.curr);
        self.curr_dir = read.curr_dir;
        self.prev_tag = read.prev_tag;
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
            curr: self.curr.as_read(),
            curr_dir: self.curr_dir,
            prev_tag: self.prev_tag,
        }
    }

    #[inline]
    fn release(&mut self) {
        self.ancestor.release();
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
}

// COMMENT(@jeehoonkang): write down the invariant of the tree
struct NMTreeMap<K, V> {
    // It was `Node<K, V>` in EBR implementation,
    // but it will be difficult to initialize fake ancestor
    // with the previous definition.
    r: Atomic<Node<K, V>>,
}

impl<K, V> Default for NMTreeMap<K, V>
where
    K: Ord + Clone + Display + Debug + 'static,
    V: Clone + Display + Debug + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> NMTreeMap<K, V>
where
    K: Ord + Clone + Display + Debug + 'static,
    V: Clone + Display + Debug + 'static,
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
        // println!("seek start");
        let record = &mut handle.0;

        record.ancestor.defend(&self.r, guard);
        // Note: `ancester.left` never changes.
        record
            .successor
            .defend(&record.ancestor.as_ref().unwrap().left, guard);
        record
            .parent
            .defend(&record.ancestor.as_ref().unwrap().left, guard);
        record
            .leaf
            .defend(&record.parent.as_ref().unwrap().left, guard);
        record.successor_dir = Direction::L;
        record.leaf_dir = Direction::L;
        record.leaf.set_tag(0);

        record.prev_tag = Marks::from_bits_truncate(record.leaf.tag()).tag();
        record.curr_dir = Direction::L;
        record
            .curr
            .defend(&record.leaf.as_ref().unwrap().left, guard);

        while !record.curr.is_null() {
            // println!("curr: {:?}, {:?}", record.curr.as_ref().unwrap().key, record.curr.as_ref().unwrap().value);
            if !record.prev_tag {
                // untagged edge: advance ancestor and successor pointers
                record.ancestor.defend(&record.parent, guard);
                record.successor.defend(&record.leaf, guard);
                record.successor_dir = record.leaf_dir;
            }

            let curr_tag = record.curr.tag();

            // advance parent and leaf pointers
            swap(&mut record.parent, &mut record.leaf);
            record.curr.set_tag(0);
            swap(&mut record.leaf, &mut record.curr);
            record.leaf_dir = record.curr_dir;

            // update other variables
            record.prev_tag = Marks::from_bits_truncate(curr_tag).tag();
            let curr_node = record.leaf.as_ref().unwrap();
            if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                record.curr_dir = Direction::L;
                record.curr.defend(&curr_node.left, guard);
            } else {
                record.curr_dir = Direction::R;
                record.curr.defend(&curr_node.right, guard);
            }
        }
    }

    // All `Shared<_>` fields are unmarked.
    fn seek_read(&self, key: &K, handle: &mut Handle<K, V>, guard: &mut EpochGuard) {
        let record = &mut handle.0;

        guard.read(record, |guard| {
            let r = self.r.load(Ordering::Relaxed, guard);
            let s = r.as_ref(guard).unwrap().left.load(Ordering::Relaxed, guard);
            let s_node = s.as_ref(guard).unwrap();
            let leaf = s_node.left.load(Ordering::Relaxed, guard).with_tag(0);
            let leaf_node = leaf.as_ref(guard).unwrap();

            let mut record = ReadSeekRecord {
                ancestor: r,
                successor: s,
                successor_dir: Direction::L,
                parent: s,
                leaf,
                leaf_dir: Direction::L,
                curr: leaf_node.left.load(Ordering::Relaxed, guard),
                curr_dir: Direction::L,
                prev_tag: Marks::from_bits_truncate(leaf.tag()).tag(),
            };

            while let Some(curr_node) = record.curr.as_ref(guard) {
                if !record.prev_tag {
                    // untagged edge: advance ancestor and successor pointers
                    record.ancestor = record.parent;
                    record.successor = record.leaf;
                    record.successor_dir = record.leaf_dir;
                }

                // advance parent and leaf pointers
                record.parent = record.leaf;
                record.leaf = record.curr.with_tag(Marks::empty().bits());
                record.leaf_dir = record.curr_dir;

                // update other variables
                record.prev_tag = Marks::from_bits_truncate(record.curr.tag()).tag();
                if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                    record.curr_dir = Direction::L;
                    record.curr = curr_node.left.load(Ordering::Acquire, guard);
                } else {
                    record.curr_dir = Direction::R;
                    record.curr = curr_node.right.load(Ordering::Acquire, guard);
                }
            }

            record
        });
    }

    // All `Shared<_>` fields are unmarked.
    fn seek_read_loop(&self, key: &K, handle: &mut Handle<K, V>, guard: &mut EpochGuard) {
        guard.read_loop(
            &mut handle.0,
            &mut handle.1,
            |guard| {
                let r = self.r.load(Ordering::Relaxed, guard);
                let s = r.as_ref(guard).unwrap().left.load(Ordering::Relaxed, guard);
                let s_node = s.as_ref(guard).unwrap();
                let leaf = s_node.left.load(Ordering::Relaxed, guard).with_tag(0);
                let leaf_node = leaf.as_ref(guard).unwrap();

                ReadSeekRecord {
                    ancestor: r,
                    successor: s,
                    successor_dir: Direction::L,
                    parent: s,
                    leaf,
                    leaf_dir: Direction::L,
                    curr: leaf_node.left.load(Ordering::Relaxed, guard),
                    curr_dir: Direction::L,
                    prev_tag: Marks::from_bits_truncate(leaf.tag()).tag(),
                }
            },
            |record, guard| {
                if let Some(curr_node) = record.curr.as_ref(guard) {
                    if !record.prev_tag {
                        // untagged edge: advance ancestor and successor pointers
                        record.ancestor = record.parent;
                        record.successor = record.leaf;
                        record.successor_dir = record.leaf_dir;
                    }

                    // advance parent and leaf pointers
                    record.parent = record.leaf;
                    record.leaf = record.curr.with_tag(Marks::empty().bits());
                    record.leaf_dir = record.curr_dir;

                    // update other variables
                    record.prev_tag = Marks::from_bits_truncate(record.curr.tag()).tag();
                    if curr_node.key.cmp(key) == cmp::Ordering::Greater {
                        record.curr_dir = Direction::L;
                        record.curr = curr_node.left.load(Ordering::Acquire, guard);
                    } else {
                        record.curr_dir = Direction::R;
                        record.curr = curr_node.right.load(Ordering::Acquire, guard);
                    }
                    return ReadStatus::Continue;
                }

                ReadStatus::Finished
            },
        );
    }

    /// Physically removes node.
    ///
    /// Returns true if it successfully unlinks the flagged node in `record`.
    fn cleanup(&self, handle: &mut Handle<K, V>, guard: &mut EpochGuard) -> bool {
        let record = &mut handle.0;
        // Identify the node(subtree) that will replace `successor`.
        let leaf_marked = record.leaf_addr().load(Ordering::Acquire, guard);
        let leaf_flag = Marks::from_bits_truncate(leaf_marked.tag()).flag();

        let target_sibling_addr = if leaf_flag {
            match record.leaf_dir {
                Direction::L => &record.parent.as_ref().unwrap().right,
                Direction::R => &record.parent.as_ref().unwrap().left,
            }
        } else {
            match record.leaf_dir {
                Direction::L => &record.parent.as_ref().unwrap().left,
                Direction::R => &record.parent.as_ref().unwrap().right,
            }
        };

        // NOTE: the ibr implementation uses CAS
        // tag (parent, sibling) edge -> all of the parent's edges can't change now
        // TODO: Is Release enough?
        target_sibling_addr.fetch_or(Marks::TAG.bits(), Ordering::AcqRel, guard);

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        record.curr.defend(target_sibling_addr, guard);
        let flag = Marks::from_bits_truncate(record.curr.tag()).flag();
        record
            .successor_addr()
            .compare_exchange(
                record.successor.shared(),
                record.curr.with_tag(Marks::new(flag, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            )
            .is_ok()
    }

    pub fn get<F>(
        &self,
        find: F,
        key: &K,
        handle: &mut Handle<K, V>,
        guard: &mut EpochGuard,
    ) -> bool
    where
        F: Fn(&NMTreeMap<K, V>, &K, &mut Handle<K, V>, &mut EpochGuard),
    {
        find(self, key, handle, guard);
        handle.0.leaf.as_ref().unwrap().key.cmp(key) == cmp::Ordering::Equal
    }

    pub fn insert<F>(
        &self,
        find: F,
        key: K,
        value: V,
        handle: &mut Handle<K, V>,
        guard: &mut EpochGuard,
    ) -> bool
    where
        F: Fn(&NMTreeMap<K, V>, &K, &mut Handle<K, V>, &mut EpochGuard),
    {
        let mut new_leaf = Rc::new(Node::new_leaf(Key::Fin(key.clone()), Some(value)), guard);

        let mut new_internal = Rc::new(
            Node {
                key: Key::Inf, // temporary placeholder
                value: None,
                left: Atomic::null(),
                right: Atomic::null(),
            },
            guard,
        );

        loop {
            find(self, &key, handle, guard);

            let new_leaf_dir = {
                let record = &handle.0;
                let new_internal_node = new_internal.as_mut().unwrap();

                match record.leaf.as_ref().unwrap().key.cmp(&key) {
                    cmp::Ordering::Equal => return false,
                    cmp::Ordering::Greater => {
                        new_internal_node.key = record.leaf.as_ref().unwrap().key.clone();
                        new_internal_node
                            .left
                            .swap(new_leaf, Ordering::Relaxed, guard);
                        new_internal_node.right.swap(
                            record.leaf.to_rc(guard),
                            Ordering::Relaxed,
                            guard,
                        );
                        Direction::L
                    }
                    cmp::Ordering::Less => {
                        new_internal_node.key = new_leaf.as_ref().unwrap().key.clone();
                        new_internal_node.left.swap(
                            record.leaf.to_rc(guard),
                            Ordering::Relaxed,
                            guard,
                        );
                        new_internal_node
                            .right
                            .swap(new_leaf, Ordering::Relaxed, guard);
                        Direction::R
                    }
                }
            };

            let cas_result = {
                let record = &handle.0;
                // NOTE: record.leaf_addr is called childAddr in the paper.
                match record.leaf_addr().compare_exchange(
                    record.leaf.shared(),
                    new_internal,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    guard,
                ) {
                    Ok(_) => Ok(()),
                    Err(e) => Err((
                        e.actual.with_tag(Marks::empty().bits()) == record.leaf.shared(),
                        e.desired,
                    )),
                }
            };

            match cas_result {
                Ok(_) => return true,
                Err((helped, desired)) => {
                    // Insertion failed. Help the conflicting remove operation if needed.
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if helped {
                        self.cleanup(handle, guard);
                    }
                    new_internal = desired;
                    new_leaf = match new_leaf_dir {
                        Direction::L => &new_internal.as_ref().unwrap().left,
                        Direction::R => &new_internal.as_ref().unwrap().right,
                    }
                    .swap(Rc::null(), Ordering::Relaxed, guard);
                }
            }
        }
    }

    pub fn remove<F>(
        &self,
        find: F,
        key: &K,
        handle: &mut Handle<K, V>,
        guard: &mut EpochGuard,
    ) -> bool
    where
        F: Fn(&NMTreeMap<K, V>, &K, &mut Handle<K, V>, &mut EpochGuard),
    {
        // `leaf` and `value` are the snapshot of the node to be deleted.
        // NOTE: The paper version uses one big loop for both phases.
        // injection phase
        let leaf = loop {
            find(self, key, handle, guard);

            let cas_result = {
                let record = &handle.0;
                let leaf_node = record.leaf.as_ref().unwrap();

                if leaf_node.key.cmp(key) != cmp::Ordering::Equal {
                    return false;
                }

                // Try injecting the deletion flag.
                match record.leaf_addr().compare_exchange(
                    record.leaf.shared(),
                    record.leaf.with_tag(Marks::new(true, false).bits()),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    guard,
                ) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(record.leaf.shared() == e.actual.with_tag(Marks::empty().bits())),
                }
            };

            match cas_result {
                Ok(_) => {
                    // Finalize the node to be removed
                    if self.cleanup(handle, guard) {
                        return true;
                    }
                    // In-place cleanup failed. Enter the cleanup phase.
                    break handle.0.leaf.as_raw();
                }
                Err(flagged) => {
                    if flagged {
                        // Flagging failed.
                        // case 1. record.leaf_addr(e.current) points to another node: restart.
                        // case 2. Another thread flagged/tagged the edge to leaf: help and restart
                        // NOTE: The paper version checks if any of the mark is set, which is redundant.
                        self.cleanup(handle, guard);
                    }
                }
            }
        };

        // cleanup phase
        loop {
            find(self, key, handle, guard);
            let record = &mut handle.0;

            if record.leaf.as_raw() != leaf {
                // The edge to leaf flagged for deletion was removed by a helping thread
                return true;
            }

            // leaf is still present in the tree.
            if self.cleanup(handle, guard) {
                return true;
            }
        }
    }
}

pub mod naive {
    use std::fmt::{Debug, Display};

    use crate::cbr::ConcurrentMap;
    use crossbeam_cbr::EpochGuard;

    use super::Handle;

    pub struct NMTreeMap<K, V> {
        inner: super::NMTreeMap<K, V>,
    }

    impl<K, V> ConcurrentMap<K, V> for NMTreeMap<K, V>
    where
        K: Clone + Ord + Display + Debug + 'static,
        V: Clone + Display + Debug + 'static,
    {
        type Handle = Handle<K, V>;

        fn new() -> Self {
            Self {
                inner: super::NMTreeMap::new(),
            }
        }

        fn get(&self, key: &K, handle: &mut Self::Handle, guard: &mut EpochGuard) -> bool {
            self.inner
                .get(super::NMTreeMap::seek_naive, key, handle, guard)
        }

        fn insert(
            &self,
            key: K,
            value: V,
            handle: &mut Self::Handle,
            guard: &mut EpochGuard,
        ) -> bool {
            self.inner
                .insert(super::NMTreeMap::seek_naive, key, value, handle, guard)
        }

        fn remove(&self, key: &K, handle: &mut Self::Handle, guard: &mut EpochGuard) -> bool {
            self.inner
                .remove(super::NMTreeMap::seek_naive, key, handle, guard)
        }
    }

    #[test]
    fn smoke_nm_tree() {
        crate::cbr::concurrent_map::tests::smoke::<NMTreeMap<i32, String>>();
    }

    #[test]
    fn simple() {
        use crate::cbr::concurrent_map::Shields;

        let map = NMTreeMap::new();
        let mut handle = Handle::<i32, i32>::default(&crossbeam_cbr::pin());
        assert!(map.insert(1, 1, &mut handle, &mut crossbeam_cbr::pin()));
        assert!(map.insert(2, 2, &mut handle, &mut crossbeam_cbr::pin()));
        assert!(!map.insert(1, 1, &mut handle, &mut crossbeam_cbr::pin()));
    }
}

pub mod read {
    use std::fmt::{Debug, Display};

    use crate::cbr::ConcurrentMap;
    use crossbeam_cbr::EpochGuard;

    use super::Handle;

    pub struct NMTreeMap<K, V> {
        inner: super::NMTreeMap<K, V>,
    }

    impl<K, V> ConcurrentMap<K, V> for NMTreeMap<K, V>
    where
        K: Clone + Ord + Display + Debug + 'static,
        V: Clone + Display + Debug + 'static,
    {
        type Handle = Handle<K, V>;

        fn new() -> Self {
            Self {
                inner: super::NMTreeMap::new(),
            }
        }

        fn get(&self, key: &K, handle: &mut Self::Handle, guard: &mut EpochGuard) -> bool {
            self.inner
                .get(super::NMTreeMap::seek_read, key, handle, guard)
        }

        fn insert(
            &self,
            key: K,
            value: V,
            handle: &mut Self::Handle,
            guard: &mut EpochGuard,
        ) -> bool {
            self.inner
                .insert(super::NMTreeMap::seek_read, key, value, handle, guard)
        }

        fn remove(&self, key: &K, handle: &mut Self::Handle, guard: &mut EpochGuard) -> bool {
            self.inner
                .remove(super::NMTreeMap::seek_read, key, handle, guard)
        }
    }

    #[test]
    fn smoke_nm_tree() {
        crate::cbr::concurrent_map::tests::smoke::<NMTreeMap<i32, String>>();
    }

    #[test]
    fn simple() {
        use crate::cbr::concurrent_map::Shields;

        let map = NMTreeMap::new();
        let mut handle = Handle::<i32, i32>::default(&crossbeam_cbr::pin());
        assert!(map.insert(1, 1, &mut handle, &mut crossbeam_cbr::pin()));
        assert!(map.insert(2, 2, &mut handle, &mut crossbeam_cbr::pin()));
        assert!(!map.insert(1, 1, &mut handle, &mut crossbeam_cbr::pin()));
    }
}

pub mod read_loop {
    use std::fmt::{Debug, Display};

    use crate::cbr::ConcurrentMap;
    use crossbeam_cbr::EpochGuard;

    use super::Handle;

    pub struct NMTreeMap<K, V> {
        inner: super::NMTreeMap<K, V>,
    }

    impl<K, V> ConcurrentMap<K, V> for NMTreeMap<K, V>
    where
        K: Clone + Ord + Display + Debug + 'static,
        V: Clone + Display + Debug + 'static,
    {
        type Handle = Handle<K, V>;

        fn new() -> Self {
            Self {
                inner: super::NMTreeMap::new(),
            }
        }

        fn get(&self, key: &K, handle: &mut Self::Handle, guard: &mut EpochGuard) -> bool {
            self.inner
                .get(super::NMTreeMap::seek_read_loop, key, handle, guard)
        }

        fn insert(
            &self,
            key: K,
            value: V,
            handle: &mut Self::Handle,
            guard: &mut EpochGuard,
        ) -> bool {
            self.inner
                .insert(super::NMTreeMap::seek_read_loop, key, value, handle, guard)
        }

        fn remove(&self, key: &K, handle: &mut Self::Handle, guard: &mut EpochGuard) -> bool {
            self.inner
                .remove(super::NMTreeMap::seek_read_loop, key, handle, guard)
        }
    }

    #[test]
    fn smoke_nm_tree() {
        crate::cbr::concurrent_map::tests::smoke::<NMTreeMap<i32, String>>();
    }

    #[test]
    fn simple() {
        use crate::cbr::concurrent_map::Shields;

        let map = NMTreeMap::new();
        let mut handle = Handle::<i32, i32>::default(&crossbeam_cbr::pin());
        assert!(map.insert(1, 1, &mut handle, &mut crossbeam_cbr::pin()));
        assert!(map.insert(2, 2, &mut handle, &mut crossbeam_cbr::pin()));
        assert!(!map.insert(1, 1, &mut handle, &mut crossbeam_cbr::pin()));
    }
}
