use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Shared};

use std::mem::ManuallyDrop;
use std::ptr;
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
    Inf0, // TODO(@jeehoonkang): is it really necessary to have multiple types of Inf*?
    Inf1,
    Inf2,
}

impl<K> PartialOrd for Key<K>
where
    K: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Key::Fin(k1), Key::Fin(k2)) => k1.partial_cmp(k2),
            (Key::Fin(_), _) => Some(std::cmp::Ordering::Less),

            (Key::Inf0, Key::Fin(_)) => Some(std::cmp::Ordering::Greater),
            (Key::Inf0, Key::Inf0) => Some(std::cmp::Ordering::Equal),
            (Key::Inf0, _) => Some(std::cmp::Ordering::Less),

            (Key::Inf1, Key::Inf2) => Some(std::cmp::Ordering::Less),
            (Key::Inf1, Key::Inf1) => Some(std::cmp::Ordering::Equal),
            (Key::Inf1, _) => Some(std::cmp::Ordering::Greater),

            (Key::Inf2, Key::Inf2) => Some(std::cmp::Ordering::Equal),
            (Key::Inf2, _) => Some(std::cmp::Ordering::Greater),
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

#[derive(Debug)]
struct Node<K, V> {
    key: Key<K>,
    // TODO(@jeehoonkang): how about having another type that is either (1) value, or (2) left and
    // right.
    value: Option<ManuallyDrop<V>>,
    left: Atomic<Node<K, V>>,
    right: Atomic<Node<K, V>>,
}

impl<K, V> Node<K, V>
where
    K: Clone,
{
    fn new_leaf(key: Key<K>, value: Option<V>) -> Node<K, V> {
        Node {
            key,
            value: value.map(ManuallyDrop::new),
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

/// All Shared<_> are unmarked.
///
/// All of the edges of path from `successor` to `parent` are in the process of removal.
struct SeekRecord<'g, K, V> {
    /// Parent of `successor`
    ancestor: Shared<'g, Node<K, V>>,
    /// The first internal node with a marked outgoing edge
    successor: Shared<'g, Node<K, V>>,
    /// The pointer from `ancestor` to `successor`. This field is not presented in the paper, but
    /// added for convenience.
    successor_addr: &'g Atomic<Node<K, V>>,
    /// Parent of `leaf`
    parent: Shared<'g, Node<K, V>>,
    /// The end of the access path.
    leaf: Shared<'g, Node<K, V>>,
    /// The pointer from `parent` to `leaf`. This field is not presented in the paper, but
    /// added for convenience.
    leaf_addr: &'g Atomic<Node<K, V>>,
    /// The pointer from `parent` to the sibling of `leaf`. This field is not presented in the
    /// paper, but added for convenience.
    sibling_addr: &'g Atomic<Node<K, V>>,
}

// COMMENT(@jeehoonkang): write down the invariant of the tree
pub struct NMTreeMap<K, V> {
    r: Node<K, V>,
}

impl<K, V> Default for NMTreeMap<K, V>
where
    K: Ord + Clone,
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

                if let Some(value) = node_ref.value.as_mut() {
                    ManuallyDrop::drop(value);
                }

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
{
    pub fn new() -> Self {
        // An empty tree has 5 default nodes with infinite keys so that the SeekRecord is allways
        // well-defined.
        //         r(inf2)
        //          /  \
        //     s(inf1)  inf2
        //       / \
        //   inf0   inf1
        let inf0 = Node::new_leaf(Key::Inf0, None);
        let inf1 = Node::new_leaf(Key::Inf1, None);
        let inf2 = Node::new_leaf(Key::Inf2, None);
        let s = Node::new_internal(inf0, inf1);
        let r = Node::new_internal(s, inf2);
        NMTreeMap { r }
    }

    // All `Shared<_>` fields are unmarked.
    fn seek<'g>(&'g self, key: &K, guard: &'g Guard) -> SeekRecord<'g, K, V> {
        let s = self.r.left.load(Ordering::Relaxed, guard);
        let s_node = unsafe { s.deref() };
        let leaf = s_node
            .left
            .load(Ordering::Relaxed, guard)
            .with_tag(Marks::empty().bits());
        let leaf_node = unsafe { leaf.deref() };

        let mut record = SeekRecord {
            ancestor: Shared::from(&self.r as *const _),
            successor: s,
            successor_addr: &self.r.left,
            parent: s,
            leaf,
            leaf_addr: &s_node.left,
            sibling_addr: &s_node.right,
        };

        let mut prev_marked = leaf;
        let mut curr_addr = &leaf_node.left;
        let mut curr_marked = leaf_node.left.load(Ordering::Relaxed, guard);
        let mut curr = curr_marked.with_tag(Marks::empty().bits());
        let mut curr_sibling_addr = &leaf_node.right;

        while let Some(curr_node) = unsafe { curr.as_ref() } {
            let tag = Marks::from_bits_truncate(prev_marked.tag()).tag();
            if !tag {
                // untagged edge: advance ancestor and successor pointers
                record.ancestor = record.parent;
                record.successor = record.leaf;
                record.successor_addr = record.leaf_addr;
            }

            // advance parent and leaf pointers
            record.parent = record.leaf;
            record.leaf = curr;
            record.leaf_addr = curr_addr;
            record.sibling_addr = curr_sibling_addr;

            // update other variables
            prev_marked = curr_marked;
            curr_marked = if curr_node.key > *key {
                curr_addr = &curr_node.left;
                curr_sibling_addr = &curr_node.right;
                curr_node.left.load(Ordering::Acquire, guard)
            } else {
                curr_addr = &curr_node.right;
                curr_sibling_addr = &curr_node.left;
                curr_node.right.load(Ordering::Acquire, guard)
            };
            curr = curr_marked.with_tag(Marks::empty().bits());
        }

        record
    }

    /// Physically removes node.
    ///
    /// Returns true if it successfully unlinks the flagged node in `record`.
    fn cleanup(&self, record: &SeekRecord<'_, K, V>, guard: &Guard) -> bool {
        // Identify the node(subtree) that will replace `successor`.
        let leaf_marked = record.leaf_addr.load(Ordering::Acquire, guard);
        let leaf_flag = Marks::from_bits_truncate(leaf_marked.tag()).flag();
        let target_sibling_addr = if leaf_flag {
            record.sibling_addr
        } else {
            record.leaf_addr
        };

        // NOTE: the ibr implementation uses CAS
        // tag (parent, sibling) edge -> all of the parent's edges can't change now
        // TODO: Is Release enough?
        target_sibling_addr.fetch_or(1, Ordering::AcqRel, guard);

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        let target_sibling = target_sibling_addr.load(Ordering::Acquire, guard);
        let flag = Marks::from_bits_truncate(target_sibling.tag()).flag();
        let is_unlinked = record
            .successor_addr
            .compare_and_set(
                record.successor,
                target_sibling.with_tag(Marks::new(flag, false).bits()),
                Ordering::AcqRel,
                &guard,
            )
            .is_ok();

        if is_unlinked {
            unsafe {
                // destroy the subtree of successor except target_sibling
                let mut stack = vec![record.successor];

                while let Some(mut node) = stack.pop() {
                    if node.is_null()
                        || (node.with_tag(Marks::empty().bits())
                            == target_sibling.with_tag(Marks::empty().bits()))
                    {
                        continue;
                    }

                    let node_ref = node.deref_mut();

                    if let Some(value) = node_ref.value.as_mut() {
                        ManuallyDrop::drop(value);
                    }

                    stack.push(node_ref.left.load(Ordering::Relaxed, guard));
                    stack.push(node_ref.right.load(Ordering::Relaxed, guard));
                    guard.defer_destroy(node);
                }
            }
        }

        is_unlinked
    }

    pub fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        let record = self.seek(key, guard);
        let leaf_node = unsafe { record.leaf.as_ref()? };

        if leaf_node.key != *key {
            return None;
        }

        Some(leaf_node.value.as_ref().unwrap())
    }

    pub fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        let new_leaf = Owned::new(Node::new_leaf(Key::Fin(key.clone()), Some(value)))
            .into_shared(unsafe { unprotected() });

        let mut new_internal = Owned::new(Node {
            key: Key::Inf2, // temporary placeholder
            value: None,
            left: Atomic::null(),
            right: Atomic::null(),
        })
        .into_shared(unsafe { unprotected() });

        loop {
            let record = self.seek(&key, guard);
            let leaf = record.leaf;
            let leaf_node = unsafe { leaf.deref() };

            if leaf_node.key == key {
                // Newly created nodes that failed to be inserted are free'd here.
                unsafe {
                    drop(new_leaf.into_owned());
                    drop(new_internal.into_owned());
                }
                return false;
            }

            let (new_left, new_right) = if leaf_node.key > key {
                (new_leaf, leaf)
            } else {
                (leaf, new_leaf)
            };

            let new_internal_node = unsafe { new_internal.deref_mut() };
            new_internal_node.key = unsafe { new_right.deref().key.clone() };
            new_internal_node.left.store(new_left, Ordering::Relaxed);
            new_internal_node.right.store(new_right, Ordering::Relaxed);

            // NOTE: record.leaf_addr is called childAddr in the paper.
            match record.leaf_addr.compare_and_set(
                record.leaf,
                new_internal,
                Ordering::AcqRel,
                &guard,
            ) {
                Ok(_) => return true,
                Err(e) => {
                    // Insertion failed. Help the conflicting remove operation if needed.
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if e.current.with_tag(Marks::empty().bits()) == record.leaf {
                        self.cleanup(&record, guard);
                    }
                }
            }
        }
    }

    pub fn remove(&self, key: &K, guard: &Guard) -> Option<V> {
        let mut record;
        // `leaf` and `value` are the snapshot of the node to be deleted.
        let leaf;
        let value;

        // NOTE: The paper version uses one big loop for both phases.
        // injection phase
        loop {
            record = self.seek(key, guard);

            // candidates
            let temp_leaf = record.leaf;
            let temp_leaf_node = unsafe { record.leaf.as_ref().unwrap() };
            // Copy the value before the physical deletion.
            let temp_value = unsafe {
                ManuallyDrop::into_inner(ptr::read(temp_leaf_node.value.as_ref().unwrap()))
            };
            if temp_leaf_node.key != *key {
                return None;
            }

            // Try injecting the deletion flag.
            match record.leaf_addr.compare_and_set(
                record.leaf,
                record.leaf.with_tag(Marks::new(true, false).bits()),
                Ordering::AcqRel,
                &guard,
            ) {
                Ok(_) => {
                    // Finalize the node to be removed
                    leaf = temp_leaf;
                    value = temp_value;
                    if self.cleanup(&record, guard) {
                        return Some(value);
                    }
                    // In-place cleanup failed. Enter the cleanup phase.
                    break;
                }
                Err(e) => {
                    // Flagging failed.
                    // case 1. record.leaf_addr(e.current) points to another node: restart.
                    // case 2. Another thread flagged/tagged the edge to leaf: help and restart
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if record.leaf == e.current.with_tag(Marks::empty().bits()) {
                        self.cleanup(&record, guard);
                    }
                }
            }
        }

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

#[cfg(test)]
mod tests {
    extern crate rand;
    use super::NMTreeMap;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    #[test]
    fn smoke_nm_tree() {
        let nm_tree_map = &NMTreeMap::new();

        {
            let guard = crossbeam_epoch::pin();
            nm_tree_map.insert(0, (0, 100), &guard);
            nm_tree_map.remove(&0, &guard);
        }

        thread::scope(|s| {
            for t in 0..30 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..10000).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        nm_tree_map.insert(i, (i, t), &crossbeam_epoch::pin());
                    }
                });
            }
        })
        .unwrap();

        println!("start removal");
        thread::scope(|s| {
            for _ in 0..30 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (1..10000).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        nm_tree_map.remove(&i, &crossbeam_epoch::pin());
                    }
                });
            }
        })
        .unwrap();

        println!("done");

        {
            let guard = crossbeam_epoch::pin();
            assert_eq!(nm_tree_map.get(&0, &guard).unwrap().0, 0);
            assert_eq!(nm_tree_map.remove(&0, &guard).unwrap().0, 0);
            assert_eq!(nm_tree_map.get(&0, &guard), None);
        }
    }
}
