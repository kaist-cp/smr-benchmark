use crossbeam_epoch::{Atomic, Guard, Owned, Shared};

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::Ordering;

bitflags! {
    /// TODO
    struct Flags: usize {
        /// TODO: meaningful
        const FLAG = 1usize.wrapping_shl(1);

        /// TODO: meaningful
        const TAG  = 1usize.wrapping_shl(0);
    }
}

impl Flags {
    fn new(flag: bool, tag: bool) -> Self {
        (if flag { Flags::FLAG } else { Flags::empty() })
            | (if tag { Flags::TAG } else { Flags::empty() })
    }

    fn flag(self) -> bool {
        !(self & Flags::FLAG).is_empty()
    }

    fn tag(self) -> bool {
        !(self & Flags::TAG).is_empty()
    }
}

#[derive(Clone, PartialEq, Eq, Ord, Debug)]
enum Key<K> {
    Fin(K),
    Inf0,
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

// TODO: https://www.reddit.com/r/rust/comments/5115o2/type_parameter_t_must_be_used_as_the_type/
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
    // TODO: Default V vs. Option<ManuallyDrop<V>>,
    // - internal node check
    value: ManuallyDrop<V>,
    left: Atomic<Node<K, V>>,
    right: Atomic<Node<K, V>>,
}

impl<K, V> Clone for Node<K, V>
where
    K: Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        Node {
            key: self.key.clone(),
            value: self.value.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<K, V> Node<K, V>
where
    K: Clone,
    V: Default + Clone,
{
    fn new_leaf(key: Key<K>, value: Option<V>) -> Node<K, V> {
        Node {
            key,
            value: ManuallyDrop::new(value.unwrap_or_default()),
            left: Atomic::null(),
            right: Atomic::null(),
        }
    }

    /// Make a new internal node, consuming the given left and right nodes,
    /// using the right node's key.
    fn new_internal(left: Node<K, V>, right: Node<K, V>) -> Node<K, V> {
        Node {
            key: right.key.clone(),
            value: ManuallyDrop::new(V::default()),
            left: Atomic::from(left),
            right: Atomic::from(right),
        }
    }

    /// Make a new internal node from the references to the left and right nodes.
    /// The left node and the right node should live longer than the new internal node.
    fn new_internal_from_ref(left: &Node<K, V>, right: &Node<K, V>) -> Node<K, V> {
        Node {
            key: right.key.clone(),
            value: ManuallyDrop::new(V::default()),
            left: Atomic::from(left as *const Node<K, V>),
            right: Atomic::from(right as *const Node<K, V>),
        }
    }

    #[inline]
    fn load_left<'g>(&self, guard: &'g Guard) -> Shared<'g, Node<K, V>> {
        self.left.load(Ordering::Acquire, guard)
    }

    #[inline]
    fn load_right<'g>(&self, guard: &'g Guard) -> Shared<'g, Node<K, V>> {
        self.right.load(Ordering::Acquire, guard)
    }
}

struct SeekRecord<'g, K, V> {
    ancestor: Shared<'g, Node<K, V>>,
    successor: Shared<'g, Node<K, V>>,
    parent: Shared<'g, Node<K, V>>,
    leaf: Shared<'g, Node<K, V>>,
}

pub struct NMTreeMap<K, V> {
    r: Node<K, V>,
}

impl<K, V> Default for NMTreeMap<K, V>
where
    K: Ord + Clone,
    V: Default + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for NMTreeMap<K, V> {
    fn drop(&mut self) {
        // TODO: incomplete implementation. nice way to drop subtree?
        unsafe {
            let guard = &crossbeam_epoch::unprotected();
            drop(self.r.right.load(Ordering::Relaxed, guard).into_owned());
            drop(
                self.r
                    .left
                    .load(Ordering::Relaxed, guard)
                    .as_ref()
                    .unwrap()
                    .left
                    .load(Ordering::Relaxed, guard)
                    .into_owned(),
            );
            drop(
                self.r
                    .left
                    .load(Ordering::Relaxed, guard)
                    .as_ref()
                    .unwrap()
                    .right
                    .load(Ordering::Relaxed, guard)
                    .into_owned(),
            );
        }
    }
}

impl<K, V> NMTreeMap<K, V>
where
    K: Ord + Clone,
    V: Default + Clone,
{
    pub fn new() -> Self {
        let inf0 = Node::new_leaf(Key::Inf0, None);
        let inf1 = Node::new_leaf(Key::Inf1, None);
        let inf2 = Node::new_leaf(Key::Inf2, None);
        let s = Node::new_internal(inf0, inf1);
        let r = Node::new_internal(s, inf2);
        NMTreeMap { r }
    }

    #[inline]
    fn init_record<'g>(&self, guard: &'g Guard) -> SeekRecord<'g, K, V> {
        let s = self.r.left.load(Ordering::Relaxed, guard);
        SeekRecord {
            ancestor: Shared::from(&self.r as *const Node<K, V>),
            successor: s,
            parent: s,
            leaf: unsafe { s.as_ref() }
                .unwrap()
                .left
                .load(Ordering::Acquire, guard)
                .with_tag(Flags::empty().bits()),
        }
    }

    fn seek<'g>(&self, key: &K, guard: &'g Guard) -> SeekRecord<'g, K, V> {
        let mut record = self.init_record(guard);
        let mut parent_field = unsafe { record.parent.as_ref() }.unwrap().load_left(guard);
        let mut current_field = unsafe { record.leaf.as_ref() }.unwrap().load_left(guard);
        let mut current = current_field.with_tag(Flags::empty().bits());

        while let Some(curr_node) = unsafe { current.as_ref() } {
            let tag = Flags::from_bits_truncate(parent_field.tag()).tag();
            if !tag {
                // untagged edge: advance ancestor and successor pointers
                record.ancestor = record.parent;
                record.successor = record.leaf;
            }

            // advance parent and leaf pointers
            record.parent = record.leaf;
            record.leaf = current;

            // update other variables
            parent_field = current_field;
            current_field = if curr_node.key > *key {
                curr_node.load_left(guard)
            } else {
                curr_node.load_right(guard)
            };
            current = current_field.with_tag(Flags::empty().bits());
        }

        record
    }

    /// Physically remove node.
    fn cleanup(&self, key: &K, record: &SeekRecord<'_, K, V>) -> bool {
        let ancestor = unsafe { record.ancestor.as_ref().unwrap() };
        let parent = unsafe { record.parent.as_ref().unwrap() };

        let successor_addr: &Atomic<Node<K, V>> = if ancestor.key > *key {
            &ancestor.left
        } else {
            &ancestor.right
        };

        // TODO: (setting up sibling_addr) inefficient?
        let (child_addr, mut sibling_addr) = if parent.key > *key {
            (&parent.left, &parent.right)
        } else {
            (&parent.right, &parent.left)
        };

        let guard = crossbeam_epoch::pin();
        let mut child = child_addr.load(Ordering::Acquire, &guard);

        let flag = Flags::from_bits_truncate(child.tag()).flag();
        if !flag {
            // sibling is flagged for deletion: switch sibling addr
            child = sibling_addr.load(Ordering::Acquire, &guard);
            sibling_addr = child_addr;
        }

        // NOTE: the ibr implementation uses CAS
        // tag (parent, sibling) edge -> all of the parent's edges can't change now
        // TODO: Is Release enough?
        sibling_addr.fetch_or(1, Ordering::AcqRel, &guard);

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        let sibling = sibling_addr.load(Ordering::Acquire, &guard);
        let flag = Flags::from_bits_truncate(sibling.tag()).flag();
        match successor_addr.compare_and_set(
            record.successor.with_tag(Flags::empty().bits()),
            sibling.with_tag(Flags::new(flag, false).bits()),
            Ordering::AcqRel,
            &guard,
        ) {
            Ok(_) => {
                unsafe {
                    // TODO: add correctness proof
                    guard.defer_destroy(child);
                    guard.defer_destroy(record.successor);
                }
                true
            }
            Err(_) => false,
        }
    }

    // TODO: key &'g ???
    pub fn get<'g>(&self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        let record = self.seek(key, guard);
        unsafe {
            record
                .leaf
                .as_ref()
                .and_then(|n| if n.key == *key { Some(&*n.value) } else { None })
        }
    }

    pub fn insert(&self, key: K, value: V) -> bool {
        let guard = crossbeam_epoch::pin();
        loop {
            let record = self.seek(&key, &guard);
            let leaf = unsafe { record.leaf.as_ref().unwrap() };
            if leaf.key == key {
                return false;
            } else {
                // key not in the tree
                let parent = unsafe { record.parent.as_ref().unwrap() };
                let child_addr = if parent.key > key {
                    &parent.left
                } else {
                    &parent.right
                };

                let new_leaf = Box::leak(Box::new(Node::new_leaf(
                    Key::Fin(key.clone()),
                    Some(value.clone()),
                ))) as &Node<K, V>;

                let (new_left, new_right, drop_left) = if leaf.key > key {
                    (new_leaf, leaf, true)
                } else {
                    (leaf, new_leaf, false)
                };

                let new_internal = Owned::new(Node::new_internal_from_ref(new_left, new_right));

                match child_addr.compare_and_set(
                    record.leaf.with_tag(Flags::empty().bits()),
                    new_internal,
                    Ordering::AcqRel,
                    &guard,
                ) {
                    Ok(_) => return true,
                    Err(e) => {
                        // Insertion failed. Drop the leaked leaf and help the
                        // conflicting remove operation if needed.
                        let leaked = if drop_left { &e.new.left } else { &e.new.right };
                        unsafe {
                            drop(leaked.load(Ordering::Relaxed, &guard).into_owned());
                        }
                        let child = child_addr.load(Ordering::Acquire, &guard);
                        let flags = Flags::from_bits_truncate(child.tag());
                        if child.with_tag(Flags::empty().bits()) == record.leaf
                            && (flags.flag() || flags.tag())
                        {
                            self.cleanup(&key, &record);
                        }
                    }
                }
            }
        }
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let guard = crossbeam_epoch::pin();
        let mut record;
        // `leaf`, `value` is the snapshot of the node to be deleted.
        let leaf;
        let value;

        // injection phase
        loop {
            record = self.seek(key, &guard);
            let parent = unsafe { record.parent.as_ref().unwrap() };
            let child_addr = if parent.key > *key {
                &parent.left
            } else {
                &parent.right
            };

            // candidates
            let temp_leaf = record.leaf;
            let temp_leaf_node = unsafe { record.leaf.as_ref().unwrap() };
            // Copy the value before proceeding to the next step so that
            // `defer_destroy_subtree` doesn't destroy the value.
            let temp_value = unsafe { ManuallyDrop::into_inner(ptr::read(&temp_leaf_node.value)) };
            if temp_leaf_node.key != *key {
                return None;
            }

            // Try injecting the deletion flag.
            match child_addr.compare_and_set(
                record.leaf.with_tag(Flags::empty().bits()),
                record.leaf.with_tag(Flags::new(true, false).bits()),
                Ordering::AcqRel,
                &guard,
            ) {
                Ok(_) => {
                    // Finalize the node to be removed
                    leaf = temp_leaf;
                    value = temp_value;
                    if self.cleanup(key, &record) {
                        return Some(value);
                    }
                    // In-place cleanup failed. Enter the cleanup phase.
                    break;
                }
                Err(_) => {
                    // Flagging failed.
                    // 1. child_addr points to another node: restart.
                    // 2. Another thread flagged/tagged the leaf: help and restart
                    let temp_child = child_addr.load(Ordering::Acquire, &guard);
                    let flags = Flags::from_bits_truncate(temp_child.tag());
                    if record.leaf == temp_child.with_tag(Flags::empty().bits())
                        && (flags.flag() || flags.tag())
                    {
                        self.cleanup(key, &record);
                    }
                }
            }
        }

        // cleanup phase
        loop {
            record = self.seek(key, &guard);
            if record.leaf != leaf {
                // `leaf` flagged for deletion was removed by a helping thread
                return Some(value);
            } else {
                // `leaf` is still present in the tree.
                if self.cleanup(key, &record) {
                    return Some(value);
                }
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
        nm_tree_map.insert(0, (0, 100));
        nm_tree_map.remove(&0);

        thread::scope(|s| {
            for t in 0..30 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        nm_tree_map.insert(i, (i, t));
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
                    let mut keys: Vec<i32> = (1..3000).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        nm_tree_map.remove(&i);
                    }
                });
            }
        })
        .unwrap();

        println!("done");

        let guard = crossbeam_epoch::pin();
        assert_eq!(nm_tree_map.get(&0, &guard).unwrap().0, 0);
        assert_eq!(nm_tree_map.remove(&0).unwrap().0, 0);
        assert_eq!(nm_tree_map.get(&0, &guard), None);
    }
}
