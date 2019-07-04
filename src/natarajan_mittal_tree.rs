use crossbeam_epoch::{Atomic, Guard, Owned, Shared};

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::Ordering;

#[inline]
fn with_flag_tag<T>(p: Shared<T>, flag: bool, tag: bool) -> Shared<T> {
    p.with_tag(((flag as usize) << 1) | (tag as usize))
}

#[inline]
fn flag_tag<T>(p: Shared<T>) -> (bool, bool) {
    let tags = p.tag();
    ((tags & 2) == 2, (tags & 1) == 1)
}

// TODO separate key/level vs Sum type
// Ord not needed?
#[derive(Clone, PartialEq, Eq, Ord)]
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

    /// uses right's key
    fn new_internal(left: &Node<K, V>, right: &Node<K, V>) -> Node<K, V> {
        Node {
            key: right.key.clone(),
            value: ManuallyDrop::new(V::default()),
            left: Atomic::new(left.clone()),
            right: Atomic::new(right.clone()),
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

impl<K, V> NMTreeMap<K, V>
where
    K: Ord + Clone,
    V: Default + Clone,
{
    pub fn new() -> Self {
        let inf0 = Node::new_leaf(Key::Inf0, None);
        let inf1 = Node::new_leaf(Key::Inf1, None);
        let inf2 = Node::new_leaf(Key::Inf2, None);
        let s = Node::new_internal(&inf0, &inf1);
        let r = Node::new_internal(&s, &inf2);
        NMTreeMap { r }
    }

    #[inline]
    fn new_record<'g>(&self, guard: &'g Guard) -> SeekRecord<'g, K, V> {
        let s = self.r.left.load(Ordering::Relaxed, guard);
        SeekRecord {
            ancestor: Shared::from(&self.r as *const Node<K, V>),
            successor: s,
            parent: s,
            leaf: unsafe { s.as_ref() }
                .unwrap()
                .left
                .load(Ordering::Acquire, guard)
                .with_tag(0),
        }
    }

    fn seek<'g>(&self, key: &K, guard: &'g Guard) -> SeekRecord<'g, K, V> {
        let mut record = self.new_record(guard);

        let mut parent_field = unsafe { record.parent.as_ref() }.unwrap().load_left(guard);
        let mut current_field = unsafe { record.leaf.as_ref() }.unwrap().load_left(guard);
        // TODO: is current needed?
        let mut current = current_field.with_tag(0);

        while let Some(curr_node) = unsafe { current.as_ref() } {
            let (_, tag) = flag_tag(parent_field);
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
            current = current_field.with_tag(0);
        }

        record
    }

    /// physical removal is done only by this function
    fn cleanup(&self, key: &K, record: &SeekRecord<'_, K, V>) -> bool {
        // TODO: make method?
        // TODO: add some doc
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

        let (flag, _) = flag_tag(child);
        if !flag {
            // sibling is flagged for deletion: switch sibling addr
            child = sibling_addr.load(Ordering::Acquire, &guard);
            sibling_addr = child_addr;
        }

        // NOTE: the ibr implementation uses CAS
        // tag (parent, sibling) edge -> all of the parent's edges can change now
        // TODO: Is Release enough?
        sibling_addr.fetch_or(1, Ordering::AcqRel, &guard);

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        let sibling = sibling_addr.load(Ordering::Acquire, &guard);
        let (flag, _) = flag_tag(sibling);
        match successor_addr.compare_and_set(
            record.successor.with_tag(0),
            with_flag_tag(sibling, flag, false),
            Ordering::AcqRel,
            &guard,
        ) {
            Ok(_) => {
                unsafe {
                    // TODO!!!!!!!!!!!!!!!!: free all of the subtree?
                    // guard.defer_destroy(child);
                    // guard.defer_destroy(record.successor);
                    Self::defer_destroy_subtree(record.successor, &guard);
                }
                true
            }
            Err(_) => false,
        }
    }

    fn defer_destroy_subtree<'g>(root: Shared<'g, Node<K,V>>, guard: &'g Guard) {
        // recurse
        let root_node = match unsafe { root.as_ref() } {
            None => return,
            Some(root_node) => root_node,
        };
        Self::defer_destroy_subtree(root_node.load_left(guard), guard);
        Self::defer_destroy_subtree(root_node.load_right(guard), guard);

        // TODO: check if it is safe to manually drop the value
        // ManuallyDrop::drop(root.value);
        unsafe { guard.defer_destroy(root); }
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

    /// The original version in the paper doesn't update.
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

                let new_leaf = Node::new_leaf(Key::Fin(key.clone()), Some(value.clone()));
                let (new_left, new_right) = if leaf.key > key {
                    (&new_leaf, leaf)
                } else {
                    (leaf, &new_leaf)
                };
                let new_internal = Owned::new(Node::new_internal(new_left, new_right));
                match child_addr.compare_and_set(
                    record.leaf.with_tag(0),
                    new_internal,
                    Ordering::AcqRel,
                    &guard,
                ) {
                    Ok(_) => return true,
                    Err(_) => {
                        let child = child_addr.load(Ordering::Acquire, &guard);
                        let (flag, tag) = flag_tag(child);
                        if child.with_tag(0) == record.leaf && (flag || tag) {
                            self.cleanup(&key, &record);
                        }
                    }
                }
            }
        }
    }

    // TODO: put, replace?

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
                record.leaf.with_tag(0),
                with_flag_tag(record.leaf, true, false),
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
                    // 2. Another thread flagged/tagged the leaf: help and
                    //    restart
                    let temp_child = child_addr.load(Ordering::Acquire, &guard);
                    let (flag, tag) = flag_tag(temp_child);
                    if record.leaf == temp_child.with_tag(0) && (flag || tag) {
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
    use rand::prelude::*;
    use super::NMTreeMap;
    use crossbeam_utils::thread;

    #[test]
    fn smoke_nm_tree() {
        let nm_tree_map = &NMTreeMap::new();

        thread::scope(|s| {
            for t in 0..6 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000000).collect();
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
            for _ in 0..6 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (1..3000000).collect();
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
