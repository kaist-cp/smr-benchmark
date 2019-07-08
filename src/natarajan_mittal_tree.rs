use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Shared};

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
    // COMMENT(@jeehoonkang): please comment something
    ancestor: Shared<'g, Node<K, V>>,
    successor: Shared<'g, Node<K, V>>,
    parent: Shared<'g, Node<K, V>>,
    leaf: Shared<'g, Node<K, V>>,
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
        // COMMENT(@jeehoonkang): describe it...
        let inf0 = Node::new_leaf(Key::Inf0, None);
        let inf1 = Node::new_leaf(Key::Inf1, None);
        let inf2 = Node::new_leaf(Key::Inf2, None);
        let s = Node::new_internal(inf0, inf1);
        let r = Node::new_internal(s, inf2);
        NMTreeMap { r }
    }

    fn seek<'g>(&self, key: &K, guard: &'g Guard) -> SeekRecord<'g, K, V> {
        let s = self.r.left.load(Ordering::Relaxed, guard);
        let leaf = unsafe { s.deref() }
            .left
            .load(Ordering::Acquire, guard)
            .with_tag(Flags::empty().bits());

        let mut record = SeekRecord {
            ancestor: Shared::from(&self.r as *const _),
            successor: s,
            parent: s,
            leaf,
        };
        let mut parent_field = unsafe { s.as_ref() }.unwrap().load_left(guard);
        let mut current_field = unsafe { leaf.as_ref() }.unwrap().load_left(guard);
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

    /// Physically removes node.
    ///
    /// Returns whether the subtree containing `key` is removed.
    fn cleanup(&self, key: &K, record: &SeekRecord<'_, K, V>, guard: &Guard) -> bool {
        let ancestor = unsafe { record.ancestor.as_ref().unwrap() };
        let parent = unsafe { record.parent.as_ref().unwrap() };

        // COMMENT(@jeehoonkang): maybe refactoring? inefficient?
        let successor_addr: &Atomic<Node<K, V>> = if ancestor.key > *key {
            &ancestor.left
        } else {
            &ancestor.right
        };

        let (child_addr, mut sibling_addr) = if parent.key > *key {
            (&parent.left, &parent.right)
        } else {
            (&parent.right, &parent.left)
        };

        // COMMENT(@jeehoonkang): consistent variable naming
        let mut child = child_addr.load(Ordering::Acquire, guard);

        let flag = Flags::from_bits_truncate(child.tag()).flag();
        if !flag {
            // sibling is flagged for deletion: switch sibling addr
            child = sibling_addr.load(Ordering::Acquire, guard);
            sibling_addr = child_addr;
        }

        // NOTE: the ibr implementation uses CAS
        // tag (parent, sibling) edge -> all of the parent's edges can't change now
        // TODO: Is Release enough?
        //
        // COMMENT(@jeehoonkang): the value of `sibling_addr` can be changed from `child`. We remove
        // `child` later. Is it okay?
        sibling_addr.fetch_or(1, Ordering::AcqRel, guard);

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        let sibling = sibling_addr.load(Ordering::Acquire, guard);
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
            key: Key::Inf2, // TODO
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
                unsafe {
                    drop(new_leaf.into_owned());
                    drop(new_internal.into_owned());
                }
                return false;
            }

            // key not in the tree
            let parent_node = unsafe { record.parent.as_ref().unwrap() };
            let parent_child = if parent_node.key > key {
                &parent_node.left
            } else {
                &parent_node.right
            };

            let (new_left, new_right) = if leaf_node.key > key {
                (new_leaf, leaf)
            } else {
                (leaf, new_leaf)
            };

            let new_internal_node = unsafe { new_internal.deref_mut() };
            new_internal_node.key = unsafe { new_right.deref().key.clone() };
            new_internal_node.left.store(new_left, Ordering::Relaxed);
            new_internal_node.right.store(new_right, Ordering::Relaxed);

            if parent_child
                .compare_and_set(
                    record.leaf.with_tag(Flags::empty().bits()),
                    new_internal,
                    Ordering::AcqRel,
                    &guard,
                )
                .is_ok()
            {
                return true;
            }

            // Insertion failed. Help the conflicting remove operation if needed.
            let child = parent_child.load(Ordering::Acquire, guard);
            let flags = Flags::from_bits_truncate(child.tag());
            if child.with_tag(Flags::empty().bits()) == record.leaf && (flags.flag() || flags.tag())
            {
                self.cleanup(&key, &record, guard);
            }
        }
    }

    pub fn remove(&self, key: &K, guard: &Guard) -> Option<V> {
        let mut record;
        // `leaf`, `value` is the snapshot of the node to be deleted.
        let leaf;
        let value;

        // injection phase
        loop {
            record = self.seek(key, guard);
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
            let temp_value = unsafe {
                ManuallyDrop::into_inner(ptr::read(temp_leaf_node.value.as_ref().unwrap()))
            };
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
                    if self.cleanup(key, &record, guard) {
                        return Some(value);
                    }
                    // In-place cleanup failed. Enter the cleanup phase.
                    break;
                }
                Err(_) => {
                    // Flagging failed.
                    // 1. child_addr points to another node: restart.
                    // 2. Another thread flagged/tagged the leaf: help and restart
                    let temp_child = child_addr.load(Ordering::Acquire, guard);
                    let flags = Flags::from_bits_truncate(temp_child.tag());
                    if record.leaf == temp_child.with_tag(Flags::empty().bits())
                        && (flags.flag() || flags.tag())
                    {
                        self.cleanup(key, &record, guard);
                    }
                }
            }
        }

        // cleanup phase
        loop {
            record = self.seek(key, guard);
            if record.leaf != leaf {
                // `leaf` flagged for deletion was removed by a helping thread
                return Some(value);
            } else {
                // `leaf` is still present in the tree.
                if self.cleanup(key, &record, guard) {
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

        {
            let guard = crossbeam_epoch::pin();
            nm_tree_map.insert(0, (0, 100), &guard);
            nm_tree_map.remove(&0, &guard);
        }

        thread::scope(|s| {
            for t in 0..30 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).collect();
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
                    let mut keys: Vec<i32> = (1..3000).collect();
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
