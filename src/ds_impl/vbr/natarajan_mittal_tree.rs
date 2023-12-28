use num::Bounded;
use vbr_rs::{ptr_with_tag, Entry, Global, Guard, ImmAtomic, Local, MutAtomic, Shared};

use super::concurrent_map::ConcurrentMap;
use std::cmp;
use std::mem::zeroed;
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

pub struct Node<K, V>
where
    K: 'static + Copy,
    V: 'static + Copy,
{
    key: ImmAtomic<K>,
    // TODO(@jeehoonkang): how about having another type that is either (1) value, or (2) left and
    // right.
    value: ImmAtomic<V>,
    left: MutAtomic<Node<K, V>>,
    right: MutAtomic<Node<K, V>>,
}

impl<K, V> Node<K, V>
where
    K: 'static + Copy + Bounded,
    V: 'static + Copy,
{
    fn new_leaf(key: K, value: V, guard: &mut Guard<Node<K, V>>) -> Shared<Node<K, V>> {
        loop {
            guard.refresh();
            let node = ok_or!(guard.allocate(), continue);
            let node_ref = unsafe { node.deref() };
            node_ref.key.set(key);
            node_ref.value.set(value);
            node_ref.left.nullify(node, 0, guard);
            node_ref.right.nullify(node, 0, guard);
            return node;
        }
    }

    /// Make a new internal node, consuming the given left and right nodes,
    /// using the right node's key.
    fn new_internal(
        left: Shared<Node<K, V>>,
        right: Shared<Node<K, V>>,
        guard: &mut Guard<Node<K, V>>,
    ) -> Shared<Node<K, V>> {
        loop {
            guard.refresh();
            let key = ok_or!(unsafe { right.deref() }.key.get(guard), continue);
            let node = ok_or!(guard.allocate(), continue);
            let node_ref = unsafe { node.deref() };
            node_ref.key.set(key);
            node_ref.value.set(unsafe { zeroed() });
            node_ref.left.nullify(node, 0, guard);
            node_ref.right.nullify(node, 0, guard);

            node_ref
                .left
                .compare_exchange(
                    node,
                    Shared::null(),
                    left,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .unwrap();
            node_ref
                .right
                .compare_exchange(
                    node,
                    Shared::null(),
                    right,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .unwrap();
            return node;
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
struct SeekRecord<K, V>
where
    K: 'static + Copy + Bounded,
    V: 'static + Copy,
{
    /// Parent of `successor`
    ancestor: Shared<Node<K, V>>,
    /// The first internal node with a marked outgoing edge
    successor: Shared<Node<K, V>>,
    /// The direction of successor from ancestor.
    successor_dir: Direction,
    /// Parent of `leaf`
    parent: Shared<Node<K, V>>,
    /// The end of the access path.
    leaf: Shared<Node<K, V>>,
    /// The direction of leaf from parent.
    leaf_dir: Direction,
}

// TODO(@jeehoonkang): code duplication...
impl<K, V> SeekRecord<K, V>
where
    K: 'static + Copy + Bounded,
    V: 'static + Copy,
{
    fn successor_addr(&self) -> &MutAtomic<Node<K, V>> {
        match self.successor_dir {
            Direction::L => &unsafe { self.ancestor.deref() }.left,
            Direction::R => &unsafe { self.ancestor.deref() }.right,
        }
    }

    fn leaf_addr(&self) -> &MutAtomic<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => &unsafe { self.parent.deref() }.left,
            Direction::R => &unsafe { self.parent.deref() }.right,
        }
    }

    fn leaf_sibling_addr(&self) -> &MutAtomic<Node<K, V>> {
        match self.leaf_dir {
            Direction::L => &unsafe { self.parent.deref() }.right,
            Direction::R => &unsafe { self.parent.deref() }.left,
        }
    }
}

pub struct NMTreeMap<K, V>
where
    K: 'static + Copy,
    V: 'static + Copy,
{
    r: Entry<Node<K, V>>,
}

impl<K, V> NMTreeMap<K, V>
where
    K: 'static + Copy + Ord + Bounded,
    V: 'static + Copy,
{
    pub fn new(local: &Local<Node<K, V>>) -> Self {
        // An empty tree has 5 default nodes with infinite keys so that the SeekRecord is allways
        // well-defined.
        //          r
        //         / \
        //        s  inf2
        //       / \
        //   inf0   inf1
        let guard = &mut local.guard();
        let inf0 = Node::new_leaf(Bounded::max_value(), unsafe { zeroed() }, guard);
        let inf1 = Node::new_leaf(Bounded::max_value(), unsafe { zeroed() }, guard);
        let inf2 = Node::new_leaf(Bounded::max_value(), unsafe { zeroed() }, guard);
        let s = Node::new_internal(inf0, inf1, guard);
        let r = Node::new_internal(s, inf2, guard);
        NMTreeMap { r: Entry::new(r) }
    }

    // All `Shared<_>` fields are unmarked.
    fn seek(&self, key: &K, guard: &Guard<Node<K, V>>) -> Result<SeekRecord<K, V>, ()> {
        let r = self.r.load(guard)?;
        let s = unsafe { r.deref() }.left.load(Ordering::Relaxed, guard)?;
        let s_node = unsafe { s.deref() };
        let leaf = s_node
            .left
            .load(Ordering::Relaxed, guard)?
            .with_tag(Marks::empty().bits());
        let leaf_node = unsafe { leaf.deref() };

        let mut record = SeekRecord {
            ancestor: r,
            successor: s,
            successor_dir: Direction::L,
            parent: s,
            leaf,
            leaf_dir: Direction::L,
        };

        let mut prev_tag = Marks::from_bits_truncate(leaf.tag()?).tag();
        let mut curr_dir = Direction::L;
        let mut curr = leaf_node.left.load(Ordering::Relaxed, guard)?;

        while let Some(curr_node) = curr.as_ref() {
            if !prev_tag {
                // untagged edge: advance ancestor and successor pointers
                record.ancestor = record.parent;
                record.successor = record.leaf;
                record.successor_dir = record.leaf_dir;
            }

            // advance parent and leaf pointers
            record.parent = record.leaf;
            record.leaf = curr.with_tag(Marks::empty().bits());
            record.leaf_dir = curr_dir;

            // update other variables
            prev_tag = Marks::from_bits_truncate(curr.tag()?).tag();
            if curr_node.key.get(guard)?.cmp(key) == cmp::Ordering::Greater {
                curr_dir = Direction::L;
                curr = curr_node.left.load(Ordering::Acquire, guard)?;
            } else {
                curr_dir = Direction::R;
                curr = curr_node.right.load(Ordering::Acquire, guard)?;
            }
        }

        Ok(record)
    }

    /// Similar to `seek`, but traverse the tree with only two pointers
    fn seek_leaf(&self, key: &K, guard: &Guard<Node<K, V>>) -> Result<SeekRecord<K, V>, ()> {
        let r = self.r.load(guard)?;
        let s = unsafe { r.deref() }.left.load(Ordering::Relaxed, guard)?;
        let s_node = unsafe { s.deref() };
        let leaf = s_node.left.load(Ordering::Acquire, guard)?.with_tag(0);

        let mut record = SeekRecord {
            ancestor: Shared::null(),
            successor: Shared::null(),
            successor_dir: Direction::L,
            parent: s,
            leaf,
            leaf_dir: Direction::L,
        };

        let mut curr = unsafe { record.leaf.deref() }
            .left
            .load(Ordering::Acquire, guard)?
            .with_tag(0);

        while let Some(curr_node) = curr.as_ref() {
            record.leaf = curr;

            if curr_node.key.get(guard)?.cmp(key) == cmp::Ordering::Greater {
                curr = curr_node.left.load(Ordering::Acquire, guard)?;
            } else {
                curr = curr_node.right.load(Ordering::Acquire, guard)?;
            }
            curr = curr.with_tag(0);
        }

        Ok(record)
    }

    /// Physically removes node.
    ///
    /// Returns true if it successfully unlinks the flagged node in `record`.
    fn cleanup(&self, record: &SeekRecord<K, V>, guard: &Guard<Node<K, V>>) -> Result<bool, ()> {
        // Identify the node(subtree) that will replace `successor`.
        let leaf_marked = record.leaf_addr().load(Ordering::Acquire, guard)?;
        let leaf_flag = Marks::from_bits_truncate(leaf_marked.tag()?).flag();
        let target_sibling_addr = if leaf_flag {
            record.leaf_sibling_addr()
        } else {
            record.leaf_addr()
        };

        // NOTE: the ibr implementation uses CAS
        // tag (parent, sibling) edge -> all of the parent's edges can't change now
        // TODO: Is Release enough?
        let target_sibling = target_sibling_addr.load(Ordering::Acquire, guard)?;
        let current_tag = target_sibling.tag()?;
        if target_sibling_addr
            .compare_exchange(
                record.parent,
                target_sibling.with_tag(current_tag),
                target_sibling.with_tag(current_tag | Marks::TAG.bits()),
                Ordering::AcqRel,
                Ordering::Relaxed,
                guard,
            )
            .is_err()
        {
            return Ok(false);
        }

        // Try to replace (ancestor, successor) w/ (ancestor, sibling).
        // Since (parent, sibling) might have been concurrently flagged, copy
        // the flag to the new edge (ancestor, sibling).
        let target_sibling = target_sibling_addr.load(Ordering::Acquire, guard)?;
        let flag = Marks::from_bits_truncate(target_sibling.tag()?).flag();
        let is_unlinked = record
            .successor_addr()
            .compare_exchange(
                record.ancestor,
                record.successor.with_tag(Marks::empty().bits()),
                target_sibling.with_tag(Marks::new(flag, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
                &guard,
            )
            .is_ok();

        if is_unlinked {
            unsafe {
                // destroy the subtree of successor except target_sibling
                let mut stack = vec![record.successor];

                while let Some(node) = stack.pop() {
                    if node.is_null()
                        || (node.with_tag(0).as_raw() == target_sibling.with_tag(0).as_raw())
                    {
                        continue;
                    }

                    let node_ref = node.deref();

                    stack.push(node_ref.left.load_unchecked(Ordering::Relaxed));
                    stack.push(node_ref.right.load_unchecked(Ordering::Relaxed));
                    let _ = guard.retire(node);
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get(&self, key: &K, local: &Local<Node<K, V>>) -> Option<V> {
        loop {
            let guard = &local.guard();
            let record = ok_or!(self.seek_leaf(key, guard), continue);
            let leaf_node = unsafe { record.leaf.deref() };
            let leaf_key = ok_or!(leaf_node.key.get(guard), continue);
            let leaf_value = ok_or!(leaf_node.value.get(guard), continue);

            if leaf_key.cmp(key) != cmp::Ordering::Equal {
                return None;
            }

            return Some(leaf_value);
        }
    }

    fn insert_inner(
        &self,
        key: K,
        value: V,
        new_leaf: Shared<Node<K, V>>,
        new_internal: Shared<Node<K, V>>,
        guard: &Guard<Node<K, V>>,
    ) -> Result<bool, ()> {
        let new_leaf_node = unsafe { new_leaf.deref() };
        new_leaf_node.key.set(key);
        new_leaf_node.value.set(value);

        let new_internal_node = unsafe { new_internal.deref() };
        new_internal_node.key.set(K::max_value());
        new_internal_node.value.set(unsafe { zeroed() });

        loop {
            let record = self.seek(&key, guard)?;
            let leaf = record.leaf;

            let (new_left, new_right) = match unsafe { leaf.deref() }.key.get(guard)?.cmp(&key) {
                cmp::Ordering::Equal => return Ok(false),
                cmp::Ordering::Greater => (new_leaf, leaf),
                cmp::Ordering::Less => (leaf, new_leaf),
            };

            new_internal_node
                .key
                .set(unsafe { new_right.deref() }.key.get(guard)?);
            let left = new_internal_node.left.nullify(new_internal, 0, guard);
            let right = new_internal_node.right.nullify(new_internal, 0, guard);
            new_internal_node
                .left
                .compare_exchange(
                    new_internal,
                    left,
                    new_left,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .unwrap();
            new_internal_node
                .right
                .compare_exchange(
                    new_internal,
                    right,
                    new_right,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .unwrap();

            // NOTE: record.leaf_addr is called childAddr in the paper.
            match record.leaf_addr().compare_exchange(
                record.parent,
                record.leaf,
                new_internal,
                Ordering::AcqRel,
                Ordering::Acquire,
                &guard,
            ) {
                Ok(_) => return Ok(true),
                Err(e) => {
                    // Insertion failed. Help the conflicting remove operation if needed.
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if e.1 == record.leaf.as_raw() {
                        self.cleanup(&record, guard)?;
                    }
                }
            }
        }
    }

    pub fn insert(&self, key: K, value: V, local: &Local<Node<K, V>>) -> bool {
        let guard = &mut local.guard();
        let new_leaf = Node::new_leaf(key, value, guard);
        let new_internal = loop {
            guard.refresh();
            let node = ok_or!(guard.allocate(), continue);
            let node_ref = unsafe { node.deref() };
            node_ref.left.nullify(node, 0, guard);
            node_ref.right.nullify(node, 0, guard);
            break node;
        };

        loop {
            guard.refresh();
            if let Ok(inserted) = self.insert_inner(key, value, new_leaf, new_internal, guard) {
                if inserted {
                    return true;
                } else {
                    unsafe {
                        let _ = guard.retire(new_leaf);
                        let _ = guard.retire(new_internal);
                    }
                    return false;
                }
            }
        }
    }

    pub fn remove_inner(&self, key: &K, guard: &mut Guard<Node<K, V>>) -> Result<Option<V>, ()> {
        // `leaf` and `value` are the snapshot of the node to be deleted.
        // NOTE: The paper version uses one big loop for both phases.
        // injection phase
        let (leaf, value) = loop {
            let record = self.seek(key, guard)?;

            // candidates
            let leaf = record.leaf;
            let leaf_node = record.leaf.as_ref().unwrap();

            if leaf_node.key.get(guard)?.cmp(key) != cmp::Ordering::Equal {
                return Ok(None);
            }

            let value = leaf_node.value.get(guard)?;

            // Try injecting the deletion flag.
            match record.leaf_addr().compare_exchange(
                record.parent,
                record.leaf,
                record.leaf.with_tag(Marks::new(true, false).bits()),
                Ordering::AcqRel,
                Ordering::Acquire,
                &guard,
            ) {
                Ok(_) => {
                    // Finalize the node to be removed
                    if Ok(true) == self.cleanup(&record, guard) {
                        return Ok(Some(value));
                    }
                    // In-place cleanup failed. Enter the cleanup phase.
                    break (leaf, value);
                }
                Err(e) => {
                    // Flagging failed.
                    // case 1. record.leaf_addr(e.current) points to another node: restart.
                    // case 2. Another thread flagged/tagged the edge to leaf: help and restart
                    // NOTE: The paper version checks if any of the mark is set, which is redundant.
                    if record.leaf.as_raw() == ptr_with_tag(e.1, Marks::empty().bits()) {
                        self.cleanup(&record, guard)?;
                    }
                }
            }
        };

        // cleanup phase
        loop {
            guard.refresh();
            let record = ok_or!(self.seek(key, guard), continue);
            if record.leaf.as_raw() != leaf.as_raw() {
                // The edge to leaf flagged for deletion was removed by a helping thread
                return Ok(Some(value));
            }

            // leaf is still present in the tree.
            if Ok(true) == self.cleanup(&record, guard) {
                return Ok(Some(value));
            }
        }
    }

    pub fn remove(&self, key: &K, local: &Local<Node<K, V>>) -> Option<V> {
        loop {
            let guard = &mut local.guard();
            match self.remove_inner(key, guard) {
                Ok(result) => return result,
                Err(_) => continue,
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for NMTreeMap<K, V>
where
    K: 'static + Copy + Ord + Bounded,
    V: 'static + Copy,
{
    type Global = Global<Node<K, V>>;

    type Local = Local<Node<K, V>>;

    fn global(key_range_hint: usize) -> Self::Global {
        Global::new(key_range_hint * 2)
    }

    fn local(global: &Self::Global) -> Self::Local {
        Local::new(global)
    }

    fn new(local: &Self::Local) -> Self {
        NMTreeMap::new(local)
    }

    #[inline(always)]
    fn get(&self, key: &K, local: &Self::Local) -> Option<V> {
        self.get(key, local)
    }

    #[inline(always)]
    fn insert(&self, key: K, value: V, local: &Self::Local) -> bool {
        self.insert(key, value, local)
    }

    #[inline(always)]
    fn remove(&self, key: &K, local: &Self::Local) -> Option<V> {
        self.remove(key, local)
    }
}

#[cfg(test)]
mod tests {
    use super::NMTreeMap;
    use crate::ds_impl::vbr::concurrent_map;

    #[test]
    fn smoke_nm_tree() {
        concurrent_map::tests::smoke::<NMTreeMap<i32, i32>>();
    }
}
