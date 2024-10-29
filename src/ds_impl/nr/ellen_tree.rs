use std::sync::atomic::Ordering;

use super::concurrent_map::{ConcurrentMap, OutputHolder};
use super::pointers::{Atomic, Shared};

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct UpdateTag: usize {
        const CLEAN = 0usize;
        const DFLAG = 1usize;
        const IFLAG = 2usize;
        const MARK = 3usize;
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Key<K> {
    Fin(K),
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
            (Key::Fin(_), Key::Inf1) => Some(std::cmp::Ordering::Less),
            (Key::Fin(_), Key::Inf2) => Some(std::cmp::Ordering::Less),
            (Key::Inf1, Key::Fin(_)) => Some(std::cmp::Ordering::Greater),
            (Key::Inf1, Key::Inf1) => Some(std::cmp::Ordering::Equal),
            (Key::Inf1, Key::Inf2) => Some(std::cmp::Ordering::Less),
            (Key::Inf2, Key::Fin(_)) => Some(std::cmp::Ordering::Greater),
            (Key::Inf2, Key::Inf1) => Some(std::cmp::Ordering::Greater),
            (Key::Inf2, Key::Inf2) => Some(std::cmp::Ordering::Equal),
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
    value: Option<V>,
    // tag on low bits: {Clean, DFlag, IFlag, Mark}
    update: Atomic<Update<K, V>>,
    left: Atomic<Node<K, V>>,
    right: Atomic<Node<K, V>>,
}

pub enum Update<K, V> {
    Insert {
        p: Atomic<Node<K, V>>,
        new_internal: Atomic<Node<K, V>>,
        l: Atomic<Node<K, V>>,
    },
    Delete {
        gp: Atomic<Node<K, V>>,
        p: Atomic<Node<K, V>>,
        l: Atomic<Node<K, V>>,
        pupdate: Atomic<Update<K, V>>,
    },
}

impl<K, V> Node<K, V> {
    pub fn internal(key: Key<K>, value: Option<V>, left: Self, right: Self) -> Self {
        Self {
            key,
            value,
            update: Atomic::null(),
            left: Atomic::new(left),
            right: Atomic::new(right),
        }
    }

    pub fn leaf(key: Key<K>, value: Option<V>) -> Self {
        Self {
            key,
            value,
            update: Atomic::null(),
            left: Atomic::null(),
            right: Atomic::null(),
        }
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.left.load(Ordering::Acquire).is_null()
    }
}

struct Cursor<K, V> {
    gp: Shared<Node<K, V>>,
    p: Shared<Node<K, V>>,
    l: Shared<Node<K, V>>,
    pupdate: Shared<Update<K, V>>,
    gpupdate: Shared<Update<K, V>>,
}

impl<K, V> Cursor<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new(root: Shared<Node<K, V>>) -> Self {
        Self {
            gp: Shared::null(),
            p: Shared::null(),
            l: root,
            pupdate: Shared::null(),
            gpupdate: Shared::null(),
        }
    }

    /// Used by Insert, Delete and Find to traverse a branch of the BST.
    ///
    /// # Safety
    /// It satisfies following postconditions:
    ///
    /// 1. l points to a Leaf node and p points to an Internal node
    /// 2. Either p → left has contained l (if k<p → key) or p → right has contained l (if k ≥ p → key)
    /// 3. p → update has contained pupdate
    /// 4. if l → key != Inf1, then the following three statements hold:
    ///     - gp points to an Internal node
    ///     - either gp → left has contained p (if k < gp → key) or gp → right has contained p (if k ≥ gp → key)
    ///     - gp → update has contained gpupdate
    #[inline]
    fn search(&mut self, key: &K) {
        loop {
            let l_node = unsafe { self.l.as_ref() }.unwrap();
            if l_node.is_leaf() {
                break;
            }
            self.gp = self.p;
            self.p = self.l;
            self.gpupdate = self.pupdate;
            self.pupdate = l_node.update.load(Ordering::Acquire);
            self.l = match l_node.key.cmp(key) {
                std::cmp::Ordering::Greater => l_node.left.load(Ordering::Acquire),
                _ => l_node.right.load(Ordering::Acquire),
            }
        }
    }
}

pub struct EFRBTree<K, V> {
    root: Atomic<Node<K, V>>,
}

impl<K, V> Default for EFRBTree<K, V>
where
    K: Ord + Clone + 'static,
    V: Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> EFRBTree<K, V>
where
    K: Ord + Clone + 'static,
    V: Clone + 'static,
{
    pub fn new() -> Self {
        Self {
            root: Atomic::new(Node::internal(
                Key::Inf2,
                None,
                Node::leaf(Key::Inf1, None),
                Node::leaf(Key::Inf2, None),
            )),
        }
    }

    pub fn find(&self, key: &K) -> Option<&'static Node<K, V>> {
        let mut cursor = Cursor::new(self.root.load(Ordering::Relaxed));
        cursor.search(key);
        let l_node = unsafe { cursor.l.as_ref().unwrap() };
        if l_node.key.eq(key) {
            Some(l_node)
        } else {
            None
        }
    }

    pub fn insert(&self, key: &K, value: V) -> bool {
        loop {
            let mut cursor = Cursor::new(self.root.load(Ordering::Relaxed));
            cursor.search(key);
            let l_node = unsafe { cursor.l.as_ref().unwrap() };
            let p_node = unsafe { cursor.p.as_ref().unwrap() };

            if l_node.key == *key {
                return false;
            } else if cursor.pupdate.tag() != UpdateTag::CLEAN.bits() {
                self.help(cursor.pupdate);
            } else {
                let new = Node::leaf(Key::Fin(key.clone()), Some(value.clone()));
                let new_sibling = Node::leaf(l_node.key.clone(), l_node.value.clone());

                let (left, right) = match new.key.partial_cmp(&new_sibling.key) {
                    Some(std::cmp::Ordering::Less) => (new, new_sibling),
                    _ => (new_sibling, new),
                };

                let new_internal = Shared::from_owned(Node::internal(
                    // key field max(k, l → key)
                    right.key.clone(),
                    None,
                    // two child fields equal to new and newSibling
                    // (the one with the smaller key is the left child)
                    left,
                    right,
                ));

                let op = Update::Insert {
                    p: Atomic::from(cursor.p),
                    new_internal: Atomic::from(new_internal),
                    l: Atomic::from(cursor.l),
                };

                let new_pupdate = Shared::from_owned(op).with_tag(UpdateTag::IFLAG.bits());

                match p_node.update.compare_exchange(
                    cursor.pupdate,
                    new_pupdate,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        self.help_insert(new_pupdate);
                        return true;
                    }
                    Err(e) => {
                        unsafe {
                            let new_pupdate_failed = new_pupdate.into_owned();
                            if let Update::Insert { new_internal, .. } = new_pupdate_failed {
                                let new_internal_failed = new_internal.into_owned();
                                drop(new_internal_failed.left.into_owned());
                                drop(new_internal_failed.right.into_owned());
                            }
                        }
                        self.help(e.current);
                    }
                }
            }
        }
    }

    pub fn delete(&self, key: &K) -> Option<&'static V> {
        loop {
            let mut cursor = Cursor::new(self.root.load(Ordering::Relaxed));
            cursor.search(key);

            if cursor.gp.is_null() {
                // The tree is empty. There's no more things to do.
                return None;
            }

            let l_node = unsafe { cursor.l.as_ref().unwrap() };

            if l_node.key != Key::Fin(key.clone()) {
                return None;
            }
            if cursor.gpupdate.tag() != UpdateTag::CLEAN.bits() {
                self.help(cursor.gpupdate);
            } else if cursor.pupdate.tag() != UpdateTag::CLEAN.bits() {
                self.help(cursor.pupdate);
            } else {
                let op = Update::Delete {
                    gp: Atomic::from(cursor.gp),
                    p: Atomic::from(cursor.p),
                    l: Atomic::from(cursor.l),
                    pupdate: Atomic::from(cursor.pupdate),
                };
                let new_update = Shared::from_owned(op).with_tag(UpdateTag::DFLAG.bits());
                match unsafe { cursor.gp.as_ref().unwrap() }
                    .update
                    .compare_exchange(
                        cursor.gpupdate,
                        new_update,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                    Ok(_) => {
                        if self.help_delete(new_update) {
                            return Some(l_node.value.as_ref().unwrap());
                        }
                    }
                    Err(e) => {
                        self.help(e.current);
                    }
                }
            }
        }
    }

    #[inline]
    fn help(&self, update: Shared<Update<K, V>>) {
        match UpdateTag::from_bits_truncate(update.tag()) {
            UpdateTag::IFLAG => self.help_insert(update),
            UpdateTag::MARK => self.help_marked(update),
            UpdateTag::DFLAG => {
                let _ = self.help_delete(update);
            }
            _ => {}
        }
    }

    fn help_delete(&self, op: Shared<Update<K, V>>) -> bool {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.as_ref().unwrap() };
        if let Update::Delete { gp, p, pupdate, .. } = op_ref {
            let p_ref = unsafe { p.load(Ordering::Relaxed).as_ref().unwrap() };
            let pupdate_sh = pupdate.load(Ordering::Relaxed);
            let new_op = op.with_tag(UpdateTag::MARK.bits());

            match p_ref.update.compare_exchange(
                pupdate_sh,
                new_op,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // (prev value) = op → pupdate
                    self.help_marked(new_op);
                    true
                }
                Err(e) => {
                    if e.current == new_op {
                        // (prev value) = <Mark, op>
                        self.help_marked(new_op);
                        true
                    } else {
                        self.help(e.current);
                        let _ = unsafe { gp.load(Ordering::Acquire).as_ref().unwrap() }
                            .update
                            .compare_exchange(
                                op.with_tag(UpdateTag::DFLAG.bits()),
                                op.with_tag(UpdateTag::CLEAN.bits()),
                                Ordering::Release,
                                Ordering::Relaxed,
                            );
                        false
                    }
                }
            }
        } else {
            panic!("op is not pointing to a DInfo record")
        }
    }

    fn help_marked(&self, op: Shared<Update<K, V>>) {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.as_ref().unwrap() };
        if let Update::Delete { gp, p, l, .. } = op_ref {
            // Set other to point to the sibling of the node to which op → l points
            let gp = gp.load(Ordering::Relaxed);
            let p = p.load(Ordering::Relaxed);
            let l = l.load(Ordering::Relaxed);

            let p_ref = unsafe { p.as_ref().unwrap() };
            let other = if p_ref.right.load(Ordering::Acquire) == l {
                &p_ref.left
            } else {
                &p_ref.right
            };
            // Splice the node to which op → p points out of the tree, replacing it by other
            let other_sh = other.load(Ordering::Acquire);

            self.cas_child(gp, p, other_sh);
            let _ = unsafe { gp.as_ref().unwrap() }.update.compare_exchange(
                op.with_tag(UpdateTag::DFLAG.bits()),
                op.with_tag(UpdateTag::CLEAN.bits()),
                Ordering::Release,
                Ordering::Relaxed,
            );
        } else {
            panic!("op is not pointing to a DInfo record")
        }
    }

    fn help_insert(&self, op: Shared<Update<K, V>>) {
        // Precondition: op points to an IInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.as_ref().unwrap() };
        if let Update::Insert { p, new_internal, l } = op_ref {
            let p = p.load(Ordering::Relaxed);
            let new_internal = new_internal.load(Ordering::Relaxed);
            let l = l.load(Ordering::Relaxed);

            self.cas_child(p, l, new_internal);
            let p_ref = unsafe { p.as_ref().unwrap() };
            let _ = p_ref.update.compare_exchange(
                op.with_tag(UpdateTag::IFLAG.bits()),
                op.with_tag(UpdateTag::CLEAN.bits()),
                Ordering::Release,
                Ordering::Relaxed,
            );
        } else {
            panic!("op is not pointing to an IInfo record")
        }
    }

    #[inline]
    fn cas_child(
        &self,
        parent: Shared<Node<K, V>>,
        old: Shared<Node<K, V>>,
        new: Shared<Node<K, V>>,
    ) {
        // Precondition: parent points to an Internal node and new points to a Node (i.e., neither is ⊥)
        // This routine tries to change one of the child fields of the node that parent points to from old to new.
        let new_node = unsafe { new.as_ref().unwrap() };
        let parent_node = unsafe { parent.as_ref().unwrap() };
        let node_to_cas = if new_node.key < parent_node.key {
            &parent_node.left
        } else {
            &parent_node.right
        };
        let _ = node_to_cas.compare_exchange(old, new, Ordering::Release, Ordering::Acquire);
    }
}

impl<K, V> ConcurrentMap<K, V> for EFRBTree<K, V>
where
    K: Ord + Clone + 'static,
    V: Clone + 'static,
{
    fn new() -> Self {
        EFRBTree::new()
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<impl OutputHolder<V>> {
        match self.find(key) {
            Some(node) => Some(node.value.as_ref().unwrap()),
            None => None,
        }
    }

    #[inline(always)]
    fn insert(&self, key: K, value: V) -> bool {
        self.insert(&key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &K) -> Option<impl OutputHolder<V>> {
        self.delete(key)
    }
}

#[cfg(test)]
mod tests {
    use super::EFRBTree;
    use crate::ds_impl::nr::concurrent_map;

    #[test]
    fn smoke_efrb_tree() {
        concurrent_map::tests::smoke::<EFRBTree<i32, String>>();
    }
}
