use std::sync::atomic::Ordering;

use crossbeam_ebr::{unprotected, Atomic, CompareExchangeError, Guard, Owned, Shared};

use super::concurrent_map::ConcurrentMap;

bitflags! {
    struct UpdateTag: usize {
        const CLEAN = 0usize;
        const DFLAG = 1usize;
        const IFLAG = 2usize;
        const MARK = 3usize;
    }
}

#[derive(Clone, PartialEq, Eq, Ord, Debug)]
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
        leaf: Atomic<Node<K, V>>,
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
    pub fn is_leaf(&self, guard: &Guard) -> bool {
        self.left.load(Ordering::Acquire, guard).is_null()
    }
}

struct Cursor<'g, K, V> {
    gp: Shared<'g, Node<K, V>>,
    p: Shared<'g, Node<K, V>>,
    l: Shared<'g, Node<K, V>>,
    pupdate: Shared<'g, Update<K, V>>,
    gpupdate: Shared<'g, Update<K, V>>,
}

impl<'g, K, V> Cursor<'g, K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new(root: Shared<'g, Node<K, V>>) -> Self {
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
    fn search(&mut self, key: &K, guard: &'g Guard) {
        loop {
            let l_node = unsafe { self.l.as_ref() }.unwrap();
            if l_node.is_leaf(guard) {
                break;
            }
            self.gp = self.p;
            self.p = self.l;
            self.gpupdate = self.pupdate;
            self.pupdate = l_node.update.load(Ordering::Acquire, guard);
            self.l = match l_node.key.cmp(key) {
                std::cmp::Ordering::Greater => l_node.left.load(Ordering::Acquire, guard),
                _ => l_node.right.load(Ordering::Acquire, guard),
            }
        }
    }
}

pub struct EFRBTree<K, V> {
    root: Atomic<Node<K, V>>,
}

impl<K, V> Drop for EFRBTree<K, V> {
    fn drop(&mut self) {
        unsafe {
            let root = self
                .root
                .load(Ordering::Relaxed, unprotected())
                .into_owned()
                .into_box();
            let mut stack = vec![
                root.left.load(Ordering::Relaxed, unprotected()),
                root.right.load(Ordering::Relaxed, unprotected()),
            ];

            while let Some(mut node) = stack.pop() {
                if node.is_null() {
                    continue;
                }

                let node_ref = node.deref_mut();

                stack.push(node_ref.left.load(Ordering::Relaxed, unprotected()));
                stack.push(node_ref.right.load(Ordering::Relaxed, unprotected()));
                let update = node_ref.update.load(Ordering::Relaxed, unprotected());
                if !update.is_null() {
                    drop(update.into_owned());
                }
                drop(node.into_owned());
            }
            let update = root.update.load(Ordering::Relaxed, unprotected());
            if !update.is_null() {
                drop(update.into_owned());
            }
        }
    }
}

impl<K, V> EFRBTree<K, V>
where
    K: Ord + Clone,
    V: Clone,
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

    pub fn find<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g Node<K, V>> {
        let mut cursor = Cursor::new(self.root.load(Ordering::Acquire, guard));
        cursor.search(key, guard);
        let l_node = unsafe { cursor.l.as_ref().unwrap() };
        if l_node.key.eq(key) {
            Some(l_node)
        } else {
            None
        }
    }

    pub fn insert(&self, key: &K, value: V, guard: &Guard) -> bool {
        loop {
            let mut cursor = Cursor::new(self.root.load(Ordering::Acquire, guard));
            cursor.search(key, guard);
            let l_node = unsafe { cursor.l.as_ref().unwrap() };
            let p_node = unsafe { cursor.p.as_ref().unwrap() };

            // Validate cursor: Is l a child of p?
            if cursor.l != p_node.left.load(Ordering::Acquire, guard)
                && cursor.l != p_node.right.load(Ordering::Acquire, guard)
            {
                continue;
            }

            if l_node.key == *key {
                return false;
            } else if cursor.pupdate.tag() != UpdateTag::CLEAN.bits() {
                self.help(cursor.pupdate, guard);
            } else {
                let new = Node::leaf(Key::Fin(key.clone()), Some(value.clone()));
                let new_sibling = Node::leaf(l_node.key.clone(), l_node.value.clone());

                let (left, right) = match new.key.partial_cmp(&new_sibling.key) {
                    Some(std::cmp::Ordering::Less) => (new, new_sibling),
                    _ => (new_sibling, new),
                };

                let new_internal = Owned::new(Node::internal(
                    // key field max(k, l → key)
                    right.key.clone(),
                    None,
                    // two child fields equal to new and newSibling
                    // (the one with the smaller key is the left child)
                    left,
                    right,
                ))
                .into_shared(unsafe { unprotected() });

                let op = Update::Insert {
                    p: Atomic::from(cursor.p),
                    new_internal: Atomic::from(new_internal),
                    leaf: Atomic::from(cursor.l),
                };

                let new_pupdate = Owned::new(op)
                    .into_shared(unsafe { unprotected() })
                    .with_tag(UpdateTag::IFLAG.bits());

                match p_node.update.compare_exchange(
                    cursor.pupdate,
                    new_pupdate,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                ) {
                    Ok(_) => {
                        if !cursor.pupdate.is_null() {
                            unsafe {
                                guard.defer_destroy(cursor.pupdate);
                            }
                        }
                        self.help_insert(new_pupdate, guard);
                        return true;
                    }
                    Err(e) => {
                        unsafe {
                            let new_pupdate_failed = new_pupdate.into_owned().into_box();
                            if let Update::Insert { new_internal, .. } = *new_pupdate_failed {
                                let new_internal_failed = new_internal.into_owned().into_box();
                                drop(new_internal_failed.left.into_owned());
                                drop(new_internal_failed.right.into_owned());
                            }
                        }
                        self.help(e.current, guard);
                    }
                }
            }
        }
    }

    pub fn delete<'g>(&'g self, key: &K, guard: &'g Guard) -> bool {
        loop {
            let mut cursor = Cursor::new(self.root.load(Ordering::Acquire, guard));
            cursor.search(key, guard);

            if cursor.gp.is_null() {
                // The tree is empty. There's no more things to do.
                return false;
            }

            let l_node = unsafe { cursor.l.as_ref().unwrap() };
            let p_node = unsafe { cursor.p.as_ref().unwrap() };
            let gp_node = unsafe { cursor.gp.as_ref().unwrap() };

            // Validate cursor 1: Is l a child of p?
            if cursor.l != p_node.left.load(Ordering::Acquire, guard)
                && cursor.l != p_node.right.load(Ordering::Acquire, guard)
            {
                continue;
            }
            // Validate cursor 2: Is p a child of gp?
            if cursor.p != gp_node.left.load(Ordering::Acquire, guard)
                && cursor.p != gp_node.right.load(Ordering::Acquire, guard)
            {
                continue;
            }

            if l_node.key != Key::Fin(key.clone()) {
                return false;
            }
            if cursor.gpupdate.tag() != UpdateTag::CLEAN.bits() {
                self.help(cursor.gpupdate, guard);
            } else if cursor.pupdate.tag() != UpdateTag::CLEAN.bits() {
                self.help(cursor.pupdate, guard);
            } else {
                let op = Update::Delete {
                    gp: Atomic::from(cursor.gp),
                    p: Atomic::from(cursor.p),
                    l: Atomic::from(cursor.l),
                    pupdate: Atomic::from(cursor.pupdate),
                };
                let new_update = Owned::new(op)
                    .into_shared(unsafe { unprotected() })
                    .with_tag(UpdateTag::DFLAG.bits());
                match unsafe { cursor.gp.as_ref().unwrap() }
                    .update
                    .compare_exchange(
                        cursor.gpupdate,
                        new_update,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    ) {
                    Ok(_) => {
                        if !cursor.gpupdate.is_null() {
                            unsafe {
                                guard.defer_destroy(cursor.gpupdate);
                            }
                        }
                        return self.help_delete(new_update, guard);
                    }
                    Err(e) => {
                        unsafe { drop(new_update.into_owned()) };
                        self.help(e.current, guard);
                    }
                }
            }
        }
    }

    fn help<'g>(&'g self, update: Shared<'g, Update<K, V>>, guard: &'g Guard) {
        match UpdateTag::from_bits_truncate(update.tag()) {
            UpdateTag::IFLAG => self.help_insert(update, guard),
            UpdateTag::MARK => self.help_marked(update, guard),
            UpdateTag::DFLAG => {
                let _ = self.help_delete(update, guard);
            }
            _ => {}
        }
    }

    fn help_delete<'g>(&'g self, op: Shared<'g, Update<K, V>>, guard: &'g Guard) -> bool {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.as_ref().unwrap() };
        if let Update::Delete { gp, p, pupdate, .. } = op_ref {
            let p_ref = unsafe { p.load(Ordering::Acquire, guard).as_ref().unwrap() };
            let pupdate_sh = pupdate.load(Ordering::Acquire, guard);
            let new_op = op.with_tag(UpdateTag::MARK.bits());

            match p_ref.update.compare_exchange(
                pupdate_sh,
                new_op,
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            ) {
                Ok(_) => {
                    if !pupdate_sh.is_null() {
                        unsafe {
                            guard.defer_destroy(pupdate_sh);
                        }
                    }
                    // (prev value) = op → pupdate
                    self.help_marked(new_op, guard);
                    return true;
                }
                Err(e) => {
                    if e.current == new_op {
                        // (prev value) = <Mark, op>
                        self.help_marked(new_op, guard);
                        return true;
                    } else {
                        self.help(e.current, guard);
                        let _ = unsafe { gp.load(Ordering::Acquire, guard).as_ref().unwrap() }
                            .update
                            .compare_exchange(
                                op.with_tag(UpdateTag::DFLAG.bits()),
                                op.with_tag(UpdateTag::CLEAN.bits()),
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                guard,
                            );
                        return false;
                    }
                }
            }
        } else {
            panic!("op is not pointing to a DInfo record")
        }
    }

    fn help_marked<'g>(&'g self, op: Shared<'g, Update<K, V>>, guard: &'g Guard) {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.as_ref().unwrap() };
        if let Update::Delete { gp, p, l, .. } = op_ref {
            // Set other to point to the sibling of the node to which op → l points
            let gp = gp.load(Ordering::Acquire, guard);
            let p = p.load(Ordering::Acquire, guard);
            let l = l.load(Ordering::Acquire, guard);

            let p_ref = unsafe { p.as_ref().unwrap() };
            let other = if p_ref.right.load(Ordering::Acquire, guard) == l {
                &p_ref.left
            } else {
                &p_ref.right
            };
            // Splice the node to which op → p points out of the tree, replacing it by other
            let other_sh = other.load(Ordering::Acquire, guard);

            if self.cas_child(gp, p, other_sh, guard).is_ok() {
                unsafe {
                    guard.defer_destroy(p);
                    guard.defer_destroy(l);
                }
            }
            let _ = unsafe { gp.as_ref().unwrap() }.update.compare_exchange(
                op.with_tag(UpdateTag::DFLAG.bits()),
                op.with_tag(UpdateTag::CLEAN.bits()),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            );
        } else {
            panic!("op is not pointing to a DInfo record")
        }
    }

    fn help_insert<'g>(&'g self, op: Shared<'g, Update<K, V>>, guard: &'g Guard) {
        // Precondition: op points to an IInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.as_ref().unwrap() };
        if let Update::Insert {
            p,
            new_internal,
            leaf,
        } = op_ref
        {
            let p = p.load(Ordering::Acquire, guard);
            let new_internal = new_internal.load(Ordering::Acquire, guard);
            let leaf = leaf.load(Ordering::Acquire, guard);

            if self.cas_child(p, leaf, new_internal, guard).is_ok() {
                unsafe {
                    guard.defer_destroy(leaf);
                };
            }
            let p_ref = unsafe { p.as_ref().unwrap() };
            let _ = p_ref.update.compare_exchange(
                op.with_tag(UpdateTag::IFLAG.bits()),
                op.with_tag(UpdateTag::CLEAN.bits()),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            );
        } else {
            panic!("op is not pointing to an IInfo record")
        }
    }

    #[inline]
    fn cas_child<'g>(
        &'g self,
        parent: Shared<'g, Node<K, V>>,
        old: Shared<'g, Node<K, V>>,
        new: Shared<'g, Node<K, V>>,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Node<K, V>>, CompareExchangeError<'g, Node<K, V>, Shared<'g, Node<K, V>>>>
    {
        // Precondition: parent points to an Internal node and new points to a Node (i.e., neither is ⊥)
        // This routine tries to change one of the child fields of the node that parent points to from old to new.
        let new_node = unsafe { new.as_ref().unwrap() };
        let parent_node = unsafe { parent.as_ref().unwrap() };
        let node_to_cas = if new_node.key < parent_node.key {
            &parent_node.left
        } else {
            &parent_node.right
        };
        node_to_cas.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard)
    }
}

impl<K, V> ConcurrentMap<K, V> for EFRBTree<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new() -> Self {
        EFRBTree::new()
    }

    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        match self.find(key, guard) {
            Some(node) => Some(node.value.as_ref().unwrap()),
            None => None,
        }
    }

    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.insert(&key, value, guard)
    }

    fn remove<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        let res = self.get(key, guard);
        if res.is_some() {
            self.delete(key, guard);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::EFRBTree;
    use crate::ebr::concurrent_map;

    #[test]
    fn smoke_efrb_tree() {
        concurrent_map::tests::smoke::<EFRBTree<i32, String>>();
    }
}