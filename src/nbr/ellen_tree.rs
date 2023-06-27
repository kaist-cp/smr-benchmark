use hp_pp::{tag, tagged, untagged};
use nbr_rs::{read_phase, Guard};
use std::{
    mem, ptr,
    sync::atomic::{AtomicPtr, Ordering},
};

use super::concurrent_map::ConcurrentMap;

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

#[derive(Clone, Copy, Debug)]
enum Direction {
    L,
    R,
}

pub struct Node<K, V> {
    key: Key<K>,
    value: Option<V>,
    // tag on low bits: {Clean, DFlag, IFlag, Mark}
    update: AtomicPtr<Update<K, V>>,
    left: AtomicPtr<Node<K, V>>,
    right: AtomicPtr<Node<K, V>>,
    is_leaf: bool,
}

enum Update<K, V> {
    Insert {
        p: *mut Node<K, V>,
        new_internal: *mut Node<K, V>,
        l: *mut Node<K, V>,
        p_l_dir: Direction,
    },
    Delete {
        gp: *mut Node<K, V>,
        p: *mut Node<K, V>,
        l: *mut Node<K, V>,
        l_other: *mut Node<K, V>,
        pupdate: *mut Update<K, V>,
        gp_p_dir: Direction,
    },
}

impl<K, V> Default for Update<K, V> {
    fn default() -> Self {
        Update::Insert {
            p: ptr::null_mut(),
            new_internal: ptr::null_mut(),
            l: ptr::null_mut(),
            p_l_dir: Direction::L,
        }
    }
}

impl<K, V> Update<K, V> {
    #[inline]
    pub fn protect(&self, guard: &Guard) {
        match self {
            Update::Insert { p, .. } => {
                guard.protect(*p);
            }
            Update::Delete { gp, p, .. } => {
                guard.protect(*gp);
                guard.protect(*p);
            }
        }
    }
}

impl<K, V> Node<K, V> {
    pub fn internal(key: Key<K>, value: Option<V>, left: Self, right: Self) -> Self {
        Self {
            key,
            value,
            update: AtomicPtr::new(ptr::null_mut()),
            left: AtomicPtr::new(Box::into_raw(Box::new(left))),
            right: AtomicPtr::new(Box::into_raw(Box::new(right))),
            is_leaf: false,
        }
    }

    pub fn leaf(key: Key<K>, value: Option<V>) -> Self {
        Self {
            key,
            value,
            update: AtomicPtr::new(ptr::null_mut()),
            left: AtomicPtr::new(ptr::null_mut()),
            right: AtomicPtr::new(ptr::null_mut()),
            is_leaf: true,
        }
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.is_leaf
    }

    #[inline]
    fn child<'g>(&'g self, dir: Direction) -> &'g AtomicPtr<Self> {
        match dir {
            Direction::L => &self.left,
            Direction::R => &self.right,
        }
    }
}

struct Cursor<K, V> {
    gp: *mut Node<K, V>,
    p: *mut Node<K, V>,
    l: *mut Node<K, V>,
    l_other: *mut Node<K, V>,
    gp_p_dir: Direction,
    p_l_dir: Direction,
    pupdate: *mut Update<K, V>,
    gpupdate: *mut Update<K, V>,
}

impl<K, V> Cursor<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new(root: *mut Node<K, V>) -> Self {
        Self {
            gp: ptr::null_mut(),
            p: ptr::null_mut(),
            l: root,
            l_other: ptr::null_mut(),
            gp_p_dir: Direction::L,
            p_l_dir: Direction::L,
            pupdate: ptr::null_mut(),
            gpupdate: ptr::null_mut(),
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
            let l_node = unsafe { &*untagged(self.l) };
            if l_node.is_leaf() {
                break;
            }
            self.gp = self.p;
            self.p = self.l;
            self.gp_p_dir = self.p_l_dir;
            self.gpupdate = self.pupdate;
            self.pupdate = l_node.update.load(Ordering::Acquire);

            (self.l, self.l_other) = (
                l_node.left.load(Ordering::Acquire),
                l_node.right.load(Ordering::Acquire),
            );

            if l_node.key.cmp(key) != std::cmp::Ordering::Greater {
                mem::swap(&mut self.l, &mut self.l_other);
                self.p_l_dir = Direction::R;
            } else {
                self.p_l_dir = Direction::L
            }
        }
    }

    #[inline]
    fn protect(&self, guard: &Guard) {
        guard.protect(self.gp);
        guard.protect(self.p);
        guard.protect(self.l);
        guard.protect(self.l_other);
        guard.protect(untagged(self.gpupdate));
        guard.protect(untagged(self.pupdate));
    }
}

pub struct EFRBTree<K, V> {
    root: AtomicPtr<Node<K, V>>,
}

impl<K, V> Drop for EFRBTree<K, V> {
    fn drop(&mut self) {
        unsafe {
            let root = Box::from_raw(self.root.load(Ordering::Relaxed));
            let mut stack = vec![
                root.left.load(Ordering::Relaxed),
                root.right.load(Ordering::Relaxed),
            ];

            while let Some(node) = stack.pop() {
                if node.is_null() {
                    continue;
                }

                let node_ref = &*node;

                stack.push(node_ref.left.load(Ordering::Relaxed));
                stack.push(node_ref.right.load(Ordering::Relaxed));
                let update = node_ref.update.load(Ordering::Relaxed);
                if !untagged(update).is_null() {
                    drop(Box::from_raw(update));
                }
                drop(Box::from_raw(node));
            }
            let update = root.update.load(Ordering::Relaxed);
            if !untagged(update).is_null() {
                drop(Box::from_raw(update));
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
            root: AtomicPtr::new(Box::into_raw(Box::new(Node::internal(
                Key::Inf2,
                None,
                Node::leaf(Key::Inf1, None),
                Node::leaf(Key::Inf2, None),
            )))),
        }
    }

    fn search(&self, key: &K, guard: &Guard) -> (Cursor<K, V>, *mut Update<K, V>) {
        // Create a memory space for a new Update operation of Insert/Delete
        let empty_update = Box::into_raw(Box::new(Update::<K, V>::default()));
        let mut cursor;

        read_phase!(guard => {
            cursor = Cursor::new(self.root.load(Ordering::Relaxed));
            cursor.search(key);

            guard.protect(empty_update);
            cursor.protect(guard);
            if let Some(update) = unsafe { untagged(cursor.gpupdate).as_ref() } {
                update.protect(guard);
            }
            if let Some(update) = unsafe { untagged(cursor.pupdate).as_ref() } {
                update.protect(guard);
            }
        });
        (cursor, empty_update)
    }

    pub fn find<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g Node<K, V>> {
        let (cursor, empty_update) = self.search(key, guard);

        // In Find, previously allocated `empty_update` is not needed.
        drop(unsafe { Box::from_raw(empty_update) });

        let l_node = unsafe { &*untagged(cursor.l) };
        if l_node.key.eq(key) {
            Some(l_node)
        } else {
            None
        }
    }

    pub fn insert(&self, key: &K, value: V, guard: &Guard) -> bool {
        loop {
            let (cursor, new_pupdate) = self.search(key, guard);
            let l_node = unsafe { &*cursor.l };
            let p_node = unsafe { &*cursor.p };

            if l_node.key == *key {
                drop(unsafe { Box::from_raw(new_pupdate) });
                return false;
            } else if tag(cursor.pupdate) != UpdateTag::CLEAN.bits() {
                drop(unsafe { Box::from_raw(new_pupdate) });
                self.help(cursor.pupdate, guard);
            } else {
                let new = Node::leaf(Key::Fin(key.clone()), Some(value.clone()));
                let new_sibling = Node::leaf(l_node.key.clone(), l_node.value.clone());

                let (left, right) = match new.key.partial_cmp(&new_sibling.key) {
                    Some(std::cmp::Ordering::Less) => (new, new_sibling),
                    _ => (new_sibling, new),
                };

                let new_internal = Box::into_raw(Box::new(Node::internal(
                    // key field max(k, l → key)
                    right.key.clone(),
                    None,
                    // two child fields equal to new and newSibling
                    // (the one with the smaller key is the left child)
                    left,
                    right,
                )));

                *unsafe { &mut *new_pupdate } = Update::Insert {
                    p: cursor.p,
                    new_internal,
                    l: cursor.l,
                    p_l_dir: cursor.p_l_dir,
                };

                let new_pupdate = tagged(new_pupdate, UpdateTag::IFLAG.bits());

                match p_node.update.compare_exchange(
                    cursor.pupdate,
                    new_pupdate,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        if !cursor.pupdate.is_null() {
                            unsafe { guard.retire(cursor.pupdate) };
                        }
                        self.help_insert(new_pupdate, guard);
                        return true;
                    }
                    Err(_) => unsafe {
                        let new_pupdate_failed = Box::from_raw(untagged(new_pupdate));
                        if let Update::Insert { new_internal, .. } = *new_pupdate_failed {
                            let new_internal_failed = Box::from_raw(new_internal);
                            drop(Box::from_raw(
                                new_internal_failed.left.load(Ordering::Relaxed),
                            ));
                            drop(Box::from_raw(
                                new_internal_failed.right.load(Ordering::Relaxed),
                            ));
                        }
                    },
                }
            }
        }
    }

    pub fn delete<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        loop {
            let (cursor, new_update) = self.search(key, guard);

            if cursor.gp.is_null() {
                // The tree is empty. There's no more things to do.
                drop(unsafe { Box::from_raw(new_update) });
                return None;
            }

            let l_node = unsafe { cursor.l.as_ref().unwrap() };

            if l_node.key != Key::Fin(key.clone()) {
                drop(unsafe { Box::from_raw(new_update) });
                return None;
            }
            if tag(cursor.gpupdate) != UpdateTag::CLEAN.bits() {
                drop(unsafe { Box::from_raw(new_update) });
                self.help(cursor.gpupdate, guard);
            } else if tag(cursor.pupdate) != UpdateTag::CLEAN.bits() {
                drop(unsafe { Box::from_raw(new_update) });
                self.help(cursor.pupdate, guard);
            } else {
                *unsafe { &mut *new_update } = Update::Delete {
                    gp: cursor.gp,
                    p: cursor.p,
                    l: cursor.l,
                    l_other: cursor.l_other,
                    pupdate: cursor.pupdate,
                    gp_p_dir: cursor.gp_p_dir,
                };

                let new_update = tagged(new_update, UpdateTag::DFLAG.bits());

                match unsafe { cursor.gp.as_ref().unwrap() }
                    .update
                    .compare_exchange(
                        cursor.gpupdate,
                        new_update,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                    Ok(_) => {
                        if !cursor.gpupdate.is_null() {
                            unsafe { guard.retire(cursor.gpupdate) };
                        }
                        if self.help_delete(new_update, guard) {
                            return Some(l_node.value.as_ref().unwrap());
                        }
                    }
                    Err(_) => {
                        unsafe { drop(Box::from_raw(untagged(new_update))) };
                    }
                }
            }
        }
    }

    fn help(&self, update: *mut Update<K, V>, guard: &Guard) {
        match UpdateTag::from_bits_truncate(tag(update)) {
            UpdateTag::IFLAG => self.help_insert(update, guard),
            UpdateTag::MARK => self.help_marked(update, guard),
            UpdateTag::DFLAG => {
                let _ = self.help_delete(update, guard);
            }
            _ => {}
        }
    }

    fn help_delete(&self, op: *mut Update<K, V>, guard: &Guard) -> bool {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { untagged(op).as_ref().unwrap() };
        if let &Update::Delete { gp, p, pupdate, .. } = op_ref {
            let p_ref = unsafe { p.as_ref().unwrap() };
            let new_op = tagged(op, UpdateTag::MARK.bits());

            match p_ref.update.compare_exchange(
                pupdate,
                new_op,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if !pupdate.is_null() {
                        unsafe { guard.retire(pupdate) };
                    }
                    // (prev value) = op → pupdate
                    self.help_marked(new_op, guard);
                    return true;
                }
                Err(e) => {
                    if e == new_op {
                        // (prev value) = <Mark, op>
                        self.help_marked(new_op, guard);
                        return true;
                    } else {
                        let _ = unsafe { gp.as_ref().unwrap() }.update.compare_exchange(
                            tagged(op, UpdateTag::DFLAG.bits()),
                            tagged(op, UpdateTag::CLEAN.bits()),
                            Ordering::Release,
                            Ordering::Relaxed,
                        );
                        return false;
                    }
                }
            }
        } else {
            panic!("op is not pointing to a DInfo record")
        }
    }

    fn help_marked(&self, op: *mut Update<K, V>, guard: &Guard) {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { untagged(op).as_ref().unwrap() };
        if let &Update::Delete {
            gp,
            p,
            l,
            l_other,
            gp_p_dir,
            ..
        } = op_ref
        {
            let gp_ref = unsafe { gp.as_ref().unwrap() };

            // dchild CAS
            if gp_ref
                .child(gp_p_dir)
                .compare_exchange(p, l_other, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                unsafe {
                    guard.retire(l);
                    guard.retire(p);
                }
            }

            // dunflag CAS
            let _ = gp_ref.update.compare_exchange(
                tagged(op, UpdateTag::DFLAG.bits()),
                tagged(op, UpdateTag::CLEAN.bits()),
                Ordering::Release,
                Ordering::Relaxed,
            );
        } else {
            panic!("op is not pointing to a DInfo record")
        }
    }

    fn help_insert(&self, op: *mut Update<K, V>, guard: &Guard) {
        // Precondition: op points to an IInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { untagged(op).as_ref().unwrap() };
        if let &Update::Insert {
            p,
            new_internal,
            l,
            p_l_dir,
            ..
        } = op_ref
        {
            let p_ref = unsafe { p.as_ref().unwrap() };

            // ichild CAS
            if p_ref
                .child(p_l_dir)
                .compare_exchange(l, new_internal, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                unsafe { guard.retire(l) };
            }

            // iunflag CAS
            let _ = p_ref.update.compare_exchange(
                tagged(op, UpdateTag::IFLAG.bits()),
                tagged(op, UpdateTag::CLEAN.bits()),
                Ordering::Release,
                Ordering::Relaxed,
            );
        } else {
            panic!("op is not pointing to an IInfo record")
        }
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
        self.delete(key, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::EFRBTree;
    use crate::nbr::concurrent_map;

    #[test]
    fn smoke_efrb_tree() {
        concurrent_map::tests::smoke::<EFRBTree<i32, String>>(11);
    }
}
