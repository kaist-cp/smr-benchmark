use core::{mem, ptr};
use std::sync::atomic::{AtomicPtr, Ordering};

use hp_pp::{light_membarrier, retire, tag, tagged, untagged, HazardPointer};

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
    update: AtomicPtr<Update<K, V>>,
    left: AtomicPtr<Node<K, V>>,
    right: AtomicPtr<Node<K, V>>,
}

#[derive(Clone, Copy)]
pub enum Direction {
    L,
    R,
}

impl Direction {
    #[inline]
    pub fn src<'g, K, V>(&self, node: &'g Node<K, V>) -> &'g AtomicPtr<Node<K, V>> {
        match self {
            Direction::L => &node.left,
            Direction::R => &node.right,
        }
    }

    #[inline]
    pub fn oppo(&self) -> Direction {
        match self {
            Direction::L => Direction::R,
            Direction::R => Direction::L,
        }
    }
}

pub enum Update<K, V> {
    Insert {
        p: AtomicPtr<Node<K, V>>,
        new_internal: AtomicPtr<Node<K, V>>,
        l: AtomicPtr<Node<K, V>>,
    },
    Delete {
        gp: AtomicPtr<Node<K, V>>,
        gp_p_dir: Direction,
        p: AtomicPtr<Node<K, V>>,
        p_l_dir: Direction,
        l: AtomicPtr<Node<K, V>>,
        l_other: AtomicPtr<Node<K, V>>,
        pupdate: AtomicPtr<Update<K, V>>,
    },
}

impl<K, V> Node<K, V> {
    pub fn internal(key: Key<K>, value: Option<V>, left: Self, right: Self) -> Self {
        Self {
            key,
            value,
            update: AtomicPtr::new(ptr::null_mut()),
            left: AtomicPtr::new(Box::into_raw(Box::new(left))),
            right: AtomicPtr::new(Box::into_raw(Box::new(right))),
        }
    }

    pub fn leaf(key: Key<K>, value: Option<V>) -> Self {
        Self {
            key,
            value,
            update: AtomicPtr::new(ptr::null_mut()),
            left: AtomicPtr::new(ptr::null_mut()),
            right: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.left.load(Ordering::Acquire).is_null()
    }
}

pub struct Handle<'domain> {
    gp_h: HazardPointer<'domain>,
    p_h: HazardPointer<'domain>,
    l_h: HazardPointer<'domain>,
    l_other_h: HazardPointer<'domain>,
    pupdate_h: HazardPointer<'domain>,
    gpupdate_h: HazardPointer<'domain>,
    aux_update_h: HazardPointer<'domain>,
    aux_node_h: HazardPointer<'domain>,
}

impl Default for Handle<'static> {
    fn default() -> Self {
        Self {
            gp_h: HazardPointer::default(),
            p_h: HazardPointer::default(),
            l_h: HazardPointer::default(),
            l_other_h: HazardPointer::default(),
            pupdate_h: HazardPointer::default(),
            gpupdate_h: HazardPointer::default(),
            aux_update_h: HazardPointer::default(),
            aux_node_h: HazardPointer::default(),
        }
    }
}

impl<'domain> Handle<'domain> {
    // bypass E0499-E0503, etc that are supposed to be fixed by polonius
    #[inline]
    fn launder<'hp1, 'hp2>(&'hp1 mut self) -> &'hp2 mut Self {
        unsafe { core::mem::transmute(self) }
    }
}

struct Cursor<'domain, 'hp, K, V> {
    gp: *mut Node<K, V>,
    gp_p_dir: Direction,
    p: *mut Node<K, V>,
    p_l_dir: Direction,
    l: *mut Node<K, V>,
    l_other: *mut Node<K, V>,
    pupdate: *mut Update<K, V>,
    gpupdate: *mut Update<K, V>,
    handle: &'hp mut Handle<'domain>,
}

impl<'domain, 'hp, K, V> Cursor<'domain, 'hp, K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new(handle: &'hp mut Handle<'domain>) -> Self {
        Self {
            gp: ptr::null_mut(),
            gp_p_dir: Direction::L,
            p: ptr::null_mut(),
            p_l_dir: Direction::L,
            l: ptr::null_mut(),
            l_other: ptr::null_mut(),
            pupdate: ptr::null_mut(),
            gpupdate: ptr::null_mut(),
            handle,
        }
    }

    #[inline]
    fn validate_lower<'g>(&'g self) -> Option<(&'g Node<K, V>, &'g Node<K, V>)> {
        let p_node = unsafe { self.p.as_ref().unwrap() };
        let l_node = unsafe { self.l.as_ref().unwrap() };

        // Is l a child of p?
        if self.p_l_dir.src(p_node).load(Ordering::SeqCst) != self.l {
            return None;
        }
        Some((p_node, l_node))
    }

    #[inline]
    fn validate_full<'g>(&'g self) -> Option<(&'g Node<K, V>, &'g Node<K, V>, &'g Node<K, V>)> {
        let (p_node, l_node) = some_or!(self.validate_lower(), return None);
        let gp_node = unsafe { self.gp.as_ref().unwrap() };

        // Is p a child of gp?
        if self.gp_p_dir.src(gp_node).load(Ordering::SeqCst) != self.p {
            return None;
        }
        Some((gp_node, p_node, l_node))
    }
}

pub struct EFRBTree<K, V> {
    root: AtomicPtr<Node<K, V>>,
}

impl<K, V> Default for EFRBTree<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
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
                let update = untagged(node_ref.update.load(Ordering::Relaxed));
                if !update.is_null() {
                    drop(Box::from_raw(update));
                }
                drop(Box::from_raw(node));
            }
            let update = untagged(root.update.load(Ordering::Relaxed));
            if !update.is_null() {
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
    fn search<'domain, 'hp>(&self, key: &K, cursor: &mut Cursor<'domain, 'hp, K, V>) {
        cursor.l = self.root.load(Ordering::SeqCst);
        cursor.handle.l_h.protect_raw(cursor.l);

        loop {
            let l_node = unsafe { cursor.l.as_ref() }.unwrap();
            if l_node.is_leaf() {
                break;
            }
            cursor.gp = cursor.p;
            cursor.p = cursor.l;
            cursor.gp_p_dir = cursor.p_l_dir;
            mem::swap(&mut cursor.handle.gp_h, &mut cursor.handle.p_h);
            mem::swap(&mut cursor.handle.p_h, &mut cursor.handle.l_h);

            cursor.gpupdate = cursor.pupdate;
            mem::swap(&mut cursor.handle.gpupdate_h, &mut cursor.handle.pupdate_h);

            // Acquire correct `update` of the parent node. (currently it is l_node)
            cursor.pupdate = l_node.update.load(Ordering::SeqCst);
            loop {
                cursor
                    .handle
                    .pupdate_h
                    .protect_raw(untagged(cursor.pupdate));
                light_membarrier();
                let new_pupdate = l_node.update.load(Ordering::SeqCst);
                if cursor.pupdate == new_pupdate {
                    break;
                }
                cursor.pupdate = new_pupdate;
            }

            // Acquire correct next nodes of the leaf.
            loop {
                let (l_src, l_other_src, dir) = match l_node.key.cmp(key) {
                    std::cmp::Ordering::Greater => (&l_node.left, &l_node.right, Direction::L),
                    _ => (&l_node.right, &l_node.left, Direction::R),
                };
                cursor.p_l_dir = dir;
                cursor.l = l_src.load(Ordering::SeqCst);
                cursor.l_other = l_other_src.load(Ordering::SeqCst);
                cursor.handle.l_h.protect_raw(cursor.l);
                cursor.handle.l_other_h.protect_raw(cursor.l_other);
                light_membarrier();
                if cursor.l == l_src.load(Ordering::SeqCst)
                    && cursor.l_other == l_other_src.load(Ordering::SeqCst)
                {
                    break;
                }
            }
        }
    }

    pub fn find<'domain, 'hp>(&self, key: &K, handle: &'hp mut Handle<'domain>) -> Option<&'hp V> {
        let mut cursor = Cursor::new(handle);
        self.search(key, &mut cursor);
        let l_node = unsafe { &*cursor.l };
        if l_node.key.eq(key) {
            l_node.value.as_ref()
        } else {
            None
        }
    }

    pub fn insert<'domain, 'hp>(
        &self,
        key: &K,
        value: V,
        handle: &'hp mut Handle<'domain>,
    ) -> bool {
        loop {
            let mut cursor = Cursor::new(handle.launder());
            self.search(key, &mut cursor);
            let (p_node, l_node) = some_or!(cursor.validate_lower(), continue);

            if l_node.key == *key {
                return false;
            } else if tag(cursor.pupdate) != UpdateTag::CLEAN.bits() {
                self.help(cursor.pupdate, &p_node.update, &cursor, handle);
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

                let op = Update::Insert {
                    p: AtomicPtr::new(cursor.p),
                    new_internal: AtomicPtr::new(new_internal),
                    l: AtomicPtr::new(cursor.l),
                };

                let new_pupdate = tagged(Box::into_raw(Box::new(op)), UpdateTag::IFLAG.bits());

                handle.aux_node_h.protect_raw(new_internal);
                handle.aux_update_h.protect_raw(untagged(new_pupdate));
                light_membarrier();

                match p_node.update.compare_exchange(
                    cursor.pupdate,
                    new_pupdate,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        if !untagged(cursor.pupdate).is_null() {
                            unsafe { retire(untagged(cursor.pupdate)) };
                        }
                        self.help_insert(new_pupdate);
                        return true;
                    }
                    Err(current) => {
                        unsafe {
                            let new_pupdate_failed = Box::from_raw(untagged(new_pupdate));
                            if let Update::Insert { new_internal, .. } = *new_pupdate_failed {
                                let new_internal_failed =
                                    Box::from_raw(new_internal.load(Ordering::SeqCst));
                                drop(Box::from_raw(
                                    new_internal_failed.left.load(Ordering::SeqCst),
                                ));
                                drop(Box::from_raw(
                                    new_internal_failed.right.load(Ordering::SeqCst),
                                ));
                            }
                        }
                        self.help(current, &p_node.update, &cursor, handle);
                    }
                }
            }
        }
    }

    pub fn delete<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V> {
        loop {
            let mut cursor = Cursor::new(handle.launder());
            self.search(key, &mut cursor);

            if cursor.gp.is_null() {
                // The tree is empty. There's no more things to do.
                return None;
            }
            let (gp_node, p_node, l_node) = some_or!(cursor.validate_full(), continue);

            if l_node.key != Key::Fin(key.clone()) {
                return None;
            }
            if tag(cursor.gpupdate) != UpdateTag::CLEAN.bits() {
                self.help(cursor.gpupdate, &gp_node.update, &cursor, handle);
            } else if tag(cursor.pupdate) != UpdateTag::CLEAN.bits() {
                self.help(cursor.pupdate, &p_node.update, &cursor, handle);
            } else {
                let op = Update::Delete {
                    gp: AtomicPtr::new(cursor.gp),
                    gp_p_dir: cursor.gp_p_dir,
                    p: AtomicPtr::new(cursor.p),
                    p_l_dir: cursor.p_l_dir,
                    l: AtomicPtr::new(cursor.l),
                    l_other: AtomicPtr::new(cursor.l_other),
                    pupdate: AtomicPtr::new(cursor.pupdate),
                };
                let new_update = tagged(Box::into_raw(Box::new(op)), UpdateTag::DFLAG.bits());
                handle.aux_update_h.protect_raw(untagged(new_update));
                light_membarrier();

                match gp_node.update.compare_exchange(
                    cursor.gpupdate,
                    new_update,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        if !untagged(cursor.gpupdate).is_null() {
                            unsafe { retire(untagged(cursor.gpupdate)) };
                        }
                        if self.help_delete(new_update, &cursor, handle) {
                            // SAFETY: dereferencing the value of leaf node is safe until `handle` is dropped.
                            return Some(unsafe { mem::transmute(l_node.value.as_ref().unwrap()) });
                        }
                    }
                    Err(current) => {
                        unsafe { drop(Box::from_raw(untagged(new_update))) };
                        self.help(current, &gp_node.update, &cursor, handle);
                    }
                }
            }
        }
    }

    #[inline]
    fn help<'domain, 'hp>(
        &self,
        op: *mut Update<K, V>,
        op_src: &AtomicPtr<Update<K, V>>,
        cursor: &Cursor<'domain, 'hp, K, V>,
        handle: &'hp mut Handle<'domain>,
    ) {
        handle.aux_update_h.protect_raw(untagged(op));
        light_membarrier();
        if op != op_src.load(Ordering::SeqCst) {
            return;
        }
        match UpdateTag::from_bits_truncate(tag(op)) {
            UpdateTag::IFLAG => self.help_insert(op),
            // UpdateTag::MARK => self.help_marked(op),
            UpdateTag::DFLAG => {
                if ptr::eq(op_src, &unsafe { &*cursor.p }.update) {
                    // help_delete requires to protect current node with gp_h.
                    mem::swap(&mut handle.gp_h, &mut handle.p_h);
                }
                let _ = self.help_delete(op, cursor, handle);
            }
            _ => {}
        }
    }

    fn help_delete<'domain, 'hp>(
        &self,
        op: *mut Update<K, V>,
        cursor: &Cursor<'domain, 'hp, K, V>,
        handle: &'hp mut Handle<'domain>,
    ) -> bool {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { untagged(op).as_ref().unwrap() };
        if let Update::Delete {
            gp,
            gp_p_dir,
            p,
            pupdate,
            p_l_dir,
            l,
            l_other,
        } = op_ref
        {
            // gp is already protected by gp_h.
            let gp_ptr = gp.load(Ordering::SeqCst);
            let gp_ref = unsafe { gp_ptr.as_ref() }.unwrap();

            let p_ptr = p.load(Ordering::SeqCst);
            let p_ref = unsafe { p_ptr.as_ref().unwrap() };
            let pupdate_ptr = pupdate.load(Ordering::SeqCst);
            handle.p_h.protect_raw(p_ptr);
            handle.pupdate_h.protect_raw(untagged(pupdate_ptr));
            light_membarrier();
            if p_ptr != gp_p_dir.src(gp_ref).load(Ordering::SeqCst) {
                // The subtree is changed.
                // It means somebody has already done `Delete` operation.
                let _ = gp_ref.update.compare_exchange(
                    tagged(op, UpdateTag::DFLAG.bits()),
                    tagged(op, UpdateTag::CLEAN.bits()),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                return true;
            }

            let l_ptr = l.load(Ordering::SeqCst);
            let l_other_ptr = l_other.load(Ordering::SeqCst);
            handle.l_h.protect_raw(l_ptr);
            handle.l_other_h.protect_raw(l_other_ptr);
            light_membarrier();
            if l_ptr != p_l_dir.src(p_ref).load(Ordering::SeqCst)
                || l_other_ptr != p_l_dir.oppo().src(p_ref).load(Ordering::SeqCst)
            {
                // The subtree is changed.
                // The parent is not changed but the leaf is changed?
                // Maybe somebody could inserted a new node.
                let _ = gp_ref.update.compare_exchange(
                    tagged(op, UpdateTag::DFLAG.bits()),
                    tagged(op, UpdateTag::CLEAN.bits()),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                return false;
            }

            let pupdate_ptr = pupdate.load(Ordering::SeqCst);
            let new_op = tagged(op, UpdateTag::MARK.bits());

            match p_ref.update.compare_exchange(
                pupdate_ptr,
                new_op,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if !untagged(pupdate_ptr).is_null() {
                        unsafe { retire(untagged(pupdate_ptr)) };
                    }
                    // (prev value) = op → pupdate
                    self.help_marked(new_op);
                    return true;
                }
                Err(current) => {
                    if current == new_op {
                        // (prev value) = <Mark, op>
                        self.help_marked(new_op);
                        return true;
                    } else {
                        self.help(current, &p_ref.update, cursor, handle);
                        let _ = gp_ref.update.compare_exchange(
                            tagged(op, UpdateTag::DFLAG.bits()),
                            tagged(op, UpdateTag::CLEAN.bits()),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        );
                        return false;
                    }
                }
            }
        } else {
            panic!("op is not pointing to a DInfo record")
        }
    }

    fn help_marked<'domain, 'hp>(&self, op: *mut Update<K, V>) {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { untagged(op).as_ref().unwrap() };
        if let Update::Delete {
            gp, p, l, l_other, ..
        } = op_ref
        {
            // Set other to point to the sibling of the node to which op → l points
            let gp = gp.load(Ordering::SeqCst);
            let p = p.load(Ordering::SeqCst);
            let l = l.load(Ordering::SeqCst);
            let l_other = l_other.load(Ordering::SeqCst);

            // Splice the node to which op → p points out of the tree, replacing it by other
            if self.cas_child(gp, p, l_other).is_ok() {
                let _ = unsafe { gp.as_ref().unwrap() }.update.compare_exchange(
                    tagged(op, UpdateTag::DFLAG.bits()),
                    tagged(op, UpdateTag::CLEAN.bits()),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                unsafe {
                    retire(p);
                    retire(l);
                }
            }
        } else {
            panic!("op is not pointing to a DInfo record")
        }
    }

    fn help_insert<'domain, 'hp>(&self, op: *mut Update<K, V>) {
        // Precondition: op points to an IInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { untagged(op).as_ref().unwrap() };
        if let Update::Insert { p, new_internal, l } = op_ref {
            let p = p.load(Ordering::SeqCst);
            let new_internal = new_internal.load(Ordering::SeqCst);
            let l = l.load(Ordering::SeqCst);

            if self.cas_child(p, l, new_internal).is_ok() {
                unsafe { retire(l) };
            }
            let p_ref = unsafe { p.as_ref().unwrap() };
            let _ = p_ref.update.compare_exchange(
                tagged(op, UpdateTag::IFLAG.bits()),
                tagged(op, UpdateTag::CLEAN.bits()),
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        } else {
            panic!("op is not pointing to an IInfo record")
        }
    }

    #[inline]
    fn cas_child<'g>(
        &'g self,
        parent: *mut Node<K, V>,
        old: *mut Node<K, V>,
        new: *mut Node<K, V>,
    ) -> Result<*mut Node<K, V>, *mut Node<K, V>> {
        // Precondition: parent points to an Internal node and new points to a Node (i.e., neither is ⊥)
        // This routine tries to change one of the child fields of the node that parent points to from old to new.
        let new_node = unsafe { new.as_ref().unwrap() };
        let parent_node = unsafe { parent.as_ref().unwrap() };

        let node_to_cas = if new_node.key < parent_node.key {
            &parent_node.left
        } else {
            &parent_node.right
        };
        node_to_cas.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst)
    }
}

impl<K, V> ConcurrentMap<K, V> for EFRBTree<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Handle<'domain> = Handle<'domain>;

    fn new() -> Self {
        Self::new()
    }

    fn handle() -> Self::Handle<'static> {
        Handle::default()
    }

    #[inline]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        match self.find(key, handle) {
            Some(value) => Some(value),
            None => None,
        }
    }

    #[inline]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.insert(&key, value, handle)
    }

    #[inline]
    fn remove<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: &K,
    ) -> Option<&'hp V> {
        self.delete(key, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::EFRBTree;
    use crate::hp::concurrent_map;

    #[test]
    fn smoke_efrb_tree() {
        concurrent_map::tests::smoke::<EFRBTree<i32, String>>();
    }
}
