//! To apply hazard pointers on EFRBTree, it must have slightly modified implementation.
//! (according to the authors)
//!
//! 1. Search helps Delete operations to perform their dchild CAS steps
//!   to remove from the tree marked nodes that the Search encounters.
//!
//! 2. More specifically, retirement of tree nodes and Info records
//!   could be performed when an unflag (or backtrack) CAS takes place.
//!
//! 3. Search would maintain a hazard pointer to each of the nodes pointed to by
//!   gp, p, l and l’s sibling, as it traverses its search path.
//!
//! 4. Each time an operation O helps another operation O', O first ensures
//!   that hazard pointers are set to point to the Info record f of O',
//!   and to the nodes pointed to by f.gp, f.p, f.l and f.l’s sibling.
//!
//! 5. This may require storing more information in Info records.
//!   For example, it might be helpful to store an additional bit indicating
//!   whether the Info record is retired or not.
//!   This bit can be updated to True with an additional CAS immediately
//!   after an unflag or backtrack CAS.
//!
//!   * On our implementation, instead of adding another tag bit,
//!     store null pointer with MARK tag after destroying marked node.

use core::{mem, ptr};
use std::sync::atomic::{AtomicPtr, Ordering};

use hp_pp::{light_membarrier, retire, tag, tagged, untagged, HazardPointer};

use super::concurrent_map::ConcurrentMap;

bitflags! {
    struct UpdateTag: usize {
        const CLEAN = 0b00;
        const DFLAG = 0b01;
        const IFLAG = 0b10;
        const MARK = 0b11;
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

pub enum Update<K, V> {
    Insert {
        p: AtomicPtr<Node<K, V>>,
        new_internal: AtomicPtr<Node<K, V>>,
        l: AtomicPtr<Node<K, V>>,
        l_other: AtomicPtr<Node<K, V>>,
    },
    Delete {
        gp: AtomicPtr<Node<K, V>>,
        p: AtomicPtr<Node<K, V>>,
        l: AtomicPtr<Node<K, V>>,
        l_other: AtomicPtr<Node<K, V>>,
        pupdate: AtomicPtr<Update<K, V>>,
    },
}

impl<K, V> Node<K, V> {
    pub fn internal(key: Key<K>, value: Option<V>, left: Self, right: Self) -> Self {
        let left_node = Box::into_raw(Box::new(left));
        let left = AtomicPtr::new(ptr::null_mut());
        left.store(left_node, Ordering::SeqCst);

        let right_node = Box::into_raw(Box::new(right));
        let right = AtomicPtr::new(ptr::null_mut());
        right.store(right_node, Ordering::SeqCst);

        Self {
            key,
            value,
            update: AtomicPtr::new(ptr::null_mut()),
            left,
            right,
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
        let left = self.left.load(Ordering::SeqCst);
        let right = self.right.load(Ordering::SeqCst);
        assert!(left as usize != 5);
        assert!(right as usize != 5);
        assert!(left.is_null() == right.is_null(), "{:p}, {:p}", left, right);
        left.is_null()
    }
}

pub struct Handle<'domain> {
    gp_h: HazardPointer<'domain>,
    p_h: HazardPointer<'domain>,
    l_h: HazardPointer<'domain>,
    l_other_h: HazardPointer<'domain>,
    pupdate_h: HazardPointer<'domain>,
    gpupdate_h: HazardPointer<'domain>,
    // Used for protecting new updates and updates from other nodes to help
    aux_update_h: HazardPointer<'domain>,
    // Used for protecting a new node of insertion
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

    fn release(&mut self) {
        self.gp_h.reset_protection();
        self.p_h.reset_protection();
        self.l_h.reset_protection();
        self.l_other_h.reset_protection();
        self.pupdate_h.reset_protection();
        self.gpupdate_h.reset_protection();
        self.aux_update_h.reset_protection();
        self.aux_node_h.reset_protection();
    }
}

struct Cursor<'domain, 'hp, K, V> {
    gp: *mut Node<K, V>,
    p: *mut Node<K, V>,
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
            p: ptr::null_mut(),
            l: ptr::null_mut(),
            l_other: ptr::null_mut(),
            pupdate: ptr::null_mut(),
            gpupdate: ptr::null_mut(),
            handle,
        }
    }

    fn reset(&mut self) {
        self.gp = ptr::null_mut();
        self.p = ptr::null_mut();
        self.l = ptr::null_mut();
        self.l_other = ptr::null_mut();
        self.pupdate = ptr::null_mut();
        self.gpupdate = ptr::null_mut();
        self.handle.release();
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
            let root = Box::from_raw(self.root.load(Ordering::SeqCst));
            let mut stack = vec![
                root.left.load(Ordering::SeqCst),
                root.right.load(Ordering::SeqCst),
            ];

            while let Some(node) = stack.pop() {
                if node.is_null() {
                    continue;
                }

                let node_ref = &*node;

                stack.push(node_ref.left.load(Ordering::SeqCst));
                stack.push(node_ref.right.load(Ordering::SeqCst));
                let update = node_ref.update.load(Ordering::SeqCst);
                if !untagged(update).is_null()
                    && tag(update) != UpdateTag::CLEAN.bits()
                    && tag(update) != UpdateTag::MARK.bits()
                {
                    drop(Box::from_raw(update));
                }
                drop(Box::from_raw(node));
            }
            let update = root.update.load(Ordering::SeqCst);
            if !untagged(update).is_null()
                && tag(update) != UpdateTag::CLEAN.bits()
                && tag(update) != UpdateTag::MARK.bits()
            {
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
        let root_node = Box::into_raw(Box::new(Node::internal(
            Key::Inf2,
            None,
            Node::leaf(Key::Inf1, None),
            Node::leaf(Key::Inf2, None),
        )));
        let root = AtomicPtr::new(ptr::null_mut());
        root.store(root_node, Ordering::SeqCst);

        Self { root }
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
    fn search_inner<'domain, 'hp>(&self, key: &K, cursor: &mut Cursor<'domain, 'hp, K, V>) -> bool {
        cursor.l = self.root.load(Ordering::SeqCst);
        cursor.handle.l_h.protect_raw(cursor.l);
        assert!(cursor.l as usize != 5);
        light_membarrier();

        loop {
            // Check if the parent node is marked.
            if tag(cursor.pupdate) == UpdateTag::MARK.bits() {
                // If update is already reclaimed, it will be a null pointer.
                // Even if the update is null, current searching must be restarted.
                if !untagged(cursor.pupdate).is_null() {
                    // Help cleaning marked node on search, and restart.
                    if let Update::Delete {
                        gp, p, l, l_other, ..
                    } = unsafe { &*untagged(cursor.pupdate) }
                    {
                        let op_gp = gp.load(Ordering::Relaxed);
                        let op_p = p.load(Ordering::Relaxed);
                        let op_l = l.load(Ordering::Relaxed);
                        let op_l_other = l_other.load(Ordering::Relaxed);
                        // Check whether all required items are protected.
                        // This check is necessary because the operation's nodes
                        // may be retired by other thread.
                        if op_gp == cursor.gp
                            && op_p == cursor.p
                            && ((op_l == cursor.l && op_l_other == cursor.l_other)
                                || (op_l == cursor.l_other && op_l_other == cursor.l))
                        {
                            self.help_marked(cursor.pupdate);
                        }
                    }
                }
                return false;
            }

            let l_node = unsafe { cursor.l.as_ref() }.unwrap();
            if l_node.is_leaf() {
                return true;
            }
            cursor.gp = cursor.p;
            cursor.p = cursor.l;
            mem::swap(&mut cursor.handle.gp_h, &mut cursor.handle.p_h);
            mem::swap(&mut cursor.handle.p_h, &mut cursor.handle.l_h);

            cursor.gpupdate = cursor.pupdate;
            mem::swap(&mut cursor.handle.gpupdate_h, &mut cursor.handle.pupdate_h);

            // Load correct `update` of the parent node. (currently it is l_node)
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

            // Load correct next nodes of the leaf.
            loop {
                let (l_src, l_other_src) = match l_node.key.cmp(key) {
                    std::cmp::Ordering::Greater => (&l_node.left, &l_node.right),
                    _ => (&l_node.right, &l_node.left),
                };
                cursor.l = l_src.load(Ordering::SeqCst);
                cursor.handle.l_h.protect_raw(cursor.l);
                light_membarrier();
                if cursor.l != l_src.load(Ordering::SeqCst)
                    || cursor.l.is_null()
                {
                    return false;
                }
                cursor.l_other = l_other_src.load(Ordering::SeqCst);
                cursor.handle.l_other_h.protect_raw(cursor.l_other);
                light_membarrier();
                if cursor.l_other != l_other_src.load(Ordering::SeqCst)
                    || cursor.l_other.is_null()
                {
                    return false;
                }
                break;
            }
        }
    }

    fn search<'domain, 'hp>(&self, key: &K, cursor: &mut Cursor<'domain, 'hp, K, V>) {
        loop {
            if self.search_inner(key, cursor) {
                break;
            } else {
                cursor.reset();
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
            let p_node = unsafe { &*cursor.p };
            let l_node = unsafe { &*cursor.l };

            assert!(!cursor.l.is_null() && cursor.l as usize != 5);
            assert!(!cursor.l_other.is_null() && cursor.l_other as usize != 5);

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

                let p_ptr = AtomicPtr::new(ptr::null_mut());
                let new_internal_ptr = AtomicPtr::new(ptr::null_mut());
                let l_ptr = AtomicPtr::new(ptr::null_mut());
                let l_other_ptr = AtomicPtr::new(ptr::null_mut());

                p_ptr.store(cursor.p, Ordering::SeqCst);
                new_internal_ptr.store(new_internal, Ordering::SeqCst);
                l_ptr.store(cursor.l, Ordering::SeqCst);
                l_other_ptr.store(cursor.l_other, Ordering::SeqCst);

                let op = Update::Insert {
                    p: p_ptr,
                    new_internal: new_internal_ptr,
                    l: l_ptr,
                    l_other: l_other_ptr,
                };

                let new_pupdate = tagged(Box::into_raw(Box::new(op)), UpdateTag::IFLAG.bits());

                handle.aux_node_h.protect_raw(new_internal);
                handle.aux_update_h.protect_raw(untagged(new_pupdate));
                light_membarrier();

                // iflag CAS
                match p_node.update.compare_exchange(
                    cursor.pupdate,
                    new_pupdate,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
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
            let gp_node = unsafe { &*cursor.gp };
            let p_node = unsafe { &*cursor.p };
            let l_node = unsafe { &*cursor.l };

            if l_node.key != Key::Fin(key.clone()) {
                return None;
            }
            if tag(cursor.gpupdate) != UpdateTag::CLEAN.bits() {
                self.help(cursor.gpupdate, &gp_node.update, &cursor, handle);
            } else if tag(cursor.pupdate) != UpdateTag::CLEAN.bits() {
                self.help(cursor.pupdate, &p_node.update, &cursor, handle);
            } else {
                let gp = AtomicPtr::new(ptr::null_mut());
                let p = AtomicPtr::new(ptr::null_mut());
                let l = AtomicPtr::new(ptr::null_mut());
                let l_other = AtomicPtr::new(ptr::null_mut());
                let pupdate = AtomicPtr::new(ptr::null_mut());

                gp.store(cursor.gp, Ordering::SeqCst);
                p.store(cursor.p, Ordering::SeqCst);
                l.store(cursor.l, Ordering::SeqCst);
                l_other.store(cursor.l_other, Ordering::SeqCst);
                pupdate.store(cursor.pupdate, Ordering::SeqCst);

                let op = Update::Delete {
                    gp,
                    p,
                    l,
                    l_other,
                    pupdate,
                };
                let new_update = tagged(Box::into_raw(Box::new(op)), UpdateTag::DFLAG.bits());
                handle.aux_update_h.protect_raw(untagged(new_update));
                light_membarrier();

                // dflag CAS
                match gp_node.update.compare_exchange(
                    cursor.gpupdate,
                    new_update,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
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
        if op == op_src.load(Ordering::SeqCst)
            && tag(op) != UpdateTag::CLEAN.bits()
            && tag(op) != UpdateTag::MARK.bits()
        {
            // Protect all nodes in op
            match unsafe { &*untagged(op) } {
                Update::Insert {
                    p,
                    l,
                    l_other,
                    new_internal,
                } => {
                    handle
                        .aux_node_h
                        .protect_raw(new_internal.load(Ordering::SeqCst));
                    light_membarrier();
                    let l = l.load(Ordering::SeqCst);
                    let l_other = l_other.load(Ordering::SeqCst);
                    if cursor.p != p.load(Ordering::SeqCst)
                        || (cursor.l != l && cursor.l != l_other)
                        || (cursor.l_other != l && cursor.l_other != l_other)
                    {
                        return;
                    }
                }
                Update::Delete {
                    gp, p, l, l_other, ..
                } => {
                    let l = l.load(Ordering::SeqCst);
                    let l_other = l_other.load(Ordering::SeqCst);
                    if cursor.gp != gp.load(Ordering::SeqCst)
                        || cursor.p != p.load(Ordering::SeqCst)
                        || (cursor.l != l && cursor.l != l_other)
                        || (cursor.l_other != l && cursor.l_other != l_other)
                    {
                        return;
                    }
                }
            }
            light_membarrier();

            // NOTE: help_marked is also called during `search`.
            match UpdateTag::from_bits_truncate(tag(op)) {
                UpdateTag::IFLAG => self.help_insert(op),
                UpdateTag::DFLAG => {
                    let _ = self.help_delete(op, cursor, handle);
                }
                _ => {}
            }
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
        if let Update::Delete { gp, p, pupdate, .. } = op_ref {
            let gp_ptr = gp.load(Ordering::SeqCst);
            let gp_ref = unsafe { gp_ptr.as_ref() }.unwrap();

            let p_ptr = p.load(Ordering::SeqCst);
            let p_ref = unsafe { p_ptr.as_ref().unwrap() };

            let pupdate_ptr = pupdate.load(Ordering::SeqCst);
            let new_op = tagged(op, UpdateTag::MARK.bits());

            // mark CAS
            match p_ref.update.compare_exchange(
                pupdate_ptr,
                new_op,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
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
                        // backtrack CAS
                        if gp_ref
                            .update
                            .compare_exchange(
                                tagged(op, UpdateTag::DFLAG.bits()),
                                tagged(op, UpdateTag::CLEAN.bits()),
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                        {
                            unsafe { retire(untagged(op)) };
                        }
                        self.help(current, &p_ref.update, cursor, handle);
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

            // dchild CAS
            let _ = self.cas_child(gp, p, l_other);
            // dunflag CAS
            if unsafe { gp.as_ref().unwrap() }
                .update
                .compare_exchange(
                    tagged(op, UpdateTag::DFLAG.bits()),
                    tagged(op, UpdateTag::CLEAN.bits()),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                unsafe {
                    let _ = (&*p).update.store(
                        tagged(ptr::null_mut(), UpdateTag::MARK.bits()),
                        Ordering::SeqCst,
                    );
                    retire(untagged(op));
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
        if let Update::Insert {
            p, new_internal, l, ..
        } = op_ref
        {
            let p = p.load(Ordering::SeqCst);
            let new_internal = new_internal.load(Ordering::SeqCst);
            let l = l.load(Ordering::SeqCst);

            // ichild CAS
            let _ = self.cas_child(p, l, new_internal);
            // iunflag CAS
            if unsafe { p.as_ref().unwrap() }
                .update
                .compare_exchange(
                    tagged(op, UpdateTag::IFLAG.bits()),
                    tagged(op, UpdateTag::CLEAN.bits()),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                unsafe {
                    retire(untagged(op));
                    retire(l);
                }
            }
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
        assert!(!new.is_null() && new as usize != 5);
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
