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
    is_leaf: bool,
}

pub enum Update<K, V> {
    Insert {
        p: *mut Node<K, V>,
        new_internal: *mut Node<K, V>,
        l: *mut Node<K, V>,
    },
    Delete {
        gp: *mut Node<K, V>,
        p: *mut Node<K, V>,
        l: *mut Node<K, V>,
        pupdate: *mut Update<K, V>,
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
    /// NOTE: Use this function if `curr` is guaranteed to be one of the leaves.
    pub fn load_opposite(&self, curr: *mut Node<K, V>) -> *mut Node<K, V> {
        let left = self.left.load(Ordering::Acquire);
        if left == curr {
            self.right.load(Ordering::Acquire)
        } else {
            left
        }
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

    #[inline]
    fn validate_lower<'g>(&'g self) -> Option<(&'g Node<K, V>, &'g Node<K, V>)> {
        let p_node = unsafe { self.p.as_ref().unwrap() };
        let l_node = unsafe { self.l.as_ref().unwrap() };

        let left = p_node.left.load(Ordering::Acquire);
        let right = p_node.right.load(Ordering::Acquire);

        // Is l a child of p?
        if (self.l != left && self.l != right) || (self.l_other != left && self.l_other != right) {
            return None;
        }
        Some((p_node, l_node))
    }

    #[inline]
    fn validate_full<'g>(&'g self) -> Option<(&'g Node<K, V>, &'g Node<K, V>, &'g Node<K, V>)> {
        let (p_node, l_node) = some_or!(self.validate_lower(), return None);
        let gp_node = unsafe { self.gp.as_ref().unwrap() };

        // Is p a child of gp?
        if self.p != gp_node.left.load(Ordering::Acquire)
            && self.p != gp_node.right.load(Ordering::Acquire)
        {
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
                let update = node_ref.update.load(Ordering::Relaxed);
                if !untagged(update).is_null()
                    && tag(update) != UpdateTag::CLEAN.bits()
                    && tag(update) != UpdateTag::MARK.bits()
                {
                    drop(Box::from_raw(update));
                }
                drop(Box::from_raw(node));
            }
            let update = root.update.load(Ordering::Relaxed);
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
    fn search_inner<'domain, 'hp>(&self, key: &K, cursor: &mut Cursor<'domain, 'hp, K, V>) -> bool {
        cursor.l = self.root.load(Ordering::Relaxed);
        cursor.handle.l_h.protect_raw(cursor.l);
        light_membarrier();

        loop {
            let l_node = unsafe { cursor.l.as_ref() }.unwrap();
            if l_node.is_leaf {
                return true;
            }
            cursor.gp = cursor.p;
            cursor.p = cursor.l;
            mem::swap(&mut cursor.handle.gp_h, &mut cursor.handle.p_h);
            mem::swap(&mut cursor.handle.p_h, &mut cursor.handle.l_h);

            cursor.gpupdate = cursor.pupdate;
            mem::swap(&mut cursor.handle.gpupdate_h, &mut cursor.handle.pupdate_h);

            // Protect correct `update` of the parent node. (currently it is l_node)
            cursor.pupdate = l_node.update.load(Ordering::Acquire);
            loop {
                cursor
                    .handle
                    .pupdate_h
                    .protect_raw(untagged(cursor.pupdate));
                light_membarrier();
                let new_pupdate = l_node.update.load(Ordering::Acquire);
                if cursor.pupdate == new_pupdate {
                    break;
                }
                cursor.pupdate = new_pupdate;
            }

            // Check if the parent node is marked.
            if tag(cursor.pupdate) == UpdateTag::MARK.bits() {
                // If update is already reclaimed, it will be a null pointer.
                // Even if the update is null, current searching must be restarted.
                if !untagged(cursor.pupdate).is_null() {
                    // Help cleaning marked node on search, and restart.
                    if let &Update::Delete { gp, p, l, .. } = unsafe { &*untagged(cursor.pupdate) }
                    {
                        cursor.handle.gp_h.protect_raw(gp);
                        cursor.handle.p_h.protect_raw(p);
                        cursor.handle.l_h.protect_raw(l);
                        cursor
                            .handle
                            .l_other_h
                            .protect_raw(unsafe { &*p }.load_opposite(l));
                        light_membarrier();
                    }
                    if cursor.pupdate == l_node.update.load(Ordering::Acquire) {
                        self.help_marked(cursor.pupdate);
                    }
                }
                return false;
            }
            light_membarrier();

            // Load correct next nodes of the leaf.
            loop {
                let (l_src, l_other_src) = match l_node.key.cmp(key) {
                    std::cmp::Ordering::Greater => (&l_node.left, &l_node.right),
                    _ => (&l_node.right, &l_node.left),
                };
                cursor.l = l_src.load(Ordering::Acquire);
                cursor.handle.l_h.protect_raw(cursor.l);
                light_membarrier();
                if cursor.l != l_src.load(Ordering::Acquire) {
                    // Somebody `inserted an internal` or `deleted (leaf, parent) pair`.
                    continue;
                }
                cursor.l_other = l_other_src.load(Ordering::Acquire);
                cursor.handle.l_other_h.protect_raw(cursor.l_other);
                light_membarrier();
                if cursor.l_other != l_other_src.load(Ordering::Acquire) {
                    continue;
                }
                if cursor.l == l_src.load(Ordering::Acquire)
                    && cursor.l_other == l_other_src.load(Ordering::Acquire)
                {
                    break;
                }
            }

            // Double-check whether the parent is marked or changed.
            // The protected leaves maybe already relclaimed if right after
            // we checked a tag of cursor.pupdate, other thread retired the parent and its leaves.
            if unsafe { &*cursor.p }.update.load(Ordering::Acquire) != cursor.pupdate {
                return false;
            }
        }
    }

    fn search<'domain, 'hp>(&self, key: &K, cursor: &mut Cursor<'domain, 'hp, K, V>) {
        loop {
            cursor.reset();
            if self.search_inner(key, cursor) {
                break;
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
                    p: cursor.p,
                    new_internal,
                    l: cursor.l,
                };

                let new_pupdate = tagged(Box::into_raw(Box::new(op)), UpdateTag::IFLAG.bits());

                handle.aux_node_h.protect_raw(new_internal);
                handle.aux_update_h.protect_raw(untagged(new_pupdate));
                light_membarrier();

                // iflag CAS
                match p_node.update.compare_exchange(
                    cursor.pupdate,
                    new_pupdate,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.help_insert(new_pupdate);
                        return true;
                    }
                    Err(current) => {
                        unsafe {
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
                    gp: cursor.gp,
                    p: cursor.p,
                    l: cursor.l,
                    pupdate: cursor.pupdate,
                };
                let new_update = tagged(Box::into_raw(Box::new(op)), UpdateTag::DFLAG.bits());
                handle.aux_update_h.protect_raw(untagged(new_update));
                light_membarrier();

                // dflag CAS
                match gp_node.update.compare_exchange(
                    cursor.gpupdate,
                    new_update,
                    Ordering::Release,
                    Ordering::Acquire,
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
        if op == op_src.load(Ordering::Acquire)
            && tag(op) != UpdateTag::CLEAN.bits()
            && tag(op) != UpdateTag::MARK.bits()
        {
            // Protect all nodes in op
            match unsafe { &*untagged(op) } {
                &Update::Insert { p, l, new_internal } => {
                    handle.aux_node_h.protect_raw(new_internal);
                    if p == cursor.gp {
                        if l != cursor.p {
                            handle.l_h.protect_raw(l);
                        }
                        handle
                            .l_other_h
                            .protect_raw(unsafe { &*p }.load_opposite(l));
                    } else {
                        handle.p_h.protect_raw(p);
                        if l == cursor.l {
                            handle
                                .l_other_h
                                .protect_raw(unsafe { &*p }.load_opposite(l));
                        } else {
                            handle.l_h.protect_raw(unsafe { &*p }.load_opposite(l));
                            handle.l_other_h.protect_raw(l);
                        }
                    }
                }
                &Update::Delete { gp, p, l, .. } => {
                    handle.gp_h.protect_raw(gp);
                    handle.p_h.protect_raw(p);
                    if p == cursor.p {
                        if l == cursor.l {
                            handle
                                .l_other_h
                                .protect_raw(unsafe { &*p }.load_opposite(l));
                        } else {
                            handle.l_h.protect_raw(unsafe { &*p }.load_opposite(l));
                            handle.l_other_h.protect_raw(l);
                        }
                    } else {
                        handle.l_h.protect_raw(l);
                        handle
                            .l_other_h
                            .protect_raw(unsafe { &*p }.load_opposite(l));
                    }
                }
            }
            light_membarrier();
            // Double-check after protecting
            if op != op_src.load(Ordering::Acquire) {
                return;
            }

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
        if let &Update::Delete { gp, p, pupdate, .. } = op_ref {
            let gp_ref = unsafe { gp.as_ref() }.unwrap();
            let p_ref = unsafe { p.as_ref() }.unwrap();
            let new_op = tagged(op, UpdateTag::MARK.bits());

            // mark CAS
            match p_ref.update.compare_exchange(
                pupdate,
                new_op,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // (prev value) = op → pupdate
                    self.help_marked(new_op);
                    return true;
                }
                Err(current) => {
                    if current == tagged(ptr::null_mut(), UpdateTag::MARK.bits()) {
                        // Some very fast guy already helped mark and reclaimed.
                        // This is not present on the original paper,
                        // but it is necessary to apply hazard pointers.
                        return true;
                    } else if current == new_op {
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
                                Ordering::Release,
                                Ordering::Relaxed,
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
        if let &Update::Delete { gp, p, l, .. } = op_ref {
            // Set other to point to the sibling of the node to which op → l points
            let p_node = unsafe { &*p };
            let l_other = p_node.load_opposite(l);

            // dchild CAS
            let _ = self.cas_child(gp, p, l_other);
            // dunflag CAS
            if unsafe { gp.as_ref().unwrap() }
                .update
                .compare_exchange(
                    tagged(op, UpdateTag::DFLAG.bits()),
                    tagged(op, UpdateTag::CLEAN.bits()),
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                unsafe {
                    let _ = p_node.update.store(
                        tagged(ptr::null_mut(), UpdateTag::MARK.bits()),
                        Ordering::Release,
                    );
                    // retire(untagged(op));
                    // retire(p);
                    // retire(l);
                }
            }
        } else {
            panic!("op is not pointing to a DInfo record")
        }
    }

    fn help_insert<'domain, 'hp>(&self, op: *mut Update<K, V>) {
        // Precondition: op points to an IInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { untagged(op).as_ref().unwrap() };
        if let &Update::Insert {
            p, new_internal, l, ..
        } = op_ref
        {
            // ichild CAS
            let _ = self.cas_child(p, l, new_internal);
            // iunflag CAS
            if unsafe { p.as_ref().unwrap() }
                .update
                .compare_exchange(
                    tagged(op, UpdateTag::IFLAG.bits()),
                    tagged(op, UpdateTag::CLEAN.bits()),
                    Ordering::Release,
                    Ordering::Relaxed,
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
        let new_node = unsafe { new.as_ref().unwrap() };
        let parent_node = unsafe { parent.as_ref().unwrap() };

        let node_to_cas = if new_node.key < parent_node.key {
            &parent_node.left
        } else {
            &parent_node.right
        };
        node_to_cas.compare_exchange(old, new, Ordering::Release, Ordering::Acquire)
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
