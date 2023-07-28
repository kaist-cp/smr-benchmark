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

use core::{mem, ptr, slice};
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

use hp_pp::{
    decompose_ptr, light_membarrier, tag, tagged, untagged, HazardPointer, ProtectError, Thread,
    DEFAULT_DOMAIN,
};

use crate::hp::concurrent_map::ConcurrentMap;

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct UpdateTag: usize {
        const CLEAN = 0b00;
        const DFLAG = 0b01;
        const IFLAG = 0b10;
        const MARKED = 0b11;
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

impl Direction {
    #[inline]
    fn opposite(&self) -> Direction {
        match self {
            Direction::L => Direction::R,
            Direction::R => Direction::L,
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

pub struct Update<K, V> {
    gp: *mut Node<K, V>,
    p: *mut Node<K, V>,
    l: *mut Node<K, V>,
    l_other: *mut Node<K, V>,
    gp_p_dir: Direction,
    p_l_dir: Direction,
    pupdate: *mut Update<K, V>,
    new_internal: *mut Node<K, V>,
    retired: AtomicBool,
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

    /// Protect correct `update` of the node.
    #[inline]
    fn protect_update<'domain>(&self, hazptr: &mut HazardPointer<'domain>) -> *mut Update<K, V> {
        let mut update = self.update.load(Ordering::Acquire);
        loop {
            hazptr.protect_raw(untagged(update));
            light_membarrier();
            let new_update = self.update.load(Ordering::Acquire);
            if update == new_update {
                break;
            }
            update = new_update;
        }
        update
    }

    #[inline]
    fn protect_next<'domain>(
        &self,
        left_h: &mut HazardPointer<'domain>,
        right_h: &mut HazardPointer<'domain>,
    ) -> Result<(*mut Self, *mut Self), ()> {
        // Load correct next nodes of the leaf.
        let mut left = self.left.load(Ordering::Acquire);
        let mut right = self.right.load(Ordering::Acquire);
        loop {
            match left_h.try_protect_pp(left, &self, &self.left, &|src| {
                (tag(src.left.load(Ordering::Acquire)) & 1) != 0
            }) {
                Ok(_) => {}
                Err(ProtectError::Changed(new_left)) => {
                    left = new_left;
                    continue;
                }
                Err(ProtectError::Invalidated) => return Err(()),
            }
            match right_h.try_protect_pp(right, &self, &self.right, &|src| {
                (tag(src.right.load(Ordering::Acquire)) & 1) != 0
            }) {
                Ok(_) => {}
                Err(ProtectError::Changed(new_right)) => {
                    right = new_right;
                    continue;
                }
                Err(ProtectError::Invalidated) => return Err(()),
            }
            return Ok((left, right));
        }
    }

    #[inline]
    fn child<'g>(&'g self, dir: Direction) -> &'g AtomicPtr<Self> {
        match dir {
            Direction::L => &self.left,
            Direction::R => &self.right,
        }
    }

    #[inline]
    fn load_child(&self, dir: Direction) -> *mut Self {
        self.child(dir).load(Ordering::Acquire)
    }
}

pub struct Handle<'domain> {
    gp_h: HazardPointer<'domain>,
    p_h: HazardPointer<'domain>,
    l_h: HazardPointer<'domain>,
    l_other_h: HazardPointer<'domain>,
    pupdate_h: HazardPointer<'domain>,
    gpupdate_h: HazardPointer<'domain>,
    // Protect new updates
    aux_update_h: HazardPointer<'domain>,
    // Protect a new node of insertion
    new_internal_h: HazardPointer<'domain>,
    // Protect an owner of update which is currently being helped.
    help_src_h: HazardPointer<'domain>,
    thread: Thread<'domain>,
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
            new_internal_h: HazardPointer::default(),
            help_src_h: HazardPointer::default(),
            thread: Thread::new(&DEFAULT_DOMAIN),
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
    p: *mut Node<K, V>,
    l: *mut Node<K, V>,
    l_other: *mut Node<K, V>,
    gp_p_dir: Direction,
    p_l_dir: Direction,
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
            gp_p_dir: Direction::L,
            p_l_dir: Direction::L,
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
        self.gp_p_dir = Direction::L;
        self.p_l_dir = Direction::L;
        self.pupdate = ptr::null_mut();
        self.gpupdate = ptr::null_mut();
    }

    #[inline]
    fn validate_lower<'g>(&'g self) -> Option<(&'g Node<K, V>, &'g Node<K, V>, &'g Node<K, V>)> {
        let p_node = unsafe { self.p.as_ref().unwrap() };
        let l_node = unsafe { self.l.as_ref().unwrap() };
        let l_other_node = unsafe { self.l_other.as_ref().unwrap() };

        // Is l a child of p?
        if self.l == p_node.load_child(self.p_l_dir)
            && self.l_other == p_node.load_child(self.p_l_dir.opposite())
        {
            return Some((p_node, l_node, l_other_node));
        }
        None
    }

    #[inline]
    fn validate_full<'g>(
        &'g self,
    ) -> Option<(
        &'g Node<K, V>,
        &'g Node<K, V>,
        &'g Node<K, V>,
        &'g Node<K, V>,
    )> {
        let (p_node, l_node, l_other_node) = some_or!(self.validate_lower(), return None);
        let gp_node = unsafe { self.gp.as_ref().unwrap() };

        // Is p a child of gp?
        if self.p == gp_node.load_child(self.gp_p_dir) {
            return Some((gp_node, p_node, l_node, l_other_node));
        }
        None
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
                let node = untagged(node);
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
    fn search_inner<'domain, 'hp>(
        &self,
        key: &K,
        cursor: &mut Cursor<'domain, 'hp, K, V>,
    ) -> Result<(), ()> {
        cursor.l = self.root.load(Ordering::Relaxed);
        cursor.handle.l_h.protect_raw(cursor.l);
        light_membarrier();

        loop {
            let l_node = unsafe { cursor.l.as_ref() }.unwrap();
            if l_node.is_leaf {
                return Ok(());
            }
            cursor.gp = cursor.p;
            cursor.p = cursor.l;
            cursor.gp_p_dir = cursor.p_l_dir;
            HazardPointer::swap(&mut cursor.handle.gp_h, &mut cursor.handle.p_h);
            HazardPointer::swap(&mut cursor.handle.p_h, &mut cursor.handle.l_h);

            cursor.gpupdate = cursor.pupdate;
            HazardPointer::swap(&mut cursor.handle.gpupdate_h, &mut cursor.handle.pupdate_h);

            cursor.pupdate = l_node.protect_update(&mut cursor.handle.pupdate_h);
            (cursor.l, cursor.l_other) =
                l_node.protect_next(&mut cursor.handle.l_h, &mut cursor.handle.l_other_h)?;
            if l_node.key.cmp(key) != std::cmp::Ordering::Greater {
                mem::swap(&mut cursor.l, &mut cursor.l_other);
                HazardPointer::swap(&mut cursor.handle.l_h, &mut cursor.handle.l_other_h);
                cursor.p_l_dir = Direction::R;
            } else {
                cursor.p_l_dir = Direction::L;
            }

            // Check if the parent node is marked.
            // pupdate must be loaded again here. This is because if the current thread is stopped
            // after protecting pupdate but before protecting next nodes, and another thread
            // marked & reclaimed p and l, then protected l must be an invalid memory location.
            // (and it will pass validation.)
            let pupdate = l_node.protect_update(&mut cursor.handle.aux_update_h);
            let (pupdate_base, pupdate_tag) = decompose_ptr(pupdate);
            if pupdate_tag == UpdateTag::MARKED.bits() {
                // Check if p is still reachable from gp.
                // - If it is reachable, dchild CAS is not done. So,
                //   it is safe to deref pupdate.
                // - If it is unreachable, dchild CAS has finished,
                //   and current l is no longer valid, also dereferencing
                //   pupdate may be dangerous.
                //   (Even if the update is reclaimed, current searching must be restarted.)
                //
                // NOTE: cursor.gp might be different with pupdate.gp, and even marked & retired!
                //       To avoid this situation, we must check whether current gp's update is <pupdate, DFLAG>
                if cursor.p == unsafe { &*cursor.gp }.load_child(cursor.gp_p_dir)
                    && tagged(pupdate, UpdateTag::DFLAG.bits())
                        == unsafe { &*cursor.gp }.update.load(Ordering::Acquire)
                {
                    let pupdate_ref = unsafe { &*pupdate_base };
                    cursor.handle.gp_h.protect_raw(pupdate_ref.gp);
                    cursor.handle.p_h.protect_raw(pupdate_ref.p);
                    light_membarrier();

                    // If `retired` is not true, pupdate.gp is safe to deref.
                    // - Even though gp may be retired, there will be a hazard pointer
                    //   by another thread in help_marked.
                    //   (threads which help marked must have a hazard pointer to gp.)
                    if !pupdate_ref.retired.load(Ordering::Acquire) {
                        // Help cleaning marked node on search, and restart.
                        self.help_marked(pupdate, cursor.handle);
                    }
                }
                return Err(());
            }
        }
    }

    fn search<'domain, 'hp>(&self, key: &K, cursor: &mut Cursor<'domain, 'hp, K, V>) {
        loop {
            cursor.reset();
            if self.search_inner(key, cursor).is_ok() {
                return;
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
            let (p_node, l_node, _) = some_or!(cursor.validate_lower(), continue);

            if l_node.key == *key {
                return false;
            } else if tag(cursor.pupdate) != UpdateTag::CLEAN.bits() {
                HazardPointer::swap(&mut handle.p_h, &mut handle.help_src_h);
                self.help(cursor.pupdate, &p_node.update, handle);
            } else {
                let new = Node::leaf(Key::Fin(key.clone()), Some(value.clone()));
                let new_sibling = Node::leaf(l_node.key.clone(), l_node.value.clone());

                let (left, right) = match new.key.partial_cmp(&new_sibling.key) {
                    Some(std::cmp::Ordering::Less) => (new, new_sibling),
                    _ => (new_sibling, new),
                };

                let new_internal = Node::internal(
                    // key field max(k, l → key)
                    right.key.clone(),
                    None,
                    // two child fields equal to new and newSibling
                    // (the one with the smaller key is the left child)
                    left,
                    right,
                );

                let new_internal = Box::into_raw(Box::new(new_internal));

                let op = Update {
                    gp: ptr::null_mut(),
                    p: cursor.p,
                    l: cursor.l,
                    l_other: cursor.l_other,
                    gp_p_dir: cursor.gp_p_dir,
                    p_l_dir: cursor.p_l_dir,
                    pupdate: ptr::null_mut(),
                    new_internal,
                    retired: AtomicBool::new(false),
                };

                let new_pupdate = tagged(Box::into_raw(Box::new(op)), UpdateTag::IFLAG.bits());

                handle.new_internal_h.protect_raw(new_internal);
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
                        if !cursor.pupdate.is_null() {
                            unsafe {
                                let removed = untagged(cursor.pupdate);
                                removed
                                    .as_ref()
                                    .unwrap()
                                    .retired
                                    .store(true, Ordering::Release);
                                handle.thread.retire(removed);
                            }
                        }
                        self.help_insert(new_pupdate, handle);
                        return true;
                    }
                    Err(current) => {
                        unsafe {
                            let new_pupdate_failed = Box::from_raw(untagged(new_pupdate));
                            let new_internal = new_pupdate_failed.new_internal;
                            let new_internal_failed = Box::from_raw(new_internal);
                            drop(Box::from_raw(
                                new_internal_failed.left.load(Ordering::Relaxed),
                            ));
                            drop(Box::from_raw(
                                new_internal_failed.right.load(Ordering::Relaxed),
                            ));
                        }
                        HazardPointer::swap(&mut handle.p_h, &mut handle.help_src_h);
                        self.help(current, &p_node.update, handle);
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
            let (gp_node, p_node, l_node, _) = some_or!(cursor.validate_full(), continue);

            if l_node.key != Key::Fin(key.clone()) {
                return None;
            }
            if tag(cursor.gpupdate) != UpdateTag::CLEAN.bits() {
                HazardPointer::swap(&mut handle.gp_h, &mut handle.help_src_h);
                self.help(cursor.gpupdate, &gp_node.update, handle);
            } else if tag(cursor.pupdate) != UpdateTag::CLEAN.bits() {
                HazardPointer::swap(&mut handle.p_h, &mut handle.help_src_h);
                self.help(cursor.pupdate, &p_node.update, handle);
            } else {
                let op = Update {
                    gp: cursor.gp,
                    p: cursor.p,
                    l: cursor.l,
                    l_other: cursor.l_other,
                    gp_p_dir: cursor.gp_p_dir,
                    p_l_dir: cursor.p_l_dir,
                    pupdate: cursor.pupdate,
                    new_internal: ptr::null_mut(),
                    retired: AtomicBool::new(false),
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
                        if !cursor.gpupdate.is_null() {
                            unsafe {
                                let removed = untagged(cursor.gpupdate);
                                removed
                                    .as_ref()
                                    .unwrap()
                                    .retired
                                    .store(true, Ordering::Release);
                                handle.thread.retire(removed);
                            }
                        }
                        if self.help_delete(new_update, handle) {
                            // SAFETY: dereferencing the value of leaf node is safe until `handle` is dropped.
                            return Some(unsafe { mem::transmute(l_node.value.as_ref().unwrap()) });
                        }
                    }
                    Err(current) => {
                        unsafe { drop(Box::from_raw(untagged(new_update))) };
                        HazardPointer::swap(&mut handle.gp_h, &mut handle.help_src_h);
                        self.help(current, &gp_node.update, handle);
                    }
                }
            }
        }
    }

    // The children of the owner node of this update will not be changed
    // until the update tag is changed. In other words, callee don't have to
    // worry about whether the children nodes are retired, but it MUST
    // protect p (or gp) properly to avoid an undefined behavior.
    // Precondition:
    // 1. The owner of op_src must be protected by help_src_h.
    #[inline]
    fn help<'domain, 'hp>(
        &self,
        op: *mut Update<K, V>,
        op_src: &AtomicPtr<Update<K, V>>,
        handle: &'hp mut Handle<'domain>,
    ) {
        // Protect helping op. And it must be validated.
        handle.aux_update_h.protect_raw(untagged(op));
        light_membarrier();
        if op == op_src.load(Ordering::Acquire)
            && tag(op) != UpdateTag::CLEAN.bits()
            && tag(op) != UpdateTag::MARKED.bits()
        {
            // Protect all nodes in op
            let op_ref = unsafe { &*untagged(op) };
            handle.gp_h.protect_raw(op_ref.gp);
            handle.p_h.protect_raw(op_ref.p);
            handle.l_h.protect_raw(op_ref.l);
            handle.l_other_h.protect_raw(op_ref.l_other);
            handle.new_internal_h.protect_raw(op_ref.new_internal);
            light_membarrier();

            // Double-check after protecting
            if op != op_src.load(Ordering::Acquire) || op_ref.retired.load(Ordering::Acquire) {
                return;
            }

            // NOTE: help_marked is called during `search`.
            match UpdateTag::from_bits_truncate(tag(op)) {
                UpdateTag::IFLAG => self.help_insert(op, handle),
                UpdateTag::DFLAG => {
                    let _ = self.help_delete(op, handle);
                }
                _ => {}
            }
        }
    }

    // Precondition:
    // 1. gp and p must be protected by p_h and gp_h respectively.
    fn help_delete<'domain, 'hp>(
        &self,
        op: *mut Update<K, V>,
        handle: &'hp mut Handle<'domain>,
    ) -> bool {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { untagged(op).as_ref().unwrap().clone() };
        let Update { gp, p, pupdate, .. } = op_ref;

        let gp_ref = unsafe { gp.as_ref() }.unwrap();
        let p_ref = unsafe { p.as_ref() }.unwrap();
        let new_op = tagged(op, UpdateTag::MARKED.bits());

        // mark CAS
        match p_ref
            .update
            .compare_exchange(*pupdate, new_op, Ordering::Release, Ordering::Acquire)
        {
            Ok(_) => {
                // (prev value) = op → pupdate
                if !pupdate.is_null() {
                    unsafe {
                        let removed = untagged(*pupdate);
                        removed
                            .as_ref()
                            .unwrap()
                            .retired
                            .store(true, Ordering::Release);
                        handle.thread.retire(removed);
                    }
                }
                self.help_marked(new_op, handle);
                return true;
            }
            Err(current) => {
                if unsafe { untagged(new_op).as_ref().unwrap() }
                    .retired
                    .load(Ordering::Acquire)
                {
                    // Some very fast guy already helped DELETE & MARK operations and reclaimed.
                    // This is not present on the original paper,
                    // but it is necessary to apply hazard pointers.
                    return true;
                } else if current == new_op {
                    // (prev value) = <Mark, op>
                    self.help_marked(new_op, handle);
                    return true;
                } else {
                    // backtrack CAS
                    let _ = gp_ref.update.compare_exchange(
                        tagged(op, UpdateTag::DFLAG.bits()),
                        tagged(op, UpdateTag::CLEAN.bits()),
                        Ordering::Release,
                        Ordering::Relaxed,
                    );

                    // The hazard pointers must be preserved before dereferencing gp,
                    // so backtrack CAS must be called before helping.
                    HazardPointer::swap(&mut handle.p_h, &mut handle.help_src_h);
                    self.help(current, &p_ref.update, handle);
                    return false;
                }
            }
        }
    }

    // It deref gp and p.
    // gp may be changed or even retired right after dunflag CAS!
    // Precondition:
    // 1. gp and p must be protected by some hazard pointers.
    // 2. op must be protected by aux_update_h.
    fn help_marked<'domain, 'hp>(&self, op: *mut Update<K, V>, handle: &'hp mut Handle<'domain>) {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { untagged(op).as_ref().unwrap().clone() };
        let Update { gp, l_other, .. } = op_ref;
        let gp_node = unsafe { gp.as_ref().unwrap() };

        unsafe {
            // dchild CAS
            handle
                .thread
                .try_unlink(DChildUnlink { op }, slice::from_ref(l_other));
            // dunflag CAS
            let _ = gp_node.update.compare_exchange(
                tagged(op, UpdateTag::DFLAG.bits()),
                tagged(op, UpdateTag::CLEAN.bits()),
                Ordering::Release,
                Ordering::Relaxed,
            );
        }
    }

    // Precondition:
    // 1. p must be protected by a hazard pointer.
    // 2. op must be protected by aux_update_h.
    fn help_insert<'domain, 'hp>(&self, op: *mut Update<K, V>, handle: &'hp mut Handle<'domain>) {
        // Precondition: op points to an IInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { untagged(op).as_ref().unwrap().clone() };
        let Update {
            p, new_internal, ..
        } = op_ref;
        let p_node = unsafe { p.as_ref().unwrap() };

        unsafe {
            // ichild CAS
            handle
                .thread
                .try_unlink(IChildUnlink { op }, slice::from_ref(new_internal));
            // iunflag CAS
            let _ = p_node.update.compare_exchange(
                tagged(op, UpdateTag::IFLAG.bits()),
                tagged(op, UpdateTag::CLEAN.bits()),
                Ordering::Release,
                Ordering::Relaxed,
            );
        }
    }
}

impl<K, V> hp_pp::Invalidate for Node<K, V> {
    fn invalidate(&self) {
        self.left.fetch_or(1, Ordering::Release);
        self.right.fetch_or(1, Ordering::Release);
    }
}

impl<K, V> hp_pp::Invalidate for Update<K, V> {
    fn invalidate(&self) {}
}

struct DChildUnlink<K, V> {
    op: *mut Update<K, V>,
}

impl<K, V> hp_pp::Unlink<Node<K, V>> for DChildUnlink<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn do_unlink(&self) -> Result<Vec<*mut Node<K, V>>, ()> {
        let op_ref = unsafe { untagged(self.op).as_ref().unwrap().clone() };
        let Update {
            gp,
            p,
            l,
            l_other,
            gp_p_dir,
            ..
        } = op_ref;

        let gp_node = unsafe { gp.as_ref().unwrap() };

        // dchild CAS
        if gp_node
            .child(*gp_p_dir)
            .compare_exchange(*p, *l_other, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            Ok(vec![*p, *l])
        } else {
            Err(())
        }
    }
}

struct IChildUnlink<K, V> {
    op: *mut Update<K, V>,
}

impl<K, V> hp_pp::Unlink<Node<K, V>> for IChildUnlink<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn do_unlink(&self) -> Result<Vec<*mut Node<K, V>>, ()> {
        let op_ref = unsafe { untagged(self.op).as_ref().unwrap() };
        let Update {
            p,
            new_internal,
            l,
            p_l_dir,
            ..
        } = op_ref;

        let p_node = unsafe { p.as_ref().unwrap() };

        // ichild CAS
        if p_node
            .child(*p_l_dir)
            .compare_exchange(*l, *new_internal, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            Ok(vec![*l])
        } else {
            Err(())
        }
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
