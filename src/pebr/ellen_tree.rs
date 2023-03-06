use std::sync::atomic::Ordering;
use std::mem;

use crossbeam_pebr::{
    unprotected, Atomic, Guard, Owned, Shared, Shield, ShieldError,
};

use super::concurrent_map::ConcurrentMap;

bitflags! {
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

pub struct Node<K, V> {
    key: Key<K>,
    value: Option<V>,
    // tag on low bits: {Clean, DFlag, IFlag, Mark}
    update: Atomic<Update<K, V>>,
    left: Atomic<Node<K, V>>,
    right: Atomic<Node<K, V>>,
    is_leaf: bool,
}

pub struct Update<K, V> {
    gp: Atomic<Node<K, V>>,
    p: Atomic<Node<K, V>>,
    l: Atomic<Node<K, V>>,
    l_other: Atomic<Node<K, V>>,
    gp_p_dir: Direction,
    p_l_dir: Direction,
    pupdate: Atomic<Update<K, V>>,
    new_internal: Atomic<Node<K, V>>,
}

impl<K, V> Node<K, V> {
    pub fn internal(key: Key<K>, value: Option<V>, left: Self, right: Self) -> Self {
        Self {
            key,
            value,
            update: Atomic::null(),
            left: Atomic::new(left),
            right: Atomic::new(right),
            is_leaf: false,
        }
    }

    pub fn leaf(key: Key<K>, value: Option<V>) -> Self {
        Self {
            key,
            value,
            update: Atomic::null(),
            left: Atomic::null(),
            right: Atomic::null(),
            is_leaf: true,
        }
    }

    /// Protect `update` of the node.
    #[inline]
    fn protect_update<'g>(&'g self, hazptr: &mut Shield<Update<K, V>>, guard: &'g Guard) -> Result<Shared<'g, Update<K, V>>, ShieldError> {
        let update = self.update.load(Ordering::Acquire, guard);
        hazptr.defend(update, guard)?;
        Ok(update)
    }

    #[inline]
    fn protect_next<'g>(
        &'g self,
        left_h: &mut Shield<Node<K, V>>,
        right_h: &mut Shield<Node<K, V>>,
        guard: &'g Guard,
    ) -> Result<(Shared<'g, Node<K, V>>, Shared<'g, Node<K, V>>), ShieldError> {
        // Load correct next nodes of the leaf.
        let left = self.left.load(Ordering::Acquire, guard);
        let right = self.right.load(Ordering::Acquire, guard);
        left_h.defend(left, guard)?;
        right_h.defend(right, guard)?;
        Ok((left, right))
    }

    #[inline]
    fn child<'g>(&'g self, dir: Direction) -> &'g Atomic<Self> {
        match dir {
            Direction::L => &self.left,
            Direction::R => &self.right,
        }
    }
}

pub struct Handle<K, V> {
    gp_h: Shield<Node<K, V>>,
    p_h: Shield<Node<K, V>>,
    l_h: Shield<Node<K, V>>,
    l_other_h: Shield<Node<K, V>>,
    pupdate_h: Shield<Update<K, V>>,
    gpupdate_h: Shield<Update<K, V>>,
    // Protect new updates
    aux_update_h: Shield<Update<K, V>>,
    // Protect a new node of insertion
    new_internal_h: Shield<Node<K, V>>,
    // Protect an owner of update which is currently being helped.
    help_src_h: Shield<Node<K, V>>,
}

impl<K, V> Handle<K, V> {
    fn new(guard: &Guard) -> Self {
        Self {
            gp_h: Shield::null(guard),
            p_h: Shield::null(guard),
            l_h: Shield::null(guard),
            l_other_h: Shield::null(guard),
            pupdate_h: Shield::null(guard),
            gpupdate_h: Shield::null(guard),
            aux_update_h: Shield::null(guard),
            new_internal_h: Shield::null(guard),
            help_src_h: Shield::null(guard),
        }
    }

    fn reset(&mut self) {
        self.gp_h.release();
        self.p_h.release();
        self.l_h.release();
        self.l_other_h.release();
        self.pupdate_h.release();
        self.gpupdate_h.release();
        self.aux_update_h.release();
        self.new_internal_h.release();
        self.help_src_h.release();
    }

    // bypass E0499-E0503, etc that are supposed to be fixed by polonius
    #[inline]
    fn launder<'hp1, 'hp2>(&'hp1 mut self) -> &'hp2 mut Self {
        unsafe { core::mem::transmute(self) }
    }
}

struct Cursor<'g, K, V> {
    gp: Shared<'g, Node<K, V>>,
    p: Shared<'g, Node<K, V>>,
    l: Shared<'g, Node<K, V>>,
    l_other: Shared<'g, Node<K, V>>,
    gp_p_dir: Direction,
    p_l_dir: Direction,
    pupdate: Shared<'g, Update<K, V>>,
    gpupdate: Shared<'g, Update<K, V>>,
}

impl<'g, K, V> Cursor<'g, K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new() -> Self {
        Self {
            gp: Shared::null(),
            p: Shared::null(),
            l: Shared::null(),
            l_other: Shared::null(),
            gp_p_dir: Direction::L,
            p_l_dir: Direction::L,
            pupdate: Shared::null(),
            gpupdate: Shared::null(),
        }
    }
}

pub struct EFRBTree<K, V> {
    root: Atomic<Node<K, V>>,
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
    fn search<'g>(&self, key: &'g K, cursor: &mut Cursor<'g, K, V>, handle: &'g mut Handle<K, V>, guard: &'g Guard) -> Result<(), ShieldError> {
        cursor.l = self.root.load(Ordering::Relaxed, guard);
        handle.l_h.defend(cursor.l, guard)?;

        loop {
            let l_node = unsafe { cursor.l.as_ref() }.unwrap();
            if l_node.is_leaf {
                return Ok(());
            }
            cursor.gp = cursor.p;
            cursor.p = cursor.l;
            cursor.gp_p_dir = cursor.p_l_dir;
            mem::swap(&mut handle.gp_h, &mut handle.p_h);
            mem::swap(&mut handle.p_h, &mut handle.l_h);

            cursor.gpupdate = cursor.pupdate;
            mem::swap(&mut handle.gpupdate_h, &mut handle.pupdate_h);

            cursor.pupdate = l_node.protect_update(&mut handle.pupdate_h, guard)?;
            (cursor.l, cursor.l_other) =
                l_node.protect_next(&mut handle.l_h, &mut handle.l_other_h, guard)?;
            if l_node.key.cmp(key) != std::cmp::Ordering::Greater {
                mem::swap(&mut cursor.l, &mut cursor.l_other);
                mem::swap(&mut handle.l_h, &mut handle.l_other_h);
                cursor.p_l_dir = Direction::R;
            } else {
                cursor.p_l_dir = Direction::L;
            }
        }
    }

    pub fn find_inner<'g>(&'g self, key: &'g K, handle: &'g mut Handle<K, V>, guard: &'g Guard) -> Result<Option<&'g V>, ShieldError> {
        let mut cursor = Cursor::new();
        match self.search(key, &mut cursor, handle, guard) {
            Ok(_) => {
                let l_node = unsafe { cursor.l.as_ref().unwrap() };
                if l_node.key.eq(key) {
                    return Ok(l_node.value.as_ref());
                } else {
                    return Ok(None);
                }
            }
            Err(_) => Err(ShieldError::Ejected)
        }
    }

    pub fn find<'g>(&'g self, key: &'g K, handle: &'g mut Handle<K, V>, guard: &'g mut Guard) -> Option<&'g V> {
        loop {
            match self.find_inner(key, handle.launder(), unsafe { &mut *(guard as *mut Guard) }) {
                Ok(found) => return found,
                Err(_) => guard.repin(),
            }
        }
    }

    #[inline]
    fn insert_inner(
        &self,
        key: &K,
        value: &V,
        handle: &mut Handle<K, V>,
        guard: &Guard,
    ) -> Result<bool, ShieldError> {
        loop {
            let mut cursor = Cursor::new();
            self.search(key, &mut cursor, handle.launder(), guard)?;
            let p_node = unsafe { cursor.p.as_ref().unwrap() };
            let l_node = unsafe { cursor.l.as_ref().unwrap() };

            if l_node.key == *key {
                return Ok(false);
            } else if cursor.pupdate.tag() != UpdateTag::CLEAN.bits() {
                mem::swap(&mut handle.p_h, &mut handle.help_src_h);
                self.help(cursor.pupdate, handle, guard);
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
                )).into_shared(unsafe { unprotected() });

                if let Err(e) = handle.new_internal_h.defend(new_internal, guard) {
                    unsafe {
                        let new_internal_failed = new_internal.into_owned().into_box();
                        drop(new_internal_failed.left.into_owned());
                        drop(new_internal_failed.right.into_owned());
                    }
                    return Err(e);
                }

                let new_pupdate = Owned::new(Update {
                    gp: Atomic::null(),
                    p: Atomic::from(cursor.p),
                    l: Atomic::from(cursor.l),
                    l_other: Atomic::from(cursor.l_other),
                    gp_p_dir: cursor.gp_p_dir,
                    p_l_dir: cursor.p_l_dir,
                    pupdate: Atomic::null(),
                    new_internal: Atomic::from(new_internal),
                }).into_shared(unsafe { unprotected() }).with_tag(UpdateTag::IFLAG.bits());

                if let Err(e) = handle.aux_update_h.defend(new_pupdate, guard) {
                    unsafe {
                        let new_internal_failed = new_internal.into_owned().into_box();
                        drop(new_internal_failed.left.into_owned());
                        drop(new_internal_failed.right.into_owned());
                        drop(new_pupdate.into_owned());
                    }
                    return Err(e);
                }

                // iflag CAS
                match p_node.update.compare_and_set(
                    cursor.pupdate,
                    new_pupdate,
                    Ordering::Release,
                    guard,
                ) {
                    Ok(_) => {
                        if !cursor.pupdate.is_null() {
                            unsafe { guard.defer_destroy(cursor.pupdate) };
                        }
                        self.help_insert(new_pupdate, guard);
                        return Ok(true);
                    }
                    Err(e) => {
                        unsafe {
                            let new_pupdate_failed = new_pupdate.into_owned().into_box();
                            let new_internal_failed = new_pupdate_failed.new_internal.into_owned().into_box();
                            drop(new_internal_failed.left.into_owned());
                            drop(new_internal_failed.right.into_owned());
                        }
                        mem::swap(&mut handle.p_h, &mut handle.help_src_h);
                        self.help(e.current, handle, guard);
                    }
                }
            }
        }
    }

    pub fn insert(
        &self,
        key: &K,
        value: V,
        handle: &mut Handle<K, V>,
        guard: &mut Guard,
    ) -> bool {
        loop {
            match self.insert_inner(key, &value, handle, guard) {
                Ok(succ) => return succ,
                Err(_) => guard.repin(),
            }
        }
    }

    #[inline]
    fn delete_inner(
        &self,
        key: &K,
        handle: &mut Handle<K, V>,
        guard: &Guard,
    ) -> Result<Option<V>, ShieldError> {
        loop {
            let mut cursor = Cursor::new();
            self.search(key, &mut cursor, handle.launder(), guard)?;

            if cursor.gp.is_null() {
                // The tree is empty. There's no more things to do.
                return Ok(None);
            }
            let gp_node = unsafe { cursor.gp.as_ref().unwrap() };
            let l_node = unsafe { cursor.l.as_ref().unwrap() };

            if l_node.key != *key {
                return Ok(None);
            }
            if cursor.gpupdate.tag() != UpdateTag::CLEAN.bits() {
                mem::swap(&mut handle.gp_h, &mut handle.help_src_h);
                self.help(cursor.gpupdate, handle, guard);
            } else if cursor.pupdate.tag() != UpdateTag::CLEAN.bits() {
                mem::swap(&mut handle.p_h, &mut handle.help_src_h);
                self.help(cursor.pupdate, handle, guard);
            } else {
                let new_update = Owned::new(Update {
                    gp: Atomic::from(cursor.gp),
                    p: Atomic::from(cursor.p),
                    l: Atomic::from(cursor.l),
                    l_other: Atomic::from(cursor.l_other),
                    gp_p_dir: cursor.gp_p_dir,
                    p_l_dir: cursor.p_l_dir,
                    pupdate: Atomic::from(cursor.pupdate),
                    new_internal: Atomic::null(),
                }).into_shared(unsafe { unprotected() }).with_tag(UpdateTag::DFLAG.bits());

                if let Err(e) = handle.aux_update_h.defend(new_update, guard) {
                    unsafe { drop(new_update.into_owned()) };
                    return Err(e);
                }

                // dflag CAS
                match gp_node.update.compare_and_set(
                    cursor.gpupdate,
                    new_update,
                    Ordering::Release,
                    guard
                ) {
                    Ok(_) => {
                        if !cursor.gpupdate.is_null() {
                            unsafe { guard.defer_destroy(cursor.gpupdate) };
                        }
                        if self.help_delete(new_update, handle, guard) {
                            // SAFETY: dereferencing the value of leaf node is safe until `handle` is dropped.
                            return Ok(Some(l_node.value.as_ref().unwrap().clone()));
                        }
                    }
                    Err(e) => {
                        unsafe { drop(new_update.into_owned()) };
                        mem::swap(&mut handle.gp_h, &mut handle.help_src_h);
                        self.help(e.current, handle, guard);
                    }
                }
            }
        }
    }

    fn delete(
        &self,
        key: &K,
        handle: &mut Handle<K, V>,
        guard: &mut Guard,
    ) -> Option<V> {
        loop {
            handle.reset();
            match self.delete_inner(key, handle, guard) {
                Ok(found) => return found,
                Err(ShieldError::Ejected) => guard.repin(),
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
    fn help<'g>(
        &'g self,
        op: Shared<'g, Update<K, V>>,
        handle: &'g mut Handle<K, V>,
        guard: &'g Guard,
    ) {
        ok_or!(handle.aux_update_h.defend(op, guard), return);
        // Protect all nodes in op
        let op_ref = unsafe { op.as_ref().unwrap() };
        ok_or!(handle.gp_h.defend(op_ref.gp.load(Ordering::Relaxed, guard), guard), return);
        ok_or!(handle.p_h.defend(op_ref.p.load(Ordering::Relaxed, guard), guard), return);
        ok_or!(handle.l_h.defend(op_ref.l.load(Ordering::Relaxed, guard), guard), return);
        ok_or!(handle.l_other_h.defend(op_ref.l_other.load(Ordering::Relaxed, guard), guard), return);
        ok_or!(handle.new_internal_h.defend(op_ref.new_internal.load(Ordering::Relaxed, guard), guard), return);

        match UpdateTag::from_bits_truncate(op.tag()) {
            UpdateTag::IFLAG => self.help_insert(op, guard),
            UpdateTag::MARKED => self.help_marked(op, guard),
            UpdateTag::DFLAG => {
                let _ = self.help_delete(op, handle, guard);
            }
            _ => {}
        }
    }

    // Precondition:
    // 1. gp and p must be protected by p_h and gp_h respectively.
    fn help_delete<'g>(
        &'g self,
        op: Shared<'g, Update<K, V>>,
        handle: &'g mut Handle<K, V>,
        guard: &'g Guard,
    ) -> bool {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.as_ref().unwrap() };
        let Update { gp, p, pupdate, .. } = op_ref;

        let gp = gp.load(Ordering::Relaxed, guard);
        let p = p.load(Ordering::Relaxed, guard);
        let pupdate = pupdate.load(Ordering::Relaxed, guard);

        let gp_ref = unsafe { gp.as_ref() }.unwrap();
        let p_ref = unsafe { p.as_ref() }.unwrap();
        let new_op = op.with_tag(UpdateTag::MARKED.bits());

        // mark CAS
        match p_ref
            .update
            .compare_and_set(pupdate, new_op, Ordering::Release, guard)
        {
            Ok(_) => {
                // (prev value) = op → pupdate
                if !pupdate.is_null() {
                    unsafe { guard.defer_destroy(pupdate) };
                }
                self.help_marked(new_op, guard);
                return true;
            }
            Err(e) => {
                if e.current == new_op {
                    // (prev value) = <Mark, op>
                    self.help_marked(new_op, guard);
                    return true;
                } else {
                    // backtrack CAS
                    let _ = gp_ref.update.compare_and_set(
                        op.with_tag(UpdateTag::DFLAG.bits()),
                        op.with_tag(UpdateTag::CLEAN.bits()),
                        Ordering::Release,
                        guard
                    );

                    // The hazard pointers must be preserved before dereferencing gp,
                    // so backtrack CAS must be called before helping.
                    mem::swap(&mut handle.p_h, &mut handle.help_src_h);
                    self.help(e.current, handle, guard);
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
    fn help_marked<'g>(&'g self, op: Shared<'g, Update<K, V>>, guard: &'g Guard) {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.as_ref().unwrap() };
        let Update {
            gp,
            p,
            l,
            l_other,
            gp_p_dir,
            ..
        } = op_ref;

        let gp = gp.load(Ordering::Relaxed, guard);
        let p = p.load(Ordering::Relaxed, guard);
        let l = l.load(Ordering::Relaxed, guard);
        let l_other = l_other.load(Ordering::Relaxed, guard);

        let gp_node = unsafe { gp.as_ref().unwrap() };

        // dchild CAS
        if gp_node
            .child(*gp_p_dir)
            .compare_and_set(p, l_other, Ordering::Release, guard)
            .is_ok()
        {
            unsafe {
                guard.defer_destroy(l);
                guard.defer_destroy(p);
            }
        }

        // dunflag CAS
        let _ = gp_node.update.compare_and_set(
            op.with_tag(UpdateTag::DFLAG.bits()),
            op.with_tag(UpdateTag::CLEAN.bits()),
            Ordering::Release,
            guard
        );
    }

    // Precondition:
    // 1. p must be protected by a hazard pointer.
    // 2. op must be protected by aux_update_h.
    fn help_insert<'g>(&'g self, op: Shared<'g, Update<K, V>>, guard: &'g Guard) {
        // Precondition: op points to an IInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.as_ref().unwrap() };
        let Update {
            p,
            new_internal,
            l,
            p_l_dir,
            ..
        } = op_ref;

        let p = p.load(Ordering::Relaxed, guard);
        let l = l.load(Ordering::Relaxed, guard);
        let new_internal = new_internal.load(Ordering::Relaxed, guard);
        let p_node = unsafe { p.as_ref().unwrap() };

        // ichild CAS
        if p_node
            .child(*p_l_dir)
            .compare_and_set(l, new_internal, Ordering::Release, guard)
            .is_ok()
        {
            unsafe { guard.defer_destroy(l) };
        }

        // iunflag CAS
        let _ = p_node.update.compare_and_set(
            op.with_tag(UpdateTag::IFLAG.bits()),
            op.with_tag(UpdateTag::CLEAN.bits()),
            Ordering::Release,
            guard
        );
    }
}

impl<K, V> ConcurrentMap<K, V> for EFRBTree<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Handle = Handle<K, V>;

    fn new() -> Self {
        EFRBTree::new()
    }

    fn handle<'g>(guard: &'g Guard) -> Self::Handle {
        Handle::new(guard)
    }

    fn clear(handle: &mut Self::Handle) {
        handle.reset();
    }

    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &'g K,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        match self.find(key, handle, guard) {
            Some(node) => Some(node),
            None => None,
        }
    }

    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool {
        self.insert(&key, value, handle, guard)
    }

    fn remove(&self, handle: &mut Self::Handle, key: &K, guard: &mut Guard) -> Option<V> {
        self.delete(key, handle, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::EFRBTree;
    use crate::pebr::concurrent_map;

    #[test]
    fn smoke_efrb_tree() {
        concurrent_map::tests::smoke::<EFRBTree<i32, String>>();
    }
}