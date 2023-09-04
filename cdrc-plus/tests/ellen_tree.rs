use bitflags::bitflags;
use cdrc_rs::{AtomicRc, AtomicWeak, Guard, Pointer, Rc, Snapshot, Weak};
use std::{mem::swap, sync::atomic::Ordering};

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct UpdateTag: usize {
        const CLEAN = 0usize;
        const DFLAG = 1usize;
        const IFLAG = 2usize;
        const MARK = 3usize;
    }
}

#[derive(Clone, Copy)]
pub enum Direction {
    L,
    R,
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

pub struct Node<K, V, G: Guard> {
    key: Key<K>,
    value: Option<V>,
    // tag on low bits: {Clean, DFlag, IFlag, Mark}
    update: AtomicRc<Update<K, V, G>, G>,
    left: AtomicRc<Node<K, V, G>, G>,
    right: AtomicRc<Node<K, V, G>, G>,
    is_leaf: bool,
}

pub struct Update<K, V, G: Guard> {
    gp: AtomicWeak<Node<K, V, G>, G>,
    gp_p_dir: Direction,
    p: AtomicWeak<Node<K, V, G>, G>,
    p_l_dir: Direction,
    l: AtomicWeak<Node<K, V, G>, G>,
    l_other: AtomicWeak<Node<K, V, G>, G>,
    pupdate: AtomicWeak<Update<K, V, G>, G>,
    new_internal: AtomicWeak<Node<K, V, G>, G>,
}

impl<K, V, G: Guard> Node<K, V, G> {
    pub fn internal(key: Key<K>, value: Option<V>, left: Self, right: Self, guard: &G) -> Self {
        Self {
            key,
            value,
            update: AtomicRc::null(),
            left: AtomicRc::new(left, guard),
            right: AtomicRc::new(right, guard),
            is_leaf: false,
        }
    }

    pub fn leaf(key: Key<K>, value: Option<V>) -> Self {
        Self {
            key,
            value,
            update: AtomicRc::null(),
            left: AtomicRc::null(),
            right: AtomicRc::null(),
            is_leaf: true,
        }
    }

    pub fn child(&self, dir: Direction) -> &AtomicRc<Node<K, V, G>, G> {
        match dir {
            Direction::L => &self.left,
            Direction::R => &self.right,
        }
    }
}

pub struct Finder<K, V, G: Guard> {
    gp: Snapshot<Node<K, V, G>, G>,
    gp_p_dir: Direction,
    p: Snapshot<Node<K, V, G>, G>,
    p_l_dir: Direction,
    l: Snapshot<Node<K, V, G>, G>,
    l_other: Snapshot<Node<K, V, G>, G>,
    pupdate: Snapshot<Update<K, V, G>, G>,
    gpupdate: Snapshot<Update<K, V, G>, G>,
    new_update: Snapshot<Update<K, V, G>, G>,
}

impl<K, V, G> Finder<K, V, G>
where
    K: Ord + Clone,
    V: Clone,
    G: Guard,
{
    fn new() -> Self {
        Self {
            gp: Snapshot::new(),
            gp_p_dir: Direction::L,
            p: Snapshot::new(),
            p_l_dir: Direction::L,
            l: Snapshot::new(),
            l_other: Snapshot::new(),
            pupdate: Snapshot::new(),
            gpupdate: Snapshot::new(),
            new_update: Snapshot::new(),
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
    fn search(&mut self, root: &AtomicRc<Node<K, V, G>, G>, key: &K, guard: &G) {
        self.l.load(root, guard);
        loop {
            let l_node = unsafe { self.l.as_ref() }.unwrap();
            if l_node.is_leaf {
                break;
            }
            swap(&mut self.gp, &mut self.p);
            swap(&mut self.gp_p_dir, &mut self.p_l_dir);
            swap(&mut self.p, &mut self.l);
            swap(&mut self.gpupdate, &mut self.pupdate);
            self.pupdate.load(&l_node.update, guard);
            let (l, l_other, dir) = match l_node.key.cmp(key) {
                std::cmp::Ordering::Greater => (&l_node.left, &l_node.right, Direction::L),
                _ => (&l_node.right, &l_node.left, Direction::R),
            };
            self.l.load(l, guard);
            self.l_other.load(l_other, guard);
            self.p_l_dir = dir;
        }
    }
}

pub struct Helper<K, V, G: Guard> {
    gp: Snapshot<Node<K, V, G>, G>,
    p: Snapshot<Node<K, V, G>, G>,
    l: Snapshot<Node<K, V, G>, G>,
    l_other: Snapshot<Node<K, V, G>, G>,
    new_internal: Snapshot<Node<K, V, G>, G>,
    pupdate: Snapshot<Update<K, V, G>, G>,
}

impl<K, V, G: Guard> Helper<K, V, G> {
    fn new() -> Self {
        Self {
            gp: Snapshot::new(),
            p: Snapshot::new(),
            l: Snapshot::new(),
            l_other: Snapshot::new(),
            new_internal: Snapshot::new(),
            pupdate: Snapshot::new(),
        }
    }

    fn load_insert<'g>(&'g mut self, op: &'g Update<K, V, G>, guard: &G) -> bool {
        self.p.load_from_weak(&op.p, guard)
            && self.l.load_from_weak(&op.l, guard)
            && self.l_other.load_from_weak(&op.l_other, guard)
            && self.new_internal.load_from_weak(&op.new_internal, guard)
    }

    fn load_delete<'g>(&'g mut self, op: &'g Update<K, V, G>, guard: &G) -> bool {
        self.gp.load_from_weak(&op.gp, guard)
            && self.p.load_from_weak(&op.p, guard)
            && self.l.load_from_weak(&op.l, guard)
            && self.l_other.load_from_weak(&op.l_other, guard)
            && self.pupdate.load_from_weak(&op.pupdate, guard)
    }
}

pub struct Cursor<K, V, G: Guard>(Finder<K, V, G>, Helper<K, V, G>);

pub struct EFRBTree<K, V, G: Guard> {
    root: AtomicRc<Node<K, V, G>, G>,
}

impl<K, V, G> EFRBTree<K, V, G>
where
    K: Ord + Clone,
    V: Clone,
    G: Guard,
{
    pub fn new(guard: &G) -> Self {
        Self {
            root: AtomicRc::new(
                Node::internal(
                    Key::Inf2,
                    None,
                    Node::leaf(Key::Inf1, None),
                    Node::leaf(Key::Inf2, None),
                    guard,
                ),
                guard,
            ),
        }
    }

    pub fn find(&self, key: &K, cursor: &mut Cursor<K, V, G>, guard: &G) -> bool {
        cursor.0.search(&self.root, key, guard);
        let l_node = unsafe { cursor.0.l.as_ref().unwrap() };
        l_node.key.eq(key)
    }

    pub fn insert(&self, key: K, value: V, cursor: &mut Cursor<K, V, G>, guard: &G) -> bool {
        loop {
            let finder = &mut cursor.0;
            finder.search(&self.root, &key, guard);
            let l_node = unsafe { finder.l.as_ref().unwrap() };
            let p_node = unsafe { finder.p.as_ref().unwrap() };

            if l_node.key == key {
                return false;
            } else if finder.pupdate.tag() != UpdateTag::CLEAN.bits() {
                self.help(&finder.pupdate, &mut cursor.1, guard);
            } else {
                let new = Node::leaf(Key::Fin(key.clone()), Some(value.clone()));
                let new_sibling = Node::leaf(l_node.key.clone(), l_node.value.clone());

                let (left, right) = match new.key.partial_cmp(&new_sibling.key) {
                    Some(std::cmp::Ordering::Less) => (new, new_sibling),
                    _ => (new_sibling, new),
                };

                let new_internal = Rc::new(
                    Node::internal(
                        // key field max(k, l → key)
                        right.key.clone(),
                        None,
                        // two child fields equal to new and newSibling
                        // (the one with the smaller key is the left child)
                        left,
                        right,
                        guard,
                    ),
                    guard,
                );

                let op = Update {
                    p: AtomicWeak::from(Weak::from_strong(&finder.p, guard)),
                    p_l_dir: finder.p_l_dir,
                    l: AtomicWeak::from(Weak::from_strong(&finder.l, guard)),
                    l_other: AtomicWeak::from(Weak::from_strong(&finder.l_other, guard)),
                    new_internal: AtomicWeak::from(Weak::from_strong(&new_internal, guard)),
                    gp: AtomicWeak::null(),
                    gp_p_dir: Direction::L,
                    pupdate: AtomicWeak::null(),
                };

                let new_pupdate = Rc::new(op, guard).with_tag(UpdateTag::IFLAG.bits());
                finder.new_update.protect(&new_pupdate, guard);

                match p_node.update.compare_exchange(
                    finder.pupdate.as_ptr(),
                    new_pupdate,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                ) {
                    Ok(_) => {
                        self.help_insert(&finder.new_update, &mut cursor.1, guard);
                        return true;
                    }
                    Err(_) => {}
                }
            }
        }
    }

    pub fn delete(&self, key: &K, cursor: &mut Cursor<K, V, G>, guard: &G) -> bool {
        loop {
            let finder = &mut cursor.0;
            finder.search(&self.root, key, guard);

            if finder.gp.is_null() {
                // The tree is empty. There's no more things to do.
                return false;
            }

            let l_node = unsafe { finder.l.as_ref().unwrap() };

            if l_node.key != Key::Fin(key.clone()) {
                return false;
            }
            if finder.gpupdate.tag() != UpdateTag::CLEAN.bits() {
                self.help(&finder.gpupdate, &mut cursor.1, guard);
            } else if finder.pupdate.tag() != UpdateTag::CLEAN.bits() {
                self.help(&finder.pupdate, &mut cursor.1, guard);
            } else {
                let op = Update {
                    gp: AtomicWeak::from(Weak::from_strong(&finder.gp, guard)),
                    gp_p_dir: finder.gp_p_dir,
                    p: AtomicWeak::from(Weak::from_strong(&finder.p, guard)),
                    p_l_dir: finder.p_l_dir,
                    l: AtomicWeak::from(Weak::from_strong(&finder.l, guard)),
                    l_other: AtomicWeak::from(Weak::from_strong(&finder.l_other, guard)),
                    pupdate: AtomicWeak::from(Weak::from_strong(&finder.pupdate, guard)),
                    new_internal: AtomicWeak::null(),
                };

                let new_update = Rc::new(op, guard).with_tag(UpdateTag::DFLAG.bits());
                finder.pupdate.protect(&new_update, guard);

                match unsafe { finder.gp.as_ref().unwrap() }
                    .update
                    .compare_exchange(
                        finder.gpupdate.as_ptr(),
                        new_update,
                        Ordering::Release,
                        Ordering::Relaxed,
                        guard,
                    ) {
                    Ok(_) => {
                        if self.help_delete(&finder.pupdate, &mut cursor.1, guard) {
                            return true;
                        }
                    }
                    Err(_) => {}
                }
            }
        }
    }

    #[inline]
    fn help(&self, op: &Snapshot<Update<K, V, G>, G>, helper: &mut Helper<K, V, G>, guard: &G) {
        match UpdateTag::from_bits_truncate(op.tag()) {
            UpdateTag::IFLAG => self.help_insert(op, helper, guard),
            UpdateTag::MARK => self.help_marked(op, helper, guard),
            UpdateTag::DFLAG => {
                let _ = self.help_delete(op, helper, guard);
            }
            _ => unreachable!(),
        }
    }

    fn help_delete(
        &self,
        op: &Snapshot<Update<K, V, G>, G>,
        helper: &mut Helper<K, V, G>,
        guard: &G,
    ) -> bool {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        if !helper.load_delete(unsafe { op.deref() }, guard) {
            return false;
        }

        let gp_ref = unsafe { helper.gp.as_ref() }.unwrap();
        let p_ref = unsafe { helper.p.as_ref() }.unwrap();

        match p_ref.update.compare_exchange(
            helper.pupdate.as_ptr(),
            op.with_tag(UpdateTag::MARK.bits()),
            Ordering::Release,
            Ordering::Acquire,
            guard,
        ) {
            Ok(_) => {
                // (prev value) = op → pupdate
                self.help_marked(op, helper, guard);
                return true;
            }
            Err(e) => {
                if e.current == op.with_tag(UpdateTag::MARK.bits()).as_ptr() {
                    // (prev value) = <Mark, op>
                    self.help_marked(op, helper, guard);
                    return true;
                } else {
                    let _ = gp_ref.update.compare_exchange(
                        op.with_tag(UpdateTag::DFLAG.bits()).as_ptr(),
                        op.with_tag(UpdateTag::CLEAN.bits()),
                        Ordering::Release,
                        Ordering::Relaxed,
                        guard,
                    );
                    return false;
                }
            }
        }
    }

    fn help_marked(
        &self,
        op: &Snapshot<Update<K, V, G>, G>,
        helper: &mut Helper<K, V, G>,
        guard: &G,
    ) {
        // Precondition: op points to a DInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.deref() };
        if !helper.load_delete(op_ref, guard) {
            return;
        }

        let gp_ref = unsafe { helper.gp.as_ref() }.unwrap();

        // dchild CAS
        let _ = gp_ref.child(op_ref.gp_p_dir).compare_exchange(
            helper.p.as_ptr(),
            &helper.l_other,
            Ordering::Release,
            Ordering::Relaxed,
            guard,
        );

        // dunflag CAS
        let _ = gp_ref.update.compare_exchange(
            op.with_tag(UpdateTag::DFLAG.bits()).as_ptr(),
            op.with_tag(UpdateTag::CLEAN.bits()),
            Ordering::Release,
            Ordering::Relaxed,
            guard,
        );
    }

    fn help_insert(
        &self,
        op: &Snapshot<Update<K, V, G>, G>,
        helper: &mut Helper<K, V, G>,
        guard: &G,
    ) {
        // Precondition: op points to a IInfo record (i.e., it is not ⊥)
        let op_ref = unsafe { op.deref() };
        if !helper.load_insert(op_ref, guard) {
            return;
        }

        let p_ref = unsafe { helper.p.as_ref() }.unwrap();

        // ichild CAS
        let _ = p_ref.child(op_ref.p_l_dir).compare_exchange(
            helper.l.as_ptr(),
            &helper.new_internal,
            Ordering::Release,
            Ordering::Relaxed,
            guard,
        );

        // iunflag CAS
        let _ = p_ref.update.compare_exchange(
            op.with_tag(UpdateTag::IFLAG.bits()).as_ptr(),
            op.with_tag(UpdateTag::CLEAN.bits()),
            Ordering::Release,
            Ordering::Relaxed,
            guard,
        );
    }
}

#[cfg(test)]
fn smoke<G: Guard>() {
    extern crate rand;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    const THREADS: i32 = 30;
    const ELEMENTS_PER_THREADS: i32 = 1000;

    let map = &{
        let guard = &G::new();
        EFRBTree::new(guard)
    };

    thread::scope(|s| {
        for t in 0..THREADS {
            s.spawn(move |_| {
                let cursor = &mut Cursor(Finder::new(), Helper::new());
                let rng = &mut rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(rng);
                for i in keys {
                    assert!(map.insert(i, i.to_string(), cursor, &G::new()));
                }
            });
        }
    })
    .unwrap();

    thread::scope(|s| {
        for t in 0..(THREADS / 2) {
            s.spawn(move |_| {
                let cursor = &mut Cursor(Finder::new(), Helper::new());
                let rng = &mut rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(rng);
                let guard = &mut G::new();
                for i in keys {
                    assert!(map.delete(&i, cursor, guard));
                    assert_eq!(
                        i.to_string(),
                        *unsafe { cursor.0.l.deref() }.value.as_ref().unwrap()
                    );
                    guard.clear();
                }
            });
        }
    })
    .unwrap();

    thread::scope(|s| {
        for t in (THREADS / 2)..THREADS {
            s.spawn(move |_| {
                let cursor = &mut Cursor(Finder::new(), Helper::new());
                let rng = &mut rand::thread_rng();
                let mut keys: Vec<i32> =
                    (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                keys.shuffle(rng);
                let guard = &mut G::new();
                for i in keys {
                    assert!(map.find(&i, cursor, guard));
                    assert_eq!(
                        i.to_string(),
                        *unsafe { cursor.0.l.deref() }.value.as_ref().unwrap()
                    );
                    guard.clear();
                }
            });
        }
    })
    .unwrap();
}

#[test]
fn smoke_ebr() {
    smoke::<cdrc_rs::GuardEBR>();
}
