use cdrc_rs::{AcquireRetire, AtomicRcPtr, LocalPtr, RcPtr, SnapshotPtr};

use super::concurrent_map::ConcurrentMap;

use std::cmp;

static WEIGHT: usize = 2;

// TODO: optimization from the paper? IBR paper doesn't do that

bitflags! {
    /// TODO
    struct Retired: usize {
        const RETIRED = 1usize;
    }
}

impl Retired {
    fn new(retired: bool) -> Self {
        if retired {
            Retired::RETIRED
        } else {
            Retired::empty()
        }
    }

    fn retired(self) -> bool {
        !(self & Retired::RETIRED).is_empty()
    }
}

/// a real node in tree or a wrapper of State node
/// Retired node if Shared ptr of Node has RETIRED tag.
struct Node<K, V, Guard>
where
    Guard: AcquireRetire,
{
    key: K,
    value: V,
    size: usize,
    left: AtomicRcPtr<Node<K, V, Guard>, Guard>,
    right: AtomicRcPtr<Node<K, V, Guard>, Guard>,
}

impl<'g, K, V, Guard> Node<K, V, Guard>
where
    K: Ord + Clone + 'g,
    V: Clone + 'g,
    Guard: AcquireRetire + 'g,
{
    fn retired_node(guard: &'g Guard) -> RcPtr<'g, Self, Guard> {
        RcPtr::null(guard).with_mark(Retired::new(true).bits())
    }

    fn is_retired<P>(node: &P) -> bool
    where
        P: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        Retired::from_bits_truncate(node.mark()).retired()
    }

    fn is_retired_spot<P>(node: &P, guard: &'g Guard) -> bool
    where
        P: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        if Self::is_retired(node) {
            return true;
        }

        if let Some(node_ref) = unsafe { node.as_ref() } {
            Self::is_retired(&node_ref.left.load_snapshot(guard))
                || Self::is_retired(&node_ref.right.load_snapshot(guard))
        } else {
            false
        }
    }

    fn node_size<P>(node: &P) -> usize
    where
        P: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        debug_assert!(!Self::is_retired(node));
        if let Some(node_ref) = unsafe { node.as_ref() } {
            node_ref.size
        } else {
            0
        }
    }
}

/// Each op creates a new local state and tries to update (CAS) the tree with it.
///
/// Since BonsaiTreeMap.curr_state is Atomic<State<_>>, *const Node<_> can't be used here.
struct State<'g, K, V, Guard>
where
    Guard: AcquireRetire,
{
    root_link: &'g AtomicRcPtr<Node<K, V, Guard>, Guard>,
    curr_root: SnapshotPtr<'g, Node<K, V, Guard>, Guard>,
}

impl<'g, K, V, Guard> State<'g, K, V, Guard>
where
    K: Ord + Clone,
    V: Clone,
    Guard: AcquireRetire,
{
    fn new(root_link: &'g AtomicRcPtr<Node<K, V, Guard>, Guard>, guard: &'g Guard) -> Self {
        Self {
            root_link,
            curr_root: SnapshotPtr::null(guard),
        }
    }

    fn load_root(&mut self, guard: &'g Guard) {
        self.curr_root = self.root_link.load_snapshot(guard);
    }

    // TODO get ref of K, V and clone here
    fn mk_node<P1, P2>(
        &mut self,
        left: &P1,
        right: &P2,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> RcPtr<'g, Node<K, V, Guard>, Guard>
    where
        P1: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P2: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        if Node::is_retired_spot(left, guard) || Node::is_retired_spot(right, guard) {
            return Node::retired_node(guard);
        }

        let left_size = Node::node_size(left);
        let right_size = Node::node_size(right);
        let new_node = RcPtr::make_shared(
            Node {
                key,
                value,
                size: left_size + right_size + 1,
                left: AtomicRcPtr::from_local(left, guard),
                right: AtomicRcPtr::from_local(right, guard),
            },
            guard,
        );
        new_node
    }

    /// Make a new balanced tree from cur (the root of a subtree) and newly constructed left and right subtree
    fn mk_balanced<P1, P2, P3>(
        &mut self,
        cur: &P1,
        left: &P2,
        right: &P3,
        guard: &'g Guard,
    ) -> RcPtr<'g, Node<K, V, Guard>, Guard>
    where
        P1: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P2: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P3: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        if Node::is_retired_spot(cur, guard)
            || Node::is_retired_spot(left, guard)
            || Node::is_retired_spot(right, guard)
        {
            return Node::retired_node(guard);
        }

        let cur_ref = unsafe { cur.deref() };
        let key = cur_ref.key.clone();
        let value = cur_ref.value.clone();

        let l_size = Node::node_size(left);
        let r_size = Node::node_size(right);
        let res = if r_size > 0
            && ((l_size > 0 && r_size > WEIGHT * l_size) || (l_size == 0 && r_size > WEIGHT))
        {
            self.mk_balanced_left(left, right, key, value, guard)
        } else if l_size > 0
            && ((r_size > 0 && l_size > WEIGHT * r_size) || (r_size == 0 && l_size > WEIGHT))
        {
            self.mk_balanced_right(left, right, key, value, guard)
        } else {
            self.mk_node(left, right, key, value, guard)
        };
        res
    }

    #[inline]
    fn mk_balanced_left<P1, P2>(
        &mut self,
        left: &P1,
        right: &P2,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> RcPtr<'g, Node<K, V, Guard>, Guard>
    where
        P1: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P2: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        let right_ref = unsafe { right.deref() };
        let right_left = right_ref.left.load_snapshot(guard);
        let right_right = right_ref.right.load_snapshot(guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(&right_left, guard)
            || Node::is_retired_spot(&right_right, guard)
        {
            return Node::retired_node(guard);
        }

        if Node::node_size(&right_left) < Node::node_size(&right_right) {
            // single left rotation
            return self.single_left(left, right, &right_left, &right_right, key, value, guard);
        }

        // double left rotation
        return self.double_left(left, right, &right_left, &right_right, key, value, guard);
    }

    #[inline]
    fn single_left<P1, P2, P3, P4>(
        &mut self,
        left: &P1,
        right: &P2,
        right_left: &P3,
        right_right: &P4,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> RcPtr<'g, Node<K, V, Guard>, Guard>
    where
        P1: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P2: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P3: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P4: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        let right_ref = unsafe { right.deref() };
        let new_left = self.mk_node(left, right_left, key, value, guard);
        let res = self.mk_node(
            &new_left,
            right_right,
            right_ref.key.clone(),
            right_ref.value.clone(),
            guard,
        );
        return res;
    }

    #[inline]
    fn double_left<P1, P2, P3, P4>(
        &mut self,
        left: &P1,
        right: &P2,
        right_left: &P3,
        right_right: &P4,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> RcPtr<'g, Node<K, V, Guard>, Guard>
    where
        P1: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P2: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P3: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P4: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        let right_ref = unsafe { right.deref() };
        let right_left_ref = unsafe { right_left.deref() };
        let right_left_left = right_left_ref.left.load_snapshot(guard);
        let right_left_right = right_left_ref.right.load_snapshot(guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(&right_left_left, guard)
            || Node::is_retired_spot(&right_left_right, guard)
        {
            return Node::retired_node(guard);
        }

        let new_left = self.mk_node(left, &right_left_left, key, value, guard);
        let new_right = self.mk_node(
            &right_left_right,
            right_right,
            right_ref.key.clone(),
            right_ref.value.clone(),
            guard,
        );
        let res = self.mk_node(
            &new_left,
            &new_right,
            right_left_ref.key.clone(),
            right_left_ref.value.clone(),
            guard,
        );
        res
    }

    #[inline]
    fn mk_balanced_right<P1, P2>(
        &mut self,
        left: &P1,
        right: &P2,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> RcPtr<'g, Node<K, V, Guard>, Guard>
    where
        P1: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P2: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        let left_ref = unsafe { left.deref() };
        let left_right = left_ref.right.load(guard);
        let left_left = left_ref.left.load(guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(&left_right, guard)
            || Node::is_retired_spot(&left_left, guard)
        {
            return Node::retired_node(guard);
        }

        if Node::node_size(&left_right) < Node::node_size(&left_left) {
            // single right rotation (fig 3)
            return self.single_right(left, right, &left_right, &left_left, key, value, guard);
        }
        // double right rotation
        return self.double_right(left, right, &left_right, &left_left, key, value, guard);
    }

    #[inline]
    fn single_right<P1, P2, P3, P4>(
        &mut self,
        left: &P1,
        right: &P2,
        left_right: &P3,
        left_left: &P4,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> RcPtr<'g, Node<K, V, Guard>, Guard>
    where
        P1: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P2: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P3: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P4: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        let left_ref = unsafe { left.deref() };
        let new_right = self.mk_node(left_right, right, key, value, guard);
        let res = self.mk_node(
            left_left,
            &new_right,
            left_ref.key.clone(),
            left_ref.value.clone(),
            guard,
        );
        return res;
    }

    #[inline]
    fn double_right<P1, P2, P3, P4>(
        &mut self,
        left: &P1,
        right: &P2,
        left_right: &P3,
        left_left: &P4,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> RcPtr<'g, Node<K, V, Guard>, Guard>
    where
        P1: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P2: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P3: LocalPtr<'g, Node<K, V, Guard>, Guard>,
        P4: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        let left_ref = unsafe { left.deref() };
        let left_right_ref = unsafe { left_right.deref() };
        let left_right_left = left_right_ref.left.load_snapshot(guard);
        let left_right_right = left_right_ref.right.load_snapshot(guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(&left_right_left, guard)
            || Node::is_retired_spot(&left_right_right, guard)
        {
            return Node::retired_node(guard);
        }

        let new_left = self.mk_node(
            left_left,
            &left_right_left,
            left_ref.key.clone(),
            left_ref.value.clone(),
            guard,
        );
        let new_right = self.mk_node(&left_right_right, right, key, value, guard);
        let res = self.mk_node(
            &new_left,
            &new_right,
            left_right_ref.key.clone(),
            left_right_ref.value.clone(),
            guard,
        );
        res
    }

    #[inline]
    fn do_insert<P>(
        &mut self,
        node: &P,
        key: &K,
        value: &V,
        guard: &'g Guard,
    ) -> (RcPtr<'g, Node<K, V, Guard>, Guard>, bool)
    where
        P: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        if Node::is_retired_spot(node, guard) {
            return (Node::retired_node(guard), false);
        }

        if node.is_null() {
            return (
                self.mk_node(
                    &SnapshotPtr::null(guard),
                    &SnapshotPtr::null(guard),
                    key.clone(),
                    value.clone(),
                    guard,
                ),
                true,
            );
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load_snapshot(guard);
        let right = node_ref.right.load_snapshot(guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(&left, guard)
            || Node::is_retired_spot(&right, guard)
        {
            return (Node::retired_node(guard), false);
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => (node.clone(guard).as_rc(guard), false),
            cmp::Ordering::Less => {
                let (new_right, inserted) = self.do_insert(&right, key, value, guard);
                (self.mk_balanced(node, &left, &new_right, guard), inserted)
            }
            cmp::Ordering::Greater => {
                let (new_left, inserted) = self.do_insert(&left, key, value, guard);
                (self.mk_balanced(node, &new_left, &right, guard), inserted)
            }
        }
    }

    #[inline]
    fn do_remove<P>(
        &mut self,
        node: &P,
        key: &K,
        guard: &'g Guard,
    ) -> (RcPtr<'g, Node<K, V, Guard>, Guard>, Option<&'g V>)
    where
        P: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        if Node::is_retired_spot(node, guard) {
            return (Node::retired_node(guard), None);
        }

        if node.is_null() {
            return (RcPtr::null(guard), None);
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load_snapshot(guard);
        let right = node_ref.right.load_snapshot(guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(&left, guard)
            || Node::is_retired_spot(&right, guard)
        {
            return (Node::retired_node(guard), None);
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => {
                let value = Some(&node_ref.value);
                if node_ref.size == 1 {
                    return (RcPtr::null(guard), value);
                }

                if !left.is_null() {
                    let (new_left, succ) = self.pull_rightmost(&left, guard);
                    return (self.mk_balanced(&succ, &new_left, &right, guard), value);
                }
                let (new_right, succ) = self.pull_leftmost(&right, guard);
                (self.mk_balanced(&succ, &left, &new_right, guard), value)
            }
            cmp::Ordering::Less => {
                let (new_right, value) = self.do_remove(&right, key, guard);
                (self.mk_balanced(node, &left, &new_right, guard), value)
            }
            cmp::Ordering::Greater => {
                let (new_left, value) = self.do_remove(&left, key, guard);
                (self.mk_balanced(node, &new_left, &right, guard), value)
            }
        }
    }

    fn pull_leftmost<P>(
        &mut self,
        node: &P,
        guard: &'g Guard,
    ) -> (
        RcPtr<'g, Node<K, V, Guard>, Guard>,
        RcPtr<'g, Node<K, V, Guard>, Guard>,
    )
    where
        P: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        if Node::is_retired_spot(node, guard) {
            return (Node::retired_node(guard), Node::retired_node(guard));
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(guard);
        let right = node_ref.right.load(guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(&left, guard)
            || Node::is_retired_spot(&right, guard)
        {
            return (Node::retired_node(guard), Node::retired_node(guard));
        }

        if !left.is_null() {
            let (new_left, succ) = self.pull_leftmost(&left, guard);
            return (self.mk_balanced(node, &new_left, &right, guard), succ);
        }
        // node is the leftmost
        let succ = self.mk_node(
            &SnapshotPtr::null(guard),
            &SnapshotPtr::null(guard),
            node_ref.key.clone(),
            node_ref.value.clone(),
            guard,
        );
        return (right, succ);
    }

    fn pull_rightmost<P>(
        &mut self,
        node: &P,
        guard: &'g Guard,
    ) -> (
        RcPtr<'g, Node<K, V, Guard>, Guard>,
        RcPtr<'g, Node<K, V, Guard>, Guard>,
    )
    where
        P: LocalPtr<'g, Node<K, V, Guard>, Guard>,
    {
        if Node::is_retired_spot(node, guard) {
            return (Node::retired_node(guard), Node::retired_node(guard));
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(guard);
        let right = node_ref.right.load_snapshot(guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(&left, guard)
            || Node::is_retired_spot(&right, guard)
        {
            return (Node::retired_node(guard), Node::retired_node(guard));
        }

        if !right.is_null() {
            let (new_right, succ) = self.pull_rightmost(&right, guard);
            return (self.mk_balanced(node, &left, &new_right, guard), succ);
        }
        // node is the rightmost
        let succ = self.mk_node(
            &SnapshotPtr::null(guard),
            &SnapshotPtr::null(guard),
            node_ref.key.clone(),
            node_ref.value.clone(),
            guard,
        );
        return (left, succ);
    }

    pub fn check_root(&self, guard: &Guard) -> bool {
        self.curr_root == self.root_link.load_snapshot(guard)
    }
}

pub struct BonsaiTreeMap<K, V, Guard>
where
    Guard: AcquireRetire,
{
    root: AtomicRcPtr<Node<K, V, Guard>, Guard>,
}

impl<K, V, Guard> BonsaiTreeMap<K, V, Guard>
where
    K: Ord + Clone,
    V: Clone,
    Guard: AcquireRetire,
{
    pub fn new() -> Self {
        Self {
            root: AtomicRcPtr::null(),
        }
    }

    pub fn get<'g>(&self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        loop {
            let mut node = self.root.load_snapshot(guard);
            while !node.is_null() && !Node::is_retired(&node) {
                let node_ref = unsafe { node.deref() };
                match key.cmp(&node_ref.key) {
                    cmp::Ordering::Equal => break,
                    cmp::Ordering::Less => node = node_ref.left.load_snapshot(guard),
                    cmp::Ordering::Greater => node = node_ref.right.load_snapshot(guard),
                }
            }

            if Node::is_retired_spot(&node, guard) {
                continue;
            }

            if node.is_null() {
                return None;
            }

            let node_ref = unsafe { node.deref() };
            return Some(&node_ref.value);
        }
    }

    pub fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        let mut state = State::new(&self.root, guard);
        loop {
            state.load_root(guard);
            let old_root = state.curr_root.clone(guard);
            let (new_root, inserted) = state.do_insert(&old_root, &key, &value, guard);

            if Node::is_retired(&new_root) {
                continue;
            }

            if self
                .root
                .compare_exchange(&old_root, &new_root, guard)
                .is_ok()
            {
                return inserted;
            }
        }
    }

    pub fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let mut state = State::new(&self.root, guard);
        loop {
            state.load_root(guard);
            let old_root = state.curr_root.clone(guard);
            let (new_root, value) = state.do_remove(&old_root, key, guard);

            if Node::is_retired(&new_root) {
                continue;
            }

            if self
                .root
                .compare_exchange(&old_root, &new_root, guard)
                .is_ok()
            {
                return value;
            }
        }
    }
}

// TODO: move it to somewhere else...
impl<K, V, Guard> ConcurrentMap<K, V, Guard> for BonsaiTreeMap<K, V, Guard>
where
    K: Ord + Clone,
    V: Clone,
    Guard: AcquireRetire,
{
    fn new() -> Self {
        Self::new()
    }

    #[inline(never)]
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, guard)
    }
    #[inline(never)]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.insert(key, value, guard)
    }
    #[inline(never)]
    fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.remove(key, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::BonsaiTreeMap;
    use crate::cdrc::concurrent_map;
    use cdrc_rs::GuardEBR;

    #[test]
    fn smoke_nm_tree() {
        concurrent_map::tests::smoke::<GuardEBR, BonsaiTreeMap<i32, String, GuardEBR>>();
    }
}
