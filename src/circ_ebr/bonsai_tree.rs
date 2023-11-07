use circ::{AtomicRc, CsEBR, GraphNode, Pointer, Rc, Snapshot, StrongPtr, TaggedCnt};

use super::concurrent_map::{ConcurrentMap, OutputHolder};

use std::{cmp, sync::atomic::Ordering};

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
pub struct Node<K, V> {
    key: K,
    value: V,
    size: usize,
    left: AtomicRc<Node<K, V>, CsEBR>,
    right: AtomicRc<Node<K, V>, CsEBR>,
}

impl<K, V> GraphNode<CsEBR> for Node<K, V> {
    const UNIQUE_OUTDEGREE: bool = false;

    #[inline]
    fn pop_outgoings(&self) -> Vec<Rc<Self, CsEBR>>
    where
        Self: Sized,
    {
        vec![
            self.left.swap(Rc::null(), Ordering::Relaxed),
            self.right.swap(Rc::null(), Ordering::Relaxed),
        ]
    }

    #[inline]
    fn pop_unique(&self) -> Rc<Self, CsEBR>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

impl<K, V> Node<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn retired_node() -> Rc<Self, CsEBR> {
        Rc::null().with_tag(Retired::new(true).bits())
    }

    fn is_retired(node: TaggedCnt<Node<K, V>>) -> bool {
        Retired::from_bits_truncate(node.tag()).retired()
    }

    fn is_retired_spot<P>(node: &P) -> bool
    where
        P: StrongPtr<Node<K, V>, CsEBR>,
    {
        if Self::is_retired(node.as_ptr()) {
            return true;
        }

        if let Some(node_ref) = node.as_ref() {
            Self::is_retired(node_ref.left.load(Ordering::Acquire))
                || Self::is_retired(node_ref.right.load(Ordering::Acquire))
        } else {
            false
        }
    }

    fn node_size<P>(node: &P) -> usize
    where
        P: StrongPtr<Node<K, V>, CsEBR>,
    {
        debug_assert!(!Self::is_retired(node.as_ptr()));
        if let Some(node_ref) = node.as_ref() {
            node_ref.size
        } else {
            0
        }
    }

    fn load_children(&self, cs: &CsEBR) -> (Snapshot<Self, CsEBR>, Snapshot<Self, CsEBR>) {
        let mut left = Snapshot::new();
        left.load(&self.left, cs);
        let mut right = Snapshot::new();
        right.load(&self.right, cs);
        (left, right)
    }
}

/// Each op creates a new local state and tries to update (CAS) the tree with it.
struct State<'g, K, V> {
    root_link: &'g AtomicRc<Node<K, V>, CsEBR>,
    curr_root: TaggedCnt<Node<K, V>>,
}

impl<K, V> OutputHolder<V> for Snapshot<Node<K, V>, CsEBR> {
    fn output(&self) -> &V {
        self.as_ref().map(|node| &node.value).unwrap()
    }
}

impl<'g, K, V> State<'g, K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new(root_link: &'g AtomicRc<Node<K, V>, CsEBR>, curr_root: TaggedCnt<Node<K, V>>) -> Self {
        Self {
            root_link,
            curr_root,
        }
    }

    // TODO get ref of K, V and clone here
    fn mk_node<P1, P2>(
        &mut self,
        left: P1,
        right: P2,
        key: K,
        value: V,
        _: &CsEBR,
    ) -> Rc<Node<K, V>, CsEBR>
    where
        P1: StrongPtr<Node<K, V>, CsEBR>,
        P2: StrongPtr<Node<K, V>, CsEBR>,
    {
        if Node::is_retired_spot(&left) || Node::is_retired_spot(&right) {
            return Node::retired_node();
        }

        let left_size = Node::node_size(&left);
        let right_size = Node::node_size(&right);
        let new_node = Rc::new(Node {
            key,
            value,
            size: left_size + right_size + 1,
            left: AtomicRc::from(left.into_rc()),
            right: AtomicRc::from(right.into_rc()),
        });
        new_node
    }

    /// Make a new balanced tree from cur (the root of a subtree) and newly constructed left and right subtree
    fn mk_balanced<P1, P2, P3>(
        &mut self,
        cur: &P1,
        left: P2,
        right: P3,
        cs: &CsEBR,
    ) -> Rc<Node<K, V>, CsEBR>
    where
        P1: StrongPtr<Node<K, V>, CsEBR>,
        P2: StrongPtr<Node<K, V>, CsEBR>,
        P3: StrongPtr<Node<K, V>, CsEBR>,
    {
        if Node::is_retired_spot(cur)
            || Node::is_retired_spot(&left)
            || Node::is_retired_spot(&right)
        {
            return Node::retired_node();
        }

        let cur_ref = unsafe { cur.deref() };
        let key = cur_ref.key.clone();
        let value = cur_ref.value.clone();

        let l_size = Node::node_size(&left);
        let r_size = Node::node_size(&right);
        let res = if r_size > 0
            && ((l_size > 0 && r_size > WEIGHT * l_size) || (l_size == 0 && r_size > WEIGHT))
        {
            self.mk_balanced_left(left, right, key, value, cs)
        } else if l_size > 0
            && ((r_size > 0 && l_size > WEIGHT * r_size) || (r_size == 0 && l_size > WEIGHT))
        {
            self.mk_balanced_right(left, right, key, value, cs)
        } else {
            self.mk_node(left, right, key, value, cs)
        };
        res
    }

    #[inline]
    fn mk_balanced_left<P1, P2>(
        &mut self,
        left: P1,
        right: P2,
        key: K,
        value: V,
        cs: &CsEBR,
    ) -> Rc<Node<K, V>, CsEBR>
    where
        P1: StrongPtr<Node<K, V>, CsEBR>,
        P2: StrongPtr<Node<K, V>, CsEBR>,
    {
        let right_ref = unsafe { right.deref() };
        let (right_left, right_right) = right_ref.load_children(cs);

        if !self.check_root()
            || Node::is_retired_spot(&right_left)
            || Node::is_retired_spot(&right_right)
        {
            return Node::retired_node();
        }

        if Node::node_size(&right_left) < Node::node_size(&right_right) {
            // single left rotation
            return self.single_left(left, right, right_left, right_right, key, value, cs);
        }

        // double left rotation
        return self.double_left(left, right, right_left, right_right, key, value, cs);
    }

    #[inline]
    fn single_left<P1, P2, P3, P4>(
        &mut self,
        left: P1,
        right: P2,
        right_left: P3,
        right_right: P4,
        key: K,
        value: V,
        cs: &CsEBR,
    ) -> Rc<Node<K, V>, CsEBR>
    where
        P1: StrongPtr<Node<K, V>, CsEBR>,
        P2: StrongPtr<Node<K, V>, CsEBR>,
        P3: StrongPtr<Node<K, V>, CsEBR>,
        P4: StrongPtr<Node<K, V>, CsEBR>,
    {
        let right_ref = unsafe { right.deref() };
        let new_left = self.mk_node(left, right_left, key, value, cs);
        let res = self.mk_node(
            new_left,
            right_right,
            right_ref.key.clone(),
            right_ref.value.clone(),
            cs,
        );
        return res;
    }

    #[inline]
    fn double_left<P1, P2, P3, P4>(
        &mut self,
        left: P1,
        right: P2,
        right_left: P3,
        right_right: P4,
        key: K,
        value: V,
        cs: &CsEBR,
    ) -> Rc<Node<K, V>, CsEBR>
    where
        P1: StrongPtr<Node<K, V>, CsEBR>,
        P2: StrongPtr<Node<K, V>, CsEBR>,
        P3: StrongPtr<Node<K, V>, CsEBR>,
        P4: StrongPtr<Node<K, V>, CsEBR>,
    {
        let right_ref = unsafe { right.deref() };
        let right_left_ref = unsafe { right_left.deref() };
        let (right_left_left, right_left_right) = right_left_ref.load_children(cs);

        if !self.check_root()
            || Node::is_retired_spot(&right_left_left)
            || Node::is_retired_spot(&right_left_right)
        {
            return Node::retired_node();
        }

        let new_left = self.mk_node(left, right_left_left, key, value, cs);
        let new_right = self.mk_node(
            right_left_right,
            right_right,
            right_ref.key.clone(),
            right_ref.value.clone(),
            cs,
        );
        let res = self.mk_node(
            new_left,
            new_right,
            right_left_ref.key.clone(),
            right_left_ref.value.clone(),
            cs,
        );
        res
    }

    #[inline]
    fn mk_balanced_right<P1, P2>(
        &mut self,
        left: P1,
        right: P2,
        key: K,
        value: V,
        cs: &CsEBR,
    ) -> Rc<Node<K, V>, CsEBR>
    where
        P1: StrongPtr<Node<K, V>, CsEBR>,
        P2: StrongPtr<Node<K, V>, CsEBR>,
    {
        let left_ref = unsafe { left.deref() };
        let (left_left, left_right) = left_ref.load_children(cs);

        if !self.check_root()
            || Node::is_retired_spot(&left_right)
            || Node::is_retired_spot(&left_left)
        {
            return Node::retired_node();
        }

        if Node::node_size(&left_right) < Node::node_size(&left_left) {
            // single right rotation (fig 3)
            return self.single_right(left, right, left_right, left_left, key, value, cs);
        }
        // double right rotation
        return self.double_right(left, right, left_right, left_left, key, value, cs);
    }

    #[inline]
    fn single_right<P1, P2, P3, P4>(
        &mut self,
        left: P1,
        right: P2,
        left_right: P3,
        left_left: P4,
        key: K,
        value: V,
        cs: &CsEBR,
    ) -> Rc<Node<K, V>, CsEBR>
    where
        P1: StrongPtr<Node<K, V>, CsEBR>,
        P2: StrongPtr<Node<K, V>, CsEBR>,
        P3: StrongPtr<Node<K, V>, CsEBR>,
        P4: StrongPtr<Node<K, V>, CsEBR>,
    {
        let left_ref = unsafe { left.deref() };
        let new_right = self.mk_node(left_right, right, key, value, cs);
        let res = self.mk_node(
            left_left,
            new_right,
            left_ref.key.clone(),
            left_ref.value.clone(),
            cs,
        );
        return res;
    }

    #[inline]
    fn double_right<P1, P2, P3, P4>(
        &mut self,
        left: P1,
        right: P2,
        left_right: P3,
        left_left: P4,
        key: K,
        value: V,
        cs: &CsEBR,
    ) -> Rc<Node<K, V>, CsEBR>
    where
        P1: StrongPtr<Node<K, V>, CsEBR>,
        P2: StrongPtr<Node<K, V>, CsEBR>,
        P3: StrongPtr<Node<K, V>, CsEBR>,
        P4: StrongPtr<Node<K, V>, CsEBR>,
    {
        let left_ref = unsafe { left.deref() };
        let left_right_ref = unsafe { left_right.deref() };
        let (left_right_left, left_right_right) = left_right_ref.load_children(cs);

        if !self.check_root()
            || Node::is_retired_spot(&left_right_left)
            || Node::is_retired_spot(&left_right_right)
        {
            return Node::retired_node();
        }

        let new_left = self.mk_node(
            left_left,
            left_right_left,
            left_ref.key.clone(),
            left_ref.value.clone(),
            cs,
        );
        let new_right = self.mk_node(left_right_right, right, key, value, cs);
        let res = self.mk_node(
            new_left,
            new_right,
            left_right_ref.key.clone(),
            left_right_ref.value.clone(),
            cs,
        );
        res
    }

    #[inline]
    fn do_insert<P>(
        &mut self,
        node: P,
        key: &K,
        value: &V,
        cs: &CsEBR,
    ) -> (Rc<Node<K, V>, CsEBR>, bool)
    where
        P: StrongPtr<Node<K, V>, CsEBR>,
    {
        if Node::is_retired_spot(&node) {
            return (Node::retired_node(), false);
        }

        if node.is_null() {
            return (
                self.mk_node(Rc::null(), Rc::null(), key.clone(), value.clone(), cs),
                true,
            );
        }

        let node_ref = unsafe { node.deref() };
        let (left, right) = node_ref.load_children(cs);

        if !self.check_root() || Node::is_retired_spot(&left) || Node::is_retired_spot(&right) {
            return (Node::retired_node(), false);
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => (node.into_rc(), false),
            cmp::Ordering::Less => {
                let (new_right, inserted) = self.do_insert(right, key, value, cs);
                (self.mk_balanced(&node, left, new_right, cs), inserted)
            }
            cmp::Ordering::Greater => {
                let (new_left, inserted) = self.do_insert(left, key, value, cs);
                (self.mk_balanced(&node, new_left, right, cs), inserted)
            }
        }
    }

    #[inline]
    fn do_remove(
        &mut self,
        node: &Snapshot<Node<K, V>, CsEBR>,
        key: &K,
        cs: &CsEBR,
    ) -> (Rc<Node<K, V>, CsEBR>, Option<Snapshot<Node<K, V>, CsEBR>>) {
        if Node::is_retired_spot(&node) {
            return (Node::retired_node(), None);
        }

        if node.is_null() {
            return (Rc::null(), None);
        }

        let node_ref = unsafe { node.deref() };
        let (left, right) = node_ref.load_children(cs);

        if !self.check_root() || Node::is_retired_spot(&left) || Node::is_retired_spot(&right) {
            return (Node::retired_node(), None);
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => {
                if node_ref.size == 1 {
                    return (Rc::null(), Some(*node));
                }

                if !left.is_null() {
                    let (new_left, succ) = self.pull_rightmost(left, cs);
                    return (self.mk_balanced(&succ, new_left, right, cs), Some(*node));
                }
                let (new_right, succ) = self.pull_leftmost(right, cs);
                (self.mk_balanced(&succ, left, new_right, cs), Some(*node))
            }
            cmp::Ordering::Less => {
                let (new_right, found) = self.do_remove(&right, key, cs);
                (self.mk_balanced(&node, left, new_right, cs), found)
            }
            cmp::Ordering::Greater => {
                let (new_left, found) = self.do_remove(&left, key, cs);
                (self.mk_balanced(&node, new_left, right, cs), found)
            }
        }
    }

    fn pull_leftmost<P>(
        &mut self,
        node: P,
        cs: &CsEBR,
    ) -> (Rc<Node<K, V>, CsEBR>, Rc<Node<K, V>, CsEBR>)
    where
        P: StrongPtr<Node<K, V>, CsEBR>,
    {
        if Node::is_retired_spot(&node) {
            return (Node::retired_node(), Node::retired_node());
        }

        let node_ref = unsafe { node.deref() };
        let (left, right) = node_ref.load_children(cs);

        if !self.check_root() || Node::is_retired_spot(&left) || Node::is_retired_spot(&right) {
            return (Node::retired_node(), Node::retired_node());
        }

        if !left.is_null() {
            let (new_left, succ) = self.pull_leftmost(left, cs);
            return (self.mk_balanced(&node, new_left, right, cs), succ);
        }
        // node is the leftmost
        let succ = self.mk_node(
            Rc::null(),
            Rc::null(),
            node_ref.key.clone(),
            node_ref.value.clone(),
            cs,
        );
        return (right.into_rc(), succ);
    }

    fn pull_rightmost<P>(
        &mut self,
        node: P,
        cs: &CsEBR,
    ) -> (Rc<Node<K, V>, CsEBR>, Rc<Node<K, V>, CsEBR>)
    where
        P: StrongPtr<Node<K, V>, CsEBR>,
    {
        if Node::is_retired_spot(&node) {
            return (Node::retired_node(), Node::retired_node());
        }

        let node_ref = unsafe { node.deref() };
        let (left, right) = node_ref.load_children(cs);

        if !self.check_root() || Node::is_retired_spot(&left) || Node::is_retired_spot(&right) {
            return (Node::retired_node(), Node::retired_node());
        }

        if !right.is_null() {
            let (new_right, succ) = self.pull_rightmost(right, cs);
            return (self.mk_balanced(&node, left, new_right, cs), succ);
        }
        // node is the rightmost
        let succ = self.mk_node(
            Rc::null(),
            Rc::null(),
            node_ref.key.clone(),
            node_ref.value.clone(),
            cs,
        );
        return (left.into_rc(), succ);
    }

    pub fn check_root(&self) -> bool {
        self.curr_root == self.root_link.load(Ordering::Acquire)
    }
}

pub struct BonsaiTreeMap<K, V> {
    root: AtomicRc<Node<K, V>, CsEBR>,
}

impl<K, V> BonsaiTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            root: AtomicRc::null(),
        }
    }

    pub fn get(&self, key: &K, cs: &CsEBR) -> Option<Snapshot<Node<K, V>, CsEBR>> {
        loop {
            let mut node = self.root.load_ss(cs);
            while !node.is_null() && !Node::is_retired(node.as_ptr()) {
                let node_ref = unsafe { node.deref() };
                match key.cmp(&node_ref.key) {
                    cmp::Ordering::Equal => break,
                    cmp::Ordering::Less => node = node_ref.left.load_ss(cs),
                    cmp::Ordering::Greater => node = node_ref.right.load_ss(cs),
                }
            }

            if Node::is_retired_spot(&node) {
                continue;
            }

            if node.is_null() {
                return None;
            }

            return Some(node);
        }
    }

    pub fn insert(&self, key: K, value: V, cs: &CsEBR) -> bool {
        loop {
            let curr_root = self.root.load_ss(cs);
            let mut state = State::new(&self.root, curr_root.as_ptr());
            let (new_root, inserted) = state.do_insert(curr_root, &key, &value, cs);

            if Node::is_retired(new_root.as_ptr()) {
                continue;
            }

            if self
                .root
                .compare_exchange(
                    curr_root.as_ptr(),
                    new_root,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    cs,
                )
                .is_ok()
            {
                return inserted;
            }
        }
    }

    pub fn remove(&self, key: &K, cs: &CsEBR) -> Option<Snapshot<Node<K, V>, CsEBR>> {
        loop {
            let curr_root = self.root.load_ss(cs);
            let mut state = State::new(&self.root, curr_root.as_ptr());
            let (new_root, found) = state.do_remove(&curr_root, key, cs);

            if Node::is_retired(new_root.as_ptr()) {
                continue;
            }

            if self
                .root
                .compare_exchange(
                    curr_root.as_ptr(),
                    new_root,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    cs,
                )
                .is_ok()
            {
                return found;
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for BonsaiTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Output = Snapshot<Node<K, V>, CsEBR>;

    fn new() -> Self {
        BonsaiTreeMap::new()
    }

    fn get(&self, key: &K, cs: &CsEBR) -> Option<Self::Output> {
        self.get(key, cs)
    }

    fn insert(&self, key: K, value: V, cs: &CsEBR) -> bool {
        self.insert(key, value, cs)
    }

    fn remove(&self, key: &K, cs: &CsEBR) -> Option<Self::Output> {
        self.remove(key, cs)
    }
}

#[cfg(test)]
mod tests {
    use super::BonsaiTreeMap;
    use crate::circ_ebr::concurrent_map;

    #[test]
    fn smoke_bonsai_tree() {
        concurrent_map::tests::smoke::<BonsaiTreeMap<i32, String>>();
    }
}
