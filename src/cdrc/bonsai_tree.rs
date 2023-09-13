use cdrc_rs::{AtomicRc, Cs, Pointer, Rc, Snapshot, StrongPtr, TaggedCnt};

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
struct Node<K, V, C: Cs> {
    key: K,
    value: V,
    size: usize,
    left: AtomicRc<Node<K, V, C>, C>,
    right: AtomicRc<Node<K, V, C>, C>,
}

impl<K, V, C> Node<K, V, C>
where
    K: Ord + Clone,
    V: Clone,
    C: Cs,
{
    fn retired_node() -> Rc<Self, C> {
        Rc::null().with_tag(Retired::new(true).bits())
    }

    fn is_retired(node: TaggedCnt<Node<K, V, C>>) -> bool {
        Retired::from_bits_truncate(node.tag()).retired()
    }

    fn is_retired_spot<P>(node: &P) -> bool
    where
        P: StrongPtr<Node<K, V, C>, C>,
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
        P: StrongPtr<Node<K, V, C>, C>,
    {
        debug_assert!(!Self::is_retired(node.as_ptr()));
        if let Some(node_ref) = node.as_ref() {
            node_ref.size
        } else {
            0
        }
    }

    fn load_children(&self, temp: &mut Snapshot<Self, C>, cs: &C) -> (Rc<Self, C>, Rc<Self, C>) {
        temp.load(&self.left, cs);
        let left = Rc::from_snapshot(temp, cs);
        temp.load(&self.right, cs);
        let right = Rc::from_snapshot(temp, cs);
        (left, right)
    }
}

pub struct Holder<K, V, C: Cs> {
    root: TaggedCnt<Node<K, V, C>>,
    curr: Snapshot<Node<K, V, C>, C>,
    temp: Snapshot<Node<K, V, C>, C>,
    found: Option<V>,
}

/// Each op creates a new local state and tries to update (CAS) the tree with it.
struct State<'g, K, V, C: Cs> {
    root_link: &'g AtomicRc<Node<K, V, C>, C>,
    holder: &'g mut Holder<K, V, C>,
}

pub struct Cursor<K, V, C: Cs>(Holder<K, V, C>, Snapshot<Node<K, V, C>, C>);

impl<K, V, C: Cs> OutputHolder<V> for Cursor<K, V, C> {
    fn default() -> Self {
        Self(
            Holder {
                root: Default::default(),
                curr: Default::default(),
                temp: Default::default(),
                found: None,
            },
            Default::default(),
        )
    }

    fn output(&self) -> &V {
        self.0.found.as_ref().unwrap()
    }
}

impl<'g, K, V, C> State<'g, K, V, C>
where
    K: Ord + Clone,
    V: Clone,
    C: Cs,
{
    fn new(root_link: &'g AtomicRc<Node<K, V, C>, C>, holder: &'g mut Holder<K, V, C>) -> Self {
        Self { root_link, holder }
    }

    // TODO get ref of K, V and clone here
    fn mk_node(
        &mut self,
        left: Rc<Node<K, V, C>, C>,
        right: Rc<Node<K, V, C>, C>,
        key: K,
        value: V,
        cs: &C,
    ) -> Rc<Node<K, V, C>, C> {
        if Node::is_retired_spot(&left) || Node::is_retired_spot(&right) {
            return Node::retired_node();
        }

        let left_size = Node::node_size(&left);
        let right_size = Node::node_size(&right);
        let new_node = Rc::new(
            Node {
                key,
                value,
                size: left_size + right_size + 1,
                left: AtomicRc::from(left),
                right: AtomicRc::from(right),
            },
            cs,
        );
        new_node
    }

    /// Make a new balanced tree from cur (the root of a subtree) and newly constructed left and right subtree
    fn mk_balanced<P>(
        &mut self,
        cur: &P,
        left: Rc<Node<K, V, C>, C>,
        right: Rc<Node<K, V, C>, C>,
        cs: &C,
    ) -> Rc<Node<K, V, C>, C>
    where
        P: StrongPtr<Node<K, V, C>, C>,
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
    fn mk_balanced_left(
        &mut self,
        left: Rc<Node<K, V, C>, C>,
        right: Rc<Node<K, V, C>, C>,
        key: K,
        value: V,
        cs: &C,
    ) -> Rc<Node<K, V, C>, C> {
        let right_ref = unsafe { right.deref() };
        let (right_left, right_right) = right_ref.load_children(&mut self.holder.temp, cs);

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
    fn single_left(
        &mut self,
        left: Rc<Node<K, V, C>, C>,
        right: Rc<Node<K, V, C>, C>,
        right_left: Rc<Node<K, V, C>, C>,
        right_right: Rc<Node<K, V, C>, C>,
        key: K,
        value: V,
        cs: &C,
    ) -> Rc<Node<K, V, C>, C> {
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
    fn double_left(
        &mut self,
        left: Rc<Node<K, V, C>, C>,
        right: Rc<Node<K, V, C>, C>,
        right_left: Rc<Node<K, V, C>, C>,
        right_right: Rc<Node<K, V, C>, C>,
        key: K,
        value: V,
        cs: &C,
    ) -> Rc<Node<K, V, C>, C> {
        let right_ref = unsafe { right.deref() };
        let right_left_ref = unsafe { right_left.deref() };
        let (right_left_left, right_left_right) =
            right_left_ref.load_children(&mut self.holder.temp, cs);

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
    fn mk_balanced_right(
        &mut self,
        left: Rc<Node<K, V, C>, C>,
        right: Rc<Node<K, V, C>, C>,
        key: K,
        value: V,
        cs: &C,
    ) -> Rc<Node<K, V, C>, C> {
        let left_ref = unsafe { left.deref() };
        let (left_left, left_right) = left_ref.load_children(&mut self.holder.temp, cs);

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
    fn single_right(
        &mut self,
        left: Rc<Node<K, V, C>, C>,
        right: Rc<Node<K, V, C>, C>,
        left_right: Rc<Node<K, V, C>, C>,
        left_left: Rc<Node<K, V, C>, C>,
        key: K,
        value: V,
        cs: &C,
    ) -> Rc<Node<K, V, C>, C> {
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
    fn double_right(
        &mut self,
        left: Rc<Node<K, V, C>, C>,
        right: Rc<Node<K, V, C>, C>,
        left_right: Rc<Node<K, V, C>, C>,
        left_left: Rc<Node<K, V, C>, C>,
        key: K,
        value: V,
        cs: &C,
    ) -> Rc<Node<K, V, C>, C> {
        let left_ref = unsafe { left.deref() };
        let left_right_ref = unsafe { left_right.deref() };
        let (left_right_left, left_right_right) =
            left_right_ref.load_children(&mut self.holder.temp, cs);

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
    fn do_insert<P>(&mut self, node: P, key: &K, value: &V, cs: &C) -> (Rc<Node<K, V, C>, C>, bool)
    where
        P: StrongPtr<Node<K, V, C>, C>,
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
        let (left, right) = node_ref.load_children(&mut self.holder.temp, cs);

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
    fn do_remove<P>(&mut self, node: P, key: &K, cs: &C) -> (Rc<Node<K, V, C>, C>, bool)
    where
        P: StrongPtr<Node<K, V, C>, C>,
    {
        if Node::is_retired_spot(&node) {
            return (Node::retired_node(), false);
        }

        if node.is_null() {
            return (Rc::null(), false);
        }

        let node_ref = unsafe { node.deref() };
        let (left, right) = node_ref.load_children(&mut self.holder.temp, cs);

        if !self.check_root() || Node::is_retired_spot(&left) || Node::is_retired_spot(&right) {
            return (Node::retired_node(), false);
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => {
                self.holder.found = Some(node_ref.value.clone());
                if node_ref.size == 1 {
                    return (Rc::null(), true);
                }

                if !left.is_null() {
                    let (new_left, succ) = self.pull_rightmost(left, cs);
                    return (self.mk_balanced(&succ, new_left, right, cs), true);
                }
                let (new_right, succ) = self.pull_leftmost(right, cs);
                (self.mk_balanced(&succ, left, new_right, cs), true)
            }
            cmp::Ordering::Less => {
                let (new_right, found) = self.do_remove(right, key, cs);
                (self.mk_balanced(&node, left, new_right, cs), found)
            }
            cmp::Ordering::Greater => {
                let (new_left, found) = self.do_remove(left, key, cs);
                (self.mk_balanced(&node, new_left, right, cs), found)
            }
        }
    }

    fn pull_leftmost<P>(&mut self, node: P, cs: &C) -> (Rc<Node<K, V, C>, C>, Rc<Node<K, V, C>, C>)
    where
        P: StrongPtr<Node<K, V, C>, C>,
    {
        if Node::is_retired_spot(&node) {
            return (Node::retired_node(), Node::retired_node());
        }

        let node_ref = unsafe { node.deref() };
        let (left, right) = node_ref.load_children(&mut self.holder.temp, cs);

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
        return (right, succ);
    }

    fn pull_rightmost<P>(&mut self, node: P, cs: &C) -> (Rc<Node<K, V, C>, C>, Rc<Node<K, V, C>, C>)
    where
        P: StrongPtr<Node<K, V, C>, C>,
    {
        if Node::is_retired_spot(&node) {
            return (Node::retired_node(), Node::retired_node());
        }

        let node_ref = unsafe { node.deref() };
        let (left, right) = node_ref.load_children(&mut self.holder.temp, cs);

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
        return (left, succ);
    }

    pub fn check_root(&self) -> bool {
        self.holder.root == self.root_link.load(Ordering::Acquire)
    }
}

pub struct BonsaiTreeMap<K, V, C: Cs> {
    root: AtomicRc<Node<K, V, C>, C>,
}

impl<K, V, C> BonsaiTreeMap<K, V, C>
where
    K: Ord + Clone,
    V: Clone,
    C: Cs,
{
    pub fn new() -> Self {
        Self {
            root: AtomicRc::null(),
        }
    }

    pub fn get(&self, key: &K, holder: &mut Holder<K, V, C>, cs: &C) -> bool {
        loop {
            // NOTE: In this context, `holder.curr` and `holder.temp` is similar
            // to `curr` and `next` in a HHSList traversal.
            holder.curr.load(&self.root, cs);
            loop {
                let curr_node = some_or!(holder.curr.as_ref(), return false);
                let next_link = match key.cmp(&curr_node.key) {
                    cmp::Ordering::Equal => break,
                    cmp::Ordering::Less => &curr_node.left,
                    cmp::Ordering::Greater => &curr_node.right,
                };
                holder.temp.load(next_link, cs);
                Snapshot::swap(&mut holder.curr, &mut holder.temp);
            }

            if Node::is_retired_spot(&holder.curr) {
                continue;
            }

            if holder.curr.is_null() {
                return false;
            }

            holder.found = Some(unsafe { holder.curr.deref() }.value.clone());
            return true;
        }
    }

    pub fn insert(&self, key: K, value: V, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        let mut state = State::new(&self.root, &mut cursor.0);
        loop {
            cursor.1.load(&self.root, cs);
            state.holder.root = cursor.1.as_ptr();
            let (new_root, inserted) = state.do_insert(&cursor.1, &key, &value, cs);

            if Node::is_retired(new_root.as_ptr()) {
                continue;
            }

            if self
                .root
                .compare_exchange(
                    cursor.1.as_ptr(),
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

    pub fn remove(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        let mut state = State::new(&self.root, &mut cursor.0);
        loop {
            cursor.1.load(&self.root, cs);
            state.holder.root = cursor.1.as_ptr();
            let (new_root, found) = state.do_remove(&cursor.1, key, cs);

            if Node::is_retired(new_root.as_ptr()) {
                continue;
            }

            if self
                .root
                .compare_exchange(
                    cursor.1.as_ptr(),
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

impl<K, V, C> ConcurrentMap<K, V, C> for BonsaiTreeMap<K, V, C>
where
    K: Ord + Clone,
    V: Clone,
    C: Cs,
{
    type Output = Cursor<K, V, C>;

    fn new() -> Self {
        BonsaiTreeMap::new()
    }

    fn get(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        self.get(key, &mut output.0, cs)
    }

    fn insert(&self, key: K, value: V, output: &mut Self::Output, cs: &C) -> bool {
        self.insert(key, value, output, cs)
    }

    fn remove(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        self.remove(key, output, cs)
    }
}

#[cfg(test)]
mod tests {
    use super::BonsaiTreeMap;
    use crate::cdrc::concurrent_map;
    use cdrc_rs::{CsEBR, CsHP};

    #[test]
    fn smoke_bonsai_tree_ebr() {
        concurrent_map::tests::smoke::<CsEBR, BonsaiTreeMap<i32, String, CsEBR>>();
    }

    #[test]
    fn smoke_bonsai_tree_hp() {
        concurrent_map::tests::smoke::<CsHP, BonsaiTreeMap<i32, String, CsHP>>();
    }
}
