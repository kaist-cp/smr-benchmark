use std::{cmp, sync::atomic::Ordering};

use hp_brcu::{
    Atomic, CsGuard, Owned, RaGuard, RollbackProof, Shared, Shield, Thread, Unprotected,
};

use super::concurrent_map::{ConcurrentMap, OutputHolder};

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
struct Node<K, V> {
    key: K,
    value: V,
    size: usize,
    left: Atomic<Node<K, V>>,
    right: Atomic<Node<K, V>>,
}

impl<K, V> Node<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn retired_node<'g>() -> Shared<'g, Self> {
        Shared::null().with_tag(Retired::new(true).bits())
    }

    fn is_retired<'g>(node: Shared<'g, Self>) -> bool {
        Retired::from_bits_truncate(node.tag()).retired()
    }

    fn is_retired_spot<'g>(node: Shared<'g, Self>, guard: &'g CsGuard) -> bool {
        if Self::is_retired(node) {
            return true;
        }

        if let Some(node_ref) = unsafe { node.as_ref() } {
            Self::is_retired(node_ref.left.load(Ordering::Acquire, guard))
                || Self::is_retired(node_ref.right.load(Ordering::Acquire, guard))
        } else {
            false
        }
    }

    fn node_size<'g>(node: Shared<'g, Self>, _: &'g CsGuard) -> usize {
        debug_assert!(!Self::is_retired(node));
        if let Some(node_ref) = unsafe { node.as_ref() } {
            node_ref.size
        } else {
            0
        }
    }
}

/// Rollback-proof buffer to memorize retired nodes and newly allocated nodes.
struct RBProofBuf<K, V> {
    /// Nodes that current op wants to remove from the tree. Should be retired if CAS succeeds.
    /// (`retire`). If not, ignore.
    retired_nodes: Vec<Atomic<Node<K, V>>>,
    /// Nodes newly constructed by the op. Should be destroyed if CAS fails. (`destroy`)
    new_nodes: Vec<Atomic<Node<K, V>>>,
}

impl<K, V> RBProofBuf<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new() -> Self {
        Self {
            retired_nodes: Vec::new(),
            new_nodes: Vec::new(),
        }
    }

    /// Destroy the newly created state (self) that lost the race (reclaim_state)
    fn abort<G: RollbackProof>(&mut self, _: &mut G) {
        self.retired_nodes.clear();

        for node in self.new_nodes.drain(..) {
            drop(unsafe { node.into_owned() });
        }
    }

    /// Retire the old state replaced by the new_state and the new_state.retired_nodes
    fn commit<G: RollbackProof>(&mut self, guard: &mut G) {
        self.new_nodes.clear();

        for node in self.retired_nodes.drain(..) {
            let node = node.load(Ordering::Relaxed, guard);
            unsafe {
                node.deref()
                    .left
                    .store(Node::retired_node(), Ordering::Release, guard);
                node.deref()
                    .right
                    .store(Node::retired_node(), Ordering::Release, guard);
                guard.retire(node);
            }
        }
    }

    fn retire_node(&mut self, node: Shared<Node<K, V>>, _: &mut RaGuard) {
        self.retired_nodes.push(Atomic::from(node));
    }

    fn add_new_node(&mut self, node: Shared<Node<K, V>>, _: &mut RaGuard) {
        self.new_nodes.push(Atomic::from(node));
    }
}

pub struct Protectors<K, V> {
    old_root: Shield<Node<K, V>>,
    new_root: Shield<Node<K, V>>,
    found_node: Shield<Node<K, V>>,
}

impl<K, V> OutputHolder<V> for Protectors<K, V> {
    fn default(thread: &mut Thread) -> Self {
        Self {
            old_root: Shield::null(thread),
            new_root: Shield::null(thread),
            found_node: Shield::null(thread),
        }
    }

    fn output(&self) -> &V {
        &self.found_node.as_ref().unwrap().value
    }
}

/// Each op creates a new local state and tries to update (CAS) the tree with it.
struct State<'g, K, V> {
    root_link: &'g Atomic<Node<K, V>>,
    curr_root: Shared<'g, Node<K, V>>,
    buf: &'g mut RBProofBuf<K, V>,
}

impl<'g, K, V> State<'g, K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new(
        root_link: &'g Atomic<Node<K, V>>,
        buf: &'g mut RBProofBuf<K, V>,
        guard: &'g CsGuard,
    ) -> Self {
        Self {
            root_link,
            curr_root: root_link.load(Ordering::Acquire, guard),
            buf,
        }
    }

    // TODO get ref of K, V and clone here
    fn mk_node(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g CsGuard,
    ) -> Shared<'g, Node<K, V>> {
        if Node::is_retired_spot(left, guard) || Node::is_retired_spot(right, guard) {
            return Node::retired_node();
        }

        let left_size = Node::node_size(left, guard);
        let right_size = Node::node_size(right, guard);
        guard.mask_light(|guard| {
            let new_node = Owned::new(Node {
                key: key.clone(),
                value: value.clone(),
                size: left_size + right_size + 1,
                left: Atomic::from(left),
                right: Atomic::from(right),
            })
            .into_shared();
            self.buf.add_new_node(new_node, guard);
            new_node
        })
    }

    /// Make a new balanced tree from cur (the root of a subtree) and newly constructed left and right subtree
    fn mk_balanced(
        &mut self,
        cur: Shared<'g, Node<K, V>>,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        guard: &'g CsGuard,
    ) -> Shared<'g, Node<K, V>> {
        if Node::is_retired_spot(cur, guard)
            || Node::is_retired_spot(left, guard)
            || Node::is_retired_spot(right, guard)
        {
            return Node::retired_node();
        }

        let cur_ref = unsafe { cur.deref() };
        let key = &cur_ref.key;
        let value = &cur_ref.value;

        let l_size = Node::node_size(left, guard);
        let r_size = Node::node_size(right, guard);
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
        guard.mask_light(|guard| self.buf.retire_node(cur, guard));
        res
    }

    #[inline]
    fn mk_balanced_left(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g CsGuard,
    ) -> Shared<'g, Node<K, V>> {
        let right_ref = unsafe { right.deref() };
        let right_left = right_ref.left.load(Ordering::Acquire, guard);
        let right_right = right_ref.right.load(Ordering::Acquire, guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(right_left, guard)
            || Node::is_retired_spot(right_right, guard)
        {
            return Node::retired_node();
        }

        if Node::node_size(right_left, guard) < Node::node_size(right_right, guard) {
            // single left rotation
            return self.single_left(left, right, right_left, right_right, key, value, guard);
        }

        // double left rotation
        return self.double_left(left, right, right_left, right_right, key, value, guard);
    }

    #[inline]
    fn single_left(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        right_left: Shared<'g, Node<K, V>>,
        right_right: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g CsGuard,
    ) -> Shared<'g, Node<K, V>> {
        let right_ref = unsafe { right.deref() };
        let new_left = self.mk_node(left, right_left, key, value, guard);
        let res = self.mk_node(
            new_left,
            right_right,
            &right_ref.key,
            &right_ref.value,
            guard,
        );
        guard.mask_light(|guard| self.buf.retire_node(right, guard));
        res
    }

    #[inline]
    fn double_left(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        right_left: Shared<'g, Node<K, V>>,
        right_right: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g CsGuard,
    ) -> Shared<'g, Node<K, V>> {
        let right_ref = unsafe { right.deref() };
        let right_left_ref = unsafe { right_left.deref() };
        let right_left_left = right_left_ref.left.load(Ordering::Acquire, guard);
        let right_left_right = right_left_ref.right.load(Ordering::Acquire, guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(right_left_left, guard)
            || Node::is_retired_spot(right_left_right, guard)
        {
            return Node::retired_node();
        }

        let new_left = self.mk_node(left, right_left_left, key, value, guard);
        let new_right = self.mk_node(
            right_left_right,
            right_right,
            &right_ref.key,
            &right_ref.value,
            guard,
        );
        let res = self.mk_node(
            new_left,
            new_right,
            &right_left_ref.key,
            &right_left_ref.value,
            guard,
        );
        guard.mask_light(|guard| {
            self.buf.retire_node(right_left, guard);
            self.buf.retire_node(right, guard);
        });
        res
    }

    #[inline]
    fn mk_balanced_right(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g CsGuard,
    ) -> Shared<'g, Node<K, V>> {
        let left_ref = unsafe { left.deref() };
        let left_right = left_ref.right.load(Ordering::Acquire, guard);
        let left_left = left_ref.left.load(Ordering::Acquire, guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(left_right, guard)
            || Node::is_retired_spot(left_left, guard)
        {
            return Node::retired_node();
        }

        if Node::node_size(left_right, guard) < Node::node_size(left_left, guard) {
            // single right rotation (fig 3)
            return self.single_right(left, right, left_right, left_left, key, value, guard);
        }
        // double right rotation
        return self.double_right(left, right, left_right, left_left, key, value, guard);
    }

    #[inline]
    fn single_right(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        left_right: Shared<'g, Node<K, V>>,
        left_left: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g CsGuard,
    ) -> Shared<'g, Node<K, V>> {
        let left_ref = unsafe { left.deref() };
        let new_right = self.mk_node(left_right, right, key, value, guard);
        let res = self.mk_node(left_left, new_right, &left_ref.key, &left_ref.value, guard);
        guard.mask_light(|guard| self.buf.retire_node(left, guard));
        res
    }

    #[inline]
    fn double_right(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        left_right: Shared<'g, Node<K, V>>,
        left_left: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g CsGuard,
    ) -> Shared<'g, Node<K, V>> {
        let left_ref = unsafe { left.deref() };
        let left_right_ref = unsafe { left_right.deref() };
        let left_right_left = left_right_ref.left.load(Ordering::Acquire, guard);
        let left_right_right = left_right_ref.right.load(Ordering::Acquire, guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(left_right_left, guard)
            || Node::is_retired_spot(left_right_right, guard)
        {
            return Node::retired_node();
        }

        let new_left = self.mk_node(
            left_left,
            left_right_left,
            &left_ref.key,
            &left_ref.value,
            guard,
        );
        let new_right = self.mk_node(left_right_right, right, key, value, guard);
        let res = self.mk_node(
            new_left,
            new_right,
            &left_right_ref.key,
            &left_right_ref.value,
            guard,
        );
        guard.mask_light(|guard| {
            self.buf.retire_node(left_right, guard);
            self.buf.retire_node(left, guard);
        });
        res
    }

    #[inline]
    fn do_insert(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g CsGuard,
    ) -> (Shared<'g, Node<K, V>>, bool) {
        if Node::is_retired_spot(node, guard) {
            return (Node::retired_node(), false);
        }

        if node.is_null() {
            return (
                self.mk_node(Shared::null(), Shared::null(), key, value, guard),
                true,
            );
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(left, guard)
            || Node::is_retired_spot(right, guard)
        {
            return (Node::retired_node(), false);
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => (node, false),
            cmp::Ordering::Less => {
                let (new_right, inserted) = self.do_insert(right, key, value, guard);
                (self.mk_balanced(node, left, new_right, guard), inserted)
            }
            cmp::Ordering::Greater => {
                let (new_left, inserted) = self.do_insert(left, key, value, guard);
                (self.mk_balanced(node, new_left, right, guard), inserted)
            }
        }
    }

    /// Hint: Returns a tuple of (new node, removed node if exists).
    #[inline]
    fn do_remove(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        key: &K,
        guard: &'g CsGuard,
    ) -> (Shared<'g, Node<K, V>>, Option<Shared<'g, Node<K, V>>>) {
        if Node::is_retired_spot(node, guard) {
            return (Node::retired_node(), None);
        }

        if node.is_null() {
            return (Shared::null(), None);
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(left, guard)
            || Node::is_retired_spot(right, guard)
        {
            return (Node::retired_node(), None);
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => {
                guard.mask_light(|guard| self.buf.retire_node(node, guard));
                if node_ref.size == 1 {
                    return (Shared::null(), Some(node));
                }

                if !left.is_null() {
                    let (new_left, succ) = self.pull_rightmost(left, guard);
                    return (self.mk_balanced(succ, new_left, right, guard), Some(node));
                }
                let (new_right, succ) = self.pull_leftmost(right, guard);
                (self.mk_balanced(succ, left, new_right, guard), Some(node))
            }
            cmp::Ordering::Less => {
                let (new_right, removed) = self.do_remove(right, key, guard);
                (self.mk_balanced(node, left, new_right, guard), removed)
            }
            cmp::Ordering::Greater => {
                let (new_left, removed) = self.do_remove(left, key, guard);
                (self.mk_balanced(node, new_left, right, guard), removed)
            }
        }
    }

    fn pull_leftmost(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        guard: &'g CsGuard,
    ) -> (Shared<'g, Node<K, V>>, Shared<'g, Node<K, V>>) {
        if Node::is_retired_spot(node, guard) {
            return (Node::retired_node(), Node::retired_node());
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(left, guard)
            || Node::is_retired_spot(right, guard)
        {
            return (Node::retired_node(), Node::retired_node());
        }

        if !left.is_null() {
            let (new_left, succ) = self.pull_leftmost(left, guard);
            return (self.mk_balanced(node, new_left, right, guard), succ);
        }
        // node is the leftmost
        let succ = self.mk_node(
            Shared::null(),
            Shared::null(),
            &node_ref.key,
            &node_ref.value,
            guard,
        );
        guard.mask_light(|guard| self.buf.retire_node(node, guard));
        (right, succ)
    }

    fn pull_rightmost(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        guard: &'g CsGuard,
    ) -> (Shared<'g, Node<K, V>>, Shared<'g, Node<K, V>>) {
        if Node::is_retired_spot(node, guard) {
            return (Node::retired_node(), Node::retired_node());
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);

        if !self.check_root(guard)
            || Node::is_retired_spot(left, guard)
            || Node::is_retired_spot(right, guard)
        {
            return (Node::retired_node(), Node::retired_node());
        }

        if !right.is_null() {
            let (new_right, succ) = self.pull_rightmost(right, guard);
            return (self.mk_balanced(node, left, new_right, guard), succ);
        }
        // node is the rightmost
        let succ = self.mk_node(
            Shared::null(),
            Shared::null(),
            &node_ref.key,
            &node_ref.value,
            guard,
        );
        guard.mask_light(|guard| self.buf.retire_node(node, guard));
        (left, succ)
    }

    pub fn check_root(&self, guard: &CsGuard) -> bool {
        self.curr_root == self.root_link.load(Ordering::Acquire, guard)
    }
}

pub struct BonsaiTreeMap<K, V> {
    root: Atomic<Node<K, V>>,
}

impl<K, V> Default for BonsaiTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> BonsaiTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            root: Atomic::null(),
        }
    }

    pub fn get_inner(
        &self,
        key: &K,
        output: &mut Protectors<K, V>,
        guard: &CsGuard,
    ) -> Result<bool, ()> {
        let mut node = self.root.load(Ordering::Acquire, guard);
        while !node.is_null() && !Node::is_retired(node) {
            let node_ref = unsafe { node.deref() };
            match key.cmp(&node_ref.key) {
                cmp::Ordering::Equal => break,
                cmp::Ordering::Less => node = node_ref.left.load(Ordering::Acquire, guard),
                cmp::Ordering::Greater => node = node_ref.right.load(Ordering::Acquire, guard),
            }
        }
        if Node::is_retired_spot(node, guard) {
            return Err(());
        } else if node.is_null() {
            return Ok(false);
        }
        output.found_node.protect(node);
        Ok(true)
    }

    pub fn get(&self, key: &K, output: &mut Protectors<K, V>, handle: &mut Thread) -> bool {
        loop {
            let result =
                unsafe { handle.critical_section(|guard| self.get_inner(key, output, guard)) };
            if let Ok(found) = result {
                return found;
            }
        }
    }

    pub fn insert(
        &self,
        key: K,
        value: V,
        output: &mut Protectors<K, V>,
        handle: &mut Thread,
    ) -> bool {
        let mut buf = RBProofBuf::new();
        loop {
            let inserted = unsafe {
                handle.critical_section(|guard| {
                    output.found_node.release();
                    guard.mask_light(|guard| buf.abort(guard));
                    let mut state = State::new(&self.root, &mut buf, guard);
                    let old_root = state.curr_root;
                    let (new_node, inserted) = state.do_insert(old_root, &key, &value, guard);
                    output.old_root.protect(old_root);
                    output.new_root.protect(new_node);
                    inserted
                })
            };

            if Node::is_retired(output.new_root.shared()) {
                buf.abort(handle);
                continue;
            }

            if self
                .root
                .compare_exchange(
                    output.old_root.shared(),
                    output.new_root.shared(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    handle,
                )
                .is_ok()
            {
                buf.commit(handle);
                return inserted;
            }
            buf.abort(handle);
        }
    }

    pub fn remove(&self, key: &K, output: &mut Protectors<K, V>, handle: &mut Thread) -> bool {
        let mut buf = RBProofBuf::new();
        loop {
            unsafe {
                handle.critical_section(|guard| {
                    output.found_node.release();
                    guard.mask_light(|guard| buf.abort(guard));
                    let mut state = State::new(&self.root, &mut buf, guard);
                    let old_root = state.curr_root;
                    let (new_root, removed) = state.do_remove(old_root, key, guard);
                    if let Some(removed) = removed {
                        output.found_node.protect(removed);
                    }
                    output.old_root.protect(old_root);
                    output.new_root.protect(new_root);
                })
            }

            if Node::is_retired(output.new_root.shared()) {
                buf.abort(handle);
                continue;
            }

            if self
                .root
                .compare_exchange(
                    output.old_root.shared(),
                    output.new_root.shared(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    handle,
                )
                .is_ok()
            {
                buf.commit(handle);
                return !output.found_node.is_null();
            }
            buf.abort(handle);
        }
    }
}

impl<K, V> Drop for BonsaiTreeMap<K, V> {
    fn drop(&mut self) {
        unsafe {
            let guard = &Unprotected::new();
            let mut stack = vec![self.root.load(Ordering::Relaxed, guard)];

            while let Some(mut node) = stack.pop() {
                if node.is_null() {
                    continue;
                }

                let node_ref = node.deref_mut();

                stack.push(node_ref.left.load(Ordering::Relaxed, guard));
                stack.push(node_ref.right.load(Ordering::Relaxed, guard));
                drop(node.into_owned());
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for BonsaiTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Output = Protectors<K, V>;

    fn new() -> Self {
        Self::new()
    }

    fn get(&self, key: &K, output: &mut Self::Output, thread: &mut Thread) -> bool {
        self.get(key, output, thread)
    }

    fn insert(&self, key: K, value: V, output: &mut Self::Output, thread: &mut Thread) -> bool {
        self.insert(key, value, output, thread)
    }

    fn remove<'domain, 'hp>(
        &self,
        key: &K,
        output: &mut Self::Output,
        thread: &mut Thread,
    ) -> bool {
        self.remove(key, output, thread)
    }
}

#[cfg(test)]
mod tests {
    use super::BonsaiTreeMap;
    use crate::ds_impl::hp_brcu::concurrent_map;

    #[test]
    fn smoke_bonsai_tree() {
        concurrent_map::tests::smoke::<BonsaiTreeMap<i32, String>>();
    }
}
