use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Shared};

use crate::concurrent_map::ConcurrentMap;

use std::sync::atomic::Ordering;

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
#[derive(Debug)]
struct Node<K, V> {
    key: K,
    value: V, // TODO(@jeehoonkang): clone?
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

    fn is_retired(node: Shared<Self>) -> bool {
        Retired::from_bits_truncate(node.tag()).retired()
    }

    fn is_retired_spot(node: Shared<Self>, guard: &Guard) -> bool {
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

    fn node_size(node: Shared<Self>) -> usize {
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
#[derive(Debug)]
struct State<K, V> {
    // TODO: this doesn't need to be Atomic (Owned is sufficient but nullable)
    root: Atomic<Node<K, V>>,

    /// Nodes that current op wants to remove from the tree. Should be retired if CAS succeeds.
    /// (`retire`). If not, ignore.
    retired_nodes: Vec<Atomic<Node<K, V>>>,
    /// Nodes newly constructed by the op. Should be destroyed if CAS fails. (`destroy`)
    new_nodes: Vec<Atomic<Node<K, V>>>,
}

impl<K, V> Drop for State<K, V> {
    fn drop(&mut self) {
        // TODO ??
    }
}

impl<K, V> State<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new<'g>() -> Self {
        Self {
            root: Atomic::null(),
            retired_nodes: Vec::new(),
            new_nodes: Vec::new(),
        }
    }

    /// Destroy the newly created state (self) that lost the race
    /// reclaim_state
    /// TODO: reclaim_state + retire_state?
    fn destroy(mut new_state: Shared<Self>) {
        unsafe {
            let state_ref = new_state.deref_mut();
            for node in state_ref.new_nodes.drain(..) {
                drop(node.into_owned());
            }
            drop(new_state.into_owned());
        }
    }

    /// Retire the old state replaced by the new_state and the new_state.retired_nodes
    fn retire<'g>(
        old_state: Shared<'g, Self>,
        retired_nodes: &mut Vec<Atomic<Node<K, V>>>,
        guard: &'g Guard,
    ) {
        unsafe {
            for node in retired_nodes.drain(..) {
                let node = node.load(Ordering::Relaxed, guard);
                node.deref().left.store(Node::retired_node(), Ordering::Release);
                node.deref().right.store(Node::retired_node(), Ordering::Release);
                guard.defer_destroy(node);
            }
            guard.defer_destroy(old_state);
        }
    }

    fn retire_node(&mut self, node: Shared<Node<K, V>>) {
        self.retired_nodes.push(Atomic::from(node));
    }

    fn add_new_node(&mut self, node: Shared<Node<K, V>>) {
        self.new_nodes.push(Atomic::from(node));
    }

    // TODO get ref of K, V and clone here
    fn mk_node<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> Shared<'g, Node<K, V>> {
        if Node::is_retired_spot(left, guard) || Node::is_retired_spot(right, guard) {
            return Node::retired_node();
        }

        let left_size = Node::node_size(left);
        let right_size = Node::node_size(right);
        let new_node = Owned::new(Node {
            key,
            value,
            size: left_size + right_size + 1,
            left: Atomic::from(left),
            right: Atomic::from(right),
        })
        .into_shared(guard);
        self.add_new_node(new_node);
        new_node
    }

    /// Make a new balanced tree from cur (the root of a subtree) and newly constructed left and right subtree
    fn mk_balanced<'g>(
        &mut self,
        cur: Shared<'g, Node<K, V>>,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        guard: &'g Guard,
    ) -> Shared<'g, Node<K, V>> {
        if Node::is_retired_spot(cur, guard)
            || Node::is_retired_spot(left, guard)
            || Node::is_retired_spot(right, guard)
        {
            return Node::retired_node();
        }

        let cur_ref = unsafe { cur.deref() };
        let l_size = Node::node_size(left);
        let r_size = Node::node_size(right);
        let res = if r_size > 0
            && ((l_size > 0 && r_size > WEIGHT * l_size) || (l_size == 0 && r_size > WEIGHT))
        {
            self.mk_balanced_left(left, right, &cur_ref.key, &cur_ref.value, guard)
        } else if l_size > 0
            && ((r_size > 0 && l_size > WEIGHT * r_size) || (r_size == 0 && l_size > WEIGHT))
        {
            self.mk_balanced_right(left, right, &cur_ref.key, &cur_ref.value, guard)
        } else {
            self.mk_node(
                left,
                right,
                cur_ref.key.clone(),
                cur_ref.value.clone(),
                guard,
            )
        };
        self.retire_node(cur);
        res
    }

    fn mk_balanced_left<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g Guard,
    ) -> Shared<'g, Node<K, V>> {
        let right_ref = unsafe { right.deref() };
        let right_left = right_ref.left.load(Ordering::Acquire, guard);
        let right_right = right_ref.right.load(Ordering::Acquire, guard);

        if Node::is_retired_spot(right_left, guard) || Node::is_retired_spot(right_right, guard) {
            return Node::retired_node();
        }

        if Node::node_size(right_left) < Node::node_size(right_right) {
            // single left rotation
            return self.single_left(left, right, right_left, right_right, key, value, guard);
        }

        // double left rotation
        return self.double_left(left, right, right_left, right_right, key, value, guard);
    }

    fn single_left<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        right_left: Shared<'g, Node<K, V>>,
        right_right: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g Guard,
    ) -> Shared<'g, Node<K, V>> {
        let right_ref = unsafe { right.deref() };
        let new_left = self.mk_node(left, right_left, key.clone(), value.clone(), guard);
        let res = self.mk_node(
            new_left,
            right_right,
            right_ref.key.clone(),
            right_ref.value.clone(),
            guard,
        );
        self.retire_node(right);
        return res;
    }

    fn double_left<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        right_left: Shared<'g, Node<K, V>>,
        right_right: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g Guard,
    ) -> Shared<'g, Node<K, V>> {
        let right_ref = unsafe { right.deref() };
        let right_left_ref = unsafe { right_left.deref() };
        let right_left_left = right_left_ref.left.load(Ordering::Acquire, guard);
        let right_left_right = right_left_ref.right.load(Ordering::Acquire, guard);

        let new_left = self.mk_node(left, right_left_left, key.clone(), value.clone(), guard);
        let new_right = self.mk_node(
            right_left_right,
            right_right,
            right_ref.key.clone(),
            right_ref.value.clone(),
            guard,
        );
        let res = self.mk_node(
            new_left,
            new_right,
            right_left_ref.key.clone(),
            right_left_ref.value.clone(),
            guard,
        );
        self.retire_node(right_left);
        self.retire_node(right);
        res
    }

    fn mk_balanced_right<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g Guard,
    ) -> Shared<'g, Node<K, V>> {
        let left_ref = unsafe { left.deref() };
        let left_right = left_ref.right.load(Ordering::Acquire, guard);
        let left_left = left_ref.left.load(Ordering::Acquire, guard);
        if Node::node_size(left_right) < Node::node_size(left_left) {
            // single right rotation (fig 3)
            return self.single_right(left, right, left_right, left_left, key, value, guard);
        }
        // double right rotation
        return self.double_right(left, right, left_right, left_left, key, value, guard);
    }

    fn single_right<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        left_right: Shared<'g, Node<K, V>>,
        left_left: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g Guard,
    ) -> Shared<'g, Node<K, V>> {
        let left_ref = unsafe { left.deref() };
        let new_right = self.mk_node(left_right, right, key.clone(), value.clone(), guard);
        let res = self.mk_node(
            left_left,
            new_right,
            left_ref.key.clone(),
            left_ref.value.clone(),
            guard,
        );
        self.retire_node(left);
        return res;
    }

    fn double_right<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        left_right: Shared<'g, Node<K, V>>,
        left_left: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g Guard,
    ) -> Shared<'g, Node<K, V>> {
        let left_ref = unsafe { left.deref() };
        let left_right_ref = unsafe { left_right.deref() };
        let left_right_left = left_right_ref.left.load(Ordering::Acquire, guard);
        let left_right_right = left_right_ref.right.load(Ordering::Acquire, guard);

        let new_left = self.mk_node(
            left_left,
            left_right_left,
            left_ref.key.clone(),
            left_ref.value.clone(),
            guard,
        );
        let new_right = self.mk_node(left_right_right, right, key.clone(), value.clone(), guard);
        let res = self.mk_node(
            new_left,
            new_right,
            left_right_ref.key.clone(),
            left_right_ref.value.clone(),
            guard,
        );
        self.retire_node(left_right);
        self.retire_node(left);
        res
    }

    fn do_insert<'g>(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g Guard,
    ) -> (Shared<'g, Node<K, V>>, bool) {
        if node.is_null() {
            return (
                self.mk_node(
                    Shared::null(),
                    Shared::null(),
                    key.clone(),
                    value.clone(),
                    guard,
                ),
                true,
            );
        }
        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);
        if *key < node_ref.key {
            let (new_left, inserted) = self.do_insert(left, key, value, guard);
            return (self.mk_balanced(node, new_left, right, guard), inserted);
        }
        if *key > node_ref.key {
            let (new_right, inserted) = self.do_insert(right, key, value, guard);
            return (self.mk_balanced(node, left, new_right, guard), inserted);
        }
        (node, false)
    }

    fn do_remove<'g>(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        key: &K,
        guard: &'g Guard,
    ) -> (Shared<'g, Node<K, V>>, Option<V>) {
        if node.is_null() {
            return (Shared::null(), None);
        }
        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);

        if *key == node_ref.key {
            let value = Some(node_ref.value.clone());
            self.retire_node(node);
            if node_ref.size == 1 {
                return (Shared::null(), value);
            }

            if !left.is_null() {
                let (new_left, succ) = self.pull_rightmost(left, guard);
                return (self.mk_balanced(succ, new_left, right, guard), value);
            }
            let (new_right, succ) = self.pull_leftmost(right, guard);
            return (self.mk_balanced(succ, left, new_right, guard), value);
        }

        if *key < node_ref.key {
            let new_left = self.do_remove(left, key, guard).0;
            return (self.mk_balanced(node, new_left, right, guard), None);
        } else {
            let new_right = self.do_remove(right, key, guard).0;
            return (self.mk_balanced(node, left, new_right, guard), None);
        }
    }

    fn pull_leftmost<'g>(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        guard: &'g Guard,
    ) -> (Shared<'g, Node<K, V>>, Shared<'g, Node<K, V>>) {
        if Node::is_retired_spot(node, guard) {
            return (Node::retired_node(), Node::retired_node());
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);
        if !left.is_null() {
            let (new_left, succ) = self.pull_leftmost(left, guard);
            return (self.mk_balanced(node, new_left, right, guard), succ);
        }
        // node is the leftmost
        let succ = self.mk_node(
            Shared::null(),
            Shared::null(),
            node_ref.key.clone(),
            node_ref.value.clone(),
            guard,
        );
        self.retire_node(node);
        return (right, succ);
    }

    fn pull_rightmost<'g>(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        guard: &'g Guard,
    ) -> (Shared<'g, Node<K, V>>, Shared<'g, Node<K, V>>) {
        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);
        if !right.is_null() {
            let (new_right, succ) = self.pull_rightmost(right, guard);
            return (self.mk_balanced(node, left, new_right, guard), succ);
        }
        // node is the rightmost
        let succ = self.mk_node(
            Shared::null(),
            Shared::null(),
            node_ref.key.clone(),
            node_ref.value.clone(),
            guard,
        );
        self.retire_node(node);
        return (left, succ);
    }
}

pub struct BonsaiTreeMap<K, V> {
    curr_state: Atomic<State<K, V>>,
}

impl<K, V> BonsaiTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        BonsaiTreeMap {
            curr_state: Atomic::from(State::new()),
        }
    }
    pub fn get<'g>(&self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        let curr_state = unsafe { self.curr_state.load(Ordering::Acquire, guard).deref() };
        let mut node;
        loop {
            node = curr_state.root.load(Ordering::Acquire, guard);
            while !node.is_null() && Node::is_retired(node) {
                let node_ref = unsafe { node.deref() };
                if *key == node_ref.key {
                    break;
                }
                node = if *key < node_ref.key {
                    node_ref.left.load(Ordering::Acquire, guard)
                } else {
                    node_ref.right.load(Ordering::Acquire, guard)
                }
            }

            if Node::is_retired_spot(node, guard) {
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
        loop {
            let old_state = self.curr_state.load(Ordering::Acquire, guard);
            let old_state_ref = unsafe { old_state.deref() };
            let mut new_state = Owned::new(State::new()).into_shared(unsafe { unprotected() });
            let new_state_ref = unsafe { new_state.deref_mut() };
            let (new_root, inserted) = new_state_ref.do_insert(
                old_state_ref.root.load(Ordering::Acquire, guard),
                &key,
                &value,
                guard,
            );
            new_state_ref.root.store(new_root, Ordering::Relaxed);

            if Node::is_retired(new_root) {
                // TODO: reclaim_state(new_state, new_state.new_nodes)
                State::destroy(new_state);
                continue;
            }

            if self
                .curr_state
                .compare_and_set(old_state, new_state, Ordering::AcqRel, guard)
                .is_ok()
            {
                // TODO: retire_state(old_state, new_state.retired_nodes)
                State::retire(old_state, &mut new_state_ref.retired_nodes, guard);
                return inserted;
            }

            // TODO: reclaim_state(new_state, new_state.new_nodes)
            State::destroy(new_state);
        }
    }

    pub fn remove(&self, key: &K, guard: &Guard) -> Option<V> {
        loop {
            let old_state = self.curr_state.load(Ordering::Acquire, guard);
            let old_state_ref = unsafe { old_state.deref() };
            let mut new_state = Owned::new(State::new()).into_shared(unsafe { unprotected() });
            let new_state_ref = unsafe { new_state.deref_mut() };
            let (new_root, value) = new_state_ref.do_remove(
                old_state_ref.root.load(Ordering::Acquire, guard),
                key,
                guard,
            );
            new_state_ref.root.store(new_root, Ordering::Relaxed);

            if Node::is_retired(new_root) {
                // TODO: reclaim_state(new_state, new_state.new_nodes)
                State::destroy(new_state);
                continue;
            }

            if self
                .curr_state
                .compare_and_set(old_state, new_state, Ordering::AcqRel, guard)
                .is_ok()
            {
                // TODO: retire_state(old_state, new_state.retired_nodes)
                State::retire(old_state, &mut new_state_ref.retired_nodes, guard);
                return value;
            }

            // TODO: reclaim_state(new_state, new_state.new_nodes)
            State::destroy(new_state);
        }
    }
}

// TODO: move it to somewhere else...
impl<K, V> ConcurrentMap<K, V> for BonsaiTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new() -> Self {
        Self::new()
    }

    #[inline]
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        self.get(key, guard)
    }
    #[inline]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.insert(key, value, guard)
    }
    #[inline]
    fn remove(&self, key: &K, guard: &Guard) -> Option<V> {
        self.remove(key, guard)
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    use super::BonsaiTreeMap;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    #[test]
    fn smoke_bonsai_tree() {
        let bonsai_tree_map = &BonsaiTreeMap::new();

        {
            let guard = crossbeam_epoch::pin();
            bonsai_tree_map.insert(0, (0, 100), &guard);
            bonsai_tree_map.remove(&0, &guard);
        }

        thread::scope(|s| {
            for t in 0..10 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..10000).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        bonsai_tree_map.insert(i, (i, t), &crossbeam_epoch::pin());
                    }
                });
            }
        })
        .unwrap();

        println!("start removal");

        for i in 0..100 {
            assert_eq!(i, bonsai_tree_map.remove(&i, &crossbeam_epoch::pin()).unwrap().0);
        }

        thread::scope(|s| {
            for _ in 0..10 {
                s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (1..10000).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        bonsai_tree_map.remove(&i, &crossbeam_epoch::pin());
                    }
                });
            }
        })
        .unwrap();

        println!("done");

        {
            let guard = crossbeam_epoch::pin();
            assert_eq!(bonsai_tree_map.get(&0, &guard).unwrap().0, 0);
            assert_eq!(bonsai_tree_map.remove(&0, &guard).unwrap().0, 0);
            assert_eq!(bonsai_tree_map.get(&0, &guard), None);
        }
    }
}
