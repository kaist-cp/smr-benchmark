use crossbeam_pebr::{unprotected, Atomic, Guard, Owned, Shared, Shield, ShieldError};

use super::concurrent_map::ConcurrentMap;

use std::cmp;
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
pub struct Node<K, V> {
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
pub struct State<K, V> {
    shield: Shield<Node<K, V>>,
    /// Nodes that current op wants to remove from the tree. Should be retired if CAS succeeds.
    /// (`retire`). If not, ignore.
    retired_nodes: Vec<Atomic<Node<K, V>>>,
    /// Nodes newly constructed by the op. Should be destroyed if CAS fails. (`destroy`)
    new_nodes: Vec<Atomic<Node<K, V>>>,
}

impl<K, V> State<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new<'g>(guard: &'g Guard) -> Self {
        Self {
            shield: Shield::null(guard),
            retired_nodes: Vec::new(),
            new_nodes: Vec::new(),
        }
    }

    /// Destroy the newly created state (self) that lost the race (reclaim_state)
    fn abort(&mut self) {
        self.shield.release();
        self.retired_nodes.clear();

        for node in self.new_nodes.drain(..) {
            drop(unsafe { node.into_owned() });
        }
    }

    /// Retire the old state replaced by the new_state and the new_state.retired_nodes
    fn commit(&mut self, guard: &Guard) {
        self.shield.release();
        self.new_nodes.clear();

        for node in self.retired_nodes.drain(..) {
            let node = node.load(Ordering::Relaxed, guard);
            unsafe {
                node.deref()
                    .left
                    .store(Node::retired_node(), Ordering::Release);
                node.deref()
                    .right
                    .store(Node::retired_node(), Ordering::Release);
                guard.defer_destroy(node);
            }
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
    ) -> Result<Shared<'g, Node<K, V>>, ShieldError> {
        if Node::is_retired_spot(cur, guard)
            || Node::is_retired_spot(left, guard)
            || Node::is_retired_spot(right, guard)
        {
            return Ok(Node::retired_node());
        }

        self.shield.defend(cur, guard)?;
        let cur_ref = unsafe { self.shield.deref() };
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
            Ok(self.mk_node(left, right, key, value, guard))
        };
        self.retire_node(cur);
        res
    }

    #[inline]
    fn mk_balanced_left<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Node<K, V>>, ShieldError> {
        let right_ref = unsafe { right.deref() };
        let right_left = right_ref.left.load(Ordering::Acquire, guard);
        let right_right = right_ref.right.load(Ordering::Acquire, guard);

        if Node::is_retired_spot(right_left, guard) || Node::is_retired_spot(right_right, guard) {
            return Ok(Node::retired_node());
        }

        if Node::node_size(right_left) < Node::node_size(right_right) {
            // single left rotation
            return self.single_left(left, right, right_left, right_right, key, value, guard);
        }

        // double left rotation
        return self.double_left(left, right, right_left, right_right, key, value, guard);
    }

    #[inline]
    fn single_left<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        right_left: Shared<'g, Node<K, V>>,
        right_right: Shared<'g, Node<K, V>>,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Node<K, V>>, ShieldError> {
        let right_ref = unsafe { right.deref() };
        let new_left = self.mk_node(left, right_left, key, value, guard);
        let res = self.mk_node(
            new_left,
            right_right,
            right_ref.key.clone(),
            right_ref.value.clone(),
            guard,
        );
        self.retire_node(right);
        return Ok(res);
    }

    #[inline]
    fn double_left<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        right_left: Shared<'g, Node<K, V>>,
        right_right: Shared<'g, Node<K, V>>,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Node<K, V>>, ShieldError> {
        let right_ref = unsafe { right.deref() };
        let right_left_ref = unsafe { right_left.deref() };
        let right_left_left = right_left_ref.left.load(Ordering::Acquire, guard);
        let right_left_right = right_left_ref.right.load(Ordering::Acquire, guard);

        if Node::is_retired_spot(right_left_left, guard)
            || Node::is_retired_spot(right_left_right, guard)
        {
            return Ok(Node::retired_node());
        }

        let new_left = self.mk_node(left, right_left_left, key, value, guard);
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
        Ok(res)
    }

    #[inline]
    fn mk_balanced_right<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Node<K, V>>, ShieldError> {
        let left_ref = unsafe { left.deref() };
        let left_right = left_ref.right.load(Ordering::Acquire, guard);
        let left_left = left_ref.left.load(Ordering::Acquire, guard);

        if Node::is_retired_spot(left_right, guard) || Node::is_retired_spot(left_left, guard) {
            return Ok(Node::retired_node());
        }

        if Node::node_size(left_right) < Node::node_size(left_left) {
            // single right rotation (fig 3)
            return self.single_right(left, right, left_right, left_left, key, value, guard);
        }
        // double right rotation
        return self.double_right(left, right, left_right, left_left, key, value, guard);
    }

    #[inline]
    fn single_right<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        left_right: Shared<'g, Node<K, V>>,
        left_left: Shared<'g, Node<K, V>>,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Node<K, V>>, ShieldError> {
        let left_ref = unsafe { left.deref() };
        let new_right = self.mk_node(left_right, right, key, value, guard);
        let res = self.mk_node(
            left_left,
            new_right,
            left_ref.key.clone(),
            left_ref.value.clone(),
            guard,
        );
        self.retire_node(left);
        return Ok(res);
    }

    #[inline]
    fn double_right<'g>(
        &mut self,
        left: Shared<'g, Node<K, V>>,
        right: Shared<'g, Node<K, V>>,
        left_right: Shared<'g, Node<K, V>>,
        left_left: Shared<'g, Node<K, V>>,
        key: K,
        value: V,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Node<K, V>>, ShieldError> {
        let left_ref = unsafe { left.deref() };
        let left_right_ref = unsafe { left_right.deref() };
        let left_right_left = left_right_ref.left.load(Ordering::Acquire, guard);
        let left_right_right = left_right_ref.right.load(Ordering::Acquire, guard);

        if Node::is_retired_spot(left_right_left, guard)
            || Node::is_retired_spot(left_right_right, guard)
        {
            return Ok(Node::retired_node());
        }

        let new_left = self.mk_node(
            left_left,
            left_right_left,
            left_ref.key.clone(),
            left_ref.value.clone(),
            guard,
        );
        let new_right = self.mk_node(left_right_right, right, key, value, guard);
        let res = self.mk_node(
            new_left,
            new_right,
            left_right_ref.key.clone(),
            left_right_ref.value.clone(),
            guard,
        );
        self.retire_node(left_right);
        self.retire_node(left);
        Ok(res)
    }

    #[inline]
    fn do_insert<'g>(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        key: &K,
        value: &V,
        guard: &'g Guard,
    ) -> Result<(Shared<'g, Node<K, V>>, bool), ShieldError> {
        if Node::is_retired_spot(node, guard) {
            return Ok((Node::retired_node(), false));
        }

        if node.is_null() {
            return Ok((
                self.mk_node(
                    Shared::null(),
                    Shared::null(),
                    key.clone(),
                    value.clone(),
                    guard,
                ),
                true,
            ));
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);

        if Node::is_retired_spot(left, guard) || Node::is_retired_spot(right, guard) {
            return Ok((Node::retired_node(), false));
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => Ok((node, false)),
            cmp::Ordering::Less => {
                let (new_right, inserted) = self.do_insert(right, key, value, guard)?;
                Ok((self.mk_balanced(node, left, new_right, guard)?, inserted))
            }
            cmp::Ordering::Greater => {
                let (new_left, inserted) = self.do_insert(left, key, value, guard)?;
                Ok((self.mk_balanced(node, new_left, right, guard)?, inserted))
            }
        }
    }

    #[inline]
    fn do_remove<'g>(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        key: &K,
        guard: &'g Guard,
    ) -> Result<(Shared<'g, Node<K, V>>, Option<V>), ShieldError> {
        if Node::is_retired_spot(node, guard) {
            return Ok((Node::retired_node(), None));
        }

        if node.is_null() {
            return Ok((Shared::null(), None));
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);

        if Node::is_retired_spot(left, guard) || Node::is_retired_spot(right, guard) {
            return Ok((Node::retired_node(), None));
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => {
                let value = Some(node_ref.value.clone());
                self.retire_node(node);
                if node_ref.size == 1 {
                    return Ok((Shared::null(), value));
                }

                if !left.is_null() {
                    let (new_left, succ) = self.pull_rightmost(left, guard)?;
                    return Ok((self.mk_balanced(succ, new_left, right, guard)?, value));
                }
                let (new_right, succ) = self.pull_leftmost(right, guard)?;
                Ok((self.mk_balanced(succ, left, new_right, guard)?, value))
            }
            cmp::Ordering::Less => {
                let (new_right, value) = self.do_remove(right, key, guard)?;
                Ok((self.mk_balanced(node, left, new_right, guard)?, value))
            }
            cmp::Ordering::Greater => {
                let (new_left, value) = self.do_remove(left, key, guard)?;
                Ok((self.mk_balanced(node, new_left, right, guard)?, value))
            }
        }
    }

    fn pull_leftmost<'g>(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        guard: &'g Guard,
    ) -> Result<(Shared<'g, Node<K, V>>, Shared<'g, Node<K, V>>), ShieldError> {
        if Node::is_retired_spot(node, guard) {
            return Ok((Node::retired_node(), Node::retired_node()));
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);

        if Node::is_retired_spot(left, guard) || Node::is_retired_spot(right, guard) {
            return Ok((Node::retired_node(), Node::retired_node()));
        }

        if !left.is_null() {
            let (new_left, succ) = self.pull_leftmost(left, guard)?;
            return Ok((self.mk_balanced(node, new_left, right, guard)?, succ));
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
        return Ok((right, succ));
    }

    fn pull_rightmost<'g>(
        &mut self,
        node: Shared<'g, Node<K, V>>,
        guard: &'g Guard,
    ) -> Result<(Shared<'g, Node<K, V>>, Shared<'g, Node<K, V>>), ShieldError> {
        if Node::is_retired_spot(node, guard) {
            return Ok((Node::retired_node(), Node::retired_node()));
        }

        let node_ref = unsafe { node.deref() };
        let left = node_ref.left.load(Ordering::Acquire, guard);
        let right = node_ref.right.load(Ordering::Acquire, guard);

        if Node::is_retired_spot(left, guard) || Node::is_retired_spot(right, guard) {
            return Ok((Node::retired_node(), Node::retired_node()));
        }

        if !right.is_null() {
            let (new_right, succ) = self.pull_rightmost(right, guard)?;
            return Ok((self.mk_balanced(node, left, new_right, guard)?, succ));
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
        return Ok((left, succ));
    }
}

pub struct BonsaiTreeMap<K, V> {
    root: Atomic<Node<K, V>>,
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

    pub fn get<'g>(&self, key: &'g K, state: &mut State<K, V>, guard: &'g Guard) -> Option<&'g V> {
        // TODO(@jeehoonkang): we want to use `FindError::retry`, but it requires higher-kinded
        // things...
        loop {
            match self.get_inner(
                key,
                unsafe { &mut *(&mut state.shield as &_ as *const _ as *mut Shield<_>) },
                guard,
            ) {
                Ok(r) => return r,
                Err(ShieldError::Ejected) => {
                    unsafe {
                        // HACK(@jeehoonkang): We wanted to say `guard.repin()`, which is totally
                        // fine, but the current Rust's type checker cannot verify it.
                        (&mut *(guard as &_ as *const _ as *mut Guard)).repin();
                    }
                }
            }
        }
    }

    #[inline]
    fn get_inner<'g>(
        &self,
        key: &'g K,
        shield: &'g mut Shield<Node<K, V>>,
        guard: &'g Guard,
    ) -> Result<Option<&'g V>, ShieldError> {
        loop {
            let mut node = self.root.load(Ordering::Acquire, guard);
            while !node.is_null() && !Node::is_retired(node) {
                shield.defend(node, guard)?;
                let node_ref = unsafe { shield.deref() };
                match key.cmp(&node_ref.key) {
                    cmp::Ordering::Equal => break,
                    cmp::Ordering::Less => node = node_ref.left.load(Ordering::Acquire, guard),
                    cmp::Ordering::Greater => node = node_ref.right.load(Ordering::Acquire, guard),
                }
            }

            if Node::is_retired_spot(node, guard) {
                continue;
            }

            if node.is_null() {
                return Ok(None);
            }

            return Ok(Some(&unsafe { shield.deref() }.value));
        }
    }

    pub fn insert(&self, key: K, value: V, state: &mut State<K, V>, guard: &mut Guard) -> bool {
        loop {
            let old_root = self.root.load(Ordering::Acquire, guard);
            match state.do_insert(old_root, &key, &value, guard) {
                Err(ShieldError::Ejected) => {
                    unsafe {
                        // HACK(@jeehoonkang): We wanted to say `guard.repin()`, which is totally
                        // fine, but the current Rust's type checker cannot verify it.
                        (&mut *(guard as &_ as *const _ as *mut Guard)).repin();
                    }
                    state.abort();
                }
                Ok((new_root, inserted)) => {
                    if Node::is_retired(new_root) {
                        state.abort();
                        continue;
                    }

                    if self
                        .root
                        .compare_and_set(old_root, new_root, Ordering::AcqRel, guard)
                        .is_ok()
                    {
                        state.commit(guard);
                        return inserted;
                    }

                    state.abort();
                }
            }
        }
    }

    pub fn remove(&self, key: &K, state: &mut State<K, V>, guard: &Guard) -> Option<V> {
        loop {
            let old_root = self.root.load(Ordering::Acquire, guard);
            match state.do_remove(old_root, key, guard) {
                Err(ShieldError::Ejected) => {
                    unsafe {
                        // HACK(@jeehoonkang): We wanted to say `guard.repin()`, which is totally
                        // fine, but the current Rust's type checker cannot verify it.
                        (&mut *(guard as &_ as *const _ as *mut Guard)).repin();
                    }
                    state.abort();
                }
                Ok((new_root, value)) => {
                    if Node::is_retired(new_root) {
                        state.abort();
                        continue;
                    }

                    if self
                        .root
                        .compare_and_set(old_root, new_root, Ordering::AcqRel, guard)
                        .is_ok()
                    {
                        state.commit(guard);
                        return value;
                    }

                    state.abort();
                }
            }
        }
    }
}

impl<K, V> Drop for BonsaiTreeMap<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut stack = vec![self.root.load(Ordering::Relaxed, unprotected())];

            while let Some(mut node) = stack.pop() {
                if node.is_null() {
                    continue;
                }

                let node_ref = node.deref_mut();

                stack.push(node_ref.left.load(Ordering::Relaxed, unprotected()));
                stack.push(node_ref.right.load(Ordering::Relaxed, unprotected()));
                drop(node.into_owned());
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for BonsaiTreeMap<K, V>
where
    K: Ord + Clone + 'static,
    V: Clone + 'static,
{
    type Handle = State<K, V>;

    fn new() -> Self {
        Self::new()
    }

    fn handle(guard: &Guard) -> Self::Handle {
        State::new(guard)
    }

    fn clear(handle: &mut Self::Handle) {
        handle.shield.release();
    }

    #[inline]
    fn get<'g>(
        &'g self,
        handle: &'g mut Self::Handle,
        key: &'g K,
        guard: &'g mut Guard,
    ) -> Option<&'g V> {
        self.get(key, handle, guard)
    }

    #[inline]
    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool {
        self.insert(key, value, handle, guard)
    }

    #[inline]
    fn remove(&self, handle: &mut Self::Handle, key: &K, guard: &mut Guard) -> Option<V> {
        self.remove(key, handle, guard)
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    use super::{BonsaiTreeMap, State};
    use crossbeam_pebr::Shield;
    use crossbeam_utils::thread;
    use rand::prelude::*;

    #[test]
    fn smoke_bonsai_tree() {
        let bonsai_tree_map = &BonsaiTreeMap::new();

        // insert
        thread::scope(|s| {
            for t in 0..10 {
                s.spawn(move |_| {
                    let mut state = State::new(&crossbeam_pebr::pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(bonsai_tree_map.insert(
                            i,
                            i.to_string(),
                            &mut state,
                            &mut crossbeam_pebr::pin()
                        ));
                    }
                });
            }
        })
        .unwrap();

        // remove
        thread::scope(|s| {
            for t in 0..5 {
                s.spawn(move |_| {
                    let mut state = State::new(&crossbeam_pebr::pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            bonsai_tree_map
                                .remove(&i, &mut state, &mut crossbeam_pebr::pin())
                                .unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();

        // get
        thread::scope(|s| {
            for t in 5..10 {
                s.spawn(move |_| {
                    let mut state = State::new(&crossbeam_pebr::pin());
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<i32> = (0..3000).map(|k| k * 10 + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert_eq!(
                            i.to_string(),
                            *bonsai_tree_map
                                .get(&i, &mut state, &mut crossbeam_pebr::pin())
                                .unwrap()
                        );
                    }
                });
            }
        })
        .unwrap();
    }
}
