use hp_pp::{light_membarrier, Invalidate, Thread, Unlink};
use hp_pp::{tag, tagged, untagged, HazardPointer, ProtectError, DEFAULT_DOMAIN};

use crate::hp::concurrent_map::ConcurrentMap;

use std::cmp;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

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
    left: AtomicPtr<Node<K, V>>,
    right: AtomicPtr<Node<K, V>>,
}

impl<K, V> Node<K, V> {
    fn retired_node() -> *mut Self {
        tagged(ptr::null_mut(), Retired::new(true).bits())
    }

    fn is_retired(node: *mut Self) -> bool {
        Retired::from_bits_truncate(tag(node)).retired()
    }

    fn is_retired_spot(node: *mut Self) -> bool {
        if Self::is_retired(node) {
            return true;
        }

        if let Some(node_ref) = unsafe { untagged(node).as_ref() } {
            Self::is_retired(node_ref.left.load(Ordering::Acquire))
                || Self::is_retired(node_ref.right.load(Ordering::Acquire))
        } else {
            false
        }
    }

    fn node_size(node: *mut Self) -> usize {
        debug_assert!(!Self::is_retired(node));
        if let Some(node_ref) = unsafe { untagged(node).as_ref() } {
            node_ref.size
        } else {
            0
        }
    }

    pub fn protect_next<'g>(
        &self,
        left_h: &mut HazardPointer<'g>,
        right_h: &mut HazardPointer<'g>,
    ) -> Result<(*mut Self, *mut Self), ()> {
        let mut left = self.left.load(Ordering::Relaxed);
        let mut right = self.right.load(Ordering::Relaxed);
        loop {
            match left_h.try_protect_pp(left, &self.left, &self.left, &|child_link| {
                Self::is_retired(child_link.load(Ordering::Acquire))
            }) {
                Ok(_) => {}
                Err(ProtectError::Changed(new_left)) => {
                    left = new_left;
                    continue;
                }
                Err(ProtectError::Invalidated) => return Err(()),
            }
            match right_h.try_protect_pp(right, &self.right, &self.right, &|child_link| {
                Self::is_retired(child_link.load(Ordering::Acquire))
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
}

/// Each op creates a new local state and tries to update (CAS) the tree with it.
pub struct State<'domain, K, V> {
    root_link: *const AtomicPtr<Node<K, V>>,
    curr_root: *mut Node<K, V>,
    root_h: HazardPointer<'domain>,
    succ_h: HazardPointer<'domain>, // Used for traversing in get
    removed_h: HazardPointer<'domain>,
    /// Nodes that current op wants to remove from the tree. Should be retired if CAS succeeds.
    /// (`retire`). If not, ignore.
    retired_nodes: Vec<*mut Node<K, V>>,
    /// Nodes newly constructed by the op. Should be destroyed if CAS fails. (`destroy`)
    new_nodes: Vec<*mut Node<K, V>>,
    thread: Thread<'domain>,
}

impl<K, V> Default for State<'static, K, V> {
    fn default() -> Self {
        Self {
            root_link: ptr::null(),
            curr_root: ptr::null_mut(),
            root_h: Default::default(),
            succ_h: Default::default(),
            removed_h: Default::default(),
            retired_nodes: vec![],
            new_nodes: vec![],
            thread: Thread::new(&DEFAULT_DOMAIN),
        }
    }
}

impl<'domain, K, V> State<'domain, K, V> {
    // bypass E0499-E0503, etc that are supposed to be fixed by polonius
    #[inline]
    fn launder<'hp1, 'hp2>(&'hp1 mut self) -> &'hp2 mut Self {
        unsafe { core::mem::transmute(self) }
    }
}

impl<'domain, K, V> State<'domain, K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Destroy the newly created state (self) that lost the race (reclaim_state)
    fn abort(&mut self) {
        self.root_h.reset_protection();
        self.retired_nodes.clear();

        for node in self.new_nodes.drain(..) {
            drop(unsafe { Box::from_raw(node) });
        }
    }

    fn clear(&mut self) {
        self.root_h.reset_protection();
        self.new_nodes.clear();
        self.retired_nodes.clear();
    }

    fn retire_node(&mut self, node: *mut Node<K, V>) {
        self.retired_nodes.push(node);
    }

    fn add_new_node(&mut self, node: *mut Node<K, V>) {
        self.new_nodes.push(node);
    }

    // TODO get ref of K, V and clone here
    fn mk_node(
        &mut self,
        left: *mut Node<K, V>,
        right: *mut Node<K, V>,
        key: K,
        value: V,
    ) -> *mut Node<K, V> {
        if Node::is_retired_spot(left) || Node::is_retired_spot(right) {
            return Node::retired_node();
        }

        let left_size = Node::node_size(left);
        let right_size = Node::node_size(right);
        let new_node = Box::into_raw(Box::new(Node {
            key,
            value,
            size: left_size + right_size + 1,
            left: AtomicPtr::from(left),
            right: AtomicPtr::from(right),
        }));
        self.add_new_node(new_node);
        new_node
    }

    /// Make a new balanced tree from cur (the root of a subtree) and newly constructed left and right subtree
    fn mk_balanced(
        &mut self,
        cur: *mut Node<K, V>,
        left: *mut Node<K, V>,
        right: *mut Node<K, V>,
    ) -> Result<*mut Node<K, V>, ()> {
        if Node::is_retired_spot(cur) || Node::is_retired_spot(left) || Node::is_retired_spot(right)
        {
            return Ok(Node::retired_node());
        }

        let cur_ref = unsafe { &*untagged(cur) };
        let key = cur_ref.key.clone();
        let value = cur_ref.value.clone();

        let l_size = Node::node_size(left);
        let r_size = Node::node_size(right);
        let res = if r_size > 0
            && ((l_size > 0 && r_size > WEIGHT * l_size) || (l_size == 0 && r_size > WEIGHT))
        {
            self.mk_balanced_left(left, right, key, value)
        } else if l_size > 0
            && ((r_size > 0 && l_size > WEIGHT * r_size) || (r_size == 0 && l_size > WEIGHT))
        {
            self.mk_balanced_right(left, right, key, value)
        } else {
            Ok(self.mk_node(left, right, key, value))
        };
        self.retire_node(cur);
        res
    }

    #[inline]
    fn mk_balanced_left(
        &mut self,
        left: *mut Node<K, V>,
        right: *mut Node<K, V>,
        key: K,
        value: V,
    ) -> Result<*mut Node<K, V>, ()> {
        let right_ref = unsafe { &*untagged(right) };
        let (mut right_left_h, mut right_right_h) = (
            HazardPointer::new(&mut self.thread),
            HazardPointer::new(&mut self.thread),
        );
        let (right_left, right_right) =
            right_ref.protect_next(&mut right_left_h, &mut right_right_h)?;

        if !self.check_root()
            || Node::is_retired_spot(right_left)
            || Node::is_retired_spot(right_right)
        {
            return Ok(Node::retired_node());
        }

        if Node::node_size(right_left) < Node::node_size(right_right) {
            // single left rotation
            return Ok(self.single_left(left, right, right_left, right_right, key, value));
        }

        // double left rotation
        return self.double_left(left, right, right_left, right_right, key, value);
    }

    #[inline]
    fn single_left(
        &mut self,
        left: *mut Node<K, V>,
        right: *mut Node<K, V>,
        right_left: *mut Node<K, V>,
        right_right: *mut Node<K, V>,
        key: K,
        value: V,
    ) -> *mut Node<K, V> {
        let right_ref = unsafe { &*untagged(right) };
        let new_left = self.mk_node(left, right_left, key, value);
        let res = self.mk_node(
            new_left,
            right_right,
            right_ref.key.clone(),
            right_ref.value.clone(),
        );
        self.retire_node(right);
        return res;
    }

    #[inline]
    fn double_left(
        &mut self,
        left: *mut Node<K, V>,
        right: *mut Node<K, V>,
        right_left: *mut Node<K, V>,
        right_right: *mut Node<K, V>,
        key: K,
        value: V,
    ) -> Result<*mut Node<K, V>, ()> {
        let right_ref = unsafe { &*untagged(right) };
        let right_left_ref = unsafe { &*untagged(right_left) };
        let (mut right_left_left_h, mut right_left_right_h) = (
            HazardPointer::new(&mut self.thread),
            HazardPointer::new(&mut self.thread),
        );
        let (right_left_left, right_left_right) =
            right_left_ref.protect_next(&mut right_left_left_h, &mut right_left_right_h)?;

        if !self.check_root()
            || Node::is_retired_spot(right_left_left)
            || Node::is_retired_spot(right_left_right)
        {
            return Ok(Node::retired_node());
        }

        let new_left = self.mk_node(left, right_left_left, key, value);
        let new_right = self.mk_node(
            right_left_right,
            right_right,
            right_ref.key.clone(),
            right_ref.value.clone(),
        );
        let res = self.mk_node(
            new_left,
            new_right,
            right_left_ref.key.clone(),
            right_left_ref.value.clone(),
        );
        self.retire_node(right_left);
        self.retire_node(right);
        Ok(res)
    }

    #[inline]
    fn mk_balanced_right(
        &mut self,
        left: *mut Node<K, V>,
        right: *mut Node<K, V>,
        key: K,
        value: V,
    ) -> Result<*mut Node<K, V>, ()> {
        let left_ref = unsafe { &*untagged(left) };
        let (mut left_left_h, mut left_right_h) = (
            HazardPointer::new(&mut self.thread),
            HazardPointer::new(&mut self.thread),
        );
        let (left_left, left_right) = left_ref.protect_next(&mut left_left_h, &mut left_right_h)?;

        if !self.check_root()
            || Node::is_retired_spot(left_right)
            || Node::is_retired_spot(left_left)
        {
            return Ok(Node::retired_node());
        }

        if Node::node_size(left_right) < Node::node_size(left_left) {
            // single right rotation (fig 3)
            return Ok(self.single_right(left, right, left_right, left_left, key, value));
        }
        // double right rotation
        return self.double_right(left, right, left_right, left_left, key, value);
    }

    #[inline]
    fn single_right(
        &mut self,
        left: *mut Node<K, V>,
        right: *mut Node<K, V>,
        left_right: *mut Node<K, V>,
        left_left: *mut Node<K, V>,
        key: K,
        value: V,
    ) -> *mut Node<K, V> {
        let left_ref = unsafe { &*untagged(left) };
        let new_right = self.mk_node(left_right, right, key, value);
        let res = self.mk_node(
            left_left,
            new_right,
            left_ref.key.clone(),
            left_ref.value.clone(),
        );
        self.retire_node(left);
        return res;
    }

    #[inline]
    fn double_right(
        &mut self,
        left: *mut Node<K, V>,
        right: *mut Node<K, V>,
        left_right: *mut Node<K, V>,
        left_left: *mut Node<K, V>,
        key: K,
        value: V,
    ) -> Result<*mut Node<K, V>, ()> {
        let left_ref = unsafe { &*untagged(left) };
        let left_right_ref = unsafe { &*untagged(left_right) };
        let (mut left_right_left_h, mut left_right_right_h) = (
            HazardPointer::new(&mut self.thread),
            HazardPointer::new(&mut self.thread),
        );
        let (left_right_left, left_right_right) =
            left_right_ref.protect_next(&mut left_right_left_h, &mut left_right_right_h)?;

        if !self.check_root()
            || Node::is_retired_spot(left_right_left)
            || Node::is_retired_spot(left_right_right)
        {
            return Ok(Node::retired_node());
        }

        let new_left = self.mk_node(
            left_left,
            left_right_left,
            left_ref.key.clone(),
            left_ref.value.clone(),
        );
        let new_right = self.mk_node(left_right_right, right, key, value);
        let res = self.mk_node(
            new_left,
            new_right,
            left_right_ref.key.clone(),
            left_right_ref.value.clone(),
        );
        self.retire_node(left_right);
        self.retire_node(left);
        Ok(res)
    }

    #[inline]
    fn do_insert(
        &mut self,
        node: *mut Node<K, V>,
        key: &K,
        value: &V,
    ) -> Result<(*mut Node<K, V>, bool), ()> {
        if Node::is_retired_spot(node) {
            return Ok((Node::retired_node(), false));
        }

        if node.is_null() {
            return Ok((
                self.mk_node(ptr::null_mut(), ptr::null_mut(), key.clone(), value.clone()),
                true,
            ));
        }

        let node_ref = unsafe { &*untagged(node) };
        let (mut left_h, mut right_h) = (
            HazardPointer::new(&mut self.thread),
            HazardPointer::new(&mut self.thread),
        );
        let (left, right) = node_ref.protect_next(&mut left_h, &mut right_h)?;

        if !self.check_root() || Node::is_retired_spot(left) || Node::is_retired_spot(right) {
            return Ok((Node::retired_node(), false));
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => Ok((node, false)),
            cmp::Ordering::Less => {
                let (new_right, inserted) = self.do_insert(right, key, value)?;
                Ok((self.mk_balanced(node, left, new_right)?, inserted))
            }
            cmp::Ordering::Greater => {
                let (new_left, inserted) = self.do_insert(left, key, value)?;
                Ok((self.mk_balanced(node, new_left, right)?, inserted))
            }
        }
    }

    #[inline]
    fn do_remove<'hp>(
        &'hp mut self,
        node: *mut Node<K, V>,
        key: &K,
    ) -> Result<(*mut Node<K, V>, Option<&'hp V>), ()> {
        if Node::is_retired_spot(node) {
            return Ok((Node::retired_node(), None));
        }

        if node.is_null() {
            return Ok((ptr::null_mut(), None));
        }

        let node_ref = unsafe { &*untagged(node) };
        let (mut left_h, mut right_h) = (
            HazardPointer::new(&mut self.thread),
            HazardPointer::new(&mut self.thread),
        );
        let (left, right) = node_ref.protect_next(&mut left_h, &mut right_h)?;

        if !self.check_root() || Node::is_retired_spot(left) || Node::is_retired_spot(right) {
            return Ok((Node::retired_node(), None));
        }

        match node_ref.key.cmp(key) {
            cmp::Ordering::Equal => {
                self.removed_h.protect_raw(node);
                light_membarrier();

                let value = Some(&node_ref.value);
                self.retire_node(node);
                if node_ref.size == 1 {
                    return Ok((ptr::null_mut(), value));
                }

                if !left.is_null() {
                    let (new_left, succ, _new_left_h) = self.pull_rightmost(left)?;
                    return Ok((self.mk_balanced(succ, new_left, right)?, value));
                }
                let (new_right, succ, _new_right_h) = self.pull_leftmost(right)?;
                Ok((self.mk_balanced(succ, left, new_right)?, value))
            }
            cmp::Ordering::Less => {
                let (new_right, value) = self.launder().do_remove(right, key)?;
                Ok((self.mk_balanced(node, left, new_right)?, value))
            }
            cmp::Ordering::Greater => {
                let (new_left, value) = self.launder().do_remove(left, key)?;
                Ok((self.mk_balanced(node, new_left, right)?, value))
            }
        }
    }

    fn pull_leftmost(
        &mut self,
        node: *mut Node<K, V>,
    ) -> Result<
        (
            *mut Node<K, V>,
            *mut Node<K, V>,
            Option<HazardPointer<'domain>>,
        ),
        (),
    > {
        if Node::is_retired_spot(node) {
            return Ok((Node::retired_node(), Node::retired_node(), None));
        }

        let node_ref = unsafe { &*untagged(node) };
        let (mut left_h, mut right_h) = (
            HazardPointer::new(&mut self.thread),
            HazardPointer::new(&mut self.thread),
        );
        let (left, right) = node_ref.protect_next(&mut left_h, &mut right_h)?;

        if !self.check_root() || Node::is_retired_spot(left) || Node::is_retired_spot(right) {
            return Ok((Node::retired_node(), Node::retired_node(), None));
        }

        if !left.is_null() {
            let (new_left, succ, _new_left_h) = self.pull_leftmost(left)?;
            return Ok((self.mk_balanced(node, new_left, right)?, succ, None));
        }
        // node is the leftmost
        let succ = self.mk_node(
            ptr::null_mut(),
            ptr::null_mut(),
            node_ref.key.clone(),
            node_ref.value.clone(),
        );
        self.retire_node(node);
        return Ok((right, succ, Some(right_h)));
    }

    fn pull_rightmost(
        &mut self,
        node: *mut Node<K, V>,
    ) -> Result<
        (
            *mut Node<K, V>,
            *mut Node<K, V>,
            Option<HazardPointer<'domain>>,
        ),
        (),
    > {
        if Node::is_retired_spot(node) {
            return Ok((Node::retired_node(), Node::retired_node(), None));
        }

        let node_ref = unsafe { &*untagged(node) };
        let (mut left_h, mut right_h) = (
            HazardPointer::new(&mut self.thread),
            HazardPointer::new(&mut self.thread),
        );
        let (left, right) = node_ref.protect_next(&mut left_h, &mut right_h)?;

        if !self.check_root() || Node::is_retired_spot(left) || Node::is_retired_spot(right) {
            return Ok((Node::retired_node(), Node::retired_node(), None));
        }

        if !right.is_null() {
            let (new_right, succ, _new_right_h) = self.pull_rightmost(right)?;
            return Ok((self.mk_balanced(node, left, new_right)?, succ, None));
        }
        // node is the rightmost
        let succ = self.mk_node(
            ptr::null_mut(),
            ptr::null_mut(),
            node_ref.key.clone(),
            node_ref.value.clone(),
        );
        self.retire_node(node);
        return Ok((left, succ, Some(left_h)));
    }

    pub fn check_root(&self) -> bool {
        if let Some(root_link) = unsafe { self.root_link.as_ref() } {
            if self.curr_root == root_link.load(Ordering::Acquire) {
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}

pub struct BonsaiTreeMap<K, V> {
    root: AtomicPtr<Node<K, V>>,
}

impl<K, V> BonsaiTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            root: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[inline]
    pub fn protect_root<'domain, 'hp>(&self, state: &'hp mut State<'domain, K, V>) {
        state.curr_root = Self::protect_link(&self.root, &mut state.root_h);
    }

    #[inline]
    pub fn protect_link<'domain, 'hp>(
        link: &AtomicPtr<Node<K, V>>,
        hazptr: &'hp mut HazardPointer<'domain>,
    ) -> *mut Node<K, V> {
        let mut node = link.load(Ordering::Relaxed);
        loop {
            hazptr.protect_raw(untagged(node));
            light_membarrier();
            let new_node = link.load(Ordering::Acquire);
            if node == new_node {
                break;
            }
            node = new_node;
        }
        node
    }

    pub fn get<'domain, 'hp>(
        &self,
        key: &K,
        state: &'hp mut State<'domain, K, V>,
    ) -> Option<&'hp V> {
        loop {
            self.protect_root(state);
            let mut node = state.curr_root;
            while !node.is_null() && !Node::is_retired(node) {
                let node_ref = unsafe { &*node };
                match key.cmp(&node_ref.key) {
                    cmp::Ordering::Equal => break,
                    cmp::Ordering::Less => {
                        node = Self::protect_link(&node_ref.left, &mut state.succ_h)
                    }
                    cmp::Ordering::Greater => {
                        node = Self::protect_link(&node_ref.right, &mut state.succ_h)
                    }
                }
                HazardPointer::swap(&mut state.succ_h, &mut state.root_h);
            }

            if Node::is_retired_spot(node) {
                continue;
            }

            if node.is_null() {
                return None;
            }

            return Some(&unsafe { &*node }.value);
        }
    }

    pub fn insert<'domain, 'hp>(
        &self,
        key: K,
        value: V,
        state: &'hp mut State<'domain, K, V>,
    ) -> bool {
        loop {
            self.protect_root(state);
            state.root_link = &self.root;
            let old_root = state.curr_root;
            let (new_root, inserted) = ok_or!(state.do_insert(old_root, &key, &value), {
                state.abort();
                continue;
            });
            if Node::is_retired(new_root) {
                state.abort();
                continue;
            }

            unsafe {
                let result = state.thread.try_unlink(
                    BonsaiUnlink {
                        new_nodes: &state.new_nodes,
                        retired_nodes: &state.retired_nodes,
                        curr_root: state.curr_root,
                        new_root,
                        root_link: &self.root,
                    },
                    &[],
                );
                state.clear();
                if result {
                    return inserted;
                }
            }
        }
    }

    pub fn remove<'domain, 'hp>(
        &self,
        key: &K,
        state: &'hp mut State<'domain, K, V>,
    ) -> Option<&'hp V> {
        loop {
            self.protect_root(state);
            state.root_link = &self.root;
            let old_root = state.curr_root;
            let (new_root, value) = ok_or!(state.launder().do_remove(old_root, key), {
                state.abort();
                continue;
            });
            if Node::is_retired(new_root) {
                state.abort();
                continue;
            }

            unsafe {
                let result = state.thread.try_unlink(
                    BonsaiUnlink {
                        new_nodes: &state.new_nodes,
                        retired_nodes: &state.retired_nodes,
                        curr_root: state.curr_root,
                        new_root,
                        root_link: &self.root,
                    },
                    &[],
                );
                state.clear();
                if result {
                    return value;
                }
            }
        }
    }
}

impl<K, V> Drop for BonsaiTreeMap<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut stack = vec![self.root.load(Ordering::Relaxed)];

            while let Some(node) = stack.pop() {
                if node.is_null() {
                    continue;
                }

                let node_ref = &*node;

                stack.push(node_ref.left.load(Ordering::Relaxed));
                stack.push(node_ref.right.load(Ordering::Relaxed));
                drop(Box::from_raw(node));
            }
        }
    }
}

impl<K, V> Invalidate for Node<K, V> {
    fn invalidate(&self) {
        self.left.store(Node::retired_node(), Ordering::Release);
        self.right.store(Node::retired_node(), Ordering::Release);
    }
}

struct BonsaiUnlink<'g, K, V> {
    new_nodes: &'g Vec<*mut Node<K, V>>,
    retired_nodes: &'g Vec<*mut Node<K, V>>,
    curr_root: *mut Node<K, V>,
    new_root: *mut Node<K, V>,
    root_link: &'g AtomicPtr<Node<K, V>>,
}

impl<'g, K, V> Unlink<Node<K, V>> for BonsaiUnlink<'g, K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn do_unlink(&self) -> Result<Vec<*mut Node<K, V>>, ()> {
        if self
            .root_link
            .compare_exchange(
                self.curr_root,
                self.new_root,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            // commit current state
            for &node in self.retired_nodes {
                unsafe {
                    let node_ref = &*untagged(node);
                    node_ref.left.store(Node::retired_node(), Ordering::Release);
                    node_ref
                        .right
                        .store(Node::retired_node(), Ordering::Release);
                }
            }
            Ok(self.retired_nodes.clone())
        } else {
            // abort current state
            for &node in self.new_nodes {
                drop(unsafe { Box::from_raw(node) });
            }
            Err(())
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for BonsaiTreeMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Handle<'domain> = State<'domain, K, V>;

    fn new() -> Self {
        BonsaiTreeMap::new()
    }

    fn handle() -> Self::Handle<'static> {
        Self::Handle::default()
    }

    #[inline(never)]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        self.get(key, handle)
    }

    #[inline(never)]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.insert(key, value, handle)
    }

    #[inline(never)]
    fn remove<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: &K,
    ) -> Option<&'hp V> {
        self.remove(key, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::BonsaiTreeMap;
    use crate::hp::concurrent_map;

    #[test]
    fn smoke_bonsai_tree() {
        concurrent_map::tests::smoke::<BonsaiTreeMap<i32, String>>();
    }
}
