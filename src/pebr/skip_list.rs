use std::{
    array, mem,
    sync::atomic::{fence, AtomicUsize, Ordering},
};

use crossbeam_pebr::{unprotected, Atomic, Guard, Owned, Pointer, Shared, Shield, ShieldError};

use super::concurrent_map::ConcurrentMap;

const MAX_HEIGHT: usize = 32;

type Tower<K, V> = [Atomic<Node<K, V>>; MAX_HEIGHT];

struct Node<K, V> {
    next: Tower<K, V>,
    key: K,
    value: V,
    height: usize,
    refs: AtomicUsize,
}

impl<K, V> Node<K, V> {
    pub fn new(key: K, value: V, init_refs: usize) -> Self {
        let height = Self::generate_height();
        Self {
            key,
            value,
            next: Default::default(),
            height,
            refs: AtomicUsize::new(init_refs),
        }
    }

    fn generate_height() -> usize {
        // returns 1 with probability 3/4
        if rand::random::<usize>() % 4 < 3 {
            return 1;
        }
        // returns h with probability 2^(−(h+1))
        let mut height = 2;
        while height < MAX_HEIGHT && rand::random::<bool>() {
            height += 1;
        }
        height
    }

    pub fn decrement(&self, guard: &Guard) {
        if self.refs.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);
            unsafe { guard.defer_destroy(Shared::from(self as *const _)) };
        }
    }

    pub fn mark_tower(&self) -> bool {
        for level in (0..self.height).rev() {
            // We're loading the pointer only for the tag, so it's okay to use
            // `epoch::unprotected()` in this situation.
            let tag = self.next[level]
                .fetch_or(1, Ordering::SeqCst, unsafe { unprotected() })
                .tag();
            // If the level 0 pointer was already marked, somebody else removed the node.
            if level == 0 && tag == 1 {
                return false;
            }
        }
        true
    }
}

pub struct Handle<K, V> {
    preds_h: [Shield<Node<K, V>>; MAX_HEIGHT],
    succs_h: [Shield<Node<K, V>>; MAX_HEIGHT],
    new_node_h: Shield<Node<K, V>>,
}

impl<K, V> Handle<K, V> {
    fn new(guard: &Guard) -> Self {
        Self {
            preds_h: array::from_fn(|_| Shield::null(guard)),
            succs_h: array::from_fn(|_| Shield::null(guard)),
            new_node_h: Shield::null(guard),
        }
    }

    fn reset(&mut self) {
        for i in 0..MAX_HEIGHT {
            self.preds_h[i].release();
            self.succs_h[i].release();
        }
        self.new_node_h.release();
    }

    // bypass E0499-E0503, etc that are supposed to be fixed by polonius
    #[inline]
    fn launder<'hp1, 'hp2>(&'hp1 mut self) -> &'hp2 mut Self {
        unsafe { core::mem::transmute(self) }
    }
}

struct Cursor<'g, K, V> {
    preds: [Shared<'g, Node<K, V>>; MAX_HEIGHT],
    succs: [Shared<'g, Node<K, V>>; MAX_HEIGHT],
}

impl<'g, K, V> Cursor<'g, K, V>
where
    K: Ord,
{
    fn new(head: &'g Tower<K, V>) -> Self {
        Self {
            preds: [unsafe { Shared::from_usize(head as *const _ as usize) }; MAX_HEIGHT],
            succs: [Shared::null(); MAX_HEIGHT],
        }
    }

    fn found(&self, key: &K) -> Option<&Node<K, V>> {
        let node = unsafe { self.succs[0].as_ref() }?;
        if node.key.eq(key) {
            Some(node)
        } else {
            None
        }
    }
}

pub struct SkipList<K, V> {
    head: Tower<K, V>,
}

impl<K, V> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        let mut node = self.head[0].load(Ordering::Relaxed, unsafe { unprotected() });

        while let Some(node_ref) = unsafe { node.as_ref() } {
            let next = node_ref.next[0].load(Ordering::Relaxed, unsafe { unprotected() });
            drop(unsafe { node.into_owned() });
            node = next;
        }
    }
}

impl<K, V> SkipList<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            head: Default::default(),
        }
    }

    fn find_optimistic_inner<'g>(
        &'g self,
        key: &K,
        handle: &'g mut Handle<K, V>,
        guard: &'g Guard,
    ) -> Result<Option<Cursor<'g, K, V>>, ShieldError> {
        let mut cursor = Cursor::new(&self.head);

        let mut level = MAX_HEIGHT;
        while level >= 1
            && self.head[level - 1]
                .load(Ordering::Relaxed, guard)
                .is_null()
        {
            level -= 1;
        }

        let mut pred = &self.head;
        while level >= 1 {
            level -= 1;
            let mut curr = pred[level].load(Ordering::Acquire, guard);

            loop {
                handle.succs_h[level].defend(curr, guard)?;
                let curr_node = some_or!(unsafe { curr.as_ref() }, break);

                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        pred = &curr_node.next;
                        curr = curr_node.next[level].load(Ordering::Acquire, guard);
                        mem::swap(&mut handle.succs_h[level], &mut handle.preds_h[level]);
                    }
                    std::cmp::Ordering::Equal => {
                        if curr_node.next[level].load(Ordering::Acquire, guard).tag() == 0 {
                            cursor.succs[0] = curr;
                            return Ok(Some(cursor));
                        } else {
                            return Ok(None);
                        }
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
        }
        return Ok(None);
    }

    fn find_optimistic<'g>(
        &'g self,
        key: &K,
        handle: &'g mut Handle<K, V>,
        guard: &'g mut Guard,
    ) -> Option<Cursor<'g, K, V>> {
        loop {
            match self.find_optimistic_inner(key, handle.launder(), unsafe {
                &mut *(guard as *mut Guard)
            }) {
                Ok(cursor) => return cursor,
                Err(ShieldError::Ejected) => guard.repin(),
            }
        }
    }

    fn find_inner<'g>(
        &'g self,
        key: &K,
        handle: &'g mut Handle<K, V>,
        guard: &'g Guard,
    ) -> Result<Cursor<'g, K, V>, ShieldError> {
        'search: loop {
            let mut cursor = Cursor::new(&self.head);

            let mut level = MAX_HEIGHT;
            while level >= 1
                && self.head[level - 1]
                    .load(Ordering::Relaxed, guard)
                    .is_null()
            {
                level -= 1;
            }

            let mut pred =
                unsafe { Shared::<Node<K, V>>::from_usize(&self.head as *const _ as usize) };
            while level >= 1 {
                level -= 1;
                handle.preds_h[level].defend(pred, guard)?;
                let mut curr = unsafe { pred.deref() }.next[level].load_consume(guard);
                // If `curr` is marked, that means `pred` is removed and we have to restart the
                // search.
                if curr.tag() == 1 {
                    continue 'search;
                }

                while let Some(curr_ref) = unsafe { curr.as_ref() } {
                    handle.succs_h[level].defend(curr, guard)?;
                    let succ = curr_ref.next[level].load_consume(guard);

                    if succ.tag() == 1 {
                        if self.help_unlink(&unsafe { pred.deref() }.next[level], curr, succ, guard)
                        {
                            curr = succ.with_tag(0);
                            continue;
                        } else {
                            // On failure, we cannot do anything reasonable to continue
                            // searching from the current position. Restart the search.
                            continue 'search;
                        }
                    }

                    // If `curr` contains a key that is greater than or equal to `key`, we're
                    // done with this level.
                    match curr_ref.key.cmp(key) {
                        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
                            break;
                        }
                        std::cmp::Ordering::Less => {}
                    }

                    // Move one step forward.
                    pred = curr;
                    curr = succ;
                    mem::swap(&mut handle.preds_h[level], &mut handle.succs_h[level]);
                }

                cursor.preds[level] = pred;
                cursor.succs[level] = curr;
            }

            return Ok(cursor);
        }
    }

    fn find<'g>(
        &'g self,
        key: &K,
        handle: &'g mut Handle<K, V>,
        guard: &'g mut Guard,
    ) -> Cursor<'g, K, V> {
        loop {
            match self.find_inner(key, handle.launder(), unsafe {
                &mut *(guard as *mut Guard)
            }) {
                Ok(cursor) => return cursor,
                Err(ShieldError::Ejected) => guard.repin(),
            }
        }
    }

    fn help_unlink<'g>(
        &'g self,
        pred: &'g Atomic<Node<K, V>>,
        curr: Shared<'g, Node<K, V>>,
        succ: Shared<'g, Node<K, V>>,
        guard: &'g Guard,
    ) -> bool {
        let success = pred
            .compare_and_set(curr.with_tag(0), succ.with_tag(0), Ordering::Release, guard)
            .is_ok();

        if success {
            unsafe { curr.deref().decrement(guard) };
        }
        success
    }

    pub fn insert(&self, key: K, value: V, handle: &mut Handle<K, V>, guard: &mut Guard) -> bool {
        let mut cursor = self.find(&key, handle, unsafe { &mut *(guard as *mut Guard) });
        if cursor.found(&key).is_some() {
            return false;
        }

        // The reference count is initially two to account for
        // 1. The link at the level 0 of the tower.
        // 2. The current reference in this function.
        let new_node = Owned::new(Node::new(key, value, 2)).into_shared(unsafe { unprotected() });
        let new_node_ref = unsafe { new_node.deref() };
        let height = new_node_ref.height;

        loop {
            new_node_ref.next[0].store(cursor.succs[0], Ordering::Relaxed);

            if unsafe { cursor.preds[0].deref() }.next[0]
                .compare_and_set(cursor.succs[0], new_node, Ordering::SeqCst, guard)
                .is_ok()
            {
                break;
            }

            // We failed. Let's search for the key and try again.
            cursor = self.find(&new_node_ref.key, handle, unsafe {
                &mut *(guard as *mut Guard)
            });
            if cursor.found(&new_node_ref.key).is_some() {
                drop(unsafe { new_node.into_owned() });
                return false;
            }
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        'build: for level in 1..height {
            loop {
                let pred = cursor.preds[level];
                let succ = cursor.succs[level];
                let next = new_node_ref.next[level].load(Ordering::SeqCst, guard);

                // If the current pointer is marked, that means another thread is already
                // removing the node we've just inserted. In that case, let's just stop
                // building the tower.
                if next.tag() == 1 {
                    break 'build;
                }

                if unsafe { succ.as_ref() }.map(|node| &node.key) == Some(&new_node_ref.key) {
                    cursor = self.find(&new_node_ref.key, handle, unsafe {
                        &mut *(guard as *mut Guard)
                    });
                    continue;
                }

                if new_node_ref.next[level]
                    .compare_and_set(next, succ, Ordering::SeqCst, guard)
                    .is_err()
                {
                    break 'build;
                }

                new_node_ref.refs.fetch_add(1, Ordering::Relaxed);
                // Try installing the new node at the current level.
                if unsafe { pred.deref() }.next[level]
                    .compare_and_set(succ, new_node, Ordering::SeqCst, guard)
                    .is_ok()
                {
                    // Success! Continue on the next level.
                    break;
                }

                // Installation failed.
                new_node_ref.refs.fetch_sub(1, Ordering::Relaxed);
                cursor = self.find(&new_node_ref.key, handle, unsafe {
                    &mut *(guard as *mut Guard)
                });
            }
        }

        if new_node_ref.next[height - 1]
            .load(Ordering::SeqCst, guard)
            .tag()
            == 1
        {
            self.find(&new_node_ref.key, handle, guard);
        }

        new_node_ref.decrement(guard);
        true
    }

    pub fn remove<'g>(
        &'g self,
        key: &K,
        handle: &mut Handle<K, V>,
        guard: &'g mut Guard,
    ) -> Option<V> {
        loop {
            let cursor = self.find(key, handle.launder(), unsafe {
                &mut *(guard as *mut Guard)
            });
            let node = cursor.found(key)?;
            let value = node.value.clone();

            // Try removing the node by marking its tower.
            if node.mark_tower() {
                for level in (0..node.height).rev() {
                    let succ = node.next[level].load(Ordering::SeqCst, guard);

                    // Try linking the predecessor and successor at this level.
                    if unsafe { cursor.preds[level].deref() }.next[level]
                        .compare_and_set(
                            Shared::from(node as *const _),
                            succ.with_tag(0),
                            Ordering::SeqCst,
                            guard,
                        )
                        .is_ok()
                    {
                        node.decrement(guard);
                    } else {
                        self.find(key, handle, guard);
                        break;
                    }
                }
            }
            return Some(value);
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for SkipList<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Handle = Handle<K, V>;

    fn new() -> Self {
        SkipList::new()
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
        let cursor = self.find_optimistic(key, handle, guard)?;
        let node = unsafe { cursor.succs[0].deref() };
        if node.key.eq(&key) {
            Some(&node.value)
        } else {
            None
        }
    }

    fn insert(&self, handle: &mut Self::Handle, key: K, value: V, guard: &mut Guard) -> bool {
        self.insert(key, value, handle, guard)
    }

    fn remove(&self, handle: &mut Self::Handle, key: &K, guard: &mut Guard) -> Option<V> {
        self.remove(key, handle, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::SkipList;
    use crate::pebr::concurrent_map;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<SkipList<i32, String>>();
    }
}
