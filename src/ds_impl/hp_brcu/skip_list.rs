use std::{
    mem::swap,
    sync::atomic::{fence, AtomicUsize, Ordering},
};

use hp_brcu::{
    Atomic, CsGuard, Owned, Pointer, RollbackProof, Shared, Shield, Thread, Unprotected,
};

use super::concurrent_map::{ConcurrentMap, OutputHolder};

const MAX_HEIGHT: usize = 32;

type Tower<K, V> = [Atomic<Node<K, V>>; MAX_HEIGHT];

// `#[repr(C)]` is used to ensure the first field
// is also the first data in the memory alignment.
#[repr(C)]
struct Node<K, V> {
    next: Tower<K, V>,
    key: K,
    value: V,
    height: usize,
    refs: AtomicUsize,
}

impl<K, V> Node<K, V> {
    pub fn new(key: K, value: V) -> Self {
        let height = Self::generate_height();
        let next: [Atomic<Node<K, V>>; MAX_HEIGHT] = Default::default();
        for link in next.iter().take(height) {
            link.store(Shared::null().with_tag(2), Ordering::Relaxed, unsafe {
                &Unprotected::new()
            });
        }
        Self {
            key,
            value,
            next,
            height,
            refs: AtomicUsize::new(height + 1),
        }
    }

    fn generate_height() -> usize {
        // returns 1 with probability 3/4
        if rand::random::<usize>() % 4 < 3 || MAX_HEIGHT == 1 {
            return 1;
        }
        // returns h with probability 2^(−(h+1))
        let mut height = 2;
        while height < MAX_HEIGHT && rand::random::<bool>() {
            height += 1;
        }
        height
    }

    pub fn decrement<G: RollbackProof>(&self, guard: &mut G) {
        if self.refs.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);
            unsafe {
                guard.retire(Shared::<'_, Node<K, V>>::from_usize(
                    self as *const _ as usize,
                ))
            };
        }
    }

    pub fn mark_tower(&self, handle: &Thread) -> bool {
        for level in (0..self.height).rev() {
            let tag = self.next[level].fetch_or(1, Ordering::SeqCst, handle).tag();
            // If the level 0 pointer was already marked, somebody else removed the node.
            if level == 0 && (tag & 1) != 0 {
                return false;
            }
        }
        true
    }
}

pub struct Cursor<K, V> {
    preds: [Shield<Node<K, V>>; MAX_HEIGHT],
    succs: [Shield<Node<K, V>>; MAX_HEIGHT],
    found: Shield<Node<K, V>>,
}

impl<K, V> Cursor<K, V> {
    fn empty(handle: &mut Thread) -> Self {
        Self {
            preds: [(); MAX_HEIGHT].map(|_| Shield::null(handle)),
            succs: [(); MAX_HEIGHT].map(|_| Shield::null(handle)),
            found: Shield::null(handle),
        }
    }

    fn initialize(&mut self, head: &Tower<K, V>) {
        unsafe {
            for pred in &mut self.preds {
                // Safety: `head` is always a valid location.
                pred.store_wo_prot(Shared::from_usize(head as *const _ as usize));
            }
            for succ in &mut self.succs {
                // Safety: A null pointer is never dereferenced.
                succ.store_wo_prot(Shared::null())
            }
        }
    }
}

impl<K, V> OutputHolder<V> for Cursor<K, V> {
    fn default(handle: &mut Thread) -> Self {
        Self::empty(handle)
    }

    fn output(&self) -> &V {
        self.found.as_ref().map(|node| &node.value).unwrap()
    }
}

pub struct SkipList<K, V> {
    head: Tower<K, V>,
}

impl<K, V> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        let guard = unsafe { Unprotected::new() };
        let mut node = self.head[0].load(Ordering::Relaxed, &guard);

        while let Some(node_ref) = unsafe { node.as_ref() } {
            let next = node_ref.next[0].load(Ordering::Relaxed, &guard);
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

    fn find_optimistic_inner<'r>(
        &self,
        key: &K,
        cursor: &mut Cursor<K, V>,
        guard: &'r CsGuard,
    ) -> bool {
        let mut level = MAX_HEIGHT;
        while level >= 1
            && self.head[level - 1]
                .load(Ordering::Relaxed, guard)
                .is_null()
        {
            level -= 1;
        }

        let mut pred = &self.head;
        let mut curr = Shared::null();
        while level >= 1 {
            level -= 1;
            curr = pred[level].load(Ordering::Acquire, guard);

            loop {
                let curr_node = some_or!(unsafe { curr.as_ref() }, break);
                let succ = curr_node.next[level].load(Ordering::Acquire, guard);

                if succ.tag() != 0 {
                    curr = succ;
                    continue;
                }

                if curr_node.key < *key {
                    pred = &curr_node.next;
                    curr = succ;
                } else {
                    break;
                }
            }
        }

        if let Some(curr_node) = unsafe { curr.as_ref() } {
            if curr_node.key == *key {
                cursor.found.protect(curr);
                return true;
            }
        };
        false
    }

    fn find_optimistic(&self, key: &K, cursor: &mut Cursor<K, V>, handle: &mut Thread) -> bool {
        unsafe { handle.critical_section(|guard| self.find_optimistic_inner(key, cursor, guard)) }
    }

    fn find_inner<'r>(
        &self,
        key: &K,
        output: &mut Cursor<K, V>,
        guard: &'r CsGuard,
    ) -> Result<bool, ()> {
        output.initialize(&self.head);

        let mut level = MAX_HEIGHT;
        while level >= 1
            && self.head[level - 1]
                .load(Ordering::Relaxed, guard)
                .is_null()
        {
            level -= 1;
        }

        let mut found = false;
        let mut pred =
            unsafe { Shared::<'_, Node<K, V>>::from_usize(&self.head as *const _ as usize) };
        while level >= 1 {
            level -= 1;
            let mut curr = unsafe { pred.deref() }.next[level].load(Ordering::Acquire, guard);
            // If `curr` is marked, that means `pred` is removed and we have to restart
            // the search.
            if curr.tag() != 0 {
                return Err(());
            }

            while let Some(curr_ref) = unsafe { curr.as_ref() } {
                let succ = curr_ref.next[level].load(Ordering::Acquire, guard);

                if succ.tag() != 0 {
                    output.preds[level].protect(pred);
                    output.succs[level].protect(curr);
                    unsafe {
                        guard.mask(|guard| {
                            pred.deref().next[level]
                                .compare_exchange(
                                    curr.with_tag(0),
                                    succ.with_tag(0),
                                    Ordering::Release,
                                    Ordering::Relaxed,
                                    guard,
                                )
                                .map(|_| curr.deref().decrement(guard))
                                .map_err(|_| ())
                        })
                    }?;
                    curr = succ.with_tag(0);
                    continue;
                }

                // If `curr` contains a key that is greater than or equal to `key`, we're
                // done with this level.
                match curr_ref.key.cmp(key) {
                    std::cmp::Ordering::Greater => break,
                    std::cmp::Ordering::Equal => {
                        found = level == 0;
                        break;
                    }
                    std::cmp::Ordering::Less => {}
                }

                // Move one step forward.
                pred = curr;
                curr = succ;
            }

            output.preds[level].protect(pred);
            output.succs[level].protect(curr);
        }
        Ok(found)
    }

    fn find(&self, key: &K, output: &mut Cursor<K, V>, handle: &mut Thread) -> bool {
        loop {
            let result =
                unsafe { handle.critical_section(|guard| self.find_inner(key, output, guard)) };
            if let Ok(found) = result {
                return found;
            }
        }
    }

    fn insert(&self, key: K, value: V, output: &mut Cursor<K, V>, handle: &mut Thread) -> bool {
        if self.find(&key, output, handle) {
            return false;
        }

        // The reference count is initially two to account for
        // 1. The link at the level 0 of the tower.
        // 2. The current reference in this function.
        let new_node = Owned::new(Node::new(key, value)).into_shared();
        let new_node_ref = unsafe { new_node.deref() };
        let height = new_node_ref.height;

        loop {
            new_node_ref.next[0].store(output.succs[0].shared(), Ordering::Relaxed, handle);

            if unsafe { output.preds[0].deref() }.next[0]
                .compare_exchange(
                    output.succs[0].shared(),
                    new_node,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    handle,
                )
                .is_ok()
            {
                break;
            }

            // We failed. Let's search for the key and try again.
            if self.find(&new_node_ref.key, output, handle) {
                drop(unsafe { new_node.into_owned() });
                return false;
            }
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        let guard = unsafe { &Unprotected::new() };
        'build: for level in 1..height {
            loop {
                let pred = &output.preds[level];
                let succ = &output.succs[level];
                let next = new_node_ref.next[level].load(Ordering::SeqCst, guard);

                // If the current pointer is marked, that means another thread is already
                // removing the node we've just inserted. In that case, let's just stop
                // building the tower.
                if (next.tag() & 1) != 0 {
                    new_node_ref
                        .refs
                        .fetch_sub(height - level, Ordering::SeqCst);
                    break 'build;
                }

                if new_node_ref.next[level]
                    .compare_exchange(
                        Shared::null().with_tag(2),
                        succ.shared(),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        handle,
                    )
                    .is_err()
                {
                    new_node_ref
                        .refs
                        .fetch_sub(height - level, Ordering::SeqCst);
                    break 'build;
                }

                // Try installing the new node at the current level.
                if unsafe { pred.deref() }.next[level]
                    .compare_exchange(
                        succ.shared(),
                        new_node,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        handle,
                    )
                    .is_ok()
                {
                    // Success! Continue on the next level.
                    break;
                }

                // Installation failed.
                self.find(&new_node_ref.key, output, handle);
            }
        }

        new_node_ref.decrement(handle);
        true
    }

    fn remove(&self, key: &K, output: &mut Cursor<K, V>, handle: &mut Thread) -> bool {
        loop {
            if !self.find(key, output, handle) {
                return false;
            }
            swap(&mut output.found, &mut output.succs[0]);
            let node = unsafe { output.found.deref() };

            // Try removing the node by marking its tower.
            if node.mark_tower(handle) {
                for level in (0..node.height).rev() {
                    let succ =
                        node.next[level].load(Ordering::SeqCst, unsafe { &Unprotected::new() });
                    if (succ.tag() & 2) != 0 {
                        continue;
                    }
                    // Try linking the predecessor and successor at this level.
                    if unsafe { output.preds[level].deref() }.next[level]
                        .compare_exchange(
                            unsafe { Shared::from_usize(node as *const _ as usize) },
                            succ.with_tag(0),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            handle,
                        )
                        .is_ok()
                    {
                        node.decrement(handle);
                    } else {
                        self.find(key, output, handle);
                        break;
                    }
                }
                return true;
            }
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for SkipList<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Output = Cursor<K, V>;

    fn new() -> Self {
        SkipList::new()
    }

    #[inline(always)]
    fn get(&self, key: &K, output: &mut Self::Output, handle: &mut Thread) -> bool {
        self.find_optimistic(key, output, handle)
    }

    #[inline(always)]
    fn insert(&self, key: K, value: V, output: &mut Self::Output, handle: &mut Thread) -> bool {
        self.insert(key, value, output, handle)
    }

    #[inline(always)]
    fn remove<'domain, 'hp>(
        &self,
        key: &K,
        output: &mut Self::Output,
        handle: &mut Thread,
    ) -> bool {
        self.remove(key, output, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::SkipList;
    use crate::ds_impl::hp_brcu::concurrent_map;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<_, SkipList<i32, String>, _>(&i32::to_string);
    }
}
