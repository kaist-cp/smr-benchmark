use std::sync::atomic::{fence, Ordering};

use hp_sharp::{
    AtomicRc, Counted, CsGuard, Pointer, Protector, RaGuard, Rc, Shared, Shield, Thread,
    WriteResult,
};

use crate::hp_sharp::concurrent_map::{ConcurrentMap, OutputHolder};

const MAX_HEIGHT: usize = 32;

type Tower<K, V> = [AtomicRc<Node<K, V>>; MAX_HEIGHT];

// `#[repr(C)]` is used to ensure the first field
// is also the first data in the memory alignment.
#[repr(C)]
struct Node<K, V> {
    next: Tower<K, V>,
    key: K,
    value: V,
    height: usize,
}

impl<K, V> Node<K, V>
where
    K: Default,
    V: Default,
{
    pub fn new(key: K, value: V) -> Self {
        let height = Self::generate_height();
        let next: [AtomicRc<Node<K, V>>; MAX_HEIGHT] = Default::default();
        for link in next.iter().take(height) {
            link.swap(Rc::null().with_tag(2), Ordering::Relaxed, unsafe {
                &RaGuard::unprotected()
            });
        }
        Self {
            key,
            value,
            next,
            height,
        }
    }

    pub fn head() -> Self {
        Self {
            key: K::default(),
            value: V::default(),
            next: Default::default(),
            height: MAX_HEIGHT,
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
    preds: [Shield<Counted<Node<K, V>>>; MAX_HEIGHT],
    succs: [Shield<Counted<Node<K, V>>>; MAX_HEIGHT],
    found: Shield<Counted<Node<K, V>>>,
}

impl<K, V> Cursor<K, V>
where
    K: Ord,
{
    fn found(&self, key: &K) -> Option<&Counted<Node<K, V>>> {
        let node = self.found.as_ref()?;
        if node.key.eq(key) {
            Some(node)
        } else {
            None
        }
    }
}

impl<K, V> Protector for Cursor<K, V> {
    type Target<'r> = SharedCursor<'r, K, V>;

    fn empty(handle: &mut Thread) -> Self {
        Self {
            preds: Protector::empty(handle),
            succs: Protector::empty(handle),
            found: Shield::empty(handle),
        }
    }

    fn protect_unchecked(&mut self, read: &Self::Target<'_>) {
        self.preds[MAX_HEIGHT - 1].protect_unchecked(&read.preds[MAX_HEIGHT - 1]);
        self.succs[MAX_HEIGHT - 1].protect_unchecked(&read.succs[MAX_HEIGHT - 1]);
        for i in (0..MAX_HEIGHT - 1).rev() {
            if read.preds[i + 1] == read.preds[i] {
                unsafe { self.preds[i].store(read.preds[i]) };
            } else {
                self.preds[i].protect_unchecked(&read.preds[i]);
            }
            if read.succs[i + 1] == read.succs[i] {
                unsafe { self.succs[i].store(read.succs[i]) };
            } else {
                self.succs[i].protect_unchecked(&read.succs[i]);
            }
        }
        self.found.protect_unchecked(&read.found);
    }

    fn as_target<'r>(&self, guard: &'r hp_sharp::CsGuard) -> Option<Self::Target<'r>> {
        Some(SharedCursor {
            preds: self.preds.as_target(guard)?,
            succs: self.succs.as_target(guard)?,
            found: self.found.as_target(guard)?,
        })
    }

    fn release(&mut self) {
        for i in 0..MAX_HEIGHT {
            self.preds[i].release();
            self.succs[i].release();
        }
        self.found.release();
    }
}

pub struct SharedCursor<'r, K, V> {
    preds: [Shared<'r, Counted<Node<K, V>>>; MAX_HEIGHT],
    succs: [Shared<'r, Counted<Node<K, V>>>; MAX_HEIGHT],
    found: Shared<'r, Counted<Node<K, V>>>,
}

impl<'r, K, V> Clone for SharedCursor<'r, K, V> {
    fn clone(&self) -> Self {
        Self {
            preds: self.preds.clone(),
            succs: self.succs.clone(),
            found: self.found.clone(),
        }
    }
}

impl<'r, K, V> Copy for SharedCursor<'r, K, V> {}

impl<'r, K, V> SharedCursor<'r, K, V> {
    fn new(head: Shared<'r, Counted<Node<K, V>>>, _: &'r CsGuard) -> Self {
        let preds = [head; MAX_HEIGHT];
        let succs = Default::default();
        let found = Default::default();

        Self {
            preds,
            succs,
            found,
        }
    }
}

pub struct Output<K, V>(Cursor<K, V>, [Shield<Counted<Node<K, V>>>; 2]);

impl<K, V> OutputHolder<V> for Output<K, V> {
    fn default(handle: &mut Thread) -> Self {
        Self(
            Cursor::empty(handle),
            [Shield::empty(handle), Shield::empty(handle)],
        )
    }

    fn output(&self) -> &V {
        self.0.found.as_ref().map(|node| &node.value).unwrap()
    }
}

pub struct SkipList<K, V> {
    head: AtomicRc<Node<K, V>>,
}

impl<K, V> SkipList<K, V>
where
    K: Ord + Clone + Default,
    V: Clone + Default,
{
    pub fn new() -> Self {
        Self {
            head: AtomicRc::new(Node::head()),
        }
    }

    fn find_optimistic_inner<'r>(
        &self,
        key: &K,
        guard: &'r CsGuard,
    ) -> Shared<'r, Counted<Node<K, V>>> {
        let head = self.head.load(Ordering::Relaxed, guard);
        let mut level = MAX_HEIGHT;
        while level >= 1
            && unsafe { head.deref_unchecked() }.next[level - 1]
                .load(Ordering::Relaxed, guard)
                .is_null()
        {
            level -= 1;
        }

        let mut pred = head;
        while level >= 1 {
            level -= 1;
            let mut curr =
                unsafe { pred.deref_unchecked() }.next[level].load(Ordering::Acquire, guard);

            loop {
                let curr_node = some_or!(curr.as_ref(guard), break);
                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        pred = curr;
                        curr = curr_node.next[level].load(Ordering::Acquire, guard);
                    }
                    std::cmp::Ordering::Equal => {
                        if curr_node.next[level].load(Ordering::Acquire, guard).tag() == 0 {
                            return curr;
                        } else {
                            return Shared::null();
                        }
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
        }
        Shared::null()
    }

    fn find_optimistic(&self, key: &K, cursor: &mut Output<K, V>, handle: &mut Thread) {
        unsafe {
            cursor
                .0
                .found
                .traverse(handle, |guard| self.find_optimistic_inner(key, guard));
        }
    }

    fn find_inner<'r>(
        &self,
        key: &K,
        aux: &mut [Shield<Counted<Node<K, V>>>; 2],
        guard: &'r CsGuard,
    ) -> Option<SharedCursor<'r, K, V>> {
        let head = self.head.load(Ordering::Relaxed, guard);
        let mut cursor = SharedCursor::new(head, guard);

        let mut level = MAX_HEIGHT;
        while level >= 1
            && unsafe { head.deref_unchecked() }.next[level - 1]
                .load(Ordering::Relaxed, guard)
                .is_null()
        {
            level -= 1;
        }

        let mut pred = head;
        while level >= 1 {
            level -= 1;
            let mut curr =
                unsafe { pred.deref_unchecked() }.next[level].load(Ordering::Acquire, guard);
            // If `curr` is marked, that means `pred` is removed and we have to restart
            // the search.
            if curr.tag() != 0 {
                return None;
            }

            while let Some(curr_ref) = curr.as_ref(guard) {
                let succ = curr_ref.next[level].load(Ordering::Acquire, guard);

                if succ.tag() != 0 {
                    unsafe {
                        aux.traverse_mask(guard, [pred, succ], |[pred, succ], guard| {
                            match pred.deref_unchecked().next[level].compare_exchange(
                                curr.with_tag(0),
                                succ.with_tag(0),
                                Ordering::Release,
                                Ordering::Relaxed,
                                guard,
                            ) {
                                Ok(_) => {
                                    return WriteResult::Finished;
                                }
                                Err(_) => {
                                    // On failure, we cannot do anything reasonable to
                                    // continue searching from the current position.
                                    // Restart the search.
                                    return WriteResult::RepinEpoch;
                                }
                            }
                        });
                    }
                    curr = succ.with_tag(0);
                    continue;
                }

                // If `curr` contains a key that is greater than or equal to `key`, we're
                // done with this level.
                match curr_ref.key.cmp(key) {
                    std::cmp::Ordering::Greater => break,
                    std::cmp::Ordering::Equal => {
                        cursor.found = curr;
                        break;
                    }
                    std::cmp::Ordering::Less => {}
                }

                // Move one step forward.
                pred = curr;
                curr = succ;
            }

            cursor.preds[level] = pred;
            cursor.succs[level] = curr;
        }
        Some(cursor)
    }

    fn find(&self, key: &K, output: &mut Output<K, V>, handle: &mut Thread) {
        unsafe {
            let cursor = &mut output.0;
            let aux = &mut output.1;
            cursor.traverse(handle, |guard| loop {
                if let Some(cursor) = self.find_inner(key, aux, guard) {
                    return cursor;
                }
            })
        }
    }

    fn insert(&self, key: K, value: V, output: &mut Output<K, V>, handle: &mut Thread) -> bool {
        self.find(&key, output, handle);
        if output.0.found(&key).is_some() {
            return false;
        }

        // The reference count is initially two to account for
        // 1. The link at the level 0 of the tower.
        // 2. The current reference in this function.
        let new_node = Rc::new(Node::new(key, value), handle);
        let new_node_ref = unsafe { new_node.deref() };
        let height = new_node_ref.height;

        loop {
            new_node_ref.next[0].swap(
                Rc::from_shield(&output.0.succs[0]),
                Ordering::Relaxed,
                handle,
            );

            if unsafe { output.0.preds[0].deref_unchecked() }.next[0]
                .compare_exchange(
                    output.0.succs[0].shared(),
                    new_node.clone(handle),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    handle,
                )
                .is_ok()
            {
                break;
            }

            // We failed. Let's search for the key and try again.
            self.find(&new_node_ref.key, output, handle);
            if output.0.found(&new_node_ref.key).is_some() {
                return false;
            }
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        let guard = unsafe { &CsGuard::unprotected() };
        'build: for level in 1..height {
            loop {
                let pred = &output.0.preds[level];
                let succ = &output.0.succs[level];
                let next = new_node_ref.next[level].load(Ordering::SeqCst, guard);

                // If the current pointer is marked, that means another thread is already
                // removing the node we've just inserted. In that case, let's just stop
                // building the tower.
                if (next.tag() & 1) != 0 {
                    break 'build;
                }

                if new_node_ref.next[level]
                    .compare_exchange(
                        Shared::null().with_tag(2),
                        succ,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        handle,
                    )
                    .is_err()
                {
                    break 'build;
                }

                // Try installing the new node at the current level.
                if unsafe { pred.deref_unchecked() }.next[level]
                    .compare_exchange(
                        succ.shared(),
                        new_node.clone(handle),
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

        true
    }

    fn remove(&self, key: &K, output: &mut Output<K, V>, handle: &mut Thread) -> bool {
        loop {
            self.find(key, output, handle);
            let cursor = &output.0;
            let node = some_or!(cursor.found(key), return false);

            // Try removing the node by marking its tower.
            if node.mark_tower(handle) {
                for level in (0..node.height).rev() {
                    let guard = unsafe { &CsGuard::unprotected() };
                    let succ = node.next[level].load(Ordering::SeqCst, guard);
                    if (succ.tag() & 2) != 0 {
                        continue;
                    }
                    let succ_sh = &mut output.1[0];
                    succ_sh.protect_unchecked(&succ.with_tag(0));
                    fence(Ordering::SeqCst);

                    // Try linking the predecessor and successor at this level.
                    if unsafe { cursor.preds[level].deref_unchecked() }.next[level]
                        .compare_exchange(
                            unsafe { Shared::from_usize(node as *const _ as usize) },
                            &*succ_sh,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            handle,
                        )
                        .is_err()
                    {
                        self.find(key, output, handle);
                        break;
                    }
                }
            }
            return true;
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for SkipList<K, V>
where
    K: Ord + Clone + Default,
    V: Clone + Default,
{
    type Output = Output<K, V>;

    fn new() -> Self {
        SkipList::new()
    }

    #[inline(never)]
    fn get(&self, key: &K, output: &mut Self::Output, handle: &mut Thread) -> bool {
        self.find_optimistic(key, output, handle);
        output.0.found(key).is_some()
    }

    #[inline(never)]
    fn insert(&self, key: K, value: V, output: &mut Self::Output, handle: &mut Thread) -> bool {
        self.insert(key, value, output, handle)
    }

    #[inline(never)]
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
    use crate::hp_sharp::concurrent_map;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<SkipList<i32, String>>();
    }
}
