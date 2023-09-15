use std::{fmt::Display, sync::atomic::Ordering};

use cdrc_rs::{AtomicRc, Cs, Pointer, Rc, Snapshot, StrongPtr, TaggedCnt};

use super::concurrent_map::{ConcurrentMap, OutputHolder};

const MAX_HEIGHT: usize = 32;

type Tower<K, V, C> = [AtomicRc<Node<K, V, C>, C>; MAX_HEIGHT];

struct Node<K, V, C: Cs> {
    key: K,
    value: V,
    next: Tower<K, V, C>,
    height: usize,
}

impl<K, V, C> Node<K, V, C>
where
    K: Default,
    V: Default,
    C: Cs,
{
    pub fn new(key: K, value: V) -> Self {
        let cs = unsafe { &C::without_epoch() };
        let height = Self::generate_height();
        let next: [AtomicRc<Node<K, V, C>, C>; MAX_HEIGHT] = Default::default();
        for link in next.iter().take(height) {
            link.store(Rc::null().with_tag(2), Ordering::Relaxed, cs);
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

    pub fn mark_tower(&self, cs: &C) -> bool {
        for level in (0..self.height).rev() {
            let tag = self.next[level].fetch_or(1, Ordering::SeqCst, cs).tag();
            // If the level 0 pointer was already marked, somebody else removed the node.
            if level == 0 && (tag & 1) != 0 {
                return false;
            }
        }
        true
    }
}

pub struct Cursor<K, V, C: Cs> {
    preds: [Snapshot<Node<K, V, C>, C>; MAX_HEIGHT + 1],
    pred_offset: [usize; MAX_HEIGHT + 1],
    succs: [Snapshot<Node<K, V, C>, C>; MAX_HEIGHT],
    next: Snapshot<Node<K, V, C>, C>,
    new_node: Snapshot<Node<K, V, C>, C>,
    found_level: Option<usize>,

    /// `found_value` is not set by a traversal function.
    /// It must be manually set in Get/Remove function.
    found_value: Option<V>,
}

impl<K, V, C: Cs> OutputHolder<V> for Cursor<K, V, C> {
    fn default() -> Self {
        Self {
            preds: core::array::from_fn(|_| Default::default()),
            pred_offset: core::array::from_fn(|_| Default::default()),
            succs: Default::default(),
            next: Default::default(),
            new_node: Default::default(),
            found_level: None,
            found_value: None,
        }
    }

    fn output(&self) -> &V {
        self.found_value.as_ref().unwrap()
    }
}

impl<K, V, C: Cs> Cursor<K, V, C> {
    fn initialize(&mut self, head: &AtomicRc<Node<K, V, C>, C>, cs: &C) {
        self.preds[MAX_HEIGHT].load(head, cs);
        self.pred_offset.fill(0);
        for i in 0..MAX_HEIGHT + 1 {
            self.pred_offset[MAX_HEIGHT - i] = i;
        }
        for succ in &mut self.succs {
            succ.clear();
        }
        self.found_level = None;
    }

    fn pred(&self, level: usize) -> &Snapshot<Node<K, V, C>, C> {
        &self.preds[level + self.pred_offset[level]]
    }

    fn found(&self) -> &Snapshot<Node<K, V, C>, C> {
        &self.succs[self.found_level.unwrap()]
    }
}

pub struct SkipList<K, V, C: Cs> {
    head: AtomicRc<Node<K, V, C>, C>,
}

impl<K, V, C> SkipList<K, V, C>
where
    K: Ord + Clone + Default,
    V: Clone + Default,
    C: Cs,
{
    pub fn new() -> Self {
        Self {
            head: AtomicRc::new(Node::head(), unsafe { &C::without_epoch() }),
        }
    }

    pub fn find_optimistic(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        cursor.initialize(&self.head, cs);

        let mut level = MAX_HEIGHT;
        while level >= 1
            && unsafe { cursor.pred(level - 1).deref() }.next[level - 1]
                .load(Ordering::Relaxed)
                .is_null()
        {
            level -= 1;
        }

        while level >= 1 {
            level -= 1;
            cursor.pred_offset[level] = cursor.pred_offset[level + 1] + 1;
            cursor.succs[level].load(&unsafe { cursor.pred(level).deref() }.next[level], cs);

            loop {
                let curr_node = some_or!(cursor.succs[level].as_ref(), break);
                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        cursor.pred_offset[level] = 0;
                        Snapshot::swap(&mut cursor.preds[level], &mut cursor.succs[level]);
                    }
                    std::cmp::Ordering::Equal => {
                        let clean = curr_node.next[level].load(Ordering::Acquire).tag() == 0;
                        if clean {
                            cursor.found_level = Some(level);
                        }
                        return clean;
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
        }

        return false;
    }

    fn find(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        'search: loop {
            cursor.initialize(&self.head, cs);

            let mut level = MAX_HEIGHT;
            while level >= 1
                && unsafe { cursor.pred(level - 1).deref() }.next[level - 1]
                    .load(Ordering::Relaxed)
                    .is_null()
            {
                level -= 1;
            }

            while level >= 1 {
                level -= 1;
                cursor.pred_offset[level] = cursor.pred_offset[level + 1] + 1;
                cursor.succs[level].load(&unsafe { cursor.pred(level).deref() }.next[level], cs);

                // If the next node of `pred` is marked, that means `pred` is removed and we have
                // to restart the search.
                if cursor.succs[level].tag() == 1 {
                    continue 'search;
                }

                // Order of snapshots: pred[i] -> succ[i] -> next
                while let Some(succ_ref) = cursor.succs[level].as_ref() {
                    cursor.next.load(&succ_ref.next[level], cs);

                    if cursor.next.tag() == 1 {
                        if self.help_unlink(
                            &unsafe { cursor.pred(level).deref() }.next[level],
                            &cursor.succs[level],
                            &cursor.next,
                            cs,
                        ) {
                            Snapshot::swap(&mut cursor.succs[level], &mut cursor.next);
                            cursor.succs[level].set_tag(0);
                            continue;
                        } else {
                            // On failure, we cannot do anything reasonable to continue
                            // searching from the current position. Restart the search.
                            continue 'search;
                        }
                    }

                    // If `succ` contains a key that is greater than or equal to `key`, we're
                    // done with this level.
                    match succ_ref.key.cmp(key) {
                        std::cmp::Ordering::Greater => break,
                        std::cmp::Ordering::Equal => {
                            cursor.found_level = Some(level);
                            break;
                        }
                        std::cmp::Ordering::Less => {}
                    }

                    // Move one step forward.
                    cursor.pred_offset[level] = 0;
                    Snapshot::swap(&mut cursor.preds[level], &mut cursor.succs[level]);
                    Snapshot::swap(&mut cursor.succs[level], &mut cursor.next);
                }
            }

            return cursor.found_level.is_some();
        }
    }

    fn help_unlink(
        &self,
        pred: &AtomicRc<Node<K, V, C>, C>,
        succ: &Snapshot<Node<K, V, C>, C>,
        next: &Snapshot<Node<K, V, C>, C>,
        cs: &C,
    ) -> bool {
        pred.compare_exchange(
            succ.with_tag(0).as_ptr(),
            next.with_tag(0),
            Ordering::Release,
            Ordering::Relaxed,
            cs,
        )
        .is_ok()
    }

    pub fn insert(&self, key: K, value: V, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        if self.find(&key, cursor, cs) {
            return false;
        }

        let new_node = Rc::new(Node::new(key, value), cs);
        let new_node_ref = unsafe { new_node.deref() };
        let height = new_node_ref.height;
        cursor.new_node.protect(&new_node, cs);

        loop {
            new_node_ref.next[0].store(&cursor.succs[0], Ordering::Relaxed, cs);

            if unsafe { cursor.pred(0).deref() }.next[0]
                .compare_exchange(
                    cursor.succs[0].as_ptr(),
                    &cursor.new_node,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    cs,
                )
                .is_ok()
            {
                break;
            }

            // We failed. Let's search for the key and try again.
            if self.find(&new_node_ref.key, cursor, cs) {
                return false;
            }
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        'build: for level in 1..height {
            loop {
                let next = new_node_ref.next[level].load(Ordering::SeqCst);

                // If the current pointer is marked, that means another thread is already
                // removing the node we've just inserted. In that case, let's just stop
                // building the tower.
                if (next.tag() & 1) != 0 {
                    break 'build;
                }

                if new_node_ref.next[level]
                    .compare_exchange(
                        TaggedCnt::null().with_tag(2),
                        &cursor.succs[level],
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        cs,
                    )
                    .is_err()
                {
                    break 'build;
                }

                // Try installing the new node at the current level.
                if unsafe { cursor.pred(level).deref() }.next[level]
                    .compare_exchange(
                        cursor.succs[level].as_ptr(),
                        &cursor.new_node,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        cs,
                    )
                    .is_ok()
                {
                    // Success! Continue on the next level.
                    break;
                }

                // Installation failed.
                self.find(&new_node_ref.key, cursor, cs);
            }
        }

        true
    }

    pub fn remove(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        loop {
            let found = self.find(key, cursor, cs);
            if !found {
                return false;
            }
            let height = unsafe { cursor.found().deref() }.height;
            cursor.found_value = Some(unsafe { cursor.found().deref() }.value.clone());

            // Try removing the node by marking its tower.
            if unsafe { cursor.found().deref() }.mark_tower(cs) {
                for level in (0..height).rev() {
                    cursor
                        .next
                        .load(&unsafe { cursor.found().deref() }.next[level], cs);
                    if (cursor.next.tag() & 2) != 0 {
                        continue;
                    }
                    // Try linking the predecessor and successor at this level.
                    if unsafe { cursor.pred(level).deref() }.next[level]
                        .compare_exchange(
                            cursor.found().as_ptr(),
                            cursor.next.with_tag(0),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            cs,
                        )
                        .is_err()
                    {
                        self.find(key, cursor, cs);
                        break;
                    }
                }
            }
            return true;
        }
    }
}

impl<K, V, C> ConcurrentMap<K, V, C> for SkipList<K, V, C>
where
    K: Ord + Clone + Default,
    V: Clone + Default + Display,
    C: Cs,
{
    type Output = Cursor<K, V, C>;

    fn new() -> Self {
        SkipList::new()
    }

    fn get(&self, key: &K, output: &mut Self::Output, cs: &C) -> bool {
        let found = self.find(key, output, cs);
        if found {
            output.found_value = Some(unsafe { output.found().deref() }.value.clone());
        }
        found
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
    use super::SkipList;
    use crate::cdrc::concurrent_map;
    use cdrc_rs::{CsEBR, CsHP};

    #[test]
    fn smoke_skip_list_ebr() {
        concurrent_map::tests::smoke::<CsEBR, SkipList<i32, String, CsEBR>>();
    }

    #[test]
    fn smoke_skip_list_hp() {
        concurrent_map::tests::smoke::<CsHP, SkipList<i32, String, CsHP>>();
    }
}
