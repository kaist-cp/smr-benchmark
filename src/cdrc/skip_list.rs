use std::sync::atomic::Ordering;

use cdrc_rs::{AcquireRetire, AtomicRcPtr, RcPtr, SnapshotPtr};

use super::concurrent_map::ConcurrentMap;

const MAX_HEIGHT: usize = 32;

type Tower<K, V, Guard> = [AtomicRcPtr<Node<K, V, Guard>, Guard>; MAX_HEIGHT];

struct Node<K, V, Guard>
where
    Guard: AcquireRetire,
{
    key: K,
    value: V,
    next: Tower<K, V, Guard>,
    height: usize,
}

impl<K, V, Guard> Node<K, V, Guard>
where
    K: Default,
    V: Default,
    Guard: AcquireRetire,
{
    pub fn new(key: K, value: V) -> Self {
        let height = Self::generate_height();
        Self {
            key,
            value,
            next: Default::default(),
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

    pub fn mark_tower(&self, guard: &Guard) -> bool {
        for level in (0..self.height).rev() {
            let tag = self.next[level].fetch_or(1, guard).mark();
            // If the level 0 pointer was already marked, somebody else removed the node.
            if level == 0 && tag == 1 {
                return false;
            }
        }
        true
    }
}

struct Cursor<'g, K, V, Guard>
where
    Guard: AcquireRetire,
{
    found: Option<SnapshotPtr<'g, Node<K, V, Guard>, Guard>>,
    preds: [SnapshotPtr<'g, Node<K, V, Guard>, Guard>; MAX_HEIGHT],
    succs: [SnapshotPtr<'g, Node<K, V, Guard>, Guard>; MAX_HEIGHT],
}

impl<'g, K, V, Guard> Cursor<'g, K, V, Guard>
where
    Guard: AcquireRetire,
{
    fn new(head: &SnapshotPtr<'g, Node<K, V, Guard>, Guard>, guard: &'g Guard) -> Self {
        Self {
            found: None,
            preds: [(); MAX_HEIGHT].map(|_| head.clone(guard)),
            succs: [(); MAX_HEIGHT].map(|_| SnapshotPtr::null(guard)),
        }
    }
}

pub struct SkipList<K, V, Guard>
where
    Guard: AcquireRetire,
{
    head: AtomicRcPtr<Node<K, V, Guard>, Guard>,
}

impl<K, V, Guard> SkipList<K, V, Guard>
where
    K: Ord + Clone + Default,
    V: Clone + Default,
    Guard: AcquireRetire,
{
    pub fn new() -> Self {
        Self {
            head: AtomicRcPtr::new(Node::head(), unsafe { &Guard::unprotected() }),
        }
    }

    fn find_optimistic<'g>(&'g self, key: &K, guard: &'g Guard) -> Cursor<'g, K, V, Guard> {
        let head = self.head.load_snapshot(guard);
        let mut cursor = Cursor::new(&head, guard);

        let mut level = MAX_HEIGHT;
        while level >= 1
            && unsafe { head.deref() }.next[level - 1]
                .load_snapshot(guard)
                .is_null()
        {
            level -= 1;
        }

        let mut pred = head;
        while level >= 1 {
            level -= 1;
            let mut curr = unsafe { pred.deref() }.next[level].load_snapshot(guard);

            loop {
                let curr_node = some_or!(unsafe { curr.as_ref() }, break);
                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        core::mem::swap(&mut curr, &mut pred);
                        curr = curr_node.next[level].load_snapshot(guard);
                    }
                    std::cmp::Ordering::Equal => {
                        if curr_node.next[level].load_snapshot(guard).mark() == 0 {
                            cursor.found = Some(curr)
                        }
                        return cursor;
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
        }

        cursor
    }

    fn find<'g>(&'g self, key: &K, guard: &'g Guard) -> Cursor<'g, K, V, Guard> {
        'search: loop {
            let head = self.head.load_snapshot(guard);
            let mut cursor = Cursor::new(&head, guard);

            let mut level = MAX_HEIGHT;
            while level >= 1
                && unsafe { head.deref() }.next[level - 1]
                    .load_snapshot(guard)
                    .is_null()
            {
                level -= 1;
            }

            let mut pred = head;
            while level >= 1 {
                level -= 1;
                let mut curr = unsafe { pred.deref() }.next[level].load_snapshot(guard);
                // If `curr` is marked, that means `pred` is removed and we have to restart the
                // search.
                if curr.mark() == 1 {
                    continue 'search;
                }

                while let Some(curr_ref) = unsafe { curr.as_ref() } {
                    let succ = curr_ref.next[level].load_snapshot(guard);

                    if succ.mark() == 1 {
                        if self.help_unlink(
                            &unsafe { pred.deref() }.next[level],
                            &curr,
                            &succ,
                            guard,
                        ) {
                            curr = succ.with_mark(0);
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
                        std::cmp::Ordering::Greater => break,
                        std::cmp::Ordering::Equal => {
                            cursor.found = Some(curr.clone(guard));
                            break;
                        }
                        std::cmp::Ordering::Less => {}
                    }

                    // Move one step forward.
                    pred = curr;
                    curr = succ;
                }

                cursor.preds[level] = pred.clone(guard);
                cursor.succs[level] = curr.clone(guard);
            }

            return cursor;
        }
    }

    fn help_unlink<'g>(
        &'g self,
        pred: &'g AtomicRcPtr<Node<K, V, Guard>, Guard>,
        curr: &SnapshotPtr<'g, Node<K, V, Guard>, Guard>,
        succ: &SnapshotPtr<'g, Node<K, V, Guard>, Guard>,
        guard: &'g Guard,
    ) -> bool {
        pred.compare_exchange_ss_ss(
            &curr.clone(guard).with_mark(0),
            &succ.clone(guard).with_mark(0),
            guard,
        )
        .is_ok()
    }

    pub fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        let mut cursor = self.find(&key, guard);
        if cursor.found.is_some() {
            return false;
        }

        let new_node = RcPtr::make_shared(Node::new(key, value), guard);
        let new_node_ref = unsafe { new_node.deref() };
        let height = new_node_ref.height;

        loop {
            new_node_ref.next[0].store_snapshot(
                cursor.succs[0].clone(guard),
                Ordering::Relaxed,
                guard,
            );

            if unsafe { cursor.preds[0].deref() }.next[0]
                .compare_exchange_ss_rc(&cursor.succs[0], &new_node, guard)
                .is_ok()
            {
                break;
            }

            // We failed. Let's search for the key and try again.
            cursor = self.find(&new_node_ref.key, guard);
            if cursor.found.is_some() {
                return false;
            }
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        'build: for level in 1..height {
            loop {
                let pred = cursor.preds[level].clone(guard);
                let succ = cursor.succs[level].clone(guard);
                let next = new_node_ref.next[level].load_snapshot(guard);

                // If the current pointer is marked, that means another thread is already
                // removing the node we've just inserted. In that case, let's just stop
                // building the tower.
                if next.mark() == 1 {
                    break 'build;
                }

                if unsafe { succ.as_ref() }.map(|node| &node.key) == Some(&new_node_ref.key) {
                    cursor = self.find(&new_node_ref.key, guard);
                    continue;
                }

                if new_node_ref.next[level]
                    .compare_exchange_ss_ss(&next, &succ, guard)
                    .is_err()
                {
                    break 'build;
                }

                // Try installing the new node at the current level.
                if unsafe { pred.deref() }.next[level]
                    .compare_exchange_ss_rc(&succ, &new_node, guard)
                    .is_ok()
                {
                    // Success! Continue on the next level.
                    break;
                }

                // Installation failed.
                cursor = self.find(&new_node_ref.key, guard);
            }
        }

        if new_node_ref.next[height - 1].load_snapshot(guard).mark() == 1 {
            self.find(&new_node_ref.key, guard);
        }
        true
    }

    pub fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        loop {
            let cursor = self.find(key, guard);
            let node = cursor.found?;
            let node_ref = unsafe { node.deref() };

            // Try removing the node by marking its tower.
            if node_ref.mark_tower(guard) {
                for level in (0..node_ref.height).rev() {
                    let succ = node_ref.next[level].load_snapshot(guard);

                    // Try linking the predecessor and successor at this level.
                    if unsafe { cursor.preds[level].deref() }.next[level]
                        .compare_exchange_ss_ss(&node, &succ.with_mark(0), guard)
                        .is_err()
                    {
                        self.find(key, guard);
                        break;
                    }
                }
            }
            return Some(&unsafe { node.deref() }.value);
        }
    }
}

impl<K, V, Guard> ConcurrentMap<K, V, Guard> for SkipList<K, V, Guard>
where
    K: Ord + Clone + Default,
    V: Clone + Default,
    Guard: AcquireRetire,
{
    fn new() -> Self {
        SkipList::new()
    }

    #[inline(never)]
    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        let cursor = self.find_optimistic(key, guard);
        cursor.found.map(|node| &unsafe { node.deref() }.value)
    }

    #[inline(never)]
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.insert(key, value, guard)
    }

    #[inline(never)]
    fn remove<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        self.remove(key, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::SkipList;
    use crate::cdrc::concurrent_map;
    use cdrc_rs::GuardEBR;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<GuardEBR, SkipList<i32, String, GuardEBR>>();
    }
}
