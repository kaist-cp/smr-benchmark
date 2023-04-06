use std::sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering};

use hp_pp::{tag, untagged};
use nbr_rs::{read_phase, Guard};
use std::ptr;

use super::concurrent_map::ConcurrentMap;

const MAX_HEIGHT: usize = 32;

type Tower<K, V> = [AtomicPtr<Node<K, V>>; MAX_HEIGHT];

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
            unsafe { guard.retire(self as *const _ as *mut Node<K, V>) };
        }
    }

    pub fn mark_tower(&self) -> bool {
        for level in (0..self.height).rev() {
            let tag = tag(self.next[level].fetch_or(1, Ordering::SeqCst));
            // If the level 0 pointer was already marked, somebody else removed the node.
            if level == 0 && tag == 1 {
                return false;
            }
        }
        true
    }
}

struct Cursor<K, V> {
    found: Option<*mut Node<K, V>>,
    preds: [*mut Node<K, V>; MAX_HEIGHT],
    succs: [*mut Node<K, V>; MAX_HEIGHT],
}

impl<K, V> Cursor<K, V> {
    fn new(head: &Tower<K, V>) -> Self {
        Self {
            found: None,
            preds: [head as *const _ as *mut _; MAX_HEIGHT],
            succs: [ptr::null_mut(); MAX_HEIGHT],
        }
    }
}

pub struct SkipList<K, V> {
    head: Tower<K, V>,
}

impl<K, V> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        let mut node = self.head[0].load(Ordering::Relaxed);

        while let Some(node_ref) = unsafe { untagged(node).as_ref() } {
            let next = node_ref.next[0].load(Ordering::Relaxed);
            drop(unsafe { Box::from_raw(node) });
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

    fn find<'g>(&'g self, key: &K, guard: &'g Guard) -> Cursor<K, V> {
        let mut cursor;
        read_phase!(guard => {
            'search: loop {
                cursor = Cursor::new(&self.head);

                let mut level = MAX_HEIGHT;
                while level >= 1 && self.head[level - 1].load(Ordering::Relaxed).is_null() {
                    level -= 1;
                }

                let mut pred = &self.head as *const _ as *mut Node<K, V>;
                while level >= 1 {
                    level -= 1;
                    let mut curr = unsafe { &*pred }.next[level].load(Ordering::Acquire);
                    // If `curr` is marked, that means `pred` is removed and we have to restart the
                    // search.
                    if tag(curr) == 1 {
                        continue 'search;
                    }

                    while let Some(curr_ref) = unsafe { curr.as_ref() } {
                        let succ = curr_ref.next[level].load(Ordering::Acquire);
                        let pred_next = &unsafe { &*pred }.next;

                        if tag(succ) == 1 {
                            if self.help_unlink(&pred_next[level], curr, succ, guard) {
                                curr = untagged(succ);
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
                                cursor.found = Some(curr);
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

                break;
            }

            // Manually protect all pointers before exiting `read_phase`.
            if let Some(found) = cursor.found {
                guard.protect(found);
            }
            for i in 0..MAX_HEIGHT {
                guard.protect(cursor.preds[i]);
                guard.protect(cursor.succs[i]);
            }
        });

        return cursor;
    }

    fn help_unlink(
        &self,
        pred: &AtomicPtr<Node<K, V>>,
        curr: *mut Node<K, V>,
        succ: *mut Node<K, V>,
        guard: &Guard,
    ) -> bool {
        let success = pred
            .compare_exchange(
                untagged(curr),
                untagged(succ),
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok();

        if success {
            unsafe { (*untagged(curr)).decrement(guard) };
        }
        success
    }

    pub fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        let mut cursor = self.find(&key, guard);
        if cursor.found.is_some() {
            return false;
        }

        // The reference count is initially two to account for
        // 1. The link at the level 0 of the tower.
        // 2. The current reference in this function.
        let new_node = Box::into_raw(Box::new(Node::new(key, value, 2)));
        let new_node_ref = unsafe { &*new_node };
        let height = new_node_ref.height;

        loop {
            new_node_ref.next[0].store(cursor.succs[0], Ordering::Relaxed);

            if unsafe { &*cursor.preds[0] }.next[0]
                .compare_exchange(
                    cursor.succs[0],
                    new_node,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                break;
            }

            // We failed. Let's search for the key and try again.
            cursor = self.find(&new_node_ref.key, guard);
            if cursor.found.is_some() {
                drop(unsafe { Box::from_raw(new_node) });
                return false;
            }
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        'build: for level in 1..height {
            loop {
                let pred = cursor.preds[level];
                let succ = cursor.succs[level];
                let next = new_node_ref.next[level].load(Ordering::SeqCst);

                // If the current pointer is marked, that means another thread is already
                // removing the node we've just inserted. In that case, let's just stop
                // building the tower.
                if tag(next) == 1 {
                    break 'build;
                }

                if unsafe { untagged(succ).as_ref() }.map(|node| &node.key)
                    == Some(&new_node_ref.key)
                {
                    cursor = self.find(&new_node_ref.key, guard);
                    continue;
                }

                if new_node_ref.next[level]
                    .compare_exchange(next, succ, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
                {
                    break 'build;
                }

                new_node_ref.refs.fetch_add(1, Ordering::Relaxed);
                // Try installing the new node at the current level.
                if unsafe { &*pred }.next[level]
                    .compare_exchange(succ, new_node, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    // Success! Continue on the next level.
                    break;
                }

                // Installation failed.
                new_node_ref.refs.fetch_sub(1, Ordering::Relaxed);
                cursor = self.find(&new_node_ref.key, guard);
            }
        }

        if tag(new_node_ref.next[height - 1].load(Ordering::SeqCst)) == 1 {
            self.find(&new_node_ref.key, guard);
        }

        new_node_ref.decrement(guard);
        true
    }

    pub fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        loop {
            let cursor = self.find(key, guard);
            let node = cursor.found.map(|node| unsafe { &*node })?;

            // Try removing the node by marking its tower.
            if node.mark_tower() {
                for level in (0..node.height).rev() {
                    let succ = node.next[level].load(Ordering::SeqCst);

                    // Try linking the predecessor and successor at this level.
                    if unsafe { &*cursor.preds[level] }.next[level]
                        .compare_exchange(
                            node as *const _ as *mut _,
                            untagged(succ),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        node.decrement(guard);
                    } else {
                        self.find(key, guard);
                        break;
                    }
                }
            }
            return Some(&node.value);
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for SkipList<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new() -> Self {
        SkipList::new()
    }

    fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        let cursor = self.find(key, guard);
        cursor.found.map(|node| &unsafe { &*node }.value)
    }

    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.insert(key, value, guard)
    }

    fn remove<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        self.remove(key, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::{SkipList, MAX_HEIGHT};
    use crate::nbr::concurrent_map;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<SkipList<i32, String>>(MAX_HEIGHT * 2 + 1);
    }
}
