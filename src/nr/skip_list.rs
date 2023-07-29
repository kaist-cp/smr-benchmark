use std::{
    ptr::null_mut,
    sync::atomic::{AtomicPtr, Ordering},
};

use super::{concurrent_map::ConcurrentMap, tag, untagged};

const MAX_HEIGHT: usize = 32;

type Tower<K, V> = [AtomicPtr<Node<K, V>>; MAX_HEIGHT];

struct Node<K, V> {
    key: K,
    value: V,
    next: Tower<K, V>,
    height: usize,
}

impl<K, V> Node<K, V> {
    pub fn new(key: K, value: V) -> Self {
        let height = Self::generate_height();
        Self {
            key,
            value,
            next: Default::default(),
            height,
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

    pub fn mark_tower(&self) -> bool {
        for level in (0..self.height).rev() {
            // We're loading the pointer only for the tag, so it's okay to use
            // `epoch::unprotected()` in this situation.
            let tag = tag(self.next[level].fetch_or(1, Ordering::SeqCst));
            // If the level 0 pointer was already marked, somebody else removed the node.
            if level == 0 && tag == 1 {
                return false;
            }
        }
        true
    }
}

struct Cursor<'g, K, V> {
    found: Option<&'g Node<K, V>>,
    preds: [&'g Tower<K, V>; MAX_HEIGHT],
    succs: [*mut Node<K, V>; MAX_HEIGHT],
}

impl<'g, K, V> Cursor<'g, K, V> {
    fn new(head: &'g Tower<K, V>) -> Self {
        Self {
            found: None,
            preds: [head; MAX_HEIGHT],
            succs: [null_mut(); MAX_HEIGHT],
        }
    }
}

pub struct SkipList<K, V> {
    head: Tower<K, V>,
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

    fn find_optimistic<'g>(&'g self, key: &K) -> Cursor<'g, K, V> {
        let mut cursor = Cursor::new(&self.head);

        let mut level = MAX_HEIGHT;
        while level >= 1 && untagged(self.head[level - 1].load(Ordering::Relaxed)).is_null() {
            level -= 1;
        }

        let mut pred = &self.head;
        while level >= 1 {
            level -= 1;
            let mut curr = pred[level].load(Ordering::Acquire);

            loop {
                let curr_node = some_or!(unsafe { untagged(curr).as_ref() }, break);
                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        pred = &curr_node.next;
                        curr = curr_node.next[level].load(Ordering::Acquire);
                    }
                    std::cmp::Ordering::Equal => {
                        if tag(curr_node.next[level].load(Ordering::Acquire)) == 0 {
                            cursor.found = Some(curr_node)
                        }
                        return cursor;
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
        }

        cursor
    }

    fn find<'g>(&'g self, key: &K) -> Cursor<'g, K, V> {
        'search: loop {
            let mut cursor = Cursor::new(&self.head);

            let mut level = MAX_HEIGHT;
            while level >= 1 && self.head[level - 1].load(Ordering::Relaxed).is_null() {
                level -= 1;
            }

            let mut pred = &self.head;
            while level >= 1 {
                level -= 1;
                let mut curr = pred[level].load(Ordering::Acquire);
                // If `curr` is marked, that means `pred` is removed and we have to restart the
                // search.
                if tag(curr) == 1 {
                    continue 'search;
                }

                while let Some(curr_ref) = unsafe { curr.as_ref() } {
                    let succ = curr_ref.next[level].load(Ordering::Acquire);

                    if tag(succ) == 1 {
                        if self.help_unlink(&pred[level], curr, succ) {
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
                            cursor.found = Some(curr_ref);
                            break;
                        }
                        std::cmp::Ordering::Less => {}
                    }

                    // Move one step forward.
                    pred = &curr_ref.next;
                    curr = succ;
                }

                cursor.preds[level] = pred;
                cursor.succs[level] = curr;
            }

            return cursor;
        }
    }

    fn help_unlink<'g>(
        &'g self,
        pred: &'g AtomicPtr<Node<K, V>>,
        curr: *mut Node<K, V>,
        succ: *mut Node<K, V>,
    ) -> bool {
        pred.compare_exchange(
            untagged(curr),
            untagged(succ),
            Ordering::Release,
            Ordering::Relaxed,
        )
        .is_ok()
    }

    pub fn insert(&self, key: K, value: V) -> bool {
        let mut cursor = self.find(&key);
        if cursor.found.is_some() {
            return false;
        }

        // The reference count is initially two to account for
        // 1. The link at the level 0 of the tower.
        // 2. The current reference in this function.
        let new_node = Box::into_raw(Box::new(Node::new(key, value)));
        let new_node_ref = unsafe { &*new_node };
        let height = new_node_ref.height;

        loop {
            new_node_ref.next[0].store(cursor.succs[0], Ordering::Relaxed);

            if cursor.preds[0][0]
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
            cursor = self.find(&new_node_ref.key);
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

                if unsafe { succ.as_ref() }.map(|node| &node.key) == Some(&new_node_ref.key) {
                    cursor = self.find(&new_node_ref.key);
                    continue;
                }

                if new_node_ref.next[level]
                    .compare_exchange(next, succ, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
                {
                    break 'build;
                }

                // Try installing the new node at the current level.
                if pred[level]
                    .compare_exchange(succ, new_node, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    // Success! Continue on the next level.
                    break;
                }

                // Installation failed.
                cursor = self.find(&new_node_ref.key);
            }
        }

        if tag(new_node_ref.next[height - 1].load(Ordering::SeqCst)) == 1 {
            self.find(&new_node_ref.key);
        }
        true
    }

    pub fn remove<'g>(&'g self, key: &K) -> Option<&'g V> {
        loop {
            let cursor = self.find(key);
            let node = cursor.found?;

            // Try removing the node by marking its tower.
            if node.mark_tower() {
                for level in (0..node.height).rev() {
                    let succ = node.next[level].load(Ordering::SeqCst);

                    // Try linking the predecessor and successor at this level.
                    if cursor.preds[level][level]
                        .compare_exchange(
                            node as *const _ as *mut Node<K, V>,
                            untagged(succ),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_err()
                    {
                        self.find(key);
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

    #[inline(never)]
    fn get<'g>(&'g self, key: &'g K) -> Option<&'g V> {
        let cursor = self.find_optimistic(key);
        cursor.found.map(|node| &node.value)
    }

    #[inline(never)]
    fn insert(&self, key: K, value: V) -> bool {
        self.insert(key, value)
    }

    #[inline(never)]
    fn remove<'g>(&'g self, key: &'g K) -> Option<&'g V> {
        self.remove(key)
    }
}

#[cfg(test)]
mod tests {
    use super::SkipList;
    use crate::nr::concurrent_map;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<SkipList<i32, String>>();
    }
}
