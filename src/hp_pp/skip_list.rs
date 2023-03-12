use std::mem::transmute;
use std::sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering};
use std::{ptr, slice};

use hp_pp::{
    light_membarrier, tag, tagged, untagged, HazardPointer, ProtectError, Thread, DEFAULT_DOMAIN,
};

use crate::hp::concurrent_map::ConcurrentMap;

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
            next: Default::default(),
            key,
            value,
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

    pub fn decrement<F>(&self, on_zero: F)
    where
        F: FnOnce(*mut Self),
    {
        if self.refs.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);
            on_zero(self as *const _ as _);
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

    #[inline]
    pub fn protect_next<'domain>(
        &self,
        index: usize,
        hazptr: &mut HazardPointer<'domain>,
    ) -> Result<*mut Node<K, V>, ()> {
        let mut next = self.next[index].load(Ordering::Relaxed);
        loop {
            match hazptr.try_protect_pp(untagged(next), &self, &self.next[index], &|src| {
                (tag(src.next[index].load(Ordering::Acquire)) & 1) != 0
            }) {
                Ok(_) => return Ok(next),
                Err(ProtectError::Changed(new_next)) => {
                    next = new_next;
                    continue;
                }
                Err(ProtectError::Stopped) => return Err(()),
            }
        }
    }
}

pub struct Handle<'g> {
    preds_h: [HazardPointer<'g>; MAX_HEIGHT],
    succs_h: [HazardPointer<'g>; MAX_HEIGHT],
    new_node_h: HazardPointer<'g>,
    thread: Thread<'g>,
}

impl Default for Handle<'static> {
    fn default() -> Self {
        Self {
            preds_h: Default::default(),
            succs_h: Default::default(),
            new_node_h: Default::default(),
            thread: Thread::new(&DEFAULT_DOMAIN),
        }
    }
}

struct Cursor<K, V> {
    preds: [*mut Node<K, V>; MAX_HEIGHT],
    succs: [*mut Node<K, V>; MAX_HEIGHT],
}

impl<K, V> Cursor<K, V>
where
    K: Ord,
{
    fn new(head: &Tower<K, V>) -> Self {
        Self {
            preds: [head as *const _ as *mut _; MAX_HEIGHT],
            succs: [ptr::null_mut(); MAX_HEIGHT],
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

    fn find<'domain, 'hp>(&self, key: &K, handle: &'hp mut Handle<'domain>) -> Cursor<K, V> {
        'search: loop {
            let mut cursor = Cursor::new(&self.head);

            let mut level = MAX_HEIGHT;
            while level >= 1 && self.head[level - 1].load(Ordering::Relaxed).is_null() {
                level -= 1;
            }

            let mut pred = &self.head as *const _ as *mut Node<K, V>;
            let mut curr;

            while level >= 1 {
                level -= 1;
                handle.preds_h[level].protect_raw(pred);
                light_membarrier();
                loop {
                    let pred_ref = unsafe { &*untagged(pred) };
                    curr = ok_or!(
                        pred_ref.protect_next(level, &mut handle.succs_h[level]),
                        continue 'search
                    );

                    if untagged(curr).is_null() {
                        break;
                    }

                    let curr_ref = unsafe { &*untagged(curr) };
                    let succ = curr_ref.next[level].load(Ordering::Acquire);

                    if pred_ref.next[level].load(Ordering::Acquire) != curr {
                        continue 'search;
                    }

                    if tag(succ) == 1 {
                        unsafe {
                            handle.thread.try_unlink(
                                PhysicalUnlink {
                                    pred: &pred_ref.next[level],
                                    curr,
                                    succ,
                                },
                                slice::from_ref(&untagged(succ)),
                            );
                        }
                        continue 'search;
                    }

                    match curr_ref.key.cmp(key) {
                        std::cmp::Ordering::Less => {
                            pred = curr;
                            HazardPointer::swap(
                                &mut handle.preds_h[level],
                                &mut handle.succs_h[level],
                            );
                        }
                        _ => break,
                    }
                }

                cursor.preds[level] = pred;
                cursor.succs[level] = curr;
            }

            return cursor;
        }
    }

    pub fn insert<'domain, 'hp>(&self, key: K, value: V, handle: &'hp mut Handle<'domain>) -> bool {
        let mut cursor = self.find(&key, handle);
        if cursor.found(&key).is_some() {
            return false;
        }

        // The reference count is initially two to account for
        // 1. The link at the level 0 of the tower.
        // 2. The current reference in this function.
        let new_node = Box::into_raw(Box::new(Node::new(key, value, 2)));
        let new_node_ref = unsafe { &*new_node };
        let height = new_node_ref.height;
        handle.new_node_h.protect_raw(new_node);
        light_membarrier();

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
            cursor = self.find(&new_node_ref.key, handle);
            if cursor.found(&new_node_ref.key).is_some() {
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
                    cursor = self.find(&new_node_ref.key, handle);
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
                cursor = self.find(&new_node_ref.key, handle);
            }
        }

        if tag(new_node_ref.next[height - 1].load(Ordering::SeqCst)) == 1 {
            self.find(&new_node_ref.key, handle);
        }

        new_node_ref.decrement(|ptr| unsafe { handle.thread.retire(ptr) });
        true
    }

    pub fn remove<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V> {
        loop {
            let cursor = self.find(key, handle);
            let node = cursor.found(key)?;

            // Try removing the node by marking its tower.
            if node.mark_tower() {
                for level in (0..node.height).rev() {
                    let succ = node.next[level].load(Ordering::SeqCst);

                    // Try linking the predecessor and successor at this level.
                    if !unsafe {
                        handle.thread.try_unlink(
                            PhysicalUnlink {
                                pred: &(*cursor.preds[level]).next[level],
                                curr: node as *const _ as _,
                                succ: tagged(succ, 0),
                            },
                            slice::from_ref(&untagged(succ)),
                        )
                    } {
                        self.find(key, handle);
                        break;
                    }
                }
            }
            return Some(unsafe { transmute::<&V, &'hp V>(&node.value) });
        }
    }
}

impl<K, V> hp_pp::Invalidate for Node<K, V> {
    fn invalidate(&self) {
        // There's nothing to do here.
        // Before logically remove a node,
        // the entire tower of the node is marked,
        // so additional invalidation is not needed.
    }
}

struct PhysicalUnlink<'g, K, V> {
    pred: &'g AtomicPtr<Node<K, V>>,
    curr: *mut Node<K, V>,
    succ: *mut Node<K, V>,
}

impl<'g, K, V> hp_pp::Unlink<Node<K, V>> for PhysicalUnlink<'g, K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn do_unlink(&self) -> Result<Vec<*mut Node<K, V>>, ()> {
        if self
            .pred
            .compare_exchange(
                tagged(self.curr, 0),
                tagged(self.succ, 0),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            let curr = unsafe { &*untagged(self.curr) };
            let mut became_zero = false;
            curr.decrement(|_| became_zero = true);
            Ok(if became_zero {
                vec![untagged(self.curr)]
            } else {
                vec![]
            })
        } else {
            Err(())
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for SkipList<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    type Handle<'domain> = Handle<'domain>;

    fn new() -> Self {
        SkipList::new()
    }

    fn handle() -> Self::Handle<'static> {
        Handle::default()
    }

    #[inline]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        let cursor = self.find(key, handle);
        let node = unsafe { &*cursor.succs[0] };
        if node.key.eq(&key) {
            Some(&node.value)
        } else {
            None
        }
    }

    #[inline]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.insert(key, value, handle)
    }

    #[inline]
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
    use super::SkipList;
    use crate::hp::concurrent_map;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<SkipList<i32, String>>();
    }
}
