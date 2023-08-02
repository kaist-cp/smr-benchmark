use std::mem::transmute;
use std::ptr;
use std::sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering};

use hp_pp::{light_membarrier, tagged, Thread};
use hp_pp::{tag, untagged, HazardPointer, DEFAULT_DOMAIN};

use super::concurrent_map::ConcurrentMap;

const MAX_HEIGHT: usize = 32;

type Tower<K, V> = [AtomicPtr<Node<K, V>>; MAX_HEIGHT];

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
        let next: [AtomicPtr<Node<K, V>>; MAX_HEIGHT] = Default::default();
        for link in next.iter().take(height) {
            link.store(tagged(ptr::null_mut(), 2), Ordering::Relaxed);
        }
        Self {
            next,
            key,
            value,
            height,
            refs: AtomicUsize::new(height + 1),
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

    pub fn decrement(&self, handle: &mut Handle) {
        if self.refs.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);
            unsafe { handle.thread.retire(self as *const _ as *mut Node<K, V>) };
        }
    }

    pub fn mark_tower(&self) -> bool {
        for level in (0..self.height).rev() {
            let tag = tag(self.next[level].fetch_or(1, Ordering::SeqCst));
            // If the level 0 pointer was already marked, somebody else removed the node.
            if level == 0 && (tag & 1) != 0 {
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
    ) -> *mut Node<K, V> {
        let mut next = self.next[index].load(Ordering::Relaxed);
        loop {
            hazptr.protect_raw(untagged(next));
            light_membarrier();
            let new_next = self.next[index].load(Ordering::Acquire);
            if next == new_next {
                break;
            }
            next = new_next;
        }
        next
    }
}

pub struct Handle<'g> {
    preds_h: [HazardPointer<'g>; MAX_HEIGHT],
    succs_h: [HazardPointer<'g>; MAX_HEIGHT],
    removed_h: HazardPointer<'g>,
    thread: Thread<'g>,
}

impl Default for Handle<'static> {
    fn default() -> Self {
        Self {
            preds_h: Default::default(),
            succs_h: Default::default(),
            removed_h: Default::default(),
            thread: Thread::new(&DEFAULT_DOMAIN),
        }
    }
}

struct Cursor<K, V> {
    found: Option<*mut Node<K, V>>,
    preds: [*mut Node<K, V>; MAX_HEIGHT],
    succs: [*mut Node<K, V>; MAX_HEIGHT],
}

impl<K, V> Cursor<K, V>
where
    K: Ord,
{
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
                    curr = pred_ref.protect_next(level, &mut handle.succs_h[level]);
                    if tag(curr) == 1 {
                        continue 'search;
                    }
                    if untagged(curr).is_null() {
                        break;
                    }

                    let curr_ref = unsafe { &*untagged(curr) };
                    let succ = curr_ref.next[level].load(Ordering::Acquire);

                    if pred_ref.next[level].load(Ordering::Acquire) != curr {
                        continue 'search;
                    }

                    if tag(succ) == 1 {
                        self.help_unlink(&pred_ref.next[level], curr, succ, handle);
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
                        std::cmp::Ordering::Equal => {
                            cursor.found = Some(curr);
                            break;
                        }
                        std::cmp::Ordering::Greater => break,
                    }
                }

                cursor.preds[level] = pred;
                cursor.succs[level] = curr;
            }

            return cursor;
        }
    }

    fn help_unlink<'domain, 'hp>(
        &self,
        pred: &AtomicPtr<Node<K, V>>,
        curr: *mut Node<K, V>,
        succ: *mut Node<K, V>,
        handle: &'hp mut Handle<'domain>,
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
            unsafe { (&*untagged(curr)).decrement(handle) };
        }
        success
    }

    pub fn insert<'domain, 'hp>(&self, key: K, value: V, handle: &'hp mut Handle<'domain>) -> bool {
        let mut cursor = self.find(&key, handle);
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
                if (tag(next) & 1) != 0 {
                    new_node_ref
                        .refs
                        .fetch_sub(height - level, Ordering::SeqCst);
                    break 'build;
                }

                if new_node_ref.next[level]
                    .compare_exchange(
                        tagged(ptr::null_mut(), 2),
                        succ,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_err()
                {
                    new_node_ref
                        .refs
                        .fetch_sub(height - level, Ordering::SeqCst);
                    break 'build;
                }

                // Try installing the new node at the current level.
                if unsafe { &*pred }.next[level]
                    .compare_exchange(succ, new_node, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    // Success! Continue on the next level.
                    break;
                }

                // Installation failed.
                cursor = self.find(&new_node_ref.key, handle);
            }
        }

        new_node_ref.decrement(handle);
        true
    }

    pub fn remove<'domain, 'hp>(
        &self,
        key: &K,
        handle: &'hp mut Handle<'domain>,
    ) -> Option<&'hp V> {
        loop {
            let cursor = self.find(key, handle);
            let node_ptr = cursor.found?;
            let node = unsafe { &*node_ptr };
            handle
                .removed_h
                .protect_raw(node as *const _ as *mut Node<K, V>);
            light_membarrier();

            // Try removing the node by marking its tower.
            if node.mark_tower() {
                for level in (0..node.height).rev() {
                    let succ = node.next[level].load(Ordering::SeqCst);
                    if (tag(succ) & 2) != 0 {
                        continue;
                    }

                    // Try linking the predecessor and successor at this level.
                    if unsafe { &*cursor.preds[level] }.next[level]
                        .compare_exchange(
                            node as *const _ as _,
                            untagged(succ),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        node.decrement(handle);
                    } else {
                        self.find(key, handle);
                        break;
                    }
                }
            }
            return Some(unsafe { transmute::<&V, &'hp V>(&node.value) });
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

    #[inline(always)]
    fn get<'domain, 'hp>(&self, handle: &'hp mut Self::Handle<'domain>, key: &K) -> Option<&'hp V> {
        let cursor = self.find(key, handle);
        let node = unsafe { cursor.found?.as_ref()? };
        if node.key.eq(&key) {
            Some(unsafe { transmute(&node.value) })
        } else {
            None
        }
    }

    #[inline(always)]
    fn insert<'domain, 'hp>(
        &self,
        handle: &'hp mut Self::Handle<'domain>,
        key: K,
        value: V,
    ) -> bool {
        self.insert(key, value, handle)
    }

    #[inline(always)]
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
