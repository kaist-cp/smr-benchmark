use std::{fmt::Display, mem::forget, sync::atomic::Ordering};

use circ::{AtomicRc, Cs, CsEBR, GraphNode, Pointer, Rc, Snapshot, StrongPtr};

use super::concurrent_map::{ConcurrentMap, OutputHolder};

const MAX_HEIGHT: usize = 32;

type Tower<K, V> = [AtomicRc<Node<K, V>, CsEBR>; MAX_HEIGHT];

pub struct Node<K, V> {
    key: K,
    value: V,
    next: Tower<K, V>,
    height: usize,
}

impl<K, V> GraphNode<CsEBR> for Node<K, V> {
    const UNIQUE_OUTDEGREE: bool = false;

    #[inline]
    fn pop_outgoings(&self, result: &mut Vec<Rc<Self, CsEBR>>, cs: &CsEBR)
    where
        Self: Sized,
    {
        result.extend(self.next.iter().filter_map(|next| {
            if next.load(Ordering::Acquire).is_null() {
                None
            } else {
                Some(next.swap(Rc::null(), Ordering::Relaxed, cs))
            }
        }));
    }

    #[inline]
    fn pop_unique(&self, _: &CsEBR) -> Rc<Self, CsEBR>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

impl<K, V> Node<K, V>
where
    K: Default,
    V: Default,
{
    pub fn new(key: K, value: V) -> Self {
        let cs = unsafe { &CsEBR::unprotected() };
        let height = Self::generate_height();
        let next: [AtomicRc<Node<K, V>, CsEBR>; MAX_HEIGHT] = Default::default();
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

    pub fn mark_tower(&self, cs: &CsEBR) -> bool {
        for level in (0..self.height).rev() {
            loop {
                let aux = self.next[level].load_ss(cs);
                if aux.tag() & 1 != 0 {
                    if level == 0 {
                        return false;
                    }
                    break;
                }
                match self.next[level].compare_exchange_tag(
                    &aux,
                    1 | aux.tag(),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    cs,
                ) {
                    Ok(_) => break,
                    Err(e) => {
                        // If the level 0 pointer was already marked, somebody else removed the node.
                        if level == 0 && (e.current.tag() & 1) != 0 {
                            return false;
                        }
                        if e.current.tag() & 1 != 0 {
                            break;
                        }
                    }
                }
            }
        }
        true
    }

    pub fn mark_level(&self, level: usize, cs: &CsEBR) -> usize {
        loop {
            let aux = self.next[level].load_ss(cs);
            match self.next[level].compare_exchange_tag(
                &aux,
                1 | aux.tag(),
                Ordering::SeqCst,
                Ordering::SeqCst,
                cs,
            ) {
                Ok(curr) => return curr.tag(),
                Err(_) => continue,
            }
        }
    }
}

pub struct Cursor<K, V> {
    preds: [Snapshot<Node<K, V>, CsEBR>; MAX_HEIGHT],
    succs: [Snapshot<Node<K, V>, CsEBR>; MAX_HEIGHT],
    found: Option<(Snapshot<Node<K, V>, CsEBR>, usize, usize)>,
}

impl<K, V> OutputHolder<V> for Snapshot<Node<K, V>, CsEBR> {
    fn output(&self) -> &V {
        self.as_ref().map(|node| &node.value).unwrap()
    }
}

impl<K, V> Cursor<K, V> {
    fn new(head: &AtomicRc<Node<K, V>, CsEBR>, cs: &CsEBR) -> Self {
        let head = head.load_ss(cs);
        Self {
            preds: [head; MAX_HEIGHT],
            succs: [Snapshot::new(); MAX_HEIGHT],
            found: None,
        }
    }
}

pub struct SkipList<K, V> {
    head: AtomicRc<Node<K, V>, CsEBR>,
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

    fn find_optimistic(&self, key: &K, cs: &CsEBR) -> Option<Snapshot<Node<K, V>, CsEBR>> {
        let mut pred = self.head.load_ss(cs);
        let mut level = MAX_HEIGHT;
        while level >= 1
            && unsafe { pred.deref() }.next[level - 1]
                .load(Ordering::Relaxed)
                .is_null()
        {
            level -= 1;
        }

        while level >= 1 {
            level -= 1;
            let mut curr = unsafe { pred.deref() }.next[level].load_ss(cs);

            loop {
                let curr_node = some_or!(curr.as_ref(), break);
                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        pred = curr;
                        curr = curr_node.next[level].load_ss(cs);
                    }
                    std::cmp::Ordering::Equal => {
                        if curr_node.next[level].load(Ordering::Acquire).tag() == 0 {
                            return Some(curr);
                        }
                        return None;
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
        }

        return None;
    }

    fn find(&self, key: &K, cs: &CsEBR) -> Cursor<K, V> {
        'search: loop {
            let mut cursor = Cursor::new(&self.head, cs);
            let head = cursor.preds[0];

            let mut level = MAX_HEIGHT;
            while level >= 1
                && unsafe { head.deref() }.next[level - 1]
                    .load(Ordering::Relaxed)
                    .is_null()
            {
                level -= 1;
            }

            let mut pred = head;
            while level >= 1 {
                level -= 1;
                let mut curr = unsafe { pred.deref() }.next[level].load_ss(cs);
                // If the next node of `pred` is marked, that means `pred` is removed and we have
                // to restart the search.
                if curr.tag() & 1 != 0 {
                    continue 'search;
                }

                while let Some(curr_ref) = curr.as_ref() {
                    let mut succ = curr_ref.next[level].load_ss(cs);

                    if succ.tag() & 1 != 0 {
                        succ.set_tag(0);
                        if self.help_unlink(&pred, &curr, &succ, level, cs) {
                            curr = succ;
                            continue;
                        } else {
                            // On failure, we cannot do anything reasonable to continue
                            // searching from the current position. Restart the search.
                            continue 'search;
                        }
                    }

                    // If `succ` contains a key that is greater than or equal to `key`, we're
                    // done with this level.
                    match curr_ref.key.cmp(key) {
                        std::cmp::Ordering::Greater => break,
                        std::cmp::Ordering::Equal => {
                            if level == 0 {
                                if let Some((found, max, _)) = cursor.found.take() {
                                    cursor.found = Some((found, max, level));
                                } else {
                                    cursor.found = Some((curr, level, level));
                                }
                            }
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

            if let Some((found, max, min)) = cursor.found.take() {
                if min == 0 {
                    cursor.found = Some((found, max, min));
                } else {
                    cursor.found = None;
                }
            }
            return cursor;
        }
    }

    fn help_unlink(
        &self,
        pred: &Snapshot<Node<K, V>, CsEBR>,
        curr: &Snapshot<Node<K, V>, CsEBR>,
        succ: &Snapshot<Node<K, V>, CsEBR>,
        level: usize,
        cs: &CsEBR,
    ) -> bool {
        match unsafe { pred.deref() }.next[level].compare_exchange(
            curr.as_ptr().with_tag(0),
            succ.upgrade(),
            Ordering::Release,
            Ordering::Relaxed,
            cs,
        ) {
            Ok(rc) => {
                rc.finalize(cs);
                true
            }
            Err(e) => {
                e.desired.finalize(cs);
                false
            }
        }
    }

    pub fn insert(&self, key: K, value: V, cs: &CsEBR) -> bool {
        let mut cursor = self.find(&key, cs);
        if cursor.found.is_some() {
            return false;
        }

        let inner = Node::new(key, value);
        let height = inner.height;
        let mut new_node_iter = Rc::new_many_iter(inner, height);
        let mut new_node = new_node_iter.next().unwrap();
        let new_node_ref = unsafe { new_node.deref() };

        loop {
            new_node_ref.next[0].store(cursor.succs[0].upgrade(), Ordering::Relaxed, cs);

            match unsafe { cursor.preds[0].deref() }.next[0].compare_exchange(
                cursor.succs[0].as_ptr(),
                new_node,
                Ordering::SeqCst,
                Ordering::SeqCst,
                cs,
            ) {
                Ok(_) => break,
                Err(e) => new_node = e.desired,
            }

            // We failed. Let's search for the key and try again.
            cursor = self.find(&new_node_ref.key, cs);
            if cursor.found.is_some() {
                unsafe { new_node.into_inner_unchecked() };
                forget(new_node_iter);
                return false;
            }
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        'build: for level in 1..height {
            let mut new_node = new_node_iter.next().unwrap();
            loop {
                let next = new_node_ref.next[level].load_ss(cs);

                // If the current pointer is marked, that means another thread is already
                // removing the node we've just inserted. In that case, let's just stop
                // building the tower.
                if (next.tag() & 1) != 0 {
                    new_node_iter.halt(cs);
                    break 'build;
                }

                // When searching for `key` and traversing the skip list from the highest level
                // to the lowest, it is possible to observe a node with an equal key at higher
                // levels and then find it missing at the lower levels if it gets removed
                // during traversal. Even worse, it is possible to observe completely different
                // nodes with the exact same key at different levels.
                if cursor.succs[level].as_ref().map(|s| &s.key) == Some(&new_node_ref.key) {
                    cursor = self.find(&new_node_ref.key, cs);
                    continue;
                }

                if new_node_ref.next[level]
                    .compare_exchange(
                        next.as_ptr(),
                        cursor.succs[level].upgrade(),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        cs,
                    )
                    .is_err()
                {
                    new_node_iter.halt(cs);
                    break 'build;
                }

                // Try installing the new node at the current level.
                match unsafe { cursor.preds[level].deref() }.next[level].compare_exchange(
                    cursor.succs[level].as_ptr(),
                    new_node,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    cs,
                ) {
                    Ok(_) => {
                        // Success! Continue on the next level.
                        break;
                    }
                    Err(e) => {
                        // Installation failed.
                        new_node = e.desired;
                        cursor = self.find(&new_node_ref.key, cs);
                    }
                }
            }
        }

        // If any pointer in the tower is marked, that means our node is in the process of
        // removal or already removed. It is possible that another thread (either partially or
        // completely) removed the new node while we were building the tower, and just after
        // that we installed the new node at one of the higher levels. In order to undo that
        // installation, we must repeat the search, which will unlink the new node at that
        // level.
        if new_node_ref.next[height - 1].load(Ordering::SeqCst).tag() == 1 {
            self.find(&new_node_ref.key, cs);
        }

        true
    }

    pub fn remove(&self, key: &K, cs: &CsEBR) -> Option<Snapshot<Node<K, V>, CsEBR>> {
        'remove: loop {
            let cursor = self.find(key, cs);
            let (found, max, _) = cursor.found?.clone();
            let node = unsafe { found.deref() };

            for level in (max + 1..node.height).rev() {
                node.mark_level(level, cs);
            }

            for level in (0..=max).rev() {
                // Mark the current level.
                loop {
                    let next = node.next[level].load_ss(cs);
                    if next.tag() & 1 != 0 {
                        if level == 0 {
                            // The bottom level is already marked, so our removal is not committed
                            // and other thread has removed the found node.
                            continue 'remove;
                        }
                        // If the current level is not the bottom, let's break this loop and
                        // try helping the unlink.
                        break;
                    }
                    match node.next[level].compare_exchange_tag(
                        &next,
                        1 | next.tag(),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        cs,
                    ) {
                        Ok(_) => break,
                        Err(e) => {
                            if level == 0 && (e.current.tag() & 1) != 0 {
                                // The bottom level is already marked, so our removal is not
                                // committed and other thread has removed the found node.
                                continue 'remove;
                            }
                            if e.current.tag() & 1 != 0 {
                                break;
                            }
                        }
                    }
                }

                let mut next = node.next[level].load_ss(cs);
                if next.tag() & 2 != 0 {
                    // This level is not yet installed by an inserting thread (and it is null).
                    // In this case, just skip unlinking and go to the next level.
                    continue;
                }
                // Try linking the predecessor and successor at this level.
                next.set_tag(0);
                if !self.help_unlink(&cursor.preds[level], &found, &next, level, cs) {
                    if level == 0 {
                        // If we successfully marked the bottom level but failed to unlink it,
                        // other threads must have helped this removal,
                        // so let's return the result delightfully.
                        break;
                    }
                    // Otherwise, we need to restart the search and help unlinking.
                    continue 'remove;
                }
            }

            return Some(found);
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for SkipList<K, V>
where
    K: Ord + Clone + Default,
    V: Clone + Default + Display,
{
    type Output = Snapshot<Node<K, V>, CsEBR>;

    fn new() -> Self {
        SkipList::new()
    }

    #[inline(always)]
    fn get(&self, key: &K, cs: &CsEBR) -> Option<Self::Output> {
        self.find_optimistic(key, cs)
    }

    #[inline(always)]
    fn insert(&self, key: K, value: V, cs: &CsEBR) -> bool {
        self.insert(key, value, cs)
    }

    #[inline(always)]
    fn remove(&self, key: &K, cs: &CsEBR) -> Option<Self::Output> {
        self.remove(key, cs)
    }
}

#[cfg(test)]
mod tests {
    use super::SkipList;
    use crate::ds_impl::circ_ebr::concurrent_map;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<SkipList<i32, String>>();
    }
}
