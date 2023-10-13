use std::{fmt::Display, sync::atomic::Ordering};

use circ::{AtomicRc, CompareExchangeErrorRc, Cs, Pointer, Rc, Snapshot, StrongPtr};

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
        let cs = unsafe { &C::unprotected() };
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
                    Err(CompareExchangeErrorRc::Changed { current, .. }) => {
                        // If the level 0 pointer was already marked, somebody else removed the node.
                        if level == 0 && (current.tag() & 1) != 0 {
                            return false;
                        }
                        if current.tag() & 1 != 0 {
                            break;
                        }
                    }
                    Err(CompareExchangeErrorRc::Closed { .. }) => {
                        // If the level 0 pointer was already marked, somebody else removed the node.
                        if level == 0 {
                            return false;
                        }
                        break;
                    }
                }
            }
        }
        true
    }
}

pub struct Cursor<K, V, C: Cs> {
    preds: [Snapshot<Node<K, V, C>, C>; MAX_HEIGHT],
    succs: [Snapshot<Node<K, V, C>, C>; MAX_HEIGHT],
    /// `found_value` is not set by a traversal function.
    /// It must be manually set in Get/Remove function.
    found_value: Option<V>,
}

impl<K, V, C: Cs> OutputHolder<V> for Cursor<K, V, C> {
    fn default() -> Self {
        Self {
            preds: core::array::from_fn(|_| Default::default()),
            succs: Default::default(),
            found_value: None,
        }
    }

    fn output(&self) -> &V {
        self.found_value.as_ref().unwrap()
    }
}

impl<K, V, C: Cs> Cursor<K, V, C> {
    fn initialize(&mut self, head: &AtomicRc<Node<K, V, C>, C>, cs: &C) {
        self.preds[MAX_HEIGHT - 1].load(head, cs);
        for succ in &mut self.succs {
            succ.clear();
        }
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
            head: AtomicRc::new(Node::head()),
        }
    }

    /// Returns `None` if the key is not found. Otherwise, returns `Some(found level)`.
    pub fn find_optimistic(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> Option<usize> {
        let mut pred = Snapshot::new();
        let mut succ = Snapshot::new();
        pred.load(&self.head, cs);

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
            succ.load(&unsafe { pred.deref() }.next[level], cs);

            loop {
                let curr_node = some_or!(succ.as_ref(), break);
                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        Snapshot::swap(&mut pred, &mut succ);
                        succ.load(&unsafe { pred.deref() }.next[level], cs);
                    }
                    std::cmp::Ordering::Equal => {
                        let clean = curr_node.next[level].load(Ordering::Acquire).tag() == 0;
                        if clean {
                            cursor.succs[0] = succ;
                            return Some(0);
                        }
                        return None;
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
        }

        return None;
    }

    /// Returns `None` if the key is not found. Otherwise, returns `Some(found level)`.
    fn find(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> Option<usize> {
        'search: loop {
            cursor.initialize(&self.head, cs);

            let mut level = MAX_HEIGHT;
            while level >= 1
                && unsafe { cursor.preds[level - 1].deref() }.next[level - 1]
                    .load(Ordering::Relaxed)
                    .is_null()
            {
                if level > 1 {
                    let (lower, upper) = cursor.preds.split_at_mut(level - 1);
                    unsafe { upper[0].copy_to(&mut lower[level - 2]) };
                }
                level -= 1;
            }

            let mut found_level = None;
            let mut pred = Snapshot::new();
            unsafe { cursor.preds[MAX_HEIGHT - 1].copy_to(&mut pred) };
            while level >= 1 {
                level -= 1;
                let mut succ = unsafe { pred.deref() }.next[level].load_ss(cs);

                // If the next node of `pred` is marked, that means `pred` is removed and we have
                // to restart the search.
                if succ.tag() == 1 {
                    continue 'search;
                }

                // Order of snapshots: pred[i] -> succ[i] -> next
                while let Some(succ_ref) = succ.as_ref() {
                    let mut next = succ_ref.next[level].load_ss(cs);

                    if next.tag() == 1 {
                        next.set_tag(0);
                        if self.help_unlink(&pred, &succ, &next, level, cs) {
                            succ = next;
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
                            found_level = Some(level);
                            break;
                        }
                        std::cmp::Ordering::Less => {}
                    }

                    // Move one step forward.
                    pred = succ;
                    succ = next;
                }

                cursor.preds[level] = pred;
                cursor.succs[level] = succ;
                pred = Snapshot::new();
                unsafe { cursor.preds[level].copy_to(&mut pred) };
            }

            return found_level;
        }
    }

    fn help_unlink(
        &self,
        pred: &Snapshot<Node<K, V, C>, C>,
        succ: &Snapshot<Node<K, V, C>, C>,
        next: &Snapshot<Node<K, V, C>, C>,
        level: usize,
        cs: &C,
    ) -> bool {
        debug_assert!(succ.tag() == 0);
        debug_assert!(next.tag() == 0);
        let (next_rc, next_dt) = next.loan();

        match unsafe { pred.deref() }.next[level].compare_exchange(
            succ.as_ptr().with_tag(0),
            next_rc,
            Ordering::Release,
            Ordering::Relaxed,
            cs,
        ) {
            Ok(_) => {
                next_dt.repay_frontier(&unsafe { succ.deref() }.next[level], 1, cs);
                true
            }
            Err(e) => {
                next_dt.repay(e.desired());
                false
            }
        }
    }

    pub fn insert(&self, key: K, value: V, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        if self.find(&key, cursor, cs).is_some() {
            return false;
        }

        let mut new_node = Rc::new(Node::new(key, value));
        let new_node_ref = unsafe { new_node.deref() };
        let height = new_node_ref.height;
        let mut new_node_ss = Snapshot::new();
        new_node_ss.protect(&new_node, cs);

        loop {
            let (succ_rc, succ_dt) = cursor.succs[0].loan();
            new_node_ref.next[0].store(succ_rc, Ordering::Relaxed, cs);

            match unsafe { cursor.preds[0].deref() }.next[0].compare_exchange(
                cursor.succs[0].as_ptr(),
                new_node,
                Ordering::Relaxed,
                Ordering::Relaxed,
                cs,
            ) {
                Ok(succ_rc) => {
                    succ_dt.repay(succ_rc);
                    break;
                }
                Err(e) => {
                    new_node = e.desired();
                    let succ_rc = new_node_ref.next[0].swap(Rc::null(), Ordering::Relaxed, cs);
                    succ_dt.repay(succ_rc);
                }
            }

            // We failed. Let's search for the key and try again.
            if self.find(&new_node_ref.key, cursor, cs).is_some() {
                drop(unsafe { new_node.into_inner() });
                return false;
            }
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        'build: for level in 1..height {
            loop {
                let next = new_node_ref.next[level].load_ss(cs);

                // If the current pointer is marked, that means another thread is already
                // removing the node we've just inserted. In that case, let's just stop
                // building the tower.
                if (next.tag() & 1) != 0 {
                    break 'build;
                }

                let succ_ptr = cursor.succs[level].as_ptr();
                let (succ_rc, succ_dt) = cursor.succs[level].loan();

                match new_node_ref.next[level].compare_exchange(
                    next.as_ptr(),
                    succ_rc,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    cs,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        succ_dt.repay(e.desired());
                        break 'build;
                    }
                }

                // Try installing the new node at the current level.
                match unsafe { cursor.preds[level].deref() }.next[level].compare_exchange(
                    cursor.succs[level].as_ptr(),
                    &new_node_ss,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    cs,
                ) {
                    Ok(succ_rc) => {
                        succ_dt.repay(succ_rc);
                        // Success! Continue on the next level.
                        break;
                    }
                    Err(_) => {
                        // Repay the debt and try again.
                        // Note that someone might mark the inserted node.
                        let succ_rc = match new_node_ref.next[level].compare_exchange(
                            succ_ptr,
                            Rc::null().with_tag(2),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            cs,
                        ) {
                            Ok(succ_rc) => succ_rc,
                            Err(_) => new_node_ref.next[level]
                                .compare_exchange(
                                    succ_ptr.with_tag(1),
                                    Rc::null().with_tag(1 | 2),
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                    cs,
                                )
                                .ok()
                                .unwrap(),
                        };
                        succ_dt.repay(succ_rc.with_tag(0));
                    }
                }

                // Installation failed.
                self.find(&new_node_ref.key, cursor, cs);
            }
        }

        true
    }

    pub fn remove(&self, key: &K, cursor: &mut Cursor<K, V, C>, cs: &C) -> bool {
        loop {
            let found_level = self.find(key, cursor, cs);
            if found_level.is_none() {
                return false;
            }
            let found_level = found_level.unwrap();
            let height = unsafe { cursor.succs[found_level].deref() }.height;
            cursor.found_value = Some(unsafe { cursor.succs[found_level].deref() }.value.clone());
            debug_assert!(cursor.succs[found_level].as_ptr().tag() == 0);

            // Try removing the node by marking its tower.
            if unsafe { cursor.succs[found_level].deref() }.mark_tower(cs) {
                for level in (0..height).rev() {
                    let mut next =
                        unsafe { cursor.succs[found_level].deref() }.next[level].load_ss(cs);
                    if (next.tag() & 2) != 0 {
                        continue;
                    }
                    // Try linking the predecessor and successor at this level.
                    next.set_tag(0);
                    if !self.help_unlink(
                        &cursor.preds[level],
                        &cursor.succs[found_level],
                        &next,
                        level,
                        cs,
                    ) {
                        self.find(key, cursor, cs);
                        break;
                    }
                }
                return true;
            }
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
        let found_level = self.find_optimistic(key, output, cs);
        if let Some(level) = found_level {
            output.found_value = Some(unsafe { output.succs[level].deref() }.value.clone());
        }
        found_level.is_some()
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
    use crate::circ::concurrent_map;
    use circ::CsEBR;

    #[test]
    fn smoke_skip_list_ebr() {
        concurrent_map::tests::smoke::<CsEBR, SkipList<i32, String, CsEBR>>();
    }

    #[test]
    fn smoke_skip_list_hp() {
        // TODO: Implement CIRCL for HP and uncomment
        // concurrent_map::tests::smoke::<CsHP, SkipList<i32, String, CsHP>>();
    }
}
