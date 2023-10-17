use std::{fmt::Display, sync::atomic::Ordering};

use circ::{AtomicRc, CompareExchangeErrorRc, Cs, CsEBR, Pointer, Rc, Snapshot, StrongPtr};

use super::concurrent_map::{ConcurrentMap, OutputHolder};

const MAX_HEIGHT: usize = 32;

type Tower<K, V> = [AtomicRc<Node<K, V>, CsEBR>; MAX_HEIGHT];

struct Node<K, V> {
    key: K,
    value: V,
    next: Tower<K, V>,
    height: usize,
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

pub struct Cursor<K, V> {
    preds: [Snapshot<Node<K, V>, CsEBR>; MAX_HEIGHT],
    succs: [Snapshot<Node<K, V>, CsEBR>; MAX_HEIGHT],
    /// `found_value` is not set by a traversal function.
    /// It must be manually set in Get/Remove function.
    found_value: Option<V>,
}

impl<K, V> OutputHolder<V> for Cursor<K, V> {
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

impl<K, V> Cursor<K, V> {
    fn initialize(&mut self, head: &AtomicRc<Node<K, V>, CsEBR>, cs: &CsEBR) {
        let head = head.load_ss(cs);
        self.preds.fill(head);
        self.succs.fill(Snapshot::new());
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

    fn find(&self, key: &K, cursor: &mut Cursor<K, V>, cs: &CsEBR) -> Option<usize> {
        'search: loop {
            cursor.initialize(&self.head, cs);
            let head = cursor.preds[0];

            let mut level = MAX_HEIGHT;
            while level >= 1
                && unsafe { head.deref() }.next[level - 1]
                    .load(Ordering::Relaxed)
                    .is_null()
            {
                level -= 1;
            }

            let mut found_level = None;
            let mut pred = head;
            while level >= 1 {
                level -= 1;
                let mut curr = unsafe { pred.deref() }.next[level].load_ss(cs);
                // If the next node of `pred` is marked, that means `pred` is removed and we have
                // to restart the search.
                if curr.tag() == 1 {
                    continue 'search;
                }

                while let Some(curr_ref) = curr.as_ref() {
                    let mut succ = curr_ref.next[level].load_ss(cs);

                    if succ.tag() == 1 {
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
                            found_level = Some(level);
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

            return found_level;
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
        debug_assert!(curr.tag() == 0);
        debug_assert!(succ.tag() == 0);
        let (succ_rc, succ_dt) = succ.loan();

        match unsafe { pred.deref() }.next[level].compare_exchange(
            curr.as_ptr().with_tag(0),
            succ_rc,
            Ordering::Release,
            Ordering::Relaxed,
            cs,
        ) {
            Ok(_) => {
                succ_dt.repay_frontier(&unsafe { curr.deref() }.next[level], 1, cs);
                true
            }
            Err(e) => {
                succ_dt.repay(e.desired());
                false
            }
        }
    }

    pub fn insert(&self, key: K, value: V, cursor: &mut Cursor<K, V>, cs: &CsEBR) -> bool {
        if self.find(&key, cursor, cs).is_some() {
            return false;
        }

        let new_node = Rc::new(Node::new(key, value));
        let new_node_ref = unsafe { new_node.deref() };
        let height = new_node_ref.height;
        let mut new_node_ss = Snapshot::new();
        new_node_ss.protect(&new_node, cs);

        loop {
            let (succ_rc, succ_dt) = cursor.succs[0].loan();
            new_node_ref.next[0].store(succ_rc, Ordering::Relaxed, cs);

            match unsafe { cursor.preds[0].deref() }.next[0].compare_exchange(
                cursor.succs[0].as_ptr(),
                &new_node_ss,
                Ordering::SeqCst,
                Ordering::SeqCst,
                cs,
            ) {
                Ok(succ_rc) => {
                    succ_dt.repay(succ_rc);
                    break;
                }
                Err(_) => {
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
                                    succ_ptr.with_tag(succ_ptr.tag() | 1),
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

    pub fn remove(&self, key: &K, cursor: &mut Cursor<K, V>, cs: &CsEBR) -> bool {
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

impl<K, V> ConcurrentMap<K, V> for SkipList<K, V>
where
    K: Ord + Clone + Default,
    V: Clone + Default + Display,
{
    type Output = Cursor<K, V>;

    fn new() -> Self {
        SkipList::new()
    }

    fn get(&self, key: &K, output: &mut Self::Output, cs: &CsEBR) -> bool {
        if let Some(found) = self.find_optimistic(key, cs) {
            output.found_value = Some(unsafe { found.deref().value.clone() });
            return true;
        }
        false
    }

    fn insert(&self, key: K, value: V, output: &mut Self::Output, cs: &CsEBR) -> bool {
        self.insert(key, value, output, cs)
    }

    fn remove(&self, key: &K, output: &mut Self::Output, cs: &CsEBR) -> bool {
        self.remove(key, output, cs)
    }
}

#[cfg(test)]
mod tests {
    use super::SkipList;
    use crate::circ_ebr::concurrent_map;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<SkipList<i32, String>>();
    }
}
