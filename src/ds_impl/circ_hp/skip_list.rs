use std::{fmt::Display, sync::atomic::Ordering};

use circ::{AtomicRc, Cs, CsHP, GraphNode, Pointer, Rc, Snapshot, StrongPtr};

use super::concurrent_map::{ConcurrentMap, OutputHolder};

const MAX_HEIGHT: usize = 32;

type Tower<K, V> = [AtomicRc<Node<K, V>, CsHP>; MAX_HEIGHT];

struct Node<K, V> {
    key: K,
    value: V,
    next: Tower<K, V>,
    height: usize,
}

impl<K, V> GraphNode<CsHP> for Node<K, V> {
    const UNIQUE_OUTDEGREE: bool = false;

    #[inline]
    fn pop_outgoings(&self) -> Vec<Rc<Self, CsHP>>
    where
        Self: Sized,
    {
        vec![]
    }

    #[inline]
    fn pop_unique(&self) -> Rc<Self, CsHP>
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
        let cs = unsafe { &Cs::unprotected() };
        let height = Self::generate_height();
        let next: [AtomicRc<Node<K, V>, CsHP>; MAX_HEIGHT] = Default::default();
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

    pub fn mark_tower(&self, aux: &mut Snapshot<Self, CsHP>, cs: &CsHP) -> bool {
        for level in (0..self.height).rev() {
            loop {
                aux.load(&self.next[level], cs);
                if aux.tag() & 1 != 0 {
                    if level == 0 {
                        return false;
                    }
                    break;
                }
                match self.next[level].compare_exchange_tag(
                    &*aux,
                    1,
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
}

pub struct Cursor<K, V> {
    preds: [Snapshot<Node<K, V>, CsHP>; MAX_HEIGHT + 1],
    pred_offset: [usize; MAX_HEIGHT + 1],
    succs: [Snapshot<Node<K, V>, CsHP>; MAX_HEIGHT],
    next: Snapshot<Node<K, V>, CsHP>,
    new_node: Snapshot<Node<K, V>, CsHP>,
    found_level: Option<usize>,

    /// `found_value` is not set by a traversal function.
    /// It must be manually set in Get/Remove function.
    found_value: Option<V>,
}

impl<K, V> OutputHolder<V> for Cursor<K, V> {
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

impl<K, V> Cursor<K, V> {
    fn initialize(&mut self, head: &AtomicRc<Node<K, V>, CsHP>, cs: &CsHP) {
        // Clearing all previous slots are important to reduce the memory consumption.
        for pred in &mut self.preds {
            pred.clear();
        }
        for succ in &mut self.succs {
            succ.clear();
        }
        self.preds[MAX_HEIGHT].load(head, cs);
        self.pred_offset.fill(0);
        for i in 0..MAX_HEIGHT + 1 {
            self.pred_offset[MAX_HEIGHT - i] = i;
        }
        self.found_level = None;
    }

    fn pred(&self, level: usize) -> &Snapshot<Node<K, V>, CsHP> {
        &self.preds[level + self.pred_offset[level]]
    }

    fn found(&self) -> &Snapshot<Node<K, V>, CsHP> {
        &self.succs[self.found_level.unwrap()]
    }
}

pub struct SkipList<K, V> {
    head: AtomicRc<Node<K, V>, CsHP>,
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

    pub fn find_optimistic(&self, key: &K, cursor: &mut Cursor<K, V>, cs: &CsHP) -> bool {
        cursor.found_level = None;
        cursor.preds[0].load(&self.head, cs);

        let mut level = MAX_HEIGHT;
        while level >= 1
            && unsafe { cursor.preds[0].deref() }.next[level - 1]
                .load(Ordering::Relaxed)
                .is_null()
        {
            level -= 1;
        }

        while level >= 1 {
            level -= 1;
            cursor.succs[0].load(&unsafe { cursor.preds[0].deref() }.next[level], cs);

            loop {
                let curr_node = some_or!(cursor.succs[0].as_ref(), break);
                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        Snapshot::swap(&mut cursor.preds[0], &mut cursor.succs[0]);
                        cursor.succs[0].load(&unsafe { cursor.preds[0].deref() }.next[level], cs);
                    }
                    std::cmp::Ordering::Equal => {
                        let clean = curr_node.next[level].load(Ordering::Acquire).tag() == 0;
                        if clean {
                            cursor.found_level = Some(0);
                        }
                        return clean;
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
        }

        return false;
    }

    fn find(&self, key: &K, cursor: &mut Cursor<K, V>, cs: &CsHP) -> bool {
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
                        cursor.next.set_tag(0);
                        if self.help_unlink(
                            &unsafe { cursor.pred(level).deref() }.next[level],
                            &cursor.succs[level],
                            &cursor.next,
                            cs,
                        ) {
                            Snapshot::swap(&mut cursor.succs[level], &mut cursor.next);
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
        pred: &AtomicRc<Node<K, V>, CsHP>,
        succ: &Snapshot<Node<K, V>, CsHP>,
        next: &Snapshot<Node<K, V>, CsHP>,
        cs: &CsHP,
    ) -> bool {
        if let Ok(rc) = pred.compare_exchange(
            succ.with_tag(0).as_ptr(),
            next.upgrade().with_tag(0),
            Ordering::Release,
            Ordering::Relaxed,
            cs,
        ) {
            rc.finalize(cs);
            return true;
        }
        false
    }

    pub fn insert(&self, key: K, value: V, cursor: &mut Cursor<K, V>, cs: &CsHP) -> bool {
        if self.find(&key, cursor, cs) {
            return false;
        }

        let mut new_node = Rc::new(Node::new(key, value));
        let new_node_ref = unsafe { new_node.deref() };
        let height = new_node_ref.height;
        cursor.new_node.protect(&new_node, cs);

        loop {
            let (succ_rc, succ_dt) = cursor.succs[0].loan();
            new_node_ref.next[0].store(succ_rc, Ordering::Relaxed, cs);

            match unsafe { cursor.pred(0).deref() }.next[0].compare_exchange(
                cursor.succs[0].as_ptr(),
                new_node,
                Ordering::SeqCst,
                Ordering::SeqCst,
                cs,
            ) {
                Ok(succ_rc) => {
                    succ_dt.repay(succ_rc);
                    break;
                }
                Err(e) => {
                    new_node = e.desired;
                    let succ_rc = new_node_ref.next[0].swap(Rc::null(), Ordering::Relaxed);
                    succ_dt.repay(succ_rc);
                }
            }

            // We failed. Let's search for the key and try again.
            if self.find(&new_node_ref.key, cursor, cs) {
                drop(unsafe { new_node.into_inner() });
                return false;
            }
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        'build: for level in 1..height {
            let mut new_node = cursor.new_node.upgrade();
            loop {
                cursor.next.load(&new_node_ref.next[level], cs);

                // If the current pointer is marked, that means another thread is already
                // removing the node we've just inserted. In that case, let's just stop
                // building the tower.
                if (cursor.next.tag() & 1) != 0 {
                    break 'build;
                }

                let succ_ptr = cursor.succs[level].as_ptr();
                let (succ_rc, succ_dt) = cursor.succs[level].loan();

                match new_node_ref.next[level].compare_exchange(
                    cursor.next.as_ptr(),
                    succ_rc,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    cs,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        succ_dt.repay(e.desired);
                        break 'build;
                    }
                }

                // Try installing the new node at the current level.
                match unsafe { cursor.pred(level).deref() }.next[level].compare_exchange(
                    cursor.succs[level].as_ptr(),
                    new_node,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    cs,
                ) {
                    Ok(succ_rc) => {
                        succ_dt.repay(succ_rc);
                        // Success! Continue on the next level.
                        break;
                    }
                    Err(e) => {
                        new_node = e.desired;
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

    pub fn remove(&self, key: &K, cursor: &mut Cursor<K, V>, cs: &CsHP) -> bool {
        loop {
            let found = self.find(key, cursor, cs);
            if !found {
                return false;
            }
            let height = unsafe { cursor.found().deref() }.height;
            cursor.found_value = Some(unsafe { cursor.found().deref() }.value.clone());

            // Try removing the node by marking its tower.
            if unsafe { cursor.found().deref() }.mark_tower(&mut cursor.next, cs) {
                for level in (0..height).rev() {
                    cursor
                        .next
                        .load(&unsafe { cursor.found().deref() }.next[level], cs);
                    if (cursor.next.tag() & 2) != 0 {
                        continue;
                    }
                    // Try linking the predecessor and successor at this level.
                    cursor.next.set_tag(0);
                    if !self.help_unlink(
                        &unsafe { cursor.pred(level).deref() }.next[level],
                        cursor.found(),
                        &cursor.next,
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

    fn get(&self, key: &K, output: &mut Self::Output, cs: &CsHP) -> bool {
        let found = self.find_optimistic(key, output, cs);
        if found {
            output.found_value = Some(unsafe { output.found().deref() }.value.clone());
        }
        found
    }

    fn insert(&self, key: K, value: V, output: &mut Self::Output, cs: &CsHP) -> bool {
        self.insert(key, value, output, cs)
    }

    fn remove(&self, key: &K, output: &mut Self::Output, cs: &CsHP) -> bool {
        self.remove(key, output, cs)
    }
}

#[cfg(test)]
mod tests {
    use super::SkipList;
    use crate::ds_impl::circ_hp::concurrent_map;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<SkipList<i32, String>>();
    }
}
