use std::{fmt::Display, mem::forget, sync::atomic::Ordering};

use circ::{AtomicRc, CsEBR, GraphNode, Pointer, Rc, Snapshot, StrongPtr};

use super::concurrent_map::{ConcurrentMap, OutputHolder};

const MAX_HEIGHT: usize = 32;

#[derive(Clone, PartialEq, Eq)]
enum Key<K> {
    Fin(K),
    Inf,
}

impl<K> PartialEq<K> for Key<K>
where
    K: PartialEq,
{
    fn eq(&self, rhs: &K) -> bool {
        match self {
            Key::Fin(k) => k == rhs,
            _ => false,
        }
    }
}

impl<K> PartialOrd<K> for Key<K>
where
    K: PartialOrd,
{
    fn partial_cmp(&self, rhs: &K) -> Option<std::cmp::Ordering> {
        match self {
            Key::Fin(k) => k.partial_cmp(rhs),
            _ => Some(std::cmp::Ordering::Greater),
        }
    }
}

impl<K> Key<K>
where
    K: Ord,
{
    fn cmp(&self, rhs: &K) -> std::cmp::Ordering {
        match self {
            Key::Fin(k) => k.cmp(rhs),
            _ => std::cmp::Ordering::Greater,
        }
    }
}

type Tower<K, V> = [AtomicRc<Node<K, V>, CsEBR>; MAX_HEIGHT];

pub struct Node<K, V> {
    key: Key<K>,
    value: V,
    next: Tower<K, V>,
    height: usize,
}

bitflags! {
    #[derive(Clone, Copy)]
    struct Tags: usize {
        const MARKED = 0b01;
        const INSERTING = 0b10;
    }
}

impl Tags {
    fn marked(self) -> bool {
        !(self & Tags::MARKED).is_empty()
    }

    fn inserting(self) -> bool {
        !(self & Tags::INSERTING).is_empty()
    }
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
        let height = Self::generate_height();
        let next: [AtomicRc<Node<K, V>, CsEBR>; MAX_HEIGHT] = Default::default();
        Self {
            key: Key::Fin(key),
            value,
            next,
            height,
        }
    }

    pub fn head(tail: Self) -> Self {
        let tail = Rc::new_many(tail);
        Self {
            key: Key::Fin(K::default()),
            value: V::default(),
            next: tail.map(|node| AtomicRc::from(node)),
            height: MAX_HEIGHT,
        }
    }

    pub fn tail() -> Self {
        Self {
            key: Key::Inf,
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

    pub fn mark_level(&self, level: usize, cs: &CsEBR) -> usize {
        loop {
            let aux = self.next[level].load_ss(cs);
            match self.next[level].compare_exchange_tag(
                &aux,
                Tags::MARKED.bits() | aux.tag(),
                Ordering::SeqCst,
                Ordering::SeqCst,
                cs,
            ) {
                Ok(curr) => return curr.tag(),
                Err(_) => continue,
            }
        }
    }

    fn bottom_tag(&self, cs: &CsEBR) -> Tags {
        let mut next = self.next[0].load_ss(cs);
        next.set_tag(0);
        let next_tag = match self.next[0].compare_exchange_tag(
            next,
            0,
            Ordering::SeqCst,
            Ordering::SeqCst,
            cs,
        ) {
            Ok(_) => 0,
            Err(e) => e.current.tag(),
        };
        Tags::from_bits_truncate(next_tag)
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
        let tail = Node::tail();
        let head = Node::head(tail);
        Self {
            head: AtomicRc::new(head),
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
                let next = curr_node.next[level].load_ss(cs);
                let next_tag = Tags::from_bits_truncate(next.tag());
                if !next_tag.is_empty() {
                    curr = next;
                    continue;
                }
                match curr_node.key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        pred = curr;
                        curr = curr_node.next[level].load_ss(cs);
                    }
                    std::cmp::Ordering::Equal => return Some(curr),
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
                if Tags::from_bits_truncate(curr.tag()).marked() {
                    assert!(!Tags::from_bits_truncate(curr.tag()).inserting());
                    continue 'search;
                }

                while let Some(curr_ref) = curr.as_ref() {
                    let mut succ = curr_ref.next[level].load_ss(cs);

                    if Tags::from_bits_truncate(succ.tag()).marked() {
                        if self.help_unlink(&pred, &curr, &succ, level, cs) {
                            succ.set_tag(Tags::empty().bits());
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
                            if let Some((found, _, max)) = cursor.found.take() {
                                if found == curr {
                                    cursor.found = Some((found, level, max));
                                } else {
                                    // Same key, but different nodes.
                                    cursor.found = Some((curr, level, level))
                                }
                            } else {
                                cursor.found = Some((curr, level, level))
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

            if let Some((found, min, max)) = cursor.found.take() {
                if min == 0 {
                    cursor.found = Some((found, min, max));
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
        let curr_tag = curr.tag();
        assert!(!Tags::from_bits_truncate(curr_tag).marked());
        match unsafe { pred.deref() }.next[level].compare_exchange(
            curr.as_ptr(),
            succ.upgrade().with_tag(curr_tag),
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
        if let Some((found, _, _)) = cursor.found {
            self.interrupt_if_inserting(found, cs);
            return false;
        }

        let inner = Node::new(key.clone(), value);
        let height = inner.height;
        let mut new_node_iter = Rc::new_many_iter(inner, height);
        let mut new_node = new_node_iter.next().unwrap();
        let new_node_ref = unsafe { new_node.deref() };

        loop {
            new_node_ref.next[0].swap(
                cursor.succs[0].upgrade().with_tag(Tags::INSERTING.bits()),
                Ordering::SeqCst,
                cs,
            );

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
            cursor = self.find(&key, cs);
            if let Some((found, _, _)) = cursor.found {
                unsafe { new_node.into_inner_unchecked() };
                forget(new_node_iter);
                self.interrupt_if_inserting(found, cs);
                return false;
            }
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        'build: for level in 1..height {
            let mut new_node = new_node_iter.next().unwrap();
            loop {
                let next = new_node_ref.next[level].load_ss(cs);
                let next_tag = Tags::from_bits_truncate(next.tag());

                // Other concurrent inserter or remover have interrupted this insertion.
                // In this case, just stop constructing the upper levels.
                if next_tag.marked() {
                    new_node_iter.halt(cs);
                    break 'build;
                }

                // When searching for `key` and traversing the skip list from the highest level
                // to the lowest, it is possible to observe a node with an equal key at higher
                // levels and then find it missing at the lower levels if it gets removed
                // during traversal. Even worse, it is possible to observe completely different
                // nodes with the exact same key at different levels.
                if cursor.succs[level].as_ref().map(|s| &s.key) == Some(&new_node_ref.key) {
                    cursor = self.find(&key, cs);
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
                        if new_node_ref.next[level]
                            .compare_exchange(
                                cursor.succs[level].as_ptr(),
                                Rc::null(),
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                cs,
                            )
                            .is_err()
                        {
                            assert!(new_node_ref.next[level]
                                .compare_exchange(
                                    cursor.succs[level].as_ptr().with_tag(Tags::MARKED.bits()),
                                    Rc::null().with_tag(Tags::MARKED.bits()),
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                    cs,
                                )
                                .is_ok());
                        }
                        new_node = e.desired;
                        cursor = self.find(&key, cs);
                    }
                }
            }
        }

        // Finally, let's remove the `INSERTING` tag at the bottom.
        let mut next = new_node_ref.next[0].load_ss(cs);
        while Tags::from_bits_truncate(next.tag()).inserting() {
            if new_node_ref.next[0]
                .compare_exchange_tag(
                    next,
                    next.tag() & !Tags::INSERTING.bits(),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    cs,
                )
                .is_err()
            {
                next = new_node_ref.next[0].load_ss(cs);
            }
        }

        true
    }

    fn interrupt_if_inserting(&self, node: Snapshot<Node<K, V>, CsEBR>, cs: &CsEBR) {
        let node_ref = unsafe { node.deref() };
        if !node_ref.bottom_tag(cs).inserting() {
            return;
        }

        for level in (1..node_ref.height).rev() {
            if node_ref.next[level]
                .compare_exchange_tag(
                    Snapshot::new(),
                    Tags::MARKED.bits(),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    cs,
                )
                .is_err()
            {
                break;
            }
        }

        while Tags::from_bits_truncate(node_ref.next[0].load(Ordering::Acquire).tag()).inserting() {
            core::hint::spin_loop();
        }
    }

    pub fn remove(&self, key: &K, cs: &CsEBR) -> Option<Snapshot<Node<K, V>, CsEBR>> {
        'remove: loop {
            let cursor = self.find(key, cs);
            let (found, _, max) = cursor.found?.clone();
            let node = unsafe { found.deref() };
            if node.bottom_tag(cs).inserting() {
                self.interrupt_if_inserting(found, cs);
                return None;
            }

            for level in (1..node.height).rev() {
                node.mark_level(level, cs);

                if level <= max {
                    let next = node.next[level].load_ss(cs);
                    // Try linking the predecessor and successor at this level.
                    if unsafe { cursor.preds[level].deref() }.next[level]
                        .compare_exchange(
                            found.as_ptr().with_tag(Tags::empty().bits()),
                            next.upgrade().with_tag(0),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            cs,
                        )
                        .is_err()
                    {
                        continue 'remove;
                    }
                }
            }

            // Finally, mark the bottom level.
            node.mark_level(0, cs);
            let next = node.next[0].load_ss(cs);
            let _ = unsafe { cursor.preds[0].deref() }.next[0].compare_exchange(
                found.as_ptr().with_tag(Tags::empty().bits()),
                next.upgrade().with_tag(0),
                Ordering::SeqCst,
                Ordering::SeqCst,
                cs,
            );
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
