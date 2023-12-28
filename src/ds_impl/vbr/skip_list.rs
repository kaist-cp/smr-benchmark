use std::{
    mem::zeroed,
    sync::atomic::{fence, AtomicUsize, Ordering},
};

use vbr_rs::{Entry, Global, Guard, ImmAtomic, Local, MutAtomic, Shared};

use super::concurrent_map::ConcurrentMap;

const MAX_HEIGHT: usize = 32;

type Tower<K, V> = [MutAtomic<Node<K, V>>; MAX_HEIGHT];

pub struct Node<K, V>
where
    K: 'static + Copy,
    V: 'static + Copy,
{
    key: ImmAtomic<K>,
    value: ImmAtomic<V>,
    next: Tower<K, V>,
    height: ImmAtomic<usize>,
    refs: AtomicUsize,
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

impl<K, V> Node<K, V>
where
    K: 'static + Copy,
    V: 'static + Copy,
{
    pub fn new(
        key: K,
        value: V,
        height: usize,
        tag: usize,
        guard: &Guard<Node<K, V>>,
    ) -> Result<Shared<Self>, ()> {
        let node = guard.allocate()?;
        let node_ref = unsafe { node.deref() };
        node_ref.key.set(key);
        node_ref.value.set(value);
        node_ref.height.set(height);
        for next in node_ref.next.iter() {
            next.nullify(node, tag, guard);
        }
        Ok(node)
    }

    pub fn decrement(ptr: Shared<Node<K, V>>, guard: &Guard<Node<K, V>>) -> Result<(), ()> {
        let prev = unsafe { ptr.deref() }.refs.fetch_sub(1, Ordering::Release);
        if prev == 1 {
            fence(Ordering::Acquire);
            return unsafe { guard.retire(ptr) };
        }
        Ok(())
    }

    pub fn mark_tower(ptr: Shared<Node<K, V>>, guard: &Guard<Node<K, V>>) -> Result<bool, ()> {
        let node = unsafe { ptr.deref() };
        let height = node.height.get(guard)?;

        for level in (0..height).rev() {
            let next = node.next[level].load(Ordering::Acquire, guard)?;
            let next_tag = next.tag()?;
            if level == 0 && (next_tag & 1) != 0 {
                return Ok(false);
            } else if (next_tag & 1) != 0 {
                continue;
            }

            let result = node.next[level].compare_exchange(
                ptr,
                next,
                next.with_tag(next_tag | 1),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            );

            if level == 0 && result.is_err() {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

struct Cursor<K, V>
where
    K: 'static + Copy,
    V: 'static + Copy,
{
    found: Option<Shared<Node<K, V>>>,
    preds: [Shared<Node<K, V>>; MAX_HEIGHT],
    succs: [Shared<Node<K, V>>; MAX_HEIGHT],
}

impl<K, V> Cursor<K, V>
where
    K: 'static + Copy,
    V: 'static + Copy,
{
    fn new(head: Shared<Node<K, V>>) -> Self {
        Self {
            found: None,
            preds: [head; MAX_HEIGHT],
            succs: [Shared::null(); MAX_HEIGHT],
        }
    }
}

pub struct SkipList<K, V>
where
    K: 'static + Copy,
    V: 'static + Copy,
{
    head: Entry<Node<K, V>>,
}

impl<K, V> SkipList<K, V>
where
    K: 'static + Copy + Ord,
    V: 'static + Copy,
{
    pub fn new(local: &Local<Node<K, V>>) -> Self {
        loop {
            let guard = &local.guard();
            let node = ok_or!(
                Node::new(
                    unsafe { zeroed() },
                    unsafe { zeroed() },
                    0,
                    MAX_HEIGHT,
                    guard
                ),
                continue
            );
            return Self {
                head: Entry::new(node),
            };
        }
    }

    fn find_optimistic(&self, key: &K, guard: &Guard<Node<K, V>>) -> Result<Cursor<K, V>, ()> {
        let head = self.head.load(guard)?;
        let mut cursor = Cursor::new(head);

        let mut level = MAX_HEIGHT;
        while level >= 1
            && unsafe { head.deref() }.next[level - 1]
                .load(Ordering::Relaxed, guard)?
                .is_null()
        {
            level -= 1;
        }

        let mut pred = head;
        let mut curr = Shared::null();
        while level >= 1 {
            level -= 1;
            curr = unsafe { pred.deref() }.next[level].load(Ordering::Acquire, guard)?;

            loop {
                let curr_node = some_or!(curr.as_ref(), break);
                let succ = curr_node.next[level].load(Ordering::Acquire, guard)?;

                if succ.tag()? != 0 {
                    curr = succ;
                    continue;
                }

                if curr_node.key.get(guard)? < *key {
                    pred = curr;
                    curr = succ;
                    continue;
                } else {
                    break;
                }
            }
        }

        if let Some(curr_node) = curr.as_ref() {
            if curr_node.key.get(guard)? == *key {
                cursor.found = Some(curr)
            }
        }
        Ok(cursor)
    }

    fn find(&self, key: &K, guard: &Guard<Node<K, V>>) -> Result<Cursor<K, V>, ()> {
        let head = self.head.load(guard)?;
        let mut cursor = Cursor::new(head);

        let mut level = MAX_HEIGHT;
        while level >= 1
            && unsafe { head.deref() }.next[level - 1]
                .load(Ordering::Relaxed, guard)?
                .is_null()
        {
            level -= 1;
        }

        let mut pred = head;
        while level >= 1 {
            level -= 1;
            let mut curr = unsafe { pred.deref() }.next[level].load(Ordering::Acquire, guard)?;
            // If `curr` is marked, that means `pred` is removed and we have to restart the
            // search.
            if (curr.tag()? & 1) == 1 {
                return Err(());
            }

            while let Some(curr_ref) = curr.as_ref() {
                let succ = curr_ref.next[level].load(Ordering::Acquire, guard)?;

                if (succ.tag()? & 1) == 1 {
                    if self.help_unlink(
                        pred,
                        &unsafe { pred.deref() }.next[level],
                        curr,
                        succ,
                        guard,
                    )? {
                        curr = succ.with_tag(0);
                        continue;
                    } else {
                        // On failure, we cannot do anything reasonable to continue
                        // searching from the current position. Restart the search.
                        return Err(());
                    }
                }

                // If `curr` contains a key that is greater than or equal to `key`, we're
                // done with this level.
                match curr_ref.key.get(guard)?.cmp(key) {
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

        return Ok(cursor);
    }

    fn help_unlink(
        &self,
        pred: Shared<Node<K, V>>,
        pred_link: &MutAtomic<Node<K, V>>,
        curr: Shared<Node<K, V>>,
        succ: Shared<Node<K, V>>,
        guard: &Guard<Node<K, V>>,
    ) -> Result<bool, ()> {
        let success = pred_link
            .compare_exchange(
                pred,
                curr.with_tag(0),
                succ.with_tag(0),
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            )
            .is_ok();

        if success {
            Node::decrement(curr, guard)?;
        }
        Ok(success)
    }

    fn insert_inner(
        &self,
        key: K,
        new_node: Shared<Node<K, V>>,
        height: usize,
        guard: &Guard<Node<K, V>>,
    ) -> Result<bool, ()> {
        let cursor = match self.find(&key, guard) {
            Ok(cursor) => cursor,
            Err(_) => return Err(()),
        };
        if cursor.found.is_some() {
            return Ok(false);
        }

        let new_node_ref = unsafe { new_node.deref() };
        new_node_ref.refs.store(height + 1, Ordering::SeqCst);
        let null = new_node_ref.next[0].nullify(new_node, 2, guard);
        new_node_ref.next[0]
            .compare_exchange(
                new_node,
                null,
                cursor.succs[0],
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .unwrap();
        if unsafe { cursor.preds[0].deref() }.next[0]
            .compare_exchange(
                cursor.preds[0],
                cursor.succs[0],
                new_node,
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .is_err()
        {
            return Err(());
        }

        // The new node was successfully installed.
        // Build the rest of the tower above level 0.
        for level in 1..height {
            if self.insert_level(&cursor, level, new_node, guard).is_err() {
                new_node_ref
                    .refs
                    .fetch_sub(height - level, Ordering::SeqCst);
                break;
            }
        }

        let _ = Node::decrement(new_node, guard);
        Ok(true)
    }

    fn insert_level(
        &self,
        cursor: &Cursor<K, V>,
        level: usize,
        new_node: Shared<Node<K, V>>,
        guard: &Guard<Node<K, V>>,
    ) -> Result<(), ()> {
        let new_node_ref = unsafe { new_node.deref() };

        let pred = cursor.preds[level];
        let succ = cursor.succs[level];
        let next = new_node_ref.next[level].load(Ordering::SeqCst, guard)?;

        // If the current pointer is marked, that means another thread is already
        // removing the node we've just inserted. In that case, let's just stop
        // building the tower.
        let next_tag = next.tag()?;
        if (next_tag & 1) != 0 {
            return Err(());
        }

        new_node_ref.next[level]
            .compare_exchange(
                new_node,
                next,
                succ,
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .map_err(|_| ())?;

        // Try installing the new node at the current level.
        unsafe { pred.deref() }.next[level]
            .compare_exchange(
                pred,
                succ,
                new_node,
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .map(|_| ())
            .map_err(|_| ())
    }

    fn insert(&self, key: K, value: V, local: &Local<Node<K, V>>) -> bool {
        let guard = &mut local.guard();
        let height = generate_height();
        let new_node = loop {
            match Node::new(key, value, height, 2, guard) {
                Ok(node) => break node,
                Err(_) => guard.refresh(),
            }
        };

        loop {
            guard.refresh();
            match self.insert_inner(key, new_node, height, guard) {
                Ok(inserted) => {
                    if !inserted {
                        let _ = unsafe { guard.retire(new_node) };
                    }
                    return inserted;
                }
                _ => continue,
            }
        }
    }

    pub fn remove(&self, key: &K, local: &Local<Node<K, V>>) -> Option<V> {
        'outer: loop {
            let guard = &mut local.guard();
            let cursor = match self.find(key, guard) {
                Ok(cursor) => cursor,
                Err(_) => continue,
            };
            let node = cursor.found?;
            let height = ok_or!(unsafe { node.deref() }.height.get(guard), continue);
            let value = ok_or!(unsafe { node.deref() }.value.get(guard), continue);

            // Try removing the node by marking its tower.
            let marked = ok_or!(Node::mark_tower(node, guard), continue);
            if marked {
                for level in (0..height).rev() {
                    let succ = ok_or!(
                        unsafe { node.deref() }.next[level].load(Ordering::SeqCst, guard),
                        continue 'outer
                    );
                    let succ_tag = ok_or!(succ.tag(), continue 'outer);
                    if (succ_tag & 2) != 0 {
                        continue;
                    }

                    // Try linking the predecessor and successor at this level.
                    if unsafe { cursor.preds[level].deref() }.next[level]
                        .compare_exchange(
                            cursor.preds[level],
                            node,
                            succ.with_tag(0),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        )
                        .is_ok()
                    {
                        let _ = Node::decrement(node, guard);
                    } else {
                        guard.refresh();
                        let _ = self.find(key, guard);
                        break;
                    }
                }
            }
            return Some(value);
        }
    }
}

impl<K, V> ConcurrentMap<K, V> for SkipList<K, V>
where
    K: 'static + Ord + Copy,
    V: 'static + Copy,
{
    type Global = Global<Node<K, V>>;
    type Local = Local<Node<K, V>>;

    fn global(key_range_hint: usize) -> Self::Global {
        Global::new(key_range_hint * 2)
    }

    fn local(global: &Self::Global) -> Self::Local {
        Local::new(global)
    }

    fn new(local: &Self::Local) -> Self {
        SkipList::new(local)
    }

    #[inline(always)]
    fn get<'g>(&'g self, key: &'g K, local: &Self::Local) -> Option<V> {
        loop {
            let guard = &local.guard();
            let cursor = ok_or!(self.find_optimistic(key, guard), continue);
            if let Some(node) = cursor.found {
                let value = ok_or!(unsafe { node.deref() }.value.get(guard), continue);
                return Some(value);
            } else {
                return None;
            }
        }
    }

    #[inline(always)]
    fn insert(&self, key: K, value: V, local: &Self::Local) -> bool {
        self.insert(key, value, local)
    }

    #[inline(always)]
    fn remove<'g>(&'g self, key: &'g K, local: &Self::Local) -> Option<V> {
        self.remove(key, local)
    }
}

#[cfg(test)]
mod tests {
    use super::SkipList;
    use crate::ds_impl::vbr::concurrent_map;

    #[test]
    fn smoke_skip_list() {
        concurrent_map::tests::smoke::<SkipList<i32, i32>>();
    }
}
