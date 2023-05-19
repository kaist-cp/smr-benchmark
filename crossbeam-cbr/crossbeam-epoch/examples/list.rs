extern crate crossbeam_cbr_epoch;

use crossbeam_cbr_epoch::{
    rc::{AcquiredPtr, AtomicRcPtr, LocalPtr, Localizable, RcPtr, ReadPtr},
    EpochGuard, ReadGuard, ReadStatus,
};

struct Node<K, V> {
    next: AtomicRcPtr<Self>,
    key: K,
    value: V,
}

struct List<K, V> {
    head: AtomicRcPtr<Node<K, V>>,
}

impl<K, V> Node<K, V>
where
    K: Default,
    V: Default,
{
    /// Creates a new node.
    fn new(key: K, value: V) -> Self {
        Self {
            next: AtomicRcPtr::null(),
            key,
            value,
        }
    }

    /// Creates a dummy head.
    /// We never deref key and value of this head node.
    fn head() -> Self {
        Self {
            next: AtomicRcPtr::null(),
            key: K::default(),
            value: V::default(),
        }
    }
}

/// TODO(@jeonghyeon): implement `#[derive(Localizable)]`,
/// so that `LocalizedCursor` and the trait implementation
/// is generated automatically.
struct Cursor<'r, K, V> {
    prev: ReadPtr<'r, Node<K, V>>,
    prev_next: ReadPtr<'r, Node<K, V>>,
    // Tag of `curr` should always be zero so when `curr` is stored in a `prev`, we don't store a
    // marked pointer and cause cleanup to fail.
    curr: ReadPtr<'r, Node<K, V>>,
    found: bool,
}

/// This struct definition must be generated automatically by a `derive` macro.
struct LocalizedCursor<K, V> {
    prev: LocalPtr<Node<K, V>>,
    prev_next: LocalPtr<Node<K, V>>,
    curr: LocalPtr<Node<K, V>>,
    found: bool,
}

/// This trait implementation must be generated automatically by a `derive` macro.
impl<'r, K, V> Localizable<'r> for Cursor<'r, K, V> {
    type Localized = LocalizedCursor<K, V>;

    fn protect_with(self, guard: &EpochGuard) -> Self::Localized {
        Self::Localized {
            prev: self.prev.protect_with(guard),
            prev_next: self.prev_next.protect_with(guard),
            curr: self.curr.protect_with(guard),
            found: self.found,
        }
    }
}

impl<'r, K: Ord, V> Cursor<'r, K, V> {
    /// Creates a cursor.
    fn new(head: &'r AtomicRcPtr<Node<K, V>>, guard: &'r ReadGuard) -> Self {
        let prev = head.load_read(guard);
        let curr = prev.as_ref().unwrap().next.load_read(guard);
        Self {
            prev,
            prev_next: curr,
            curr,
            found: false,
        }
    }
}

impl<K, V> List<K, V>
where
    K: Default + Ord,
    V: Default,
{
    pub fn new() -> Self {
        List {
            head: AtomicRcPtr::new(Node::head()),
        }
    }

    pub fn harris_find(&self, key: &K, guard: &mut EpochGuard) -> LocalizedCursor<K, V> {
        loop {
            let cursor = guard.read_loop(
                |guard| Cursor::new(&self.head, guard),
                |cursor, guard| {
                    let curr_node = match cursor.curr.as_ref() {
                        Some(node) => node,
                        None => {
                            cursor.found = false;
                            return ReadStatus::Finished;
                        }
                    };

                    let next = curr_node.next.load_read(guard);
                    if next.tag() > 0 {
                        cursor.curr = next.with_tag(0);
                        return ReadStatus::Continue;
                    }

                    match curr_node.key.cmp(key) {
                        std::cmp::Ordering::Less => {
                            cursor.prev = cursor.curr;
                            cursor.prev_next = next;
                            cursor.curr = next;
                            return ReadStatus::Continue;
                        }
                        std::cmp::Ordering::Equal => cursor.found = true,
                        std::cmp::Ordering::Greater => cursor.found = false,
                    }
                    ReadStatus::Finished
                },
            );

            // Perform Clean-up CAS and return the cursor.
            if cursor.prev_next.as_raw() == cursor.curr.as_raw()
                || cursor.prev.as_ref().unwrap().next.try_compare_exchange(
                    &cursor.prev_next,
                    &cursor.curr,
                    guard,
                )
            {
                return cursor;
            }
        }
    }

    pub fn get(&self, key: &K, guard: &mut EpochGuard) -> Option<LocalizedCursor<K, V>> {
        let cursor = self.harris_find(key, guard);
        if cursor.found {
            Some(cursor)
        } else {
            None
        }
    }

    pub fn insert(&self, key: K, value: V, guard: &mut EpochGuard) -> Result<(), (K, V)> {
        let mut new_node = Node::new(key, value);
        loop {
            let cursor = self.harris_find(&new_node.key, guard);
            if cursor.found {
                return Err((new_node.key, new_node.value));
            }

            new_node.next.store(&cursor.curr, guard);
            let new_node_ptr = RcPtr::from_obj(new_node);

            if cursor.prev.as_ref().unwrap().next.try_compare_exchange(
                &cursor.curr,
                &new_node_ptr,
                guard,
            ) {
                return Ok(());
            } else {
                // Safety: As we failed to insert `new_node_ptr` into the data structure,
                // only current thread has a reference to this node.
                new_node = unsafe { new_node_ptr.into_owned() };
            }
        }
    }

    pub fn remove(&self, key: &K, guard: &mut EpochGuard) -> Option<LocalizedCursor<K, V>> {
        loop {
            let cursor = self.harris_find(key, guard);
            if !cursor.found {
                return None;
            }

            let curr_node = cursor.curr.as_ref().unwrap();
            let next = curr_node.next.load_local(guard);
            if next.tag() > 0 || !curr_node.next.try_compare_exchange_tag(&next, 1, guard) {
                continue;
            }

            cursor
                .prev
                .as_ref()
                .unwrap()
                .next
                .try_compare_exchange(&cursor.curr, &next, guard);

            return Some(cursor);
        }
    }
}

fn main() {}
