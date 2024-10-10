use super::concurrent_map::ConcurrentMap;
use crossbeam_ebr::{unprotected, Atomic, Guard, Owned, Pointer, Shared};

use std::cmp::Ordering::{Equal, Greater, Less};
use std::marker::PhantomData;
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicUsize, Ordering};

// Copied from the original author's code:
// https://gitlab.com/trbot86/setbench/-/blob/f4711af3ace28d8b4fa871559db74fb4e0e62cc0/ds/srivastava_abtree_mcs/adapter.h#L17
const DEGREE: usize = 11;

struct MCSLock<K, V> {
    _marker: PhantomData<(K, V)>,
}

impl<K, V> MCSLock<K, V> {
    fn new() -> Self {
        todo!()
    }

    fn acquire(&self) -> MCSLockGuard<K, V> {
        todo!()
    }
}

struct MCSLockGuard<K, V> {
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Drop for MCSLockGuard<K, V> {
    fn drop(&mut self) {
        todo!("release lock")
    }
}

struct Node<K, V> {
    keys: [K; DEGREE],
    search_key: K,
    lock: MCSLock<K, V>,
    size: usize,
    weight: bool,
    marked: AtomicBool,
    kind: NodeSpecific<K, V>,
}

// Leaf or Internal node specific data.
enum NodeSpecific<K, V> {
    Leaf {
        values: [V; DEGREE],
        write_version: AtomicUsize,
    },
    Internal {
        next: [Atomic<Node<K, V>>; DEGREE],
    },
}

impl<K, V> Node<K, V>
where
    K: PartialOrd + Eq + Default + Copy,
    V: Default + Copy,
{
    fn internal(weight: bool, size: usize, search_key: K) -> Self {
        Self {
            keys: Default::default(),
            search_key,
            lock: MCSLock::new(),
            size,
            weight,
            marked: AtomicBool::new(false),
            kind: NodeSpecific::Internal {
                next: Default::default(),
            },
        }
    }

    fn leaf(weight: bool, size: usize, search_key: K) -> Self {
        Self {
            keys: Default::default(),
            search_key,
            lock: MCSLock::new(),
            size,
            weight,
            marked: AtomicBool::new(false),
            kind: NodeSpecific::Leaf {
                values: Default::default(),
                write_version: AtomicUsize::new(0),
            },
        }
    }

    fn next(&self) -> &[Atomic<Self>; DEGREE] {
        match &self.kind {
            NodeSpecific::Internal { next } => next,
            _ => panic!("No next pointers for a leaf node."),
        }
    }

    fn key_count(&self) -> usize {
        match &self.kind {
            NodeSpecific::Leaf { .. } => self.size,
            NodeSpecific::Internal { .. } => self.size - 1,
        }
    }

    fn child_index(&self, key: &K) -> usize {
        let mut index = 0;
        while index < self.key_count() && !(key < &self.keys[index]) {
            index += 1;
        }
        index
    }

    // Search a node for a key repeatedly until we successfully read a consistent version.
    fn read_value_version(&self, key: &K) -> (usize, Option<V>, usize) {
        if let NodeSpecific::Leaf { values, write_version } = &self.kind {
            loop {
                let mut version = write_version.load(Ordering::Acquire);
                while version & 1 > 0 {
                    version = write_version.load(Ordering::Acquire);
                }
                let mut key_index = 0;
                while key_index < DEGREE && self.keys[key_index] != *key {
                    key_index += 1;
                }
                let value = if key_index < DEGREE { Some(values[key_index]) } else { None };
                compiler_fence(Ordering::SeqCst);
                
                if version == write_version.load(Ordering::Acquire) {
                    return (key_index, value, version);
                }
            }
        }
        panic!("Attempted to read value from an internal node.")
    }
}

enum Operation {
    Insert,
    Delete,
    Balance,
}

struct Cursor<'g, K, V> {
    l: Shared<'g, Node<K, V>>,
    p: Shared<'g, Node<K, V>>,
    gp: Shared<'g, Node<K, V>>,
    /// Index of `p` in `gp`.
    gp_p_idx: usize,
    /// Index of `l` in `p`.
    p_l_idx: usize,
    /// Index of the key in `l`.
    l_key_idx: usize,
    val: V,
    node_version: usize,
}

struct ElimABTree<K, V> {
    entry: Node<K, V>,
}

impl<K, V> ElimABTree<K, V>
where
    K: PartialOrd + Eq + Default + Copy,
    V: Default + Copy,
{
    /// `a` in the original code... TODO: remove later.
    fn a() -> usize {
        (DEGREE / 4).max(2)
    }

    /// `b` in the original code... TODO: remove later.
    fn b() -> usize {
        DEGREE
    }

    pub fn new() -> Self {
        let left = Node::leaf(true, 0, K::default());
        let entry = Node::internal(true, 1, K::default());
        entry.next()[0].store(Owned::new(left), Ordering::Relaxed);
        Self { entry }
    }

    /// Performs a basic search and returns the value associated with the key,
    /// or `None` if nothing is found. Unlike other search methods, it does not return
    /// any path information, making it slightly faster.
    pub fn search_basic(&self, key: &K, guard: &Guard) -> Option<V> {
        let mut node = unsafe { self.entry.next()[0].load(Ordering::Acquire, guard).deref() };
        while let NodeSpecific::Internal { next } = &node.kind {
            let next = next[node.child_index(key)].load(Ordering::Acquire, guard);
            node = unsafe { next.deref() };
        }
        node.read_value_version(key).1
    }

    pub fn search<'g>(&self, key: &K, target: Option<Shared<'g, Node<K, V>>>, guard: &'g Guard) -> Cursor<'g, K, V> {
        // let mut cursor = Cursor::
        // let mut gp = Shared::null();
        // let mut p = unsafe { Shared::<Node<K, V>>::from_usize(&self.entry as *const _ as usize) };
        // let mut p_l_idx = 0;
        todo!()
    }
}

impl<K, V> Drop for ElimABTree<K, V> {
    fn drop(&mut self) {
        todo!()
    }
}
