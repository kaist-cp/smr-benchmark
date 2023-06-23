//! A lock-free pile.

use std::ptr::null_mut;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

/// A lock-free pile, which we can push an element or pop all elements.
#[derive(Debug)]
pub struct Pile<T> {
    head: AtomicPtr<Node<T>>,
}

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: AtomicPtr<Node<T>>,
}

impl<T> Pile<T> {
    /// Creates a new, empty pile.
    pub const fn new() -> Pile<T> {
        Pile {
            head: AtomicPtr::new(null_mut()),
        }
    }

    /// Pushes a value on top of the pile.
    pub fn push(&self, t: T) {
        let n = Box::into_raw(Box::new(Node {
            data: t,
            next: AtomicPtr::new(null_mut()),
        }));

        loop {
            let head = self.head.load(Relaxed);
            unsafe { &*n }.next.store(head, Relaxed);

            if self
                .head
                .compare_exchange(head, n, Release, Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn append(&self, mut iter: impl Iterator<Item = T>) {
        let Some(first_value) = iter.next() else { return; };
        let first_node = Box::into_raw(Box::new(Node {
            data: first_value,
            next: AtomicPtr::new(null_mut()),
        }));
        let mut last_node = first_node;

        while let Some(value) = iter.next() {
            let node = Box::into_raw(Box::new(Node {
                data: value,
                next: AtomicPtr::new(null_mut()),
            }));
            unsafe { &*last_node }.next.store(node, Relaxed);
            last_node = node;
        }

        loop {
            let head = self.head.load(Relaxed);
            unsafe { &*last_node }.next.store(head, Relaxed);

            if self
                .head
                .compare_exchange(head, first_node, Release, Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Attempts to pop all elements from the pile.
    ///
    /// Returns `None` if the pile is empty.
    #[must_use]
    pub fn pop_all(&self) -> Vec<T> {
        let mut result = vec![];
        let mut node = self.head.swap(null_mut(), AcqRel);
        while !node.is_null() {
            let node_owned = unsafe { Box::from_raw(node) };
            let data = node_owned.data;
            result.push(data);
            node = node_owned.next.load(Acquire);
        }
        result
    }
}

impl<T> Default for Pile<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Pile<T> {
    fn drop(&mut self) {
        drop(self.pop_all());
    }
}

#[test]
fn seq_append_pop() {
    let pile = Pile::new();
    pile.push(1);
    pile.push(2);
    pile.append(vec![6, 5, 4, 3].into_iter());
    pile.push(7);
    assert_eq!(pile.pop_all(), vec![7, 6, 5, 4, 3, 2, 1]);
}
