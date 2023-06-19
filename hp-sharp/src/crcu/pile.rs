//! A lock-free pile.

use std::mem::ManuallyDrop;
use std::ptr::{self, null_mut};
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

/// A lock-free pile, which we can push an element or pop all elements.
#[derive(Debug)]
pub struct Pile<T> {
    head: AtomicPtr<Node<T>>,
}

#[derive(Debug)]
struct Node<T> {
    data: ManuallyDrop<T>,
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
        let mut n = Box::into_raw(Box::new(Node {
            data: ManuallyDrop::new(t),
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
            data: ManuallyDrop::new(first_value),
            next: AtomicPtr::new(null_mut()),
        }));
        let mut last_node = first_node;

        while let Some(value) = iter.next() {
            let node = Box::into_raw(Box::new(Node {
                data: ManuallyDrop::new(value),
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
        let node = self.head.swap(null_mut(), AcqRel);
        while !node.is_null() {
            let node_ref = unsafe { &*node };
            let data = ManuallyDrop::into_inner(unsafe { ptr::read(&(*node_ref).data) });
            drop(unsafe { Box::from_raw(node) });
            result.push(data);
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
