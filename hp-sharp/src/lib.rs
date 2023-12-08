// To use  `#[cfg(sanitize = "address")]`
#![feature(cfg_sanitize)]
#![feature(thread_local)]

mod deferred;
mod epoch;
mod handle;
mod hazard;
mod internal;
mod pile;
mod pointers;
mod rollback;

pub use handle::*;
pub use internal::*;
pub use pointers::*;

use std::cell::RefCell;

pub static GLOBAL: Global = Global::new();

thread_local! {
    pub static THREAD: RefCell<Box<Thread>> = RefCell::new(Box::new(GLOBAL.register()));
}

#[cfg(test)]
mod test {
    use std::thread::scope;

    use atomic::Ordering;

    use super::THREAD;
    use crate::handle::{CsGuard, Invalidate, RollbackProof, Thread};
    use crate::pointers::{Atomic, Owned, Protector, Shared, Shield};

    struct Node {
        next: Atomic<Node>,
    }

    impl Invalidate for Node {
        fn is_invalidated(&self, _: &CsGuard) -> bool {
            // We do not use traverse_loop here.
            false
        }
    }

    struct Cursor {
        prev: Shield<Node>,
        curr: Shield<Node>,
    }

    impl Protector for Cursor {
        type Target<'r> = SharedCursor<'r>;

        fn empty(thread: &mut crate::Thread) -> Self {
            Self {
                prev: Shield::null(thread),
                curr: Shield::null(thread),
            }
        }

        fn protect_unchecked(&mut self, read: &Self::Target<'_>) {
            self.prev.protect_unchecked(&read.prev);
            self.curr.protect_unchecked(&read.curr);
        }

        fn as_target<'r>(&self, guard: &'r CsGuard) -> Option<Self::Target<'r>> {
            Some(SharedCursor {
                prev: self.prev.as_target(guard)?,
                curr: self.curr.as_target(guard)?,
            })
        }

        fn release(&mut self) {
            self.prev.release();
            self.curr.release();
        }
    }

    #[derive(Clone, Copy)]
    struct SharedCursor<'r> {
        prev: Shared<'r, Node>,
        curr: Shared<'r, Node>,
    }

    const THREADS: usize = 30;
    const COUNT_PER_THREAD: usize = 1 << 15;

    #[test]
    fn double_node() {
        let head = Atomic::new(Node {
            next: Atomic::new(Node {
                next: Atomic::null(),
            }),
        });
        scope(|s| {
            for _ in 0..THREADS {
                s.spawn(|| THREAD.with(|th| double_node_work(&mut *th.borrow_mut(), &head)));
            }
        });
        unsafe {
            let head = head.into_owned();
            drop(
                head.next
                    .load(Ordering::Relaxed, &CsGuard::unprotected())
                    .into_owned(),
            );
        }
    }

    fn double_node_work(thread: &mut Thread, head: &Atomic<Node>) {
        let mut cursor = Cursor::empty(thread);
        for _ in 0..COUNT_PER_THREAD {
            loop {
                unsafe {
                    cursor.traverse(thread, |guard| {
                        let mut cursor = SharedCursor {
                            prev: Shared::null(),
                            curr: Shared::null(),
                        };

                        cursor.prev = head.load(Ordering::Acquire, guard);
                        cursor.curr = cursor
                            .prev
                            .as_ref(guard)
                            .unwrap()
                            .next
                            .load(Ordering::Acquire, guard);

                        Some(cursor)
                    });
                }

                let new = Owned::new(Node {
                    next: Atomic::new(Node {
                        next: Atomic::null(),
                    }),
                });

                match head.compare_exchange(
                    cursor.prev.shared(),
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    thread,
                ) {
                    Ok(_) => unsafe {
                        thread.retire(cursor.prev.shared());
                        thread.retire(cursor.curr.shared());
                        break;
                    },
                    Err(e) => unsafe {
                        let new = e.new;
                        drop(
                            new.next
                                .load(Ordering::Relaxed, &CsGuard::unprotected())
                                .into_owned(),
                        );
                    },
                }
            }
        }
    }
}
