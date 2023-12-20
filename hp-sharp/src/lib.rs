// To use  `#[cfg(sanitize = "address")]`
#![feature(cfg_sanitize)]
#![feature(thread_local)]
#![feature(const_maybe_uninit_zeroed)]

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
    use crate::pointers::{Atomic, Owned, Shield};
    use crate::Unprotected;

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

    impl Cursor {
        fn default(thread: &mut crate::Thread) -> Self {
            Self {
                prev: Shield::null(thread),
                curr: Shield::null(thread),
            }
        }
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
                    .load(Ordering::Relaxed, &Unprotected::new())
                    .into_owned(),
            );
        }
    }

    fn double_node_work(thread: &mut Thread, head: &Atomic<Node>) {
        let mut cursor = Cursor::default(thread);
        for _ in 0..COUNT_PER_THREAD {
            loop {
                unsafe {
                    thread.critical_section(|guard| {
                        let prev = head.load(Ordering::Acquire, guard);
                        let curr = prev.as_ref().unwrap().next.load(Ordering::Acquire, guard);
                        cursor.prev.protect(prev);
                        cursor.curr.protect(curr);
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
                                .load(Ordering::Relaxed, &Unprotected::new())
                                .into_owned(),
                        );
                    },
                }
            }
        }
    }
}
