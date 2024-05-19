// To use  `#[cfg(sanitize = "address")]`
#![feature(cfg_sanitize)]
#![feature(thread_local)]

mod deferred;
mod epoch;
mod handle;
mod hazard;
mod internal;
mod pointers;
mod queue;
mod rollback;

pub use handle::*;
pub use internal::*;
pub use pointers::*;

use std::{cell::RefCell, sync::OnceLock};

static GLOBAL: OnceLock<Global> = OnceLock::new();

#[inline]
pub fn global() -> &'static Global {
    GLOBAL.get_or_init(Global::new)
}

thread_local! {
    pub static THREAD: RefCell<Box<Thread>> = RefCell::new(Box::new(global().register()));
}

#[cfg(test)]
mod test {
    use std::thread::scope;

    use atomic::Ordering;

    use super::THREAD;
    use crate::handle::{RollbackProof, Thread, Unprotected};
    use crate::pointers::{Atomic, Owned, Shield};

    struct Node {
        next: Atomic<Node>,
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
