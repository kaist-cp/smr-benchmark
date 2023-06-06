use core::fmt;
use core::hint::black_box;
use core::mem;
use core::mem::transmute;
use core::ptr;
use core::sync::atomic::compiler_fence;
use core::sync::atomic::fence;
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering;

use nix::sys::signal::pthread_sigmask;
use nix::sys::signal::SigmaskHow;
use nix::sys::signalfd::SigSet;

use crate::pebr_backend::{
    atomic::Shared, collector::Collector, deferred::Deferred, deferred::DeferredWithHazard,
    garbage::Garbage, internal::Local, recovery,
};

/// A guard that keeps the current thread pinned.
///
/// # Pinning
///
/// The current thread is pinned by calling [`pin`], which returns a new guard:
///
/// ```
/// use crossbeam_cbr_epoch as epoch;
///
/// // It is often convenient to prefix a call to `pin` with a `&` in order to create a reference.
/// // This is not really necessary, but makes passing references to the guard a bit easier.
/// let guard = &epoch::pin();
/// ```
///
/// When a guard gets dropped, the current thread is automatically unpinned.
///
/// # Pointers on the stack
///
/// Having a guard allows us to create pointers on the stack to heap-allocated objects.
/// For example:
///
/// ```
/// use crossbeam_cbr_epoch::pebr_backend::{self as epoch, Atomic, Owned};
/// use std::sync::atomic::Ordering::SeqCst;
///
/// // Create a heap-allocated number.
/// let a = Atomic::new(777);
///
/// // Pin the current thread.
/// let guard = &epoch::pin();
///
/// // Load the heap-allocated object and create pointer `p` on the stack.
/// let p = a.load(SeqCst, guard);
///
/// // Dereference the pointer and print the value:
/// if let Some(num) = unsafe { p.as_ref() } {
///     println!("The number is {}.", num);
/// }
/// ```
///
/// # Multiple guards
///
/// Pinning is reentrant and it is perfectly legal to create multiple guards. In that case, the
/// thread will actually be pinned only when the first guard is created and unpinned when the last
/// one is dropped:
///
/// ```
/// use crossbeam_cbr_epoch as epoch;
///
/// let guard1 = epoch::pin();
/// let guard2 = epoch::pin();
/// drop(guard1);
/// assert!(epoch::is_pinned());
/// drop(guard2);
/// assert!(!epoch::is_pinned());
/// ```
///
/// [`pin`]: fn.pin.html
pub struct EpochGuard {
    pub(crate) local: *const Local,
}

impl EpochGuard {
    #[inline]
    unsafe fn defer_garbage(&self, garbage: Garbage, internal: bool) {
        if let Some(local) = self.local.as_ref() {
            local.defer(garbage, self, internal);
        } else {
            garbage.dispose();
        }
    }

    /// Stores a function so that it can be executed at some point after all currently pinned
    /// threads get unpinned.
    ///
    /// This method first stores `f` into the thread-local (or handle-local) cache. If this cache
    /// becomes full, some functions are moved into the global cache. At the same time, some
    /// functions from both local and global caches may get executed in order to incrementally
    /// clean up the caches as they fill up.
    ///
    /// There is no guarantee when exactly `f` will be executed. The only guarantee is that it
    /// won't be executed until all currently pinned threads get unpinned. In theory, `f` might
    /// never run, but the epoch-based garbage collection will make an effort to execute it
    /// reasonably soon.
    ///
    /// If this method is called from an [`unprotected`] guard, the function will simply be
    /// executed immediately.
    ///
    /// [`unprotected`]: fn.unprotected.html
    #[inline]
    pub fn defer<F, R>(&self, f: F)
    where
        F: FnOnce() -> R,
        F: Send + 'static,
    {
        unsafe {
            self.defer_unchecked(f);
        }
    }

    /// Stores a function so that it can be executed at some point after all currently pinned
    /// threads get unpinned.
    ///
    /// This method first stores `f` into the thread-local (or handle-local) cache. If this cache
    /// becomes full, some functions are moved into the global cache. At the same time, some
    /// functions from both local and global caches may get executed in order to incrementally
    /// clean up the caches as they fill up.
    ///
    /// There is no guarantee when exactly `f` will be executed. The only guarantee is that it
    /// won't be executed until all currently pinned threads get unpinned. In theory, `f` might
    /// never run, but the epoch-based garbage collection will make an effort to execute it
    /// reasonably soon.
    ///
    /// If this method is called from an [`unprotected`] guard, the function will simply be
    /// executed immediately.
    ///
    /// # Safety
    ///
    /// The given function must not hold reference onto the stack. It is highly recommended that
    /// the passed function is **always** marked with `move` in order to prevent accidental
    /// borrows.
    ///
    /// ```
    /// use crossbeam_cbr_epoch as epoch;
    ///
    /// let guard = &epoch::pin();
    /// let message = "Hello!";
    /// unsafe {
    ///     // ALWAYS use `move` when sending a closure into `defer_unchecked`.
    ///     guard.defer_unchecked(move || {
    ///         println!("{}", message);
    ///     });
    /// }
    /// ```
    ///
    /// Apart from that, keep in mind that another thread may execute `f`, so anything accessed by
    /// the closure must be `Send`.
    ///
    /// We intentionally didn't require `F: Send`, because Rust's type systems usually cannot prove
    /// `F: Send` for typical use cases. For example, consider the following code snippet, which
    /// exemplifies the typical use case of deferring the deallocation of a shared reference:
    ///
    /// ```ignore
    /// let shared = Owned::new(7i32).into_shared(guard);
    /// guard.defer_unchecked(move || shared.into_owned()); // `Shared` is not `Send`!
    /// ```
    ///
    /// While `Shared` is not `Send`, it's safe for another thread to call the deferred function,
    /// because it's called only after the grace period and `shared` is no longer shared with other
    /// threads. But we don't expect type systems to prove this.
    ///
    /// # Examples
    ///
    /// When a heap-allocated object in a data structure becomes unreachable, it has to be
    /// deallocated. However, the current thread and other threads may be still holding references
    /// on the stack to that same object. Therefore it cannot be deallocated before those references
    /// get dropped. This method can defer deallocation until all those threads get unpinned and
    /// consequently drop all their references on the stack.
    ///
    /// ```
    /// use crossbeam_cbr_epoch::pebr_backend::{self as epoch, Atomic, Owned};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new("foo");
    ///
    /// // Now suppose that `a` is shared among multiple threads and concurrently
    /// // accessed and modified...
    ///
    /// // Pin the current thread.
    /// let guard = &epoch::pin();
    ///
    /// // Steal the object currently stored in `a` and swap it with another one.
    /// let p = a.swap(Owned::new("bar").into_shared(guard), SeqCst, guard);
    ///
    /// if !p.is_null() {
    ///     // The object `p` is pointing to is now unreachable.
    ///     // Defer its deallocation until all currently pinned threads get unpinned.
    ///     unsafe {
    ///         // ALWAYS use `move` when sending a closure into `defer_unchecked`.
    ///         guard.defer_unchecked(move || {
    ///             println!("{} is now being deallocated.", p.deref());
    ///             // Now we have unique access to the object pointed to by `p` and can turn it
    ///             // into an `Owned`. Dropping the `Owned` will deallocate the object.
    ///             drop(p.into_owned());
    ///         });
    ///     }
    /// }
    /// ```
    ///
    /// [`unprotected`]: fn.unprotected.html
    pub unsafe fn defer_unchecked<F, R>(&self, f: F)
    where
        F: FnOnce() -> R,
    {
        self.defer_garbage(
            Garbage::Deferred {
                inner: Deferred::new(move || drop(f())),
            },
            false,
        );
    }

    /// Stores a destructor for an object so that it can be deallocated and dropped at some point
    /// after all currently pinned threads get unpinned.
    ///
    /// This method first stores the destructor into the thread-local (or handle-local) cache. If
    /// this cache becomes full, some destructors are moved into the global cache. At the same
    /// time, some destructors from both local and global caches may get executed in order to
    /// incrementally clean up the caches as they fill up.
    ///
    /// There is no guarantee when exactly the destructor will be executed. The only guarantee is
    /// that it won't be executed until all currently pinned threads get unpinned. In theory, the
    /// destructor might never run, but the epoch-based garbage collection will make an effort to
    /// execute it reasonably soon.
    ///
    /// If this method is called from an [`unprotected`] guard, the destructor will simply be
    /// executed immediately.
    ///
    /// # Safety
    ///
    /// The object must not be reachable by other threads anymore, otherwise it might be still in
    /// use when the destructor runs.
    ///
    /// Apart from that, keep in mind that another thread may execute the destructor, so the object
    /// must be sendable to other threads.
    ///
    /// We intentionally didn't require `T: Send`, because Rust's type systems usually cannot prove
    /// `T: Send` for typical use cases. For example, consider the following code snippet, which
    /// exemplifies the typical use case of deferring the deallocation of a shared reference:
    ///
    /// ```ignore
    /// let shared = Owned::new(7i32).into_shared(guard);
    /// guard.defer_destroy(shared); // `Shared` is not `Send`!
    /// ```
    ///
    /// While `Shared` is not `Send`, it's safe for another thread to call the destructor, because
    /// it's called only after the grace period and `shared` is no longer shared with other
    /// threads. But we don't expect type systems to prove this.
    ///
    /// # Examples
    ///
    /// When a heap-allocated object in a data structure becomes unreachable, it has to be
    /// deallocated. However, the current thread and other threads may be still holding references
    /// on the stack to that same object. Therefore it cannot be deallocated before those references
    /// get dropped. This method can defer deallocation until all those threads get unpinned and
    /// consequently drop all their references on the stack.
    ///
    /// ```
    /// use crossbeam_cbr_epoch::pebr_backend::{self as epoch, Atomic, Owned};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new("foo");
    ///
    /// // Now suppose that `a` is shared among multiple threads and concurrently
    /// // accessed and modified...
    ///
    /// // Pin the current thread.
    /// let guard = &epoch::pin();
    ///
    /// // Steal the object currently stored in `a` and swap it with another one.
    /// let p = a.swap(Owned::new("bar").into_shared(guard), SeqCst, guard);
    ///
    /// if !p.is_null() {
    ///     // The object `p` is pointing to is now unreachable.
    ///     // Defer its deallocation until all currently pinned threads get unpinned.
    ///     unsafe {
    ///         guard.defer_destroy(p);
    ///     }
    /// }
    /// ```
    ///
    /// [`unprotected`]: fn.unprotected.html
    #[inline]
    pub unsafe fn defer_destroy<T>(&self, ptr: Shared<T>) {
        unsafe fn dtor<T>(data: usize) {
            drop(Shared::from(data as *const T).into_owned());
        }

        self.defer_garbage(
            Garbage::Destroy {
                data: ptr.as_raw() as usize,
                dtor: dtor::<T>,
            },
            false,
        );
    }

    #[inline]
    pub(crate) unsafe fn defer_destroy_internal<T>(&self, ptr: Shared<T>) {
        unsafe fn dtor<T>(data: usize) {
            drop(Shared::from(data as *const T).into_owned());
        }

        self.defer_garbage(
            Garbage::Destroy {
                data: ptr.as_raw() as usize,
                dtor: dtor::<T>,
            },
            true,
        );
    }

    /// Clears up the thread-local cache of deferred functions by executing them or moving into the
    /// global cache.
    ///
    /// Call this method after deferring execution of a function if you want to get it executed as
    /// soon as possible. Flushing will make sure it is residing in in the global cache, so that
    /// any thread has a chance of taking the function and executing it.
    ///
    /// If this method is called from an [`unprotected`] guard, it is a no-op (nothing happens).
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_cbr_epoch as epoch;
    ///
    /// let guard = &epoch::pin();
    /// unsafe {
    ///     guard.defer(move || {
    ///         println!("This better be printed as soon as possible!");
    ///     });
    /// }
    /// guard.flush();
    /// ```
    ///
    /// [`unprotected`]: fn.unprotected.html
    pub fn flush(&self) {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.flush(self);
        }
    }

    /// Unpins and then immediately re-pins the thread.
    ///
    /// This method is useful when you don't want delay the advancement of the global epoch by
    /// holding an old epoch. For safety, you should not maintain any guard-based reference across
    /// the call (the latter is enforced by `&mut self`). The thread will only be repinned if this
    /// is the only active guard for the current thread.
    ///
    /// If this method is called from an [`unprotected`] guard, then the call will be just no-op.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_cbr_epoch::pebr_backend::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let a = Atomic::new(777);
    /// let mut guard = epoch::pin();
    /// {
    ///     let p = a.load(SeqCst, &guard);
    ///     assert_eq!(unsafe { p.as_ref() }, Some(&777));
    /// }
    /// guard.repin();
    /// {
    ///     let p = a.load(SeqCst, &guard);
    ///     assert_eq!(unsafe { p.as_ref() }, Some(&777));
    /// }
    /// ```
    ///
    /// [`unprotected`]: fn.unprotected.html
    pub fn repin(&mut self) {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.repin();
        }
    }

    /// Temporarily unpins the thread, executes the given function and then re-pins the thread.
    ///
    /// This method is useful when you need to perform a long-running operation (e.g. sleeping)
    /// and don't need to maintain any guard-based reference across the call (the latter is enforced
    /// by `&mut self`). The thread will only be unpinned if this is the only active guard for the
    /// current thread.
    ///
    /// If this method is called from an [`unprotected`] guard, then the passed function is called
    /// directly without unpinning the thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_cbr_epoch::pebr_backend::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let a = Atomic::new(777);
    /// let mut guard = epoch::pin();
    /// {
    ///     let p = a.load(SeqCst, &guard);
    ///     assert_eq!(unsafe { p.as_ref() }, Some(&777));
    /// }
    /// guard.repin_after(|| thread::sleep(Duration::from_millis(50)));
    /// {
    ///     let p = a.load(SeqCst, &guard);
    ///     assert_eq!(unsafe { p.as_ref() }, Some(&777));
    /// }
    /// ```
    ///
    /// [`unprotected`]: fn.unprotected.html
    pub fn repin_after<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        if let Some(local) = unsafe { self.local.as_ref() } {
            // We need to acquire a handle here to ensure the Local doesn't
            // disappear from under us.
            local.acquire_handle();
            local.unpin();
        }

        // Ensure the Guard is re-pinned even if the function panics
        defer! {
            if let Some(local) = unsafe { self.local.as_ref() } {
                mem::forget(local.pin());
                local.release_handle();
            }
        }

        f()
    }

    /// Returns the `Collector` associated with this guard.
    ///
    /// This method is useful when you need to ensure that all guards used with
    /// a data structure come from the same collector.
    ///
    /// If this method is called from an [`unprotected`] guard, then `None` is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_cbr_epoch as epoch;
    ///
    /// let mut guard1 = epoch::pin();
    /// let mut guard2 = unsafe { epoch::unprotected() };
    /// assert_ne!(guard1.collector(), guard2.collector());
    /// ```
    ///
    /// [`unprotected`]: fn.unprotected.html
    pub fn collector(&self) -> Option<&Collector> {
        unsafe { self.local.as_ref().map(|local| local.collector()) }
    }

    unsafe fn is_ejected(&self) -> bool {
        if let Some(local) = self.local.as_ref() {
            return local.get_epoch(self).is_err();
        }
        false
    }

    /// Conducts a read phase, and stores a result with protecting with hazard pointers.
    ///
    /// It takes a mutable borrow from `EpochGuard`. This prevents accessing `EpochGuard` in a read phase.
    ///
    /// Note that you cannot make a nested read phase, because creating a new pinned guard is not allowed
    /// in a restartable read phase.
    ///
    /// ```should_panic
    /// use crossbeam_cbr_epoch as epoch;
    ///
    /// let mut guard = epoch::pin();
    /// guard.read_loop(&mut (), &mut (), |_| (), |_, _| {
    ///     // Creating a new guard in a read phase is dangerous,
    ///     // as it may be forgotten if a ejection and `longjmp` occur.
    ///     let mut guard_inner = epoch::pin();
    ///     guard_inner.read_loop(&mut (), &mut (), |_| (), |_, _| {
    ///         /* ...farewell, cruel world! */
    ///         epoch::ReadStatus::Finished
    ///     });
    ///     epoch::ReadStatus::Finished
    /// });
    /// ```
    #[inline(never)]
    pub fn read<'r, F, D>(&mut self, defender: &mut D, f: F)
    where
        F: Fn(&'r mut ReadGuard) -> D::Read<'r>,
        D: Defender,
    {
        let buf = recovery::jmp_buf();

        // A dummy loop to bypass a false stack overflow from AdressSanitizer.
        //
        // # HACK: A dummy loop and `blackbox`
        //
        // It is not needed in normal builds, but when address-sanitizing,
        // the sanitizer often gives a false positive by recognizing `longjmp` as
        // stack buffer overflow (or stack corruption).
        //
        // However, awkwardly, if it wrapped by a loop block,
        // it seems that the sanitizer recognizes `longjmp` as
        // normal `continue` operation and totally satisfies with it.
        //
        // So, they are added to avoid false positives from the sanitizer.
        loop {
            // Make a checkpoint with `sigsetjmp` for recovering in read phase.
            if unsafe { setjmp::sigsetjmp(buf, 0) } == 1 {
                fence(Ordering::SeqCst);

                // Repin the current epoch.
                self.repin();

                // Unblock the signal before restarting the phase.
                let mut oldset = SigSet::empty();
                oldset.add(unsafe { recovery::ejection_signal() });
                if pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&oldset), None).is_err() {
                    panic!("Failed to unblock signal");
                }
            }
            compiler_fence(Ordering::SeqCst);

            // Get ready to open the phase by setting atomic indicators.
            assert!(
                !recovery::is_restartable(),
                "restartable value should be false before starting read phase"
            );
            recovery::set_restartable(true);
            compiler_fence(Ordering::SeqCst);

            // After setting `RESTARTABLE` up, we must check if the current
            // epoch is ejected.
            if unsafe { self.is_ejected() } {
                recovery::set_restartable(false);
                unsafe { recovery::perform_longjmp() };
            }

            let mut guard = ReadGuard::new(self);

            // Execute the body of this read phase.
            //
            // Here, `transmute` is needed to bypass a lifetime parameter error.
            let result = f(unsafe { transmute(&mut guard) });
            compiler_fence(Ordering::SeqCst);

            // Finaly, close this read phase by unsetting the `RESTARTABLE`.
            recovery::set_restartable(false);
            compiler_fence(Ordering::SeqCst);

            // Store pointers in hazard slots and issue a light fence.
            unsafe { defender.defend_unchecked(&result) };
            membarrier::light_membarrier();

            // Check whether we are ejected.
            if unsafe { self.is_ejected() } {
                // If we are ejected while protecting, the protection may not be valid.
                // Restart this read phase manually.
                unsafe { recovery::perform_longjmp() };
            }

            // # HACK: A dummy loop and `blackbox`
            //
            // (See comments on the loop for more information.)
            if black_box(true) {
                break;
            }
        }
    }

    /// Conducts a iterative read phase, and stores a result in `def_1st`, protecting with hazard pointers.
    ///
    /// This function saves intermeditate results on a backup storage periodically, so that
    /// it avoids a starvation on crash-intensive workloads.
    ///
    /// It takes a mutable borrow from `EpochGuard`. This prevents accessing `EpochGuard` in a read phase.
    ///
    /// Note that a nested read phase is not allowed.
    ///
    /// ```should_panic
    /// use crossbeam_cbr_epoch as epoch;
    ///
    /// let mut guard = epoch::pin();
    /// guard.read_loop(&mut (), &mut (), |_| (), |_, _| {
    ///     // Creating a new guard in a read phase is dangerous,
    ///     // as it may be forgotten if a ejection and `longjmp` occur.
    ///     let mut guard_inner = epoch::pin();
    ///     guard_inner.read_loop(&mut (), &mut (), |_| (), |_, _| {
    ///         /* ...farewell, cruel world! */
    ///         epoch::ReadStatus::Finished
    ///     });
    ///     epoch::ReadStatus::Finished
    /// });
    /// ```
    #[inline(never)]
    pub fn read_loop<'r, F1, F2, D>(
        &mut self,
        def_1st: &mut D,
        def_2nd: &mut D,
        init_result: F1,
        step_forward: F2,
    ) where
        F1: Fn(&'r mut ReadGuard) -> D::Read<'r>,
        F2: Fn(&mut D::Read<'r>, &'r mut ReadGuard) -> ReadStatus,
        D: Defender,
    {
        const ITER_BETWEEN_CHECKPOINTS: usize = 512;

        // `backup_idx` indicates where we have stored a backup to `backup_def`.
        // 0 = no backup, 1 = `def_1st`, 2 = `def_2st`
        // We use an atomic type instead of regular one, to perform writes atomically,
        // so that stored data is consistent even if a crash occured during a write.
        let backup_idx = AtomicUsize::new(0);

        let buf = recovery::jmp_buf();

        // A dummy loop to bypass a false stack overflow from AdressSanitizer.
        //
        // # HACK: A dummy loop and `blackbox`
        //
        // It is not needed in normal builds, but when address-sanitizing,
        // the sanitizer often gives a false positive by recognizing `longjmp` as
        // stack buffer overflow (or stack corruption).
        //
        // However, awkwardly, if it wrapped by a loop block,
        // it seems that the sanitizer recognizes `longjmp` as
        // normal `continue` operation and totally satisfies with it.
        //
        // So, they are added to avoid false positives from the sanitizer.
        loop {
            // Make a checkpoint with `sigsetjmp` for recovering in read phase.
            if unsafe { setjmp::sigsetjmp(buf, 0) } == 1 {
                fence(Ordering::SeqCst);

                // Repin the current epoch.
                self.repin();

                // Unblock the signal before restarting the phase.
                let mut oldset = SigSet::empty();
                oldset.add(unsafe { recovery::ejection_signal() });
                if pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&oldset), None).is_err() {
                    panic!("Failed to unblock signal");
                }
            }
            compiler_fence(Ordering::SeqCst);

            // If we have reached here by restarting this read phase in a write phase,
            // we should drop previous backups.
            if recovery::is_invalidated() {
                backup_idx.store(0, Ordering::Relaxed);
                recovery::set_invalidate_backup(false);
            }

            compiler_fence(Ordering::SeqCst);

            // Get ready to open the phase by setting atomic indicators.
            assert!(
                !recovery::is_restartable(),
                "restartable value should be false before starting read phase"
            );
            recovery::set_restartable(true);
            compiler_fence(Ordering::SeqCst);

            // After setting `RESTARTABLE` up, we must check if the current
            // epoch is ejected.
            if unsafe { self.is_ejected() } {
                recovery::set_restartable(false);
                unsafe { recovery::perform_longjmp() };
            }

            let mut guard = ReadGuard::new(self);

            // Load the saved intermediate result, if one exists.
            let mut result = unsafe {
                match backup_idx.load(Ordering::Relaxed) {
                    0 => init_result(transmute(&mut guard)),
                    1 => def_1st.as_read(),
                    2 => def_2nd.as_read(),
                    _ => unreachable!(),
                }
            };

            // Execute the body of this read phase.
            //
            // Here, `transmute` is needed to bypass a lifetime parameter error.
            for iter in 0.. {
                match step_forward(&mut result, unsafe { transmute(&mut guard) }) {
                    ReadStatus::Finished => break,
                    ReadStatus::Continue => {
                        // TODO(@jeonghyeon): Apply an adaptive checkpointing.
                        if iter % ITER_BETWEEN_CHECKPOINTS == 0 {
                            // Select an available defender to protect a backup.
                            let (curr_def, next_def, next_idx) =
                                match backup_idx.load(Ordering::Relaxed) {
                                    0 | 2 => (&mut *def_2nd, &mut *def_1st, 1),
                                    1 => (&mut *def_1st, &mut *def_2nd, 2),
                                    _ => unreachable!(),
                                };

                            // Store pointers in hazard slots and issue a light fence.
                            unsafe { next_def.defend_unchecked(&result) };
                            membarrier::light_membarrier();

                            // Check whether we are ejected.
                            if unsafe { self.is_ejected() } {
                                // If we are ejected while protecting, the protection may not be valid.
                                // Restart this read phase manually.
                                recovery::set_restartable(false);
                                unsafe { recovery::perform_longjmp() };
                            } else {
                                // Success! We are not ejected so the protection is valid!
                                // Finalize backup process by storing a new backup index to `backup_idx`
                                backup_idx.store(next_idx, Ordering::Relaxed);
                                curr_def.release();
                            }
                        }
                    }
                }
            }
            compiler_fence(Ordering::SeqCst);

            // Finaly, close this read phase by unsetting the `RESTARTABLE`.
            recovery::set_restartable(false);
            compiler_fence(Ordering::SeqCst);

            // Select an available defender to protect the final result.
            let final_def = match backup_idx.load(Ordering::Relaxed) {
                0 | 2 => &mut *def_1st,
                1 => &mut *def_2nd,
                _ => unreachable!(),
            };

            // Store pointers in hazard slots and issue a light fence.
            unsafe { final_def.defend_unchecked(&result) };
            membarrier::light_membarrier();

            // Check whether we are ejected.
            if unsafe { self.is_ejected() } {
                // If we are ejected while protecting, the protection may not be valid.
                // Restart this read phase manually.
                unsafe { recovery::perform_longjmp() };
            }

            // Success! Move the final defender to `def_1st` and return.
            if ptr::eq(final_def, def_2nd) {
                mem::swap(def_1st, def_2nd);
            }
            def_2nd.release();

            // # HACK: A dummy loop and `blackbox`
            //
            // (See comments on the loop for more information.)
            if black_box(true) {
                break;
            }
        }
    }
}

impl Drop for EpochGuard {
    #[inline]
    fn drop(&mut self) {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.unpin();
        }
    }
}

impl fmt::Debug for EpochGuard {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad("Guard { .. }")
    }
}

/// Returns a reference to a dummy guard that allows unprotected access to [`Atomic`]s.
///
/// This guard should be used in special occasions only. Note that it doesn't actually keep any
/// thread pinned - it's just a fake guard that allows loading from [`Atomic`]s unsafely.
///
/// Note that calling [`defer`] with a dummy guard will not defer the function - it will just
/// execute the function immediately.
///
/// # Safety
///
/// Loading and dereferencing data from an [`Atomic`] using this guard is safe only if the
/// [`Atomic`] is not being concurrently modified by other threads.
///
/// # Examples
///
/// ```
/// use crossbeam_cbr_epoch::pebr_backend::{self as epoch, Atomic};
/// use std::sync::atomic::Ordering::Relaxed;
///
/// let a = Atomic::new(7);
///
/// unsafe {
///     // Load `a` without pinning the current thread.
///     a.load(Relaxed, epoch::unprotected());
///
///     epoch::unprotected().defer(move || {
///         println!("This gets executed immediately.");
///     });
///
///     // Dropping `dummy` doesn't affect the current thread - it's just a noop.
/// }
/// ```
///
/// The most common use of this function is when constructing or destructing a data structure.
///
/// For example, we can use a dummy guard in the destructor of a Treiber stack because at that
/// point no other thread could concurrently modify the [`Atomic`]s we are accessing.
///
/// If we were to actually pin the current thread during destruction, that would just unnecessarily
/// delay garbage collection and incur some performance cost, so in cases like these `unprotected`
/// is very helpful.
///
/// ```
/// use crossbeam_cbr_epoch::pebr_backend::{self as epoch, Atomic};
/// use std::mem::ManuallyDrop;
/// use std::sync::atomic::Ordering::Relaxed;
///
/// struct Stack<T> {
///     head: Atomic<Node<T>>,
/// }
///
/// struct Node<T> {
///     data: ManuallyDrop<T>,
///     next: Atomic<Node<T>>,
/// }
///
/// impl<T> Drop for Stack<T> {
///     fn drop(&mut self) {
///         unsafe {
///             // Unprotected load.
///             let mut node = self.head.load(Relaxed, epoch::unprotected());
///
///             while let Some(n) = node.as_ref() {
///                 // Unprotected load.
///                 let next = n.next.load(Relaxed, epoch::unprotected());
///
///                 // Take ownership of the node, then drop its data and deallocate it.
///                 let mut o = node.into_owned();
///                 ManuallyDrop::drop(&mut o.data);
///                 drop(o);
///
///                 node = next;
///             }
///         }
///     }
/// }
/// ```
///
/// [`Atomic`]: struct.Atomic.html
/// [`defer`]: struct.Guard.html#method.defer
#[inline]
pub unsafe fn unprotected() -> &'static mut EpochGuard {
    // HACK(stjepang): An unprotected guard is just a `Guard` with its field `local` set to null.
    // Since this function returns a `'static` reference to a `Guard`, we must return a reference
    // to a global guard. However, it's not possible to create a `static` `Guard` because it does
    // not implement `Sync`. To get around the problem, we create a static `usize` initialized to
    // zero and then transmute it into a `Guard`. This is safe because `usize` and `Guard`
    // (consisting of a single pointer) have the same representation in memory.
    static UNPROTECTED: usize = 0;
    &mut *(&UNPROTECTED as *const _ as *const _ as *mut EpochGuard)
}

/// A result of a single step of an iterative read phase.
#[derive(Debug)]
pub enum ReadStatus {
    /// The current read phase is finished.
    Finished,
    /// We need to take one or more steps.
    Continue,
}

/// A read phase guard which allows to load `Shared` from reference counting `Atomic`.
#[derive(Debug)]
pub struct ReadGuard {
    inner: *const EpochGuard,
}

impl ReadGuard {
    fn new(guard: &EpochGuard) -> Self {
        Self { inner: guard }
    }

    /// Starts an auxiliary write phase where writing on a localized shared memory is allowed.
    pub fn write<'r, F, D>(&'r self, to_deref: D::Read<'r>, f: F)
    where
        F: Fn(&D, &WriteGuard) -> WriteResult,
        D: Defender,
    {
        // Protecting must be conducted in a crash-free section.
        // Otherwise it may forget to drop acquired hazard slot on crashing.
        recovery::set_restartable(false);
        compiler_fence(Ordering::SeqCst);

        // Allocate fresh hazard slots to protect pointers.
        let mut localized = D::default(unsafe { &*self.inner });
        // Store pointers in hazard slots and issue a light fence.
        unsafe { localized.defend_unchecked(&to_deref) };
        membarrier::light_membarrier();

        // Check whether we are ejected.
        if unsafe { (&*self.inner).is_ejected() } {
            // While protecting, we are ejected.
            // Then the protection may not be valid.
            // Drop the shields and restart this read phase manually.
            drop(localized);
            recovery::set_restartable(false);
            unsafe { recovery::perform_longjmp() };
        }

        // We are not ejected so the protection was valid!
        // Now we are free to call the write phase body.
        let guard = WriteGuard::new(self.inner);
        let result = f(&localized, &guard);
        drop(localized);
        compiler_fence(Ordering::SeqCst);

        match result {
            WriteResult::Finished => {
                recovery::set_restartable(true);

                if unsafe { (&*self.inner).is_ejected() } {
                    recovery::set_restartable(false);
                    unsafe { recovery::perform_longjmp() };
                }
            }
            WriteResult::RestartRead => {
                recovery::set_invalidate_backup(true);
                recovery::set_restartable(false);
                unsafe { recovery::perform_longjmp() };
            }
        }
    }
}

/// A result of a write phase.
#[derive(Debug)]
pub enum WriteResult {
    /// The current read phase is finished. Resume the read phase.
    Finished,
    /// Give up the current read phase and epoch and restart the whole read phase.
    RestartRead,
}

/// A write phase guard which allows to mutate localized pointers.
#[derive(Debug)]
pub struct WriteGuard {
    inner: *mut EpochGuard,
}

impl WriteGuard {
    fn new(guard: *const EpochGuard) -> Self {
        Self {
            inner: guard as *mut EpochGuard,
        }
    }
}

/// A common trait for `Guard` types which allow mutating shared memory locations.
///
/// `EpochGuard` and `ReadGuard` implement this trait.
pub trait Readable {}

impl Readable for EpochGuard {}
impl Readable for ReadGuard {}

/// A common trait for `Guard` types which allow mutating shared memory locations.
///
/// `EpochGuard` and `WriteGuard` implement this trait.
pub trait Writable {
    /// Stores a function so that it can be executed at some point when:
    ///
    /// 1. all currently pinned threads get unpinned, and
    /// 2. no hazard pointer protects `ptr`.
    unsafe fn defer<T, F: FnOnce(*const T)>(&self, ptr: *const T, f: F);
}

impl Writable for EpochGuard {
    unsafe fn defer<T, F: FnOnce(*const T)>(&self, ptr: *const T, f: F) {
        self.defer_garbage(
            Garbage::DeferredWithHazard {
                inner: DeferredWithHazard::new(ptr, f),
            },
            false,
        );
    }
}

impl Writable for WriteGuard {
    unsafe fn defer<T, F: FnOnce(*const T)>(&self, ptr: *const T, f: F) {
        unsafe { &mut *self.inner }.defer_garbage(
            Garbage::DeferredWithHazard {
                inner: DeferredWithHazard::new(ptr, f),
            },
            false,
        );
    }
}

/// A trait for `Shield` which can protect `Shared`.
pub trait Defender {
    /// A set of `Shared` pointers which is protected by an epoch.
    type Read<'r>: Clone + Copy;

    /// Returns a default `Defender` with empty hazard pointers.
    fn default(guard: &EpochGuard) -> Self;

    /// Note: This function MUST be called by the PEBR library only in the read phase,
    /// as it may be optimized to skip checking the epoch.
    ///
    /// Do not call this function to manually defend shared pointers.
    unsafe fn defend_unchecked(&mut self, read: &Self::Read<'_>);

    /// Loads currently protected pointers and composes a new `Shared` bag.
    unsafe fn as_read<'r>(&mut self) -> Self::Read<'r>;

    /// Resets the pointer to `null`, allowing the previous memory block to be reclaimed.
    fn release(&mut self);
}

// An empty `Defender`.
impl Defender for () {
    type Read<'r> = ();

    fn default(_: &EpochGuard) -> Self {
        ()
    }

    unsafe fn defend_unchecked(&mut self, _: &Self::Read<'_>) {}

    unsafe fn as_read<'r>(&mut self) -> Self::Read<'r> {
        ()
    }

    fn release(&mut self) {}
}
