use core::ops::Deref;
use crossbeam_pebr::{Guard, Shared, Shield, ShieldError};

/// Thread-local pool of shields
#[derive(Debug)]
pub struct ShieldPool<T> {
    shields: Vec<*mut Shield<T>>,
    /// Indices of available shields in `shields`.
    available: Vec<usize>,
}

impl<T> ShieldPool<T> {
    pub fn new() -> ShieldPool<T> {
        ShieldPool {
            shields: Vec::new(),
            available: Vec::new(),
        }
    }

    pub fn defend<'g>(
        &mut self,
        ptr: Shared<'g, T>,
        guard: &Guard,
    ) -> Result<ShieldHandle<T>, ShieldError> {
        if let Some(index) = self.available.pop() {
            let shield_ref = unsafe { &mut **self.shields.get_unchecked(index) };
            shield_ref.defend(ptr, guard)?;
            return Ok(ShieldHandle { pool: self, index });
        }
        let new_shield = Box::into_raw(Box::new(Shield::new(ptr, guard)?));
        let index = self.shields.len();
        self.shields.push(new_shield);
        Ok(ShieldHandle { pool: self, index })
    }
}

impl<T> Drop for ShieldPool<T> {
    fn drop(&mut self) {
        for s in self.shields.drain(..) {
            unsafe { drop(Box::from_raw(s)) }
        }
    }
}

#[derive(Debug)]
pub struct ShieldHandle<T> {
    /// The shield pool this handle belongs to.
    pool: *mut ShieldPool<T>,
    /// The index of the underlying shield.
    index: usize,
}

impl<T> Drop for ShieldHandle<T> {
    fn drop(&mut self) {
        let pool = unsafe { &mut *self.pool };
        // release only
        unsafe { (**pool.shields.get_unchecked(self.index)).release() };
        pool.available.push(self.index);
    }
}

impl<T> Deref for ShieldHandle<T> {
    type Target = Shield<T>;
    fn deref(&self) -> &Self::Target {
        unsafe { &(**(*self.pool).shields.get_unchecked(self.index)) }
    }
}
