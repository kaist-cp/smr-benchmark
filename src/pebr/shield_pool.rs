use crossbeam_pebr::{Guard, Shared, Shield, ShieldError};
use std::cell::RefCell;
use std::rc::Rc;

/// Thread-local pool of shields
#[derive(Debug)]
pub struct ShieldPool<T> {
    shields: Vec<Rc<RefCell<Shield<T>>>>,
    available: Rc<RefCell<Vec<usize>>>,
}

impl<T> ShieldPool<T> {
    pub fn new() -> ShieldPool<T> {
        ShieldPool {
            shields: Vec::new(),
            available: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn defend<'g>(
        &mut self,
        ptr: Shared<'g, T>,
        guard: &Guard,
    ) -> Result<ShieldHandle<T>, ShieldError> {
        if let Some(index) = self.available.borrow_mut().pop() {
            let shield_ref = unsafe { self.shields.get_unchecked(index).clone() };
            shield_ref.borrow_mut().defend(ptr, guard)?;
            return Ok(ShieldHandle {
                shield: shield_ref,
                available: self.available.clone(),
                index,
            });
        }
        let new_shield = Shield::new(ptr, guard)?;
        let index = self.shields.len();
        self.shields.push(Rc::new(RefCell::new(new_shield)));
        Ok(ShieldHandle {
            shield: self.shields.last().unwrap().clone(),
            available: self.available.clone(),
            index,
        })
    }
}

#[derive(Debug)]
pub struct ShieldHandle<T> {
    shield: Rc<RefCell<Shield<T>>>,
    available: Rc<RefCell<Vec<usize>>>,
    index: usize,
}

impl<T> Drop for ShieldHandle<T> {
    fn drop(&mut self) {
        self.shield.borrow_mut().release();
        self.available.borrow_mut().push(self.index);
    }
}
