use parking_lot::{lock_api::RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug)]
pub(super) struct Latch {
    lock: RwLock<()>,
}

#[allow(unused)]
impl Latch {
    pub fn new() -> Self {
        Self {
            lock: RwLock::new(()),
        }
    }

    pub fn rlock(&self) {
        unsafe { self.lock.raw() }.lock_shared();
    }

    pub fn wlock(&self) {
        unsafe { self.lock.raw() }.lock_exclusive();
    }

    pub fn runlock(&self) {
        unsafe { self.lock.raw().unlock_shared() };
    }

    pub fn wunlock(&self) {
        unsafe { self.lock.raw().unlock_exclusive() };
    }

    pub fn rguard(&self) -> RwLockReadGuard<()> {
        self.lock.read()
    }

    pub fn wguard(&self) -> RwLockWriteGuard<()> {
        self.lock.write()
    }
}