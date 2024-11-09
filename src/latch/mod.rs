use parking_lot::lock_api::{RawRwLock, RawRwLockUpgrade};
use parking_lot::{RwLock, RwLockReadGuard};

#[derive(Debug, Default)]
pub struct Latch {
    lock: RwLock<()>,
}

impl Latch {
    pub fn new() -> Self {
        Self {
            lock: RwLock::new(()),
        }
    }

    #[cfg(test)]
    pub fn rlock(&self) {
        unsafe { self.lock.raw() }.lock_shared();
    }

    pub fn try_wlock(&self) -> bool {
        unsafe { self.lock.raw() }.try_lock_exclusive()
    }

    pub fn wunlock(&self) {
        unsafe { self.lock.raw().unlock_exclusive() };
    }

    pub fn rguard(&self) -> RwLockReadGuard<()> {
        self.lock.read()
    }

    pub fn upgradable_rlock(&self) {
        unsafe { self.lock.raw() }.lock_upgradable()
    }

    pub fn upgrade_write(&self) {
        assert!(self.is_locked());
        unsafe { self.lock.raw().upgrade() }
    }

    pub fn release_upgradable(&self) {
        unsafe { self.lock.raw().unlock_upgradable() }
    }

    #[allow(unused)]
    pub fn is_locked(&self) -> bool {
        self.lock.is_locked()
    }
}
