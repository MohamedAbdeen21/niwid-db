use parking_lot::{
    lock_api::RawRwLock, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard,
};

#[derive(Debug)]
pub(super) struct Latch {
    lock: RwLock<()>,
}

impl Latch {
    pub fn new() -> Self {
        Self {
            lock: RwLock::new(()),
        }
    }

    #[allow(unused)]
    pub fn rlock(&self) {
        unsafe { self.lock.raw() }.lock_shared();
    }

    #[allow(unused)]
    pub fn runlock(&self) {
        unsafe { self.lock.raw().unlock_shared() };
    }

    pub fn wlock(&self) {
        unsafe { self.lock.raw() }.lock_exclusive();
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

    #[allow(unused)]
    pub fn upgradable_rlock(&self) -> RwLockUpgradableReadGuard<()> {
        self.lock.upgradable_read()
    }

    #[cfg(test)]
    pub fn is_locked(&self) -> bool {
        self.lock.is_locked()
    }
}
