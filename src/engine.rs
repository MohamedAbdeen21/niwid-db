use crate::buffer_pool::{ArcBufferPool, BufferPoolManager, BUFFER_POOL_SIZE};
use crate::catalog::{ArcCatalog, Catalog};
use crate::context::Context;
use crate::txn_manager::{ArcTransactionManager, TransactionManager};
use crate::wal::manager::{ArcLogManager, LogManagerHandle};
use parking_lot::{FairMutex, RwLock};
use std::sync::Arc;

pub use crate::disk_manager::DISK_STORAGE;

/// Composition root: one Engine = one database instance rooted at a data
/// dir. Nothing in the engine is process-global, so a process can host
/// several instances (parallel tests, in-process clusters).
pub struct Engine {
    // read by recovery and checkpoints, both landing next
    #[allow(dead_code)]
    pub(crate) bpm: ArcBufferPool,
    pub(crate) txn_manager: ArcTransactionManager,
    pub(crate) catalog: ArcCatalog,
    #[allow(dead_code)]
    pub(crate) log_manager: ArcLogManager,
}

impl Engine {
    pub fn new(data_dir: &str) -> Self {
        Self::with_pool_size(data_dir, BUFFER_POOL_SIZE)
    }

    pub fn with_pool_size(data_dir: &str, pool_size: usize) -> Self {
        let log_manager = LogManagerHandle::new(data_dir);
        let bpm = Arc::new(FairMutex::new(BufferPoolManager::new(pool_size, data_dir)));
        let txn_manager = Arc::new(FairMutex::new(TransactionManager::new(
            bpm.clone(),
            log_manager.clone(),
        )));
        let catalog = Arc::new(RwLock::new(Catalog::new(
            bpm.clone(),
            txn_manager.clone(),
            log_manager.clone(),
        )));

        Engine {
            bpm,
            txn_manager,
            catalog,
            log_manager,
        }
    }

    pub fn context(&self) -> Context {
        Context::new(self.catalog.clone(), self.txn_manager.clone())
    }
}
