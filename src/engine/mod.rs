mod recovery;

use crate::buffer_pool::{ArcBufferPool, BufferPoolManager, BUFFER_POOL_SIZE};
use crate::catalog::{ArcCatalog, Catalog};
use crate::context::Context;
use crate::txn_manager::{ArcTransactionManager, TransactionManager};
use crate::wal::manager::{ArcLogManager, LogManagerHandle};
use anyhow::Result;
use parking_lot::FairMutex;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::wal::manager::LOG_START;
use crate::wal::Lsn;

pub use crate::disk_manager::DISK_STORAGE;

/// Composition root: one Engine = one database instance rooted at a data
/// dir. Nothing in the engine is process-global, so a process can host
/// several instances (parallel tests, in-process clusters).
pub struct Engine {
    // read by checkpoints, landing next
    #[allow(dead_code)]
    pub(crate) bpm: ArcBufferPool,
    pub(crate) txn_manager: ArcTransactionManager,
    pub(crate) catalog: ArcCatalog,
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

        let engine = Engine {
            bpm,
            txn_manager,
            catalog,
            log_manager,
        };

        engine.recover().expect("Recovery failed");

        engine
    }

    pub fn context(&self) -> Context {
        Context::new(self.catalog.clone(), self.txn_manager.clone())
    }

    /// Everything the log holds up to this lsn is already in the data files
    pub(crate) fn checkpoint_lsn(&self) -> Lsn {
        self.bpm.lock().image_lsn().max(LOG_START)
    }

    pub fn checkpoint(&self) -> Result<()> {
        let lsn = self.log_manager.lock().next_lsn();

        self.bpm.lock().checkpoint(lsn)
    }
}
