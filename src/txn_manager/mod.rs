use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use parking_lot::FairMutex;

use crate::buffer_pool::ArcBufferPool;
use crate::pages::PageId;
use crate::wal::manager::ArcLogManager;
use crate::wal::record::Record;

pub type TxnId = u64;

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    bpm: ArcBufferPool,
    lm: ArcLogManager,
    locked_pages: HashMap<TxnId, Vec<PageId>>,
}

pub type ArcTransactionManager = Arc<FairMutex<TransactionManager>>;

impl TransactionManager {
    pub fn new(bpm: ArcBufferPool, lm: ArcLogManager) -> Self {
        Self {
            next_txn_id: AtomicU64::new(0),
            bpm,
            lm,
            locked_pages: HashMap::new(),
        }
    }

    pub fn start(&mut self) -> Result<TxnId> {
        let id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);

        self.bpm.lock().start_txn(id)?;
        self.locked_pages.insert(id, vec![]);

        Ok(id)
    }

    /// check if page is locked by ANY transaction
    /// should check if page is locked by same txn first
    fn is_locked(&self, page_id: PageId) -> bool {
        self.locked_pages
            .values()
            .flatten()
            .any(|id| *id == page_id)
    }

    pub fn touch_page(&mut self, txn_id: TxnId, page_id: PageId) -> Result<()> {
        let txn_pages = match self.locked_pages.get(&txn_id) {
            Some(pages) => pages,
            None => return Err(anyhow!("Invalid txn id {}", txn_id)),
        };

        if txn_pages.contains(&page_id) {
            return Ok(());
        } else if self.is_locked(page_id) {
            return Err(anyhow!("page is already locked by a different transaction"));
        }

        self.bpm
            .lock()
            .shadow_page(txn_id, page_id)?
            .get_latch()
            .upgradable_rlock();

        self.locked_pages.get_mut(&txn_id).unwrap().push(page_id);

        Ok(())
    }

    pub fn commit(&mut self, txn_id: TxnId) -> Result<()> {
        // durable before visible: the txn only counts as committed once its
        // COMMIT record is synced; only then may the swap publish its writes
        if !self.lm.recovering() {
            let lsn = self.lm.lock().append(txn_id, Record::Commit);
            self.lm.lock().commit(lsn)?;
        }

        for page_id in self.locked_pages.get(&txn_id).unwrap().iter() {
            self.bpm
                .lock()
                .fetch_frame(*page_id, None)?
                .get_latch()
                .upgrade_write();
            self.bpm.lock().unpin(page_id, None);
        }

        self.bpm.lock().commit_txn(txn_id)?;

        for page_id in self.locked_pages.get(&txn_id).unwrap().iter() {
            self.bpm
                .lock()
                .fetch_frame(*page_id, None)?
                .get_latch()
                .wunlock();
            self.bpm.lock().unpin(page_id, None);
        }

        self.locked_pages.remove(&txn_id);

        Ok(())
    }

    pub fn rollback(&mut self, txn_id: TxnId) -> Result<()> {
        // no fsync needed: an un-synced abort is indistinguishable from a
        // crash mid-txn, and recovery treats both as rolled back

        if !self.lm.recovering() {
            self.lm.lock().append(txn_id, Record::Abort);
        }

        self.bpm.lock().rollback_txn(txn_id)?;

        for page_id in self.locked_pages.remove(&txn_id).unwrap().iter() {
            self.bpm
                .lock()
                .fetch_frame(*page_id, None)?
                .get_latch()
                .release_upgradable();

            self.bpm.lock().unpin(page_id, None);
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn test_arc_transaction_manager(bpm: ArcBufferPool) -> ArcTransactionManager {
        use crate::disk_manager::test_path;
        use crate::wal::manager::LogManagerHandle;

        Arc::new(FairMutex::new(TransactionManager::new(
            bpm,
            LogManagerHandle::new(&test_path()),
        )))
    }
}
