use crate::buffer_pool::ArcBufferPool;
use crate::pages::indexes::b_plus_tree::{IndexPage, Key};
use crate::pages::INVALID_PAGE;
use crate::tuple::{TupleExt, TupleId};
use crate::txn_manager::TxnId;

pub struct IndexPageIterator {
    page: IndexPage,
    index: usize,
    bpm: ArcBufferPool,
    txn_id: Option<TxnId>,
}

impl IndexPageIterator {
    pub fn new(page: IndexPage, index: usize, bpm: ArcBufferPool, txn_id: Option<TxnId>) -> Self {
        // page pinned by caller
        Self {
            page,
            index,
            bpm,
            txn_id,
        }
    }
}

impl Iterator for IndexPageIterator {
    type Item = (Key, TupleId);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.page.len() {
            self.bpm.lock().unpin(&self.page.get_page_id(), self.txn_id);
            if self.page.get_next_page_id() == INVALID_PAGE {
                return None;
            } else {
                self.page = self
                    .bpm
                    .lock()
                    .fetch_frame(self.page.get_next_page_id(), self.txn_id)
                    .unwrap()
                    .reader()
                    .into();
                self.index = 0;
            }
        }

        let (key, value) = self.page.get_pair_at(self.index);

        self.index += 1;

        Some((key, TupleId::from_bytes(&value)))
    }
}
