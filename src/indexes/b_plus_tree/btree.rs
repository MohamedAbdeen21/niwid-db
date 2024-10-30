use crate::buffer_pool::ArcBufferPool;
use crate::pages::indexes::b_plus_tree::leaf_value::LeafValue;
use crate::pages::indexes::b_plus_tree::{IndexPage, Key, PageType};
use crate::pages::PageId;
use crate::tuple::TupleId;
use crate::txn_manager::{ArcTransactionManager, TxnId};
use anyhow::Result;

use super::btree_iterator::IndexPageIterator;

pub struct BPlusTree {
    root_page_id: PageId,
    pub bpm: ArcBufferPool,
    pub txn_manager: ArcTransactionManager,
}

impl BPlusTree {
    pub fn new(bpm: ArcBufferPool, txn_manager: ArcTransactionManager, txn: Option<TxnId>) -> Self {
        let root_page_id = bpm.lock().new_page().unwrap().writer().get_page_id();

        let tree = Self {
            root_page_id,
            bpm,
            txn_manager,
        };

        if let Some(txn) = txn {
            tree.txn_manager
                .lock()
                .touch_page(txn, root_page_id)
                .unwrap();
        }

        let mut page = tree.load_page(root_page_id, txn).unwrap();
        page.set_type(PageType::Leaf);
        tree.unpin_page(root_page_id, txn);

        tree
    }

    pub fn fetch(
        root_page_id: PageId,
        bpm: ArcBufferPool,
        txn_manager: ArcTransactionManager,
    ) -> Self {
        Self {
            root_page_id,
            bpm,
            txn_manager,
        }
    }

    #[allow(unused)]
    pub fn delete(&mut self, txn: Option<TxnId>, key: impl Into<Key>) -> Result<()> {
        let key = key.into();
        let root = self.load_page(self.root_page_id, txn).unwrap();

        let mut leaf = self.find_leaf(txn, root, key);
        if let Some(txn) = txn {
            self.txn_manager
                .lock()
                .touch_page(txn, leaf.get_page_id())?;
        }

        leaf.delete(key)?;

        if txn.is_none() {
            self.bpm.lock().flush(Some(leaf.get_page_id()))?;
        }

        self.unpin_page(leaf.get_page_id(), txn);

        Ok(())
    }

    /// Helper that create a LeafValue from a page id as this is a common operation
    fn to_value(&self, page_id: PageId) -> LeafValue {
        LeafValue::new(page_id, 0)
    }

    pub fn search(&self, txn: Option<TxnId>, key: impl Into<Key>) -> Option<TupleId> {
        let key = key.into();
        let page: IndexPage = self.load_page(self.root_page_id, txn).unwrap();

        let leaf = self.find_leaf(txn, page, key);
        let value = leaf.search(key)?;

        self.unpin_page(leaf.get_page_id(), txn);

        if value.is_deleted {
            None
        } else {
            Some(value.tuple_id())
        }
    }

    /// unpins the input page once the search is done
    fn find_leaf(&self, txn: Option<TxnId>, page: IndexPage, key: Key) -> IndexPage {
        match page.get_type() {
            PageType::Inner => {
                let child_id = page.find_leaf(key);
                self.unpin_page(page.get_page_id(), txn);
                let child: IndexPage = self.load_page(child_id, txn).unwrap();
                self.find_leaf(txn, child, key)
            }
            PageType::Leaf => page,
            PageType::Invalid => unreachable!("Page type was not initialized properly"),
        }
    }

    /// returns a new pinned leaf page
    fn new_leaf_page(&self, txn: Option<TxnId>) -> Result<IndexPage> {
        let new_page_id = self.bpm.lock().new_page()?.writer().get_page_id();
        let mut new_page = self.load_page(new_page_id, txn)?; // increment pin count
        new_page.set_type(PageType::Leaf);

        if let Some(txn) = txn {
            self.txn_manager.lock().touch_page(txn, new_page_id)?;
        }

        Ok(new_page)
    }

    /// returns a new pinned inner page
    fn new_inner_page(&self, txn: Option<TxnId>) -> Result<IndexPage> {
        let new_page_id = self.bpm.lock().new_page()?.writer().get_page_id();
        let mut new_page = self.load_page(new_page_id, txn)?; // increment pin count
        new_page.set_type(PageType::Inner);

        if let Some(txn) = txn {
            self.txn_manager.lock().touch_page(txn, new_page_id)?;
        }

        Ok(new_page)
    }

    fn load_page(&self, page_id: PageId, txn_id: Option<TxnId>) -> Result<IndexPage> {
        Ok(self
            .bpm
            .lock()
            .fetch_frame(page_id, txn_id)?
            .writer()
            .into())
    }

    fn unpin_page(&self, page_id: PageId, txn_id: Option<TxnId>) {
        self.bpm.lock().unpin(&page_id, txn_id);
    }

    /// Inserts key-value pair to a page. If a split happens, return the page id of the new page
    /// and the median value to be used by parent (caller function) or None if no split happens
    fn insert_into_page(
        &self,
        txn: Option<TxnId>,
        page: &mut IndexPage,
        key: Key,
        value: LeafValue,
    ) -> Result<Option<(IndexPage, Key)>> {
        if let Some(txn) = txn {
            self.txn_manager
                .lock()
                .touch_page(txn, page.get_page_id())?;
        }

        let res = match page.get_type() {
            PageType::Leaf if page.is_full() => {
                let new_page = self.new_leaf_page(txn)?;
                let (mut right, median) = page.split_leaf(new_page);
                if key < median {
                    page.insert(key, value)?;
                } else {
                    right.insert(key, value)?;
                }
                Ok(Some((right, median)))
            }
            PageType::Leaf => {
                page.insert(key, value)?;
                Ok(None)
            }
            PageType::Inner => {
                let child_id = page.find_leaf(key);
                let mut child = self.load_page(child_id, txn)?;
                let ret = match self.insert_into_page(txn, &mut child, key, value)? {
                    None => Ok(None),
                    Some((new_page, new_key)) if page.is_full() => {
                        let value = self.to_value(new_page.get_page_id());
                        self.unpin_page(new_page.get_page_id(), txn);

                        let new_page = self.new_inner_page(txn)?;
                        let (mut right, median) = page.split_inner(new_page);

                        if key < median {
                            page.insert(new_key, value)?;
                        } else {
                            right.insert(new_key, value)?;
                        }
                        Ok(Some((right, median)))
                    }
                    Some((new_page, new_key)) => {
                        let value = self.to_value(new_page.get_page_id());
                        self.unpin_page(new_page.get_page_id(), txn);

                        page.insert(new_key, value)?;
                        Ok(None)
                    }
                };

                self.unpin_page(child_id, txn);

                ret
            }
            PageType::Invalid => unreachable!("Page type was not initialized properly"),
        };

        if txn.is_none() {
            self.bpm.lock().flush(Some(page.get_page_id()))?;
        }

        res
    }

    fn new_root(
        &mut self,
        txn: Option<TxnId>,
        mut root: IndexPage,
        right_page: IndexPage,
        median: Key,
    ) -> Result<()> {
        let mut left_page = self.new_inner_page(txn)?;

        // Keep the page id of the root node
        std::mem::swap(left_page.data_mut(), root.data_mut());

        let left_value = self.to_value(left_page.get_page_id());
        let right_value = self.to_value(right_page.get_page_id());

        root.insert_first_pair(left_value, right_value, median);

        // root node is unpinned in insertion method
        self.unpin_page(left_page.get_page_id(), txn);
        self.unpin_page(right_page.get_page_id(), txn);
        Ok(())
    }

    pub fn insert(
        &mut self,
        txn: Option<TxnId>,
        key: impl Into<Key>,
        value: TupleId,
    ) -> Result<()> {
        let key = key.into();
        let mut page = self.load_page(self.root_page_id, txn)?;

        let value = LeafValue::new(value.0, value.1);

        let ret = match self.insert_into_page(txn, &mut page, key, value) {
            Ok(Some((split_page, median))) => self.new_root(txn, page, split_page, median),
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        };

        // println!("{} {:?}", self.root_page_id);
        self.unpin_page(self.root_page_id, txn);

        ret
    }

    #[allow(unused)]
    fn iter(&self, txn_id: Option<TxnId>) -> Result<IndexPageIterator> {
        let mut page = self.load_page(self.root_page_id, txn_id)?;

        while page.get_type() == &PageType::Inner {
            let (_, LeafValue { page_id, .. }) = page.get_pair_at(0);

            self.unpin_page(page.get_page_id(), txn_id);
            page = self.load_page(page_id, txn_id)?;
        }

        // iterator expects an already pinned page
        Ok(IndexPageIterator::new(page, 0, self.bpm.clone(), txn_id))
    }

    #[allow(unused)]
    pub fn scan_from(
        &self,
        txn: Option<TxnId>,
        key: impl Into<Key>,
        mut f: impl FnMut(&(Key, TupleId)) -> Result<()>,
    ) -> Result<()> {
        // unpinned by search
        let key = key.into();
        let root = self.load_page(self.root_page_id, txn)?;
        let page = self.find_leaf(txn, root, key);
        let index = match page.find_index(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };

        IndexPageIterator::new(page, index, self.bpm.clone(), txn).try_for_each(|entry| f(&entry))
    }

    #[allow(unused)]
    pub fn scan(
        &self,
        txn_id: Option<TxnId>,
        mut f: impl FnMut(&(Key, TupleId)) -> Result<()>,
    ) -> Result<()> {
        self.iter(txn_id)?.try_for_each(|entry| f(&entry))
    }

    pub fn get_root_page_id(&self) -> PageId {
        self.root_page_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::tests::test_arc_bpm;
    use crate::pages::indexes::b_plus_tree::KEYS_PER_NODE;
    use crate::txn_manager::tests::test_arc_transaction_manager;
    use anyhow::Result;
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    fn setup_bplus_tree() -> BPlusTree {
        let bpm = test_arc_bpm(5);
        let txn_manager = test_arc_transaction_manager(bpm.clone());
        BPlusTree::new(bpm, txn_manager, None)
    }

    #[test]
    fn test_insert_and_search_single_key() {
        let mut btree = setup_bplus_tree();
        let key: Key = 42;

        // Insert a single key-value pair and verify search
        btree.insert(None, key, (0, 0)).expect("Insert failed");
        let found_value = btree.search(None, key);
        assert_eq!(found_value, Some((0, 0)));
    }

    #[test]
    fn test_insert_and_search_multiple_keys() {
        let mut btree = setup_bplus_tree();
        let keys = [1, 2, 3];

        // Insert multiple keys and verify search for each
        for key in keys {
            btree.insert(None, key, (key, 0)).expect("Insert failed");
        }

        for key in keys {
            let found_value = btree.search(None, key);
            assert_eq!(found_value, Some((key, 0)));
        }
    }

    #[test]
    fn test_search_nonexistent_key() {
        let mut btree = setup_bplus_tree();
        let key: Key = 100;

        // Insert a key-value pair and search for a nonexistent key
        btree.insert(None, key, (0, 0)).expect("Insert failed");
        let found_value = btree.search(None, 200_u32);
        assert_eq!(found_value, None);
    }

    #[test]
    fn test_split_root_on_insert() -> Result<()> {
        let mut btree = setup_bplus_tree();

        // Insert enough key-value pairs to cause a split at the root
        for i in 0..=KEYS_PER_NODE as Key {
            btree.insert(None, i, (i, 0)).expect("Insert failed");
        }

        // Check if the root split by confirming the B+ tree structure
        let root = btree.load_page(btree.root_page_id, None)?;
        assert_eq!(root.get_type(), &PageType::Inner);
        assert!(root.len() == 1);
        btree.unpin_page(btree.root_page_id, None);

        Ok(())
    }

    #[test]
    fn test_delete_multiple_keys() {
        let mut btree = setup_bplus_tree();

        let keys = vec![10, 20, 30, 40, 50];

        // Insert multiple keys
        for key in &keys {
            btree.insert(None, *key, (*key, 0)).expect("Insert failed");
        }

        // Delete each key and check that it is no longer found
        for key in &keys {
            btree.delete(None, *key).expect("Delete failed");
            assert_eq!(
                btree.search(None, *key),
                None,
                "Expected key {} to be deleted",
                key
            );
        }
    }

    #[test]
    fn test_delete_reinsert_keys() {
        let mut btree = setup_bplus_tree();

        // Insert a range of keys
        for key in 1..=50 {
            btree.insert(None, key, (key, 0)).expect("Insert failed");
        }

        // Delete half of the keys
        for key in (1_u32..=50).step_by(2) {
            btree.delete(None, key).expect("Delete failed");
            assert_eq!(
                btree.search(None, key),
                None,
                "Expected key {} to be deleted",
                key
            );
        }

        // Verify all keys are found in the B+Tree
        for key in (2..=50).step_by(2) {
            let value = LeafValue::new(key, 0).tuple_id();
            let found_value = btree.search(None, key).expect("Key not found after delete");
            assert_eq!(found_value, value, "Value mismatch for key {}", key);
        }

        // Re-insert deleted keys
        for key in (1..=50).step_by(2) {
            btree.insert(None, key, (key, 0)).expect("Reinsert failed");
        }

        // Verify all keys are found in the B+Tree
        for key in 1..=50 {
            let value = LeafValue::new(key, 0).tuple_id();
            let found_value = btree
                .search(None, key)
                .expect("Key not found after reinsert");
            assert_eq!(found_value, value, "Value mismatch for key {}", key);
        }
    }

    #[test]
    fn test_delete_existing_key() {
        let mut btree = setup_bplus_tree();
        let key: Key = 50;

        // Insert a key-value pair, delete it, and verify it's gone
        btree.insert(None, key, (50, 0)).expect("Insert failed");
        btree.delete(None, key).expect("Delete failed");
        let found_value = btree.search(None, key);
        assert_eq!(found_value, None);
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut btree = setup_bplus_tree();
        let key: Key = 99;

        // Attempt to delete a key that doesn't exist
        let result = btree.delete(None, key);
        assert!(result.is_err(), "Expected an error for nonexistent key");
    }

    #[test]
    fn test_split_and_promote_key() -> Result<()> {
        let mut btree = setup_bplus_tree();

        // Insert enough key-value pairs to cause multiple splits and promotions
        for i in 0..=(KEYS_PER_NODE * 2) as Key {
            btree.insert(None, i, (i, 0))?;
        }

        // Verify promoted keys are in the right nodes
        let root = btree.load_page(btree.root_page_id, None)?;
        assert_eq!(root.get_type(), &PageType::Inner);
        assert!(root.len() == 3); // Root should have promoted keys

        for i in 0..=(KEYS_PER_NODE * 2) as Key {
            assert_eq!(btree.search(None, i).unwrap().0, i);
        }

        btree.unpin_page(btree.root_page_id, None);
        Ok(())
    }

    #[test]
    fn test_insert_and_verify_height_three_tree() -> Result<()> {
        let mut btree = setup_bplus_tree();

        // Insert keys up to height 3 threshold
        let key_count = 408 * 408 + 1;
        let mut values: Vec<u32> = (0..=key_count).collect();

        let mut rng = thread_rng();
        values.shuffle(&mut rng);

        let root_id = btree.root_page_id;

        for i in values {
            btree.insert(None, i, (i, 0))?;
        }

        assert_eq!(btree.root_page_id, root_id);

        // Check if the root is an internal node (indicating height > 1)
        let root: IndexPage = btree.load_page(btree.root_page_id, None)?;
        assert_eq!(root.get_type(), &PageType::Inner);
        assert!(
            !root.is_empty(),
            "Root should have at least 1 key, got {}",
            root.len()
        );
        btree.unpin_page(btree.root_page_id, None);

        for i in 0..key_count as Key {
            let found = btree.search(None, i);
            assert!(found.is_some(), "Key {} not found", i);
            assert_eq!(found.unwrap().0, i, "Key {} not found", i);
        }

        Ok(())
    }

    #[test]
    fn test_single_page_iteration() {
        let mut btree = setup_bplus_tree();
        let key: Key = 1;

        btree.insert(None, key, (2, 3)).expect("Insert failed");

        let mut iter = btree.iter(None).expect("Iterator failed to initialize");
        assert_eq!(iter.next(), Some((1, (2, 3))));
        assert_eq!(iter.next(), None); // No more items
    }

    #[test]
    fn test_multiple_page_iteration() -> Result<()> {
        let mut btree = setup_bplus_tree();

        let key_count = (KEYS_PER_NODE * KEYS_PER_NODE) as u32;
        for i in key_count..=0 {
            btree.insert(None, i, (i, 0))?;
        }

        // Verify iterator moves across pages correctly
        btree.scan(None, |(key, (page, _))| {
            assert_eq!(key, page);
            Ok(())
        })?;

        let mid = KEYS_PER_NODE / 2;
        btree.scan_from(None, mid as u32, |(key, (page, _))| {
            assert_eq!(key, page);
            Ok(())
        })?;

        Ok(())
    }

    #[test]
    fn test_empty_page_iteration() {
        let btree = setup_bplus_tree();
        let mut iter = btree.iter(None).expect("Iterator failed to initialize");

        // Ensure the iterator produces no results for an empty page
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_scan_from_key_exists() {
        let mut btree = setup_bplus_tree();
        let keys = [1, 2, 3, 4, 5];

        // Insert multiple keys
        for key in keys {
            btree.insert(None, key, (key, 0)).expect("Insert failed");
        }

        // Start scanning from key 3, expecting (3, 4, 5)
        let mut collected = Vec::new();
        btree
            .scan_from(None, 3_u32, |&(key, ref value)| {
                collected.push((key, *value));
                Ok(())
            })
            .expect("Scan from key failed");

        let expected = [(3, (3, 0)), (4, (4, 0)), (5, (5, 0))];
        assert_eq!(collected, expected);
    }

    #[test]
    fn test_scan_from_key_does_not_exist_gt() {
        let mut btree = setup_bplus_tree();
        let keys = [1, 3, 5];

        for key in keys {
            btree.insert(None, key, (key, 0)).expect("Insert failed");
        }

        // Start scanning from non-existing key 2, expecting (3, 5)
        let mut collected = Vec::new();
        btree
            .scan_from(None, 2_u32, |&(key, ref value)| {
                collected.push((key, *value));
                Ok(())
            })
            .expect("Scan from non-existent key failed");

        let expected = [(3, (3, 0)), (5, (5, 0))];
        assert_eq!(collected, expected);
    }
}
