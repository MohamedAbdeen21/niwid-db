use crate::buffer_pool::ArcBufferPool;
use crate::pages::indexes::b_plus_tree::{IndexPage, Key, LeafValue, PageType};
use crate::pages::PageId;
use crate::tuple::TupleId;
use crate::txn_manager::TxnId;
use anyhow::Result;

use super::btree_index_iterator::IndexPageIterator;

#[allow(unused)]
pub struct BPlusTree {
    root_page_id: PageId,
    pub bpm: ArcBufferPool,
}

#[allow(dead_code)]
impl BPlusTree {
    pub fn new(root_page_id: PageId, bpm: ArcBufferPool) -> Self {
        let mut page: IndexPage = bpm
            .lock()
            .fetch_frame(root_page_id, None)
            .unwrap()
            .writer()
            .into();
        page.set_type(PageType::Leaf);
        Self { root_page_id, bpm }
    }

    pub fn delete(&mut self, _key: Key) -> Result<()> {
        unimplemented!()
    }

    pub fn search(&self, key: Key) -> Option<TupleId> {
        let page: IndexPage = self.load_page(self.root_page_id, None).unwrap();

        let ret = self.search_page(page, key);

        self.unpin_page(self.root_page_id, None);
        ret
    }

    fn search_page(&self, page: IndexPage, key: Key) -> Option<TupleId> {
        match page.get_type() {
            PageType::Internal => {
                let child_id = page.find_leaf(key);
                let child: IndexPage = self.load_page(child_id, None).unwrap();
                let ret = self.search_page(child, key);

                self.unpin_page(child_id, None);
                ret
            }
            PageType::Leaf => page.search(key),
            PageType::Invalid => unreachable!("Page type was not initialized properly"),
        }
    }

    fn new_leaf_page(&self) -> Result<IndexPage> {
        let new_page_id = self.bpm.lock().new_page()?.writer().get_page_id();
        let mut new_page = self.load_page(new_page_id, None)?; // increment pin count
        new_page.set_type(PageType::Leaf);
        Ok(new_page)
    }

    fn new_inner_page(&self) -> Result<IndexPage> {
        let new_page_id = self.bpm.lock().new_page()?.writer().get_page_id();
        let mut new_page = self.load_page(new_page_id, None)?; // increment pin count
        new_page.set_type(PageType::Internal);
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
        page: &mut IndexPage,
        key: Key,
        value: LeafValue,
    ) -> Result<Option<(IndexPage, Key)>> {
        match page.get_type() {
            PageType::Leaf => {
                if page.is_full() {
                    let new_page = self.new_leaf_page()?;
                    let (mut right, median) = page.split_leaf(new_page);
                    if key < median {
                        page.insert(key, value)?;
                    } else {
                        right.insert(key, value)?;
                    }
                    Ok(Some((right, median)))
                } else {
                    page.insert(key, value)?;
                    Ok(None)
                }
            }
            PageType::Internal => {
                let child_id = page.find_leaf(key);
                let mut child = self.load_page(child_id, None)?;
                let ret = match self.insert_into_page(&mut child, key, value)? {
                    None => Ok(None),
                    Some((new_page, new_key)) => {
                        let mut value = [0; 6];
                        value[..4].copy_from_slice(&new_page.get_page_id().to_ne_bytes());
                        self.unpin_page(new_page.get_page_id(), None);

                        if page.is_almost_full() {
                            let new_page = self.new_inner_page()?;
                            let (mut right, median) = page.split_internal(new_page);
                            if key < median {
                                page.insert(new_key, value)?;
                            } else {
                                right.insert(new_key, value)?;
                            }
                            Ok(Some((right, median)))
                        } else {
                            page.insert(new_key, value)?;
                            Ok(None)
                        }
                    }
                };

                self.unpin_page(child_id, None);

                ret
            }
            PageType::Invalid => unreachable!("Page type was not initialized properly"),
        }
    }

    fn new_root(
        &mut self,
        mut left_page: IndexPage,
        mut right_page: IndexPage,
        median: Key,
    ) -> Result<()> {
        let mut new_page = self.new_inner_page()?;
        self.root_page_id = new_page.get_page_id();
        left_page.set_parent_id(self.root_page_id);
        right_page.set_parent_id(self.root_page_id);

        let mut left_value = [0; 6];
        left_value[..4].copy_from_slice(&left_page.get_page_id().to_ne_bytes());
        let mut right_value = [0; 6];
        right_value[..4].copy_from_slice(&right_page.get_page_id().to_ne_bytes());

        self.unpin_page(left_page.get_page_id(), None);
        self.unpin_page(right_page.get_page_id(), None);

        new_page.insert_first_pair(left_value, right_value, median);
        Ok(())
    }

    pub fn insert(&mut self, key: Key, value: LeafValue) -> Result<()> {
        let mut page = self.load_page(self.root_page_id, None)?;

        let ret = match self.insert_into_page(&mut page, key, value) {
            Ok(Some((split_page, median))) => self.new_root(page, split_page, median),
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        };

        self.unpin_page(self.root_page_id, None);

        ret
    }

    fn first_leaf_page_iter(&self, txn_id: Option<TxnId>) -> Result<IndexPageIterator> {
        let mut page_id = self.root_page_id;
        let mut page = self.load_page(page_id, txn_id)?;

        while page.get_type() == &PageType::Internal {
            let (_, value) = page.get_pair_at(0);
            self.unpin_page(page.get_page_id(), txn_id);
            page_id = PageId::from_ne_bytes(value[..4].try_into().unwrap());
            page = self.load_page(page_id, txn_id)?;
        }

        Ok(IndexPageIterator::new(page, 0, self.bpm.clone(), txn_id))
    }

    pub fn scan(
        &self,
        txn_id: Option<TxnId>,
        mut f: impl FnMut(&(Key, TupleId)) -> Result<()>,
    ) -> Result<()> {
        self.first_leaf_page_iter(txn_id)?
            .try_for_each(|entry| f(&entry))
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
    use crate::tuple::{TupleExt, TupleId};
    use anyhow::Result;

    fn setup_bplus_tree() -> BPlusTree {
        let bpm = test_arc_bpm(5);
        let root_page: IndexPage = bpm.lock().new_page().unwrap().writer().into();
        BPlusTree::new(root_page.get_page_id(), bpm)
    }

    #[test]
    fn test_insert_and_search_single_key() {
        let mut btree = setup_bplus_tree();
        let key: Key = 42;
        let value: LeafValue = [1, 2, 3, 4, 5, 6];

        // Insert a single key-value pair and verify search
        btree.insert(key, value).expect("Insert failed");
        let found_value = btree.search(key);
        assert_eq!(found_value, Some(TupleId::from_bytes(&value)));
    }

    #[test]
    fn test_insert_and_search_multiple_keys() {
        let mut btree = setup_bplus_tree();
        let values = [
            (1, [1, 0, 0, 0, 0, 0]),
            (2, [2, 0, 0, 0, 0, 0]),
            (3, [3, 0, 0, 0, 0, 0]),
        ];

        // Insert multiple keys and verify search for each
        for (key, value) in values.iter() {
            btree.insert(*key, *value).expect("Insert failed");
        }

        for (key, value) in values.iter() {
            let found_value = btree.search(*key);
            assert_eq!(found_value, Some(TupleId::from_bytes(value)));
        }
    }

    #[test]
    fn test_search_nonexistent_key() {
        let mut btree = setup_bplus_tree();
        let key: Key = 100;
        let value: LeafValue = [1, 2, 3, 4, 5, 6];

        // Insert a key-value pair and search for a nonexistent key
        btree.insert(key, value).expect("Insert failed");
        let found_value = btree.search(200);
        assert_eq!(found_value, None);
    }

    #[test]
    fn test_split_root_on_insert() -> Result<()> {
        let mut btree = setup_bplus_tree();

        // Insert enough key-value pairs to cause a split at the root
        for i in 0..=KEYS_PER_NODE as Key {
            let value: LeafValue = [i as u8, 0, 0, 0, 0, 0];
            btree.insert(i, value).expect("Insert failed");
        }

        // Check if the root split by confirming the B+ tree structure
        let root = btree.load_page(btree.root_page_id, None)?;
        assert_eq!(root.get_type(), &PageType::Internal);
        assert!(root.len() == 1);
        btree.unpin_page(btree.root_page_id, None);

        Ok(())
    }

    // #[test]
    // fn test_delete_existing_key() {
    //     let mut btree = setup_bplus_tree();
    //     let key: Key = 50;
    //     let value: LeafValue = [5, 5, 5, 5, 5, 5];
    //
    //     // Insert a key-value pair, delete it, and verify it's gone
    //     btree.insert(key, value).expect("Insert failed");
    //     btree.delete(key).expect("Delete failed");
    //     let found_value = btree.search(key);
    //     assert_eq!(found_value, None);
    // }
    //
    // #[test]
    // fn test_delete_nonexistent_key() {
    //     let mut btree = setup_bplus_tree();
    //     let key: Key = 99;
    //
    //     // Attempt to delete a key that doesn't exist
    //     let result = btree.delete(key);
    //     assert!(result.is_err(), "Expected an error for nonexistent key");
    // }

    #[test]
    fn test_split_and_promote_key() -> Result<()> {
        let mut btree = setup_bplus_tree();

        // Insert enough key-value pairs to cause multiple splits and promotions
        for i in 0..=(KEYS_PER_NODE * 2) as Key {
            let mut value: LeafValue = [0; 6];
            value[..4].copy_from_slice(&i.to_ne_bytes());
            btree.insert(i, value)?;
        }

        // Verify promoted keys are in the right nodes
        let root = btree.load_page(btree.root_page_id, None)?;
        assert_eq!(root.get_type(), &PageType::Internal);
        assert!(root.len() == 3); // Root should have promoted keys

        for i in 0..=(KEYS_PER_NODE * 2) as Key {
            assert_eq!(btree.search(i).unwrap().0, i);
        }

        btree.unpin_page(btree.root_page_id, None);
        Ok(())
    }

    #[test]
    fn test_insert_and_verify_height_three_tree() -> Result<()> {
        let mut btree = setup_bplus_tree();

        // Insert keys up to height 3 threshold
        let key_count = 408 * 408 + 1;
        for i in 0i32..key_count {
            let mut value: LeafValue = [0; 6];
            value[..4].copy_from_slice(&i.to_ne_bytes());
            btree.insert(i as u32, value)?;
        }

        // Check if the root is an internal node (indicating height > 1)
        let root: IndexPage = btree.load_page(btree.root_page_id, None)?;
        assert_eq!(root.get_type(), &PageType::Internal);
        assert!(root.len() == 3);
        btree.unpin_page(btree.root_page_id, None);

        for i in 0..key_count as Key {
            let found = btree.search(i);
            assert!(found.is_some(), "Key {} not found", i);
            assert_eq!(found.unwrap().0, i, "Key {} not found", i);
        }

        Ok(())
    }

    #[test]
    fn test_single_page_iteration() {
        let mut btree = setup_bplus_tree();
        let key: Key = 1;
        let value: LeafValue = [1, 2, 3, 4, 5, 6];

        btree.insert(key, value).expect("Insert failed");

        let mut iter = btree
            .first_leaf_page_iter(None)
            .expect("Iterator failed to initialize");
        assert_eq!(iter.next(), Some((key, TupleId::from_bytes(&value))));
        assert_eq!(iter.next(), None); // No more items
    }

    #[test]
    fn test_multiple_page_iteration() -> Result<()> {
        let mut btree = setup_bplus_tree();

        let key_count = KEYS_PER_NODE * KEYS_PER_NODE;
        for i in key_count..=0 {
            let mut value: LeafValue = [0; 6];
            value[..4].copy_from_slice(&i.to_ne_bytes());
            btree.insert(i as u32, value)?;
        }

        let iter = btree
            .first_leaf_page_iter(None)
            .expect("Iterator failed to initialize");

        // Verify iterator moves across pages correctly
        iter.enumerate().for_each(|(i, (key, value))| {
            assert_eq!(key, i as Key);
            assert_eq!(value.0, i as u32);
        });

        Ok(())
    }

    #[test]
    fn test_empty_page_iteration() {
        let btree = setup_bplus_tree();
        let mut iter = btree
            .first_leaf_page_iter(None)
            .expect("Iterator failed to initialize");

        // Ensure the iterator produces no results for an empty page
        assert_eq!(iter.next(), None);
    }
}
