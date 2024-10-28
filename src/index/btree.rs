use std::fmt::Debug;

use crate::buffer_pool::ArcBufferPool;
use crate::pages::btree_index_page::{IndexPage, PageType};
use crate::tuple::TupleId;
use crate::txn_manager::TxnId;
use crate::{pages::PageId, tuple::TUPLE_ID_SIZE};
use anyhow::Result;

// TupleId is u32 + u16 (4 + 2 = 6), but rust pads tuples
// so we store them directly as bytes
pub type LeafValue = [u8; TUPLE_ID_SIZE];
pub type Key = u32; // currently numeric types are 4 bytes

#[allow(unused)]
pub struct BPlusTree {
    root_page_id: PageId,
    bpm: ArcBufferPool,
}

#[allow(dead_code)]
impl BPlusTree {
    pub fn new(root_page_id: PageId, bpm: ArcBufferPool) -> Self {
        Self { root_page_id, bpm }
    }

    pub fn delete(&mut self, _key: Key) -> Result<()> {
        unimplemented!()
    }

    pub fn search(&self, key: Key) -> Option<TupleId> {
        let page: IndexPage = self
            .bpm
            .lock()
            .fetch_frame(self.root_page_id, None)
            .unwrap()
            .reader()
            .into();

        let ret = match page.get_type() {
            PageType::Internal => {
                let child_id = page.find_leaf(key);
                let child: IndexPage = self
                    .bpm
                    .lock()
                    .fetch_frame(child_id, None)
                    .unwrap()
                    .reader()
                    .into();
                let ret = self.search_page(child, key);

                self.bpm.lock().unpin(&page.get_page_id(), None);
                ret
            }
            PageType::Leaf => page.search(key),
            PageType::Invalid => unreachable!("Page type was not initialized properly"),
        };

        // self.bpm.lock().unpin(&self.root_page_id, None);
        ret
    }

    fn search_page(&self, page: IndexPage, key: Key) -> Option<TupleId> {
        match page.get_type() {
            PageType::Internal => {
                let child_id = page.find_leaf(key);
                let child: IndexPage = self
                    .bpm
                    .lock()
                    .fetch_frame(child_id, None)
                    .unwrap()
                    .reader()
                    .into();
                let ret = self.search_page(child, key);

                self.bpm.lock().unpin(&child_id, None);
                ret
            }
            PageType::Leaf => page.search(key),
            _ => None,
        }
    }

    fn new_leaf_page(&self) -> Result<IndexPage> {
        let mut new_page: IndexPage = self.bpm.lock().new_page()?.writer().into();
        self.bpm.lock().fetch_frame(new_page.get_page_id(), None)?;
        new_page.set_type(PageType::Leaf);
        Ok(new_page)
    }

    fn new_inner_page(&self) -> Result<IndexPage> {
        let mut new_page: IndexPage = self.bpm.lock().new_page()?.writer().into();
        self.bpm.lock().fetch_frame(new_page.get_page_id(), None)?;
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

                self.bpm.lock().unpin(&child_id, None);

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

        self.bpm.lock().unpin(&left_page.get_page_id(), None);
        self.bpm.lock().unpin(&right_page.get_page_id(), None);

        new_page.insert_first_pair(left_value, right_value, median);
        Ok(())
    }

    pub fn insert(&mut self, key: Key, value: LeafValue) -> Result<()> {
        let mut page: IndexPage = self
            .bpm
            .lock()
            .fetch_frame(self.root_page_id, None)?
            .writer()
            .into();

        let ret = match self.insert_into_page(&mut page, key, value) {
            Ok(Some((split_page, median))) => self.new_root(page, split_page, median),
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        };

        self.bpm.lock().unpin(&self.root_page_id, None);

        ret
    }
}

impl Debug for BPlusTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let root: IndexPage = self
            .bpm
            .lock()
            .fetch_frame(self.root_page_id, None)
            .unwrap()
            .writer()
            .into();
        let x = f.debug_struct("BPlusTree").field("root", &root).finish();
        self.bpm.lock().unpin(&self.root_page_id, None);
        x
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::tests::test_arc_bpm;
    use crate::pages::btree_index_page::KEYS_PER_NODE;
    use crate::tuple::{TupleExt, TupleId};
    use anyhow::Result;

    fn setup_bplus_tree() -> BPlusTree {
        let bpm = test_arc_bpm(100_000);
        let mut root_page: IndexPage = bpm.lock().new_page().unwrap().reader().into();
        root_page.set_type(PageType::Leaf);
        BPlusTree::new(root_page.get_page_id(), bpm)
    }

    #[test]
    fn test_insert_and_search_single_key() {
        let mut btree = setup_bplus_tree();
        let key: Key = 42;
        let value: LeafValue = [1, 2, 3, 4, 5, 6];

        // Insert a single key-value pair and verify search
        btree.insert(key, value).expect("Insert failed");
        dbg!(&btree);
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
        let root: IndexPage = btree
            .bpm
            .lock()
            .fetch_frame(btree.root_page_id, None)?
            .reader()
            .into();
        assert_eq!(root.get_type(), &PageType::Internal);
        assert!(root.len() == 1);
        btree.bpm.lock().unpin(&btree.root_page_id, None);

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
        let root: IndexPage = btree
            .bpm
            .lock()
            .fetch_frame(btree.root_page_id, None)?
            .reader()
            .into();
        assert_eq!(root.get_type(), &PageType::Internal);
        assert!(root.len() == 3); // Root should have promoted keys

        for i in 0..=(KEYS_PER_NODE * 2) as Key {
            assert_eq!(btree.search(i).unwrap().0, i);
        }

        btree.bpm.lock().unpin(&btree.root_page_id, None);
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
        let root: IndexPage = btree
            .bpm
            .lock()
            .fetch_frame(btree.root_page_id, None)?
            .reader()
            .into();
        assert_eq!(root.get_type(), &PageType::Internal);
        assert!(root.len() == 3);
        btree.bpm.lock().unpin(&btree.root_page_id, None);

        for i in 0..key_count as Key {
            assert_eq!(btree.search(i).unwrap().0, i, "Key {} not found", i);
        }

        Ok(())
    }
}
