use std::sync::Arc;

use crate::index::btree::{Key, LeafValue};
use crate::latch::Latch;
use crate::tuple::{TupleExt, TupleId};
use anyhow::{anyhow, Result};
use arrayvec::ArrayVec;

use super::{Page, PageData, PageId, INVALID_PAGE};
use std::fmt::Debug;

/// B+ Branching Factor
const FACTOR: usize = 407;
// Leaf node pages can actually hold 340 keys
// but it's ok for the sake of simplicity
pub const KEYS_PER_NODE: usize = FACTOR - 1;

#[allow(unused)]
#[derive(Debug, Clone, PartialEq)]
pub enum PageType {
    /// page initialized without type
    Invalid,
    Leaf,
    Internal,
}

// shared between leaves and inner nodes for simplicity
#[repr(C)]
pub struct IndexPageData {
    _padding: [u8; 3],
    is_dirty: bool,
    page_type: PageType,
    prev: PageId,
    next: PageId,
    pub keys: ArrayVec<Key, KEYS_PER_NODE>,
    pub values: ArrayVec<LeafValue, { FACTOR + 1 }>,
    __padding: [u8; 4],
}

#[allow(unused)]
pub struct IndexPage {
    pub data: *mut IndexPageData,
    latch: Arc<Latch>,
    page_id: PageId,
    parent_page_id: PageId,
}

#[allow(unused)]
impl IndexPage {
    pub fn insert(&mut self, key: Key, value: LeafValue) -> Result<()> {
        if self.is_full() {
            return Err(anyhow!("Page is full"));
        }

        let data = self.data_mut();

        let pos = match data.keys.binary_search(&key) {
            Ok(_) => return Err(anyhow!("Key already exists")),
            Err(pos) => pos,
        };

        data.keys.insert(pos, key);
        if matches!(data.page_type, PageType::Leaf) {
            data.values.insert(pos, value);
        } else {
            data.values.insert(pos + 1, value);
        }

        Ok(())
    }

    pub fn delete(&mut self, key: Key) -> Result<()> {
        let data = self.data_mut();

        let pos = match data.keys.binary_search(&key) {
            Ok(pos) => pos,
            Err(_) => return Err(anyhow!("Key not found")),
        };

        data.keys.remove(pos);
        data.values.remove(pos);

        Ok(())
    }

    pub fn search(&self, key: Key) -> Option<TupleId> {
        assert_eq!(self.get_type(), &PageType::Leaf);
        let data = self.data();

        match data.keys.binary_search(&key) {
            Ok(pos) => Some(TupleId::from_bytes(&data.values[pos])),
            Err(_) => None,
        }
    }

    pub fn find_leaf(&self, key: Key) -> PageId {
        assert_eq!(self.get_type(), &PageType::Internal);
        let data = self.data();

        let mut pos = match data.keys.binary_search(&key) {
            Ok(pos) => pos + 1,
            Err(pos) => pos,
        };

        TupleId::from_bytes(&data.values[pos]).0
    }

    pub fn insert_first_pair(&mut self, left: LeafValue, right: LeafValue, key: Key) {
        self.data_mut().values.insert(0, left);
        self.data_mut().values.insert(1, right);
        self.data_mut().keys.insert(0, key);
    }

    pub fn split_internal(&mut self, mut new_page: IndexPage) -> (Self, Key) {
        let mid_index = self.len() / 2;

        let median = self.data().keys[mid_index];

        for key in &self.data().keys[mid_index + 1..] {
            new_page.data_mut().keys.push(*key);
        }

        for value in &self.data().values[mid_index + 1..] {
            new_page.data_mut().values.push(*value);
        }

        // Move remaining keys/values to the original node
        self.data_mut().keys.truncate(mid_index);
        self.data_mut().values.truncate(mid_index + 1);

        new_page.set_type(self.get_type().clone());

        self.data_mut().next = new_page.get_page_id();
        new_page.data_mut().prev = self.get_page_id();

        (new_page, median)
    }

    pub fn split_leaf(&mut self, mut new_page: IndexPage) -> (Self, Key) {
        let mid_index = self.len() / 2;

        let median = self.data().keys[mid_index];

        for key in &self.data().keys[mid_index..] {
            new_page.data_mut().keys.push(*key);
        }

        for value in &self.data().values[mid_index..] {
            new_page.data_mut().values.push(*value);
        }

        // Move remaining keys/values to the original node
        self.data_mut().keys.truncate(mid_index);
        self.data_mut().values.truncate(mid_index);

        new_page.set_type(self.get_type().clone());

        self.data_mut().next = new_page.get_page_id();
        new_page.data_mut().prev = self.get_page_id();

        (new_page, median)
    }

    pub fn merge(mut self, mut old_page: IndexPage) {
        unimplemented!("Merge is not implemented yet")
    }
}

impl Debug for IndexPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.data();
        f.debug_struct("IndexPage")
            .field("page_type", &s.page_type)
            .field("prev", &s.prev)
            .field("next", &s.next)
            .field("keys", &s.keys)
            .field("values", &s.values)
            .finish()
    }
}

impl<'a> From<&'a Page> for IndexPage {
    fn from(page: &'a Page) -> IndexPage {
        assert_eq!(
            std::mem::size_of::<IndexPageData>(),
            std::mem::size_of::<PageData>()
        );
        let data = &page.data as *const PageData as *mut IndexPageData;
        IndexPage {
            data,
            page_id: page.get_page_id(),
            latch: page.latch.clone(),
            parent_page_id: INVALID_PAGE,
        }
    }
}

impl<'a> From<&'a mut Page> for IndexPage {
    fn from(page: &'a mut Page) -> IndexPage {
        assert_eq!(
            std::mem::size_of::<IndexPageData>(),
            std::mem::size_of::<PageData>()
        );
        let data = &mut page.data as *mut PageData as *mut IndexPageData;
        IndexPage {
            data,
            page_id: page.get_page_id(),
            latch: page.latch.clone(),
            parent_page_id: INVALID_PAGE,
        }
    }
}

#[allow(unused)]
impl IndexPage {
    pub fn is_almost_full(&self) -> bool {
        self.len() == KEYS_PER_NODE - 1
    }

    pub fn is_full(&self) -> bool {
        self.len() == KEYS_PER_NODE
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_half_full(&self) -> bool {
        self.len() == KEYS_PER_NODE / 2
    }
    pub fn get_parent_id(&self) -> PageId {
        assert_ne!(self.parent_page_id, INVALID_PAGE);
        self.parent_page_id
    }

    pub fn set_parent_id(&mut self, parent_page_id: PageId) {
        assert_ne!(parent_page_id, INVALID_PAGE);
        self.parent_page_id = parent_page_id;
    }

    pub fn data_mut(&mut self) -> &mut IndexPageData {
        unsafe { self.data.as_mut().unwrap() }
    }

    pub fn data(&self) -> &IndexPageData {
        unsafe { self.data.as_ref().unwrap() }
    }

    pub fn set_type(&mut self, page_type: PageType) {
        self.data_mut().page_type = page_type;
    }

    pub fn get_type(&self) -> &PageType {
        &self.data().page_type
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn get_latch(&self) -> &Arc<Latch> {
        &self.latch
    }

    pub fn set_prev_page_id(&mut self, page_id: PageId) {
        self.data_mut().prev = page_id;
    }

    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.data_mut().next = page_id;
    }

    pub fn get_prev_page_id(&self) -> PageId {
        self.data().prev
    }

    pub fn get_next_page_id(&self) -> PageId {
        self.data().next
    }

    pub fn len(&self) -> usize {
        self.data().keys.len()
    }
}

#[cfg(test)]
mod tests {}
