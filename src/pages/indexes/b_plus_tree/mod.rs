use std::sync::Arc;

use crate::latch::Latch;
use crate::pages::{Page, PageData, PageId, INVALID_PAGE};
use crate::tuple::{TupleExt, TupleId, TUPLE_ID_SIZE};
use anyhow::{anyhow, Result};
use arrayvec::ArrayVec;

use std::fmt::Debug;

// TupleId is u32 + u16 (4 + 2 = 6), but rust pads tuples
// so we store them directly as bytes
pub type LeafValue = [u8; TUPLE_ID_SIZE];
pub type Key = u32; // currently numeric types are 4 bytes

/// B+ Branching Factor
const FACTOR: usize = 407;
// Leaf node pages can actually hold 340 keys
// but it's ok for the sake of simplicity
pub const KEYS_PER_NODE: usize = FACTOR - 1;

#[derive(Debug, Clone, PartialEq)]
#[repr(u32)] // avoid manual padding in IndexPageData
pub enum PageType {
    /// page initialized without type
    /// note that this variant is not initialized
    /// new empty pages (all zeroes) will automatically
    /// be read as this variant
    #[allow(unused)]
    Invalid,
    Leaf,
    Inner,
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
    pub values: ArrayVec<LeafValue, FACTOR>,
    ____padding: [u8; 8],
}

pub struct IndexPage {
    pub data: *mut IndexPageData,
    latch: Arc<Latch>,
    page_id: PageId,
    parent_page_id: PageId,
}

impl IndexPage {
    fn mark_dirty(&mut self) {
        self.data_mut().is_dirty = true;
    }

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

        self.mark_dirty();

        Ok(())
    }

    #[allow(unused)]
    pub fn delete(&mut self, key: Key) -> Result<()> {
        let data = self.data_mut();

        let pos = match data.keys.binary_search(&key) {
            Ok(pos) => pos,
            Err(_) => return Err(anyhow!("Key not found")),
        };

        data.keys.remove(pos);
        data.values.remove(pos);

        self.mark_dirty();

        Ok(())
    }

    /// Find a key in a leaf page
    pub fn search(&self, key: Key) -> Option<TupleId> {
        assert_eq!(self.get_type(), &PageType::Leaf);
        let data = self.data();

        match data.keys.binary_search(&key) {
            Ok(pos) => Some(TupleId::from_bytes(&data.values[pos])),
            Err(_) => None,
        }
    }

    /// find the index of a key in a leaf page
    pub fn find_index(&self, key: Key) -> Result<usize, usize> {
        assert_eq!(self.get_type(), &PageType::Leaf);
        let data = self.data();

        data.keys.binary_search(&key)
    }

    /// find the leaf page that contains a key
    pub fn find_leaf(&self, key: Key) -> PageId {
        assert_eq!(self.get_type(), &PageType::Inner);
        let data = self.data();

        let pos = match data.keys.binary_search(&key) {
            Ok(pos) => pos + 1,
            Err(pos) => pos,
        };

        TupleId::from_bytes(&data.values[pos]).0
    }

    /// helper to populate a new inner page
    pub fn insert_first_pair(&mut self, left: LeafValue, right: LeafValue, key: Key) {
        self.data_mut().values.insert(0, left);
        self.data_mut().values.insert(1, right);
        self.data_mut().keys.insert(0, key);
        self.mark_dirty();
    }

    pub fn split_inner(&mut self, mut new_page: IndexPage) -> (Self, Key) {
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

        assert!(self.get_type() == &PageType::Inner);
        new_page.set_type(PageType::Inner);

        self.data_mut().next = new_page.get_page_id();
        new_page.data_mut().prev = self.get_page_id();

        self.mark_dirty();
        new_page.mark_dirty();

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

        assert!(self.get_type() == &PageType::Leaf);
        new_page.set_type(PageType::Leaf);

        self.data_mut().next = new_page.get_page_id();
        new_page.data_mut().prev = self.get_page_id();

        self.mark_dirty();
        new_page.mark_dirty();

        (new_page, median)
    }

    #[allow(unused)]
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
    pub fn get_pair_at(&self, index: usize) -> (Key, LeafValue) {
        let data = self.data();
        (data.keys[index], data.values[index])
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
