use std::sync::Arc;

use crate::{latch::Latch, tuple::TUPLE_ID_SIZE};
use anyhow::{anyhow, Result};
use arrayvec::ArrayVec;

use super::{Page, PageData, PageId, SlotId};
use std::fmt::Debug;

/// B+ Branching Factor
const FACTOR: usize = 408;
// Leaf node pages can actually hold 340 keys
// but it's ok for the sake of simplicity
const KEYS_PER_NODE: usize = FACTOR - 1;

// TupleId is u32 + u16 (4 + 2 = 6), but rust pads tuples
// so we store them directly as bytes
type LeafValue = [u8; TUPLE_ID_SIZE];
type Key = u32; // currently numeric types are 4 bytes

#[allow(unused)]
#[derive(Debug)]
pub enum PageType {
    /// page initialized without type
    Invalid,
    Root,
    Leaf,
    Internal,
}

// shared between leaves and inner nodes for simplicity
#[repr(C)]
struct IndexPageData {
    _padding: [u8; 3],
    is_dirty: bool,
    page_type: PageType,
    level: u8,
    size: SlotId,
    prev: PageId,
    next: PageId,
    keys: ArrayVec<Key, KEYS_PER_NODE>,
    values: ArrayVec<LeafValue, KEYS_PER_NODE>,
    __padding: [u8; 4],
}

#[allow(unused)]
pub struct IndexPage {
    data: *mut IndexPageData,
    latch: Arc<Latch>,
    page_id: PageId,
}

#[allow(unused)]
impl IndexPage {
    fn data_mut(&mut self) -> &mut IndexPageData {
        unsafe { self.data.as_mut().unwrap() }
    }

    fn data(&self) -> &IndexPageData {
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

    pub fn insert(&mut self, key: Key, value: LeafValue) -> Result<()> {
        self.latch.wlock();
        let data = self.data_mut();

        if data.keys.is_full() {
            return Err(anyhow!("Page is full"));
        }

        let pos = match data.keys.binary_search(&key) {
            Ok(_) => return Err(anyhow!("Key already exists")),
            Err(pos) => pos, // Position where the key should be inserted
        };

        data.keys.insert(pos, key);
        data.values.insert(pos, value);
        data.size += 1;

        self.latch.wunlock();

        Ok(())
    }

    pub fn delete(&mut self, key: Key) -> Result<()> {
        self.latch.wlock();
        let data = self.data_mut();

        let pos = match data.keys.binary_search(&key) {
            Ok(pos) => pos,
            Err(_) => return Err(anyhow!("Key not found")),
        };

        data.keys.remove(pos);
        data.values.remove(pos);
        data.size -= 1;

        self.latch.wunlock();

        Ok(())
    }

    pub fn search(&self, key: Key) -> Option<LeafValue> {
        let data = self.data();

        // Perform binary search to find the key
        match data.keys.binary_search(&key) {
            Ok(pos) => Some(data.values[pos]), // If key is found, return corresponding value
            Err(_) => None,                    // If key is not found, return None
        }
    }

    pub fn split(self) -> (Self, Self) {
        unimplemented!("Split an index page")
    }

    pub fn is_full(&self) -> bool {
        self.data().size == KEYS_PER_NODE as u16
    }

    pub fn is_empty(&self) -> bool {
        self.data().size == 0
    }

    pub fn is_half_full(&self) -> bool {
        self.data().size == (KEYS_PER_NODE / 2) as u16
    }
}

impl Debug for IndexPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.data();
        f.debug_struct("IndexPage")
            .field("page_type", &s.page_type)
            .field("level", &s.level)
            .field("slots", &s.size)
            .field("prev", &s.prev)
            .field("next", &s.next)
            .field("keys", &s.keys)
            .field("values", &s.values)
            .finish()
    }
}

impl<'a> From<&'a Page> for IndexPage {
    fn from(page: &'a Page) -> IndexPage {
        let data = &page.data as *const PageData as *mut IndexPageData;
        IndexPage {
            data,
            page_id: page.get_page_id(),
            latch: page.latch.clone(),
        }
    }
}

impl<'a> From<&'a mut Page> for IndexPage {
    fn from(page: &'a mut Page) -> IndexPage {
        let data = &mut page.data as *mut PageData as *mut IndexPageData;
        IndexPage {
            data,
            page_id: page.get_page_id(),
            latch: page.latch.clone(),
        }
    }
}
