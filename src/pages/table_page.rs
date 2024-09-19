use super::traits::Serialize;

use super::{Page, PAGE_SIZE};
use anyhow::{anyhow, Result};
use std::{mem, slice};

const SLOT_SIZE: usize = mem::size_of::<TablePageSlot>();
const HEADER_SIZE: usize = mem::size_of::<TablePageHeader>();
// We take the first [`HEADER_SIZE`] bytes from the page to store the header
// This means that the last address in the page is [`PAGE_END`] and not [`PAGE_SIZE`].
const PAGE_END: usize = PAGE_SIZE - HEADER_SIZE;

/// The Table Page data that persists on disk
/// all other fields are helpers (pointers and flags)
/// that are computed on the fly
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct TablePageData {
    header: TablePageHeader,
    data: [u8; PAGE_END],
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TablePage {
    data: TablePageData,
    is_dirty: bool,
}

impl TablePage {
    pub fn new() -> Self {
        let mut p: Self = Page::new().into();
        p.header_mut().set_next_page(-1);
        p
    }

    pub fn header(&self) -> &TablePageHeader {
        &self.data.header
    }

    pub fn header_mut(&mut self) -> &mut TablePageHeader {
        &mut self.data.header
    }

    fn last_slot_offset(&self) -> Option<usize> {
        let num_tuples = self.header().num_tuples as usize;
        if num_tuples == 0 {
            None
        } else {
            Some((num_tuples - 1) * (SLOT_SIZE))
        }
    }

    #[inline]
    fn get_slot(&self, slot: usize) -> Option<TablePageSlot> {
        let offset = slot * SLOT_SIZE;
        Some(TablePageSlot::from_bytes(
            &self.data.data[offset..offset + SLOT_SIZE],
        ))
    }

    #[inline]
    fn last_slot(&self) -> Option<TablePageSlot> {
        if self.header().num_tuples == 0 {
            None
        } else {
            self.get_slot(self.header().num_tuples as usize - 1)
        }
    }

    #[inline]
    fn last_tuple_offset(&self) -> usize {
        match self.last_slot() {
            Some(slot) => slot.offset as usize,
            None => PAGE_END,
        }
    }

    #[inline]
    fn free_space(&self) -> usize {
        let slots = self.header().num_tuples as usize * SLOT_SIZE;
        let offset = self.last_tuple_offset();
        offset - slots
    }

    pub fn insert_tuple(&mut self, tuple: &[u8]) -> Result<()> {
        let tuple_size = tuple.len();
        if tuple_size + SLOT_SIZE > self.free_space() {
            return Err(anyhow!("Not enough space to insert tuple"));
        }

        let last_offset = self.last_tuple_offset();
        let tuple_offset = last_offset - tuple_size;

        let slot = TablePageSlot {
            offset: tuple_offset as u16,
            size: tuple_size as u16,
            is_null: false,
        };

        let slot_offset = match self.last_slot_offset() {
            Some(offset) => offset + SLOT_SIZE,
            None => 0,
        };

        self.data.data[slot_offset..(slot_offset + SLOT_SIZE)].copy_from_slice(slot.as_bytes());
        self.data.data[tuple_offset..(tuple_offset + tuple_size)].copy_from_slice(tuple);

        self.header_mut().add_tuple();
        self.is_dirty |= true;

        Ok(())
    }

    pub fn read_tuple(&self, slot: usize) -> &[u8] {
        let slot = self.get_slot(slot).expect("Asked for invalid slot");
        &self.data.data[slot.offset as usize..(slot.offset + slot.size) as usize]
    }
}

impl Into<Page> for TablePage {
    fn into(self) -> Page {
        let mut page = Page::from_bytes(self.as_bytes());
        page.is_dirty = self.is_dirty;
        page
    }
}

impl From<Page> for TablePage {
    fn from(page: Page) -> Self {
        assert_eq!(page.data.len(), PAGE_SIZE);
        let mut table_page = Self::from_bytes(page.as_bytes());
        table_page.is_dirty = page.is_dirty;
        table_page
    }
}

impl Serialize for TablePage {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(
                (&self.data as *const TablePageData) as *const u8,
                mem::size_of::<TablePageData>(),
            )
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), mem::size_of::<TablePageData>());
        let page_data = unsafe { *(bytes.as_ptr() as *const TablePageData) };
        TablePage {
            data: page_data,
            is_dirty: false,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TablePageHeader {
    /// -1 if there are no more pages
    next_page: i32,
    num_tuples: u16,
}

impl Serialize for TablePageHeader {
    fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts((self as *const TablePageHeader) as *const u8, HEADER_SIZE) }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), HEADER_SIZE);
        unsafe { *(bytes.as_ptr() as *const TablePageHeader) }
    }
}

impl TablePageHeader {
    pub fn set_next_page(&mut self, next_page: i32) {
        self.next_page = next_page;
    }

    #[allow(unused)]
    pub fn get_next_page(&self) -> i32 {
        self.next_page
    }

    pub fn add_tuple(&mut self) {
        self.num_tuples += 1;
    }
}

// TODO: remove packed?
// the slot should have size 2 + 2 + 1 (1 for bool) = 5
// packed makes sure it is not padded to 6
// can cause perf issues and prevents taking references to these fields
// should revisit later
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct TablePageSlot {
    offset: u16,
    size: u16,
    is_null: bool,
}

impl Serialize for TablePageSlot {
    fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts((self as *const TablePageSlot) as *const u8, SLOT_SIZE) }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), SLOT_SIZE);
        unsafe { *(bytes.as_ptr() as *const TablePageSlot) }
    }
}
