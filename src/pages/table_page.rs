use super::traits::Serialize;
use crate::tuple::TupleMetaData;

use super::{Page, PAGE_SIZE};
use anyhow::{anyhow, Result};
use std::{mem, slice};

const INVALID_PAGE: i32 = -1;

const SLOT_SIZE: usize = mem::size_of::<TablePageSlot>();
const META_SIZE: usize = mem::size_of::<TupleMetaData>();
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
    bytes: [u8; PAGE_END],
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
        p.header_mut().set_next_page(INVALID_PAGE);
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
        if slot >= self.header().num_tuples as usize {
            return None;
        }

        let offset = slot * SLOT_SIZE;
        Some(TablePageSlot::from_bytes(
            &self.data.bytes[offset..offset + SLOT_SIZE],
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
        let entry_size = tuple.len() + META_SIZE;
        if entry_size + SLOT_SIZE > self.free_space() {
            return Err(anyhow!("Not enough space to insert tuple"));
        }

        let last_offset = self.last_tuple_offset();
        let tuple_offset = last_offset - tuple.len();
        let entry_offset = tuple_offset - META_SIZE;

        let slot = TablePageSlot::new(entry_offset, entry_size);
        let meta = TupleMetaData::new();

        let slot_offset = match self.last_slot_offset() {
            Some(offset) => offset + SLOT_SIZE,
            None => 0,
        };

        self.data.bytes[slot_offset..(slot_offset + SLOT_SIZE)].copy_from_slice(slot.as_bytes());
        self.data.bytes[entry_offset..(entry_offset + META_SIZE)].copy_from_slice(meta.as_bytes());
        self.data.bytes[tuple_offset..(tuple_offset + tuple.len())].copy_from_slice(tuple);

        self.header_mut().add_tuple();
        self.is_dirty |= true;

        Ok(())
    }

    pub fn delete_tuple(&mut self, slot: usize) {
        let slot = self.get_slot(slot).expect("Asked for invalid slot");

        let mut deleted_meta = TupleMetaData::new();
        deleted_meta.mark_deleted();

        self.data.bytes[slot.offset as usize..(slot.offset as usize + META_SIZE)]
            .copy_from_slice(deleted_meta.as_bytes());
        self.is_dirty |= true;
    }

    pub fn read_tuple(&self, slot: usize) -> (TupleMetaData, &[u8]) {
        let slot = self.get_slot(slot).expect("Asked for invalid slot");

        let meta_offset = slot.offset as usize;
        let tuple_offset = slot.offset as usize + META_SIZE;
        let tuple_size = slot.size as usize - META_SIZE;

        let meta =
            TupleMetaData::from_bytes(&self.data.bytes[meta_offset..(meta_offset + META_SIZE)]);
        let tuple = &self.data.bytes[tuple_offset..(tuple_offset + tuple_size) as usize];

        return (meta, tuple);
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
    /// INVALID_PAGE (-1) if there are no more pages
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

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct TablePageSlot {
    offset: u16,
    size: u16,
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

impl TablePageSlot {
    pub fn new(offset: usize, size: usize) -> Self {
        Self {
            offset: offset as u16,
            size: size as u16,
        }
    }
}
