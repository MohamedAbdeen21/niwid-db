use super::{traits::Serialize, PageId, INVALID_PAGE};
use crate::tuple::{Entry, Tuple, TupleMetaData};

use super::{Page, PAGE_SIZE};
use anyhow::{anyhow, Result};
use std::{mem, slice};

const HEADER_SIZE: usize = mem::size_of::<TablePageHeader>();
const SLOT_SIZE: usize = mem::size_of::<TablePageSlot>();
const META_SIZE: usize = mem::size_of::<TupleMetaData>();

// We take the first [`HEADER_SIZE`] bytes from the page to store the header
// This means that the last address in the page is [`PAGE_END`] and not [`PAGE_SIZE`].
const PAGE_END: usize = PAGE_SIZE - HEADER_SIZE;

/// Page Id and slot Id
pub type TupleId = (PageId, usize);

pub trait TupleExt {
    #[allow(unused)]
    fn from_bytes(bytes: &[u8]) -> Self;
    fn to_bytes(&self) -> Vec<u8>;
}

impl TupleExt for TupleId {
    fn from_bytes(bytes: &[u8]) -> Self {
        let page_offset = std::mem::size_of::<PageId>();
        let slot_size = std::mem::size_of::<usize>();
        let page_id = isize::from_ne_bytes(bytes[0..page_offset].try_into().unwrap());
        let slot_id = usize::from_le_bytes(
            bytes[page_offset..page_offset + slot_size]
                .try_into()
                .unwrap(),
        );
        (page_id, slot_id)
    }

    fn to_bytes(&self) -> Vec<u8> {
        let page_id_size = std::mem::size_of::<PageId>();
        let slot_id_size = std::mem::size_of::<usize>();
        let mut bytes = Vec::with_capacity(page_id_size + slot_id_size);
        bytes.extend_from_slice(&self.0.to_ne_bytes());
        bytes.extend_from_slice(&self.1.to_ne_bytes());

        bytes
    }
}

/// The Table Page data that persists on disk
/// all other fields are helpers (pointers and flags)
/// that are computed on the fly
#[repr(C)]
#[derive(Debug, Clone)]
struct TablePageData {
    header: TablePageHeader,
    bytes: [u8; PAGE_END],
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct TablePage {
    data: TablePageData,
    is_dirty: bool,
    page_id: PageId,
}

impl TablePage {
    pub fn header(&self) -> &TablePageHeader {
        &self.data.header
    }

    pub fn header_mut(&mut self) -> &mut TablePageHeader {
        &mut self.data.header
    }

    #[allow(unused)]
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    fn last_slot_offset(&self) -> Option<usize> {
        let num_tuples = self.header().get_num_tuples();
        if num_tuples == 0 {
            None
        } else {
            Some((num_tuples - 1) * (SLOT_SIZE))
        }
    }

    #[inline]
    fn get_slot(&self, slot: usize) -> Option<TablePageSlot> {
        if slot >= self.header().get_num_tuples() {
            return None;
        }

        let offset = slot * SLOT_SIZE;
        Some(TablePageSlot::from_bytes(
            &self.data.bytes[offset..offset + SLOT_SIZE],
        ))
    }

    #[inline]
    fn last_slot(&self) -> Option<TablePageSlot> {
        if self.header().get_num_tuples() == 0 {
            None
        } else {
            self.get_slot(self.header().get_num_tuples() - 1)
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
        let slots = self.header().get_num_tuples() * SLOT_SIZE;
        let offset = self.last_tuple_offset();
        offset - slots
    }

    /// similar to insert tuple but avoids adding the tuple metadata
    /// used mainly in blob pages
    pub fn insert_raw(&mut self, tuple: &Tuple) -> Result<TupleId> {
        let entry_size = tuple.len();
        if entry_size + SLOT_SIZE > self.free_space() {
            return Err(anyhow!("Not enough space to insert tuple"));
        }

        let last_offset = self.last_tuple_offset();
        let tuple_offset = last_offset - tuple.len();
        let entry_offset = tuple_offset;

        let slot = TablePageSlot::new(entry_offset, entry_size);

        let slot_offset = match self.last_slot_offset() {
            Some(offset) => offset + SLOT_SIZE,
            None => 0,
        };

        self.data.bytes[slot_offset..(slot_offset + SLOT_SIZE)].copy_from_slice(slot.to_bytes());
        self.data.bytes[tuple_offset..(tuple_offset + tuple.len())]
            .copy_from_slice(tuple.to_bytes());

        self.header_mut().add_tuple();
        self.is_dirty |= true;

        Ok((self.page_id, self.header().get_num_tuples() - 1))
    }

    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<TupleId> {
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

        self.data.bytes[slot_offset..(slot_offset + SLOT_SIZE)].copy_from_slice(slot.to_bytes());
        self.data.bytes[entry_offset..(entry_offset + META_SIZE)].copy_from_slice(meta.to_bytes());
        self.data.bytes[tuple_offset..(tuple_offset + tuple.len())]
            .copy_from_slice(tuple.to_bytes());

        self.header_mut().add_tuple();
        self.is_dirty |= true;

        Ok((self.page_id, self.header().get_num_tuples() - 1))
    }

    pub fn delete_tuple(&mut self, slot: usize) {
        let slot = self.get_slot(slot).expect("Asked for invalid slot");

        let mut deleted_meta = TupleMetaData::new();
        deleted_meta.mark_deleted();

        self.data.bytes[slot.offset as usize..(slot.offset as usize + META_SIZE)]
            .copy_from_slice(deleted_meta.to_bytes());
        self.is_dirty |= true;
    }

    pub fn read_tuple(&self, slot: usize) -> Entry {
        let slot = self.get_slot(slot).expect("Asked for invalid slot");

        let meta_offset = slot.offset as usize;
        let tuple_offset = slot.offset as usize + META_SIZE;
        let tuple_size = slot.size as usize - META_SIZE;

        let meta =
            TupleMetaData::from_bytes(&self.data.bytes[meta_offset..(meta_offset + META_SIZE)]);
        let tuple_data = &self.data.bytes[tuple_offset..(tuple_offset + tuple_size)];

        (meta, Tuple::from_bytes(tuple_data))
    }
}

impl<'a> From<&'a mut Page> for *mut TablePage {
    fn from(page: &'a mut Page) -> *mut TablePage {
        unsafe {
            let p = page as *mut Page as *mut TablePage;
            (*p).set_page_id(page.get_page_id());
            (*p).is_dirty = page.is_dirty();
            if (*p).header().get_next_page() == 0 {
                (*p).header_mut().set_next_page_id(INVALID_PAGE);
            }
            p
        }
    }
}

impl<'a> From<&'a Page> for TablePage {
    fn from(page: &'a Page) -> TablePage {
        let mut p = unsafe { (page as *const Page as *const TablePage).as_ref().unwrap() }.clone();
        p.set_page_id(page.get_page_id());
        p.is_dirty = page.is_dirty();
        if p.header().get_next_page() == 0 {
            p.header_mut().set_next_page_id(INVALID_PAGE);
        }
        p
    }
}

// impl From<&Page> for TablePage {
//     fn from(page: &Page) -> Self {
//         let mut p = Self::from_bytes(page.as_bytes());
//         p.set_page_id(page.get_page_id());
//         p.is_dirty = page.is_dirty();
//         p
//     }
// }

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TablePageHeader {
    /// INVALID_PAGE (-1) if there are no more pages
    next_page: PageId,
    num_tuples: u16,
}

impl TablePageHeader {
    pub fn set_next_page_id(&mut self, next_page: PageId) {
        self.next_page = next_page;
    }

    pub fn get_next_page(&self) -> PageId {
        self.next_page
    }

    pub fn add_tuple(&mut self) {
        self.num_tuples += 1;
    }

    pub fn get_num_tuples(&self) -> usize {
        self.num_tuples as usize
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct TablePageSlot {
    offset: u16,
    size: u16,
}

impl Serialize for TablePageSlot {
    fn to_bytes(&self) -> &[u8] {
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
