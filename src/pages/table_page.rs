use super::{latch::Latch, traits::Serialize, PageId, INVALID_PAGE};
use crate::tuple::{Entry, Tuple, TupleId, TupleMetaData};

use super::{Page, PAGE_SIZE};
use anyhow::{anyhow, Result};
use std::{mem, slice, sync::Arc};

const HEADER_SIZE: usize = mem::size_of::<TablePageHeader>();
pub const SLOT_SIZE: usize = mem::size_of::<TablePageSlot>();
pub const META_SIZE: usize = mem::size_of::<TupleMetaData>();

// We take the first [`HEADER_SIZE`] bytes from the page to store the header
// This means that the last address in the page is [`PAGE_END`] and not [`PAGE_SIZE`].
pub const PAGE_END: usize = PAGE_SIZE - HEADER_SIZE;

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
    data: *mut TablePageData,
    page_id: PageId,
    latch: Arc<Latch>,
    read_only: bool,
}

impl TablePage {
    #[cfg(test)]
    pub fn is_locked(&self) -> bool {
        self.latch.is_locked()
    }

    pub fn header(&self) -> &TablePageHeader {
        &unsafe { self.data.as_ref() }.unwrap().header
    }

    fn header_mut(&mut self) -> &mut TablePageHeader {
        if self.read_only {
            panic!("Cannot modify read only page");
        }
        &mut unsafe { self.data.as_mut() }.unwrap().header
    }

    pub fn set_next_page_id(&mut self, page_id: PageId) {
        if self.read_only {
            panic!("Cannot modify read only page");
        }
        self.latch.wlock();
        self.header_mut().set_next_page_id(page_id);
        self.latch.wunlock();
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
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
            &unsafe { self.data.as_ref() }.unwrap().bytes[offset..offset + SLOT_SIZE],
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
        if self.read_only {
            panic!("Cannot modify read only page");
        }
        self.latch.wlock();

        let tuple_size = tuple.len();
        if tuple_size + SLOT_SIZE > self.free_space() {
            self.latch.wunlock();
            return Err(anyhow!("Not enough space to insert tuple"));
        }

        let last_offset = self.last_tuple_offset();
        let tuple_offset = last_offset - tuple_size;

        let slot = TablePageSlot::new(tuple_offset, tuple_size);

        let slot_offset = match self.last_slot_offset() {
            Some(offset) => offset + SLOT_SIZE,
            None => 0,
        };

        let data = unsafe { self.data.as_mut().unwrap() };

        data.bytes[slot_offset..(slot_offset + SLOT_SIZE)].copy_from_slice(slot.to_bytes());
        data.bytes[tuple_offset..(tuple_offset + tuple_size)].copy_from_slice(tuple.to_bytes());

        self.header_mut().add_tuple();
        self.header_mut().is_dirty |= true;

        self.latch.wunlock();

        Ok((self.page_id, self.header().get_num_tuples() - 1))
    }

    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<TupleId> {
        if self.read_only {
            panic!("Cannot modify read only page");
        }
        self.latch.wlock();

        let entry_size = tuple.len() + META_SIZE;
        if entry_size + SLOT_SIZE > self.free_space() {
            self.latch.wunlock();
            return Err(anyhow!("Not enough space to insert tuple"));
        }

        let last_offset = self.last_tuple_offset();
        let tuple_offset = last_offset - tuple.len();
        let entry_offset = tuple_offset - META_SIZE;

        let slot = TablePageSlot::new(entry_offset, entry_size);
        let meta = TupleMetaData::new(tuple._null_bitmap);

        let slot_offset = match self.last_slot_offset() {
            Some(offset) => offset + SLOT_SIZE,
            None => 0,
        };

        let data = unsafe { self.data.as_mut().unwrap() };

        data.bytes[slot_offset..(slot_offset + SLOT_SIZE)].copy_from_slice(slot.to_bytes());
        data.bytes[entry_offset..(entry_offset + META_SIZE)].copy_from_slice(meta.to_bytes());
        data.bytes[tuple_offset..(tuple_offset + tuple.len())].copy_from_slice(tuple.to_bytes());

        self.header_mut().add_tuple();
        self.header_mut().is_dirty |= true;

        self.latch.wunlock();

        Ok((self.page_id, self.header().get_num_tuples() - 1))
    }

    pub fn delete_tuple(&mut self, slot: usize) {
        if self.read_only {
            panic!("Cannot modify read only page");
        }
        self.latch.wlock();
        let slot = self.get_slot(slot).expect("Asked for invalid slot");

        let mut deleted_meta = TupleMetaData::default(); // TODO: Copy null bitmap
        deleted_meta.mark_deleted();

        let data = unsafe { self.data.as_mut().unwrap() };

        data.bytes[slot.offset as usize..(slot.offset as usize + META_SIZE)]
            .copy_from_slice(deleted_meta.to_bytes());
        self.header_mut().is_dirty |= true;
        self.latch.wunlock();
    }

    pub fn read_tuple(&self, slot: usize) -> Entry {
        let _rguard = self.latch.rguard();
        let slot = self.get_slot(slot).expect("Asked for invalid slot");

        let meta_offset = slot.offset as usize;
        let tuple_offset = slot.offset as usize + META_SIZE;
        let tuple_size = slot.size as usize - META_SIZE;

        let data = unsafe { self.data.as_mut().unwrap() };

        let meta = TupleMetaData::from_bytes(&data.bytes[meta_offset..(meta_offset + META_SIZE)]);
        let tuple_data = &data.bytes[tuple_offset..(tuple_offset + tuple_size)];

        let mut tuple = Tuple::from_bytes(tuple_data);
        tuple._null_bitmap = meta.get_null_bitmap();

        (meta, tuple)
    }

    /// Read tuple data without the metadata
    /// or setting the bitmap
    pub fn read_raw(&self, slot: usize) -> Tuple {
        let _rguard = self.latch.rguard();
        let slot = self.get_slot(slot).expect("Asked for invalid slot");

        let tuple_offset = slot.offset as usize;
        let tuple_size = slot.size as usize;

        let data = unsafe { self.data.as_ref().unwrap() };

        let tuple_data = &data.bytes[tuple_offset..(tuple_offset + tuple_size)];
        Tuple::from_bytes(tuple_data)
    }
}

impl<'a> From<&'a mut Page> for TablePage {
    fn from(page: &'a mut Page) -> TablePage {
        let data = page.data.as_mut_ptr() as *mut TablePageData;
        let mut p = TablePage {
            data,
            page_id: page.get_page_id(),
            latch: page.latch.clone(),
            read_only: false,
        };
        if p.header().get_next_page() == 0 {
            p.header_mut().set_next_page_id(INVALID_PAGE);
        }
        p
    }
}

impl<'a> From<&'a Page> for TablePage {
    fn from(page: &'a Page) -> TablePage {
        let data = page.data.as_ptr() as *mut TablePageData;
        let mut p = TablePage {
            data,
            page_id: page.get_page_id(),
            latch: page.latch.clone(),
            read_only: false,
        };
        if p.header().get_next_page() == 0 {
            p.header_mut().set_next_page_id(INVALID_PAGE);
        }
        p.read_only = true;
        p
    }
}

#[repr(C, align(2))]
#[derive(Debug, Clone, Copy)]
pub struct TablePageHeader {
    is_dirty: bool,
    /// INVALID_PAGE (-1) if there are no more pages
    num_tuples: u16,
    next_page: PageId,
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

    #[cfg(test)]
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
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

#[cfg(test)]
mod tests {
    use crate::{
        tuple::schema::Schema,
        types::{Types, U16},
    };

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_lock_sharing() -> Result<()> {
        let page = &mut Page::new();

        let t1: TablePage = page.into();
        let t2: TablePage = page.into();

        t1.latch.wlock();

        assert!(t2.latch.is_locked());
        assert!(page.latch.is_locked());

        t1.latch.wunlock();

        assert!(!t2.latch.is_locked());
        assert!(!page.latch.is_locked());

        t1.latch.rlock();
        let _ = page.latch.upgradable_rlock();
        t2.latch.rlock();

        Ok(())
    }

    #[test]
    fn test_underlying_page_share() -> Result<()> {
        let page = &mut Page::new();
        let mut table_page: TablePage = page.into();
        let table_page_2: TablePage = page.into();

        let tuple = Tuple::new(
            vec![U16(300).into()],
            &Schema::new(vec!["a"], vec![Types::U16]),
        );

        table_page.insert_tuple(&tuple)?;
        table_page.insert_tuple(&tuple)?;

        assert_eq!(page.read_bytes(PAGE_SIZE - 2, PAGE_SIZE), tuple.to_bytes());
        assert!(page.is_dirty());
        assert!(table_page.header().is_dirty());
        assert!(table_page_2.header().is_dirty());

        assert_eq!(table_page_2.read_tuple(0).1.to_bytes(), tuple.to_bytes());

        Ok(())
    }
}
