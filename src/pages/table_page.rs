use super::{traits::Serialize, PageData, PageId};
use super::{Page, SlotId, PAGE_SIZE};
use crate::latch::Latch;
use crate::tuple::{Entry, Tuple, TupleId, TupleMetaData};
use anyhow::{anyhow, Result};
use core::panic;
use std::{mem, slice, sync::Arc};

/// The part of the header that persists on disk (num_tuples (2) and next_page (4)).
/// The rest are computed on the fly
const HEADER_SIZE: usize = 2 + mem::size_of::<PageId>();
pub const SLOT_SIZE: usize = mem::size_of::<TablePageSlot>();
pub const META_SIZE: usize = mem::size_of::<TupleMetaData>();

// We take the first [`HEADER_SIZE`] bytes from the page to store the header
// This means that the last address in the page is [`PAGE_END`] and not [`PAGE_SIZE`].
pub const PAGE_END: usize = PAGE_SIZE - HEADER_SIZE;

#[repr(C, packed)]
#[derive(Debug)]
pub struct TablePageHeader {
    _padding: [u8; 3],
    is_dirty: bool,
    // These two fields are part of the physical page
    // the above two don't persist on disk (yes, even
    // though they are part of the header)
    num_tuples: SlotId,
    /// INVALID_PAGE (-1) if there are no more pages
    next_page: PageId,
}

/// The Table Page data that persists on disk
/// all other fields are helpers (pointers and flags)
/// that are computed on the fly
#[repr(C)]
#[derive(Debug)]
struct TablePageData {
    header: TablePageHeader,
    bytes: [u8; PAGE_END],
}

#[repr(C)]
#[derive(Debug)]
pub struct TablePage {
    data: *mut TablePageData,
    page_id: PageId,
    latch: Arc<Latch>,
    read_only: bool,
}

impl TablePage {
    #[cfg(test)]
    pub fn get_latch(&self) -> &Arc<Latch> {
        &self.latch
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

    #[cfg(test)]
    pub fn is_dirty(&self) -> bool {
        self.header().is_dirty
    }

    pub fn set_next_page_id(&mut self, page_id: PageId) {
        if self.read_only {
            panic!("Cannot modify read only page");
        }
        let locked = self.latch.try_wlock();

        self.header_mut().set_next_page_id(page_id);

        if locked {
            self.latch.wunlock();
        }
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    fn last_slot_offset(&self) -> Option<usize> {
        let num_tuples = self.header().get_num_tuples() as usize;
        if num_tuples == 0 {
            None
        } else {
            Some((num_tuples - 1) * (SLOT_SIZE))
        }
    }

    #[inline]
    fn get_slot(&self, slot: SlotId) -> Option<TablePageSlot> {
        if slot >= self.header().get_num_tuples() {
            return None;
        }

        let offset = slot as usize * SLOT_SIZE;
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
        let slots = self.header().get_num_tuples() as usize * SLOT_SIZE;
        let offset = self.last_tuple_offset();
        offset - slots
    }

    /// similar to insert tuple but avoids adding the tuple metadata
    /// used mainly in blob pages
    pub fn insert_raw(&mut self, tuple: &Tuple) -> Result<TupleId> {
        if self.read_only {
            panic!("Cannot modify read only page");
        }

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
        self.header_mut().mark_dirty();

        Ok((self.page_id, self.header().get_num_tuples() - 1))
    }

    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<TupleId> {
        if self.read_only {
            panic!("Cannot modify read only page");
        }

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
        self.header_mut().mark_dirty();

        Ok((self.page_id, self.header().get_num_tuples() - 1))
    }

    pub fn delete_tuple(&mut self, slot: SlotId) {
        if self.read_only {
            panic!("Cannot modify read only page");
        }
        let slot = self.get_slot(slot).expect("Asked for invalid slot");

        let data = unsafe { self.data.as_mut().unwrap() };

        let slice = &mut data.bytes[slot.offset as usize..(slot.offset as usize + META_SIZE)];

        let mut meta = TupleMetaData::from_bytes(slice);
        meta.mark_deleted();

        slice.copy_from_slice(meta.to_bytes());
        self.header_mut().mark_dirty();
    }

    pub fn read_tuple(&self, slot: SlotId) -> Entry {
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
    pub fn read_raw(&self, slot: SlotId) -> Tuple {
        let _rguard = self.latch.rguard();
        let slot = self.get_slot(slot).unwrap_or_else(|| {
            panic!(
                "Page: {} Asked for invalid slot {} size {}\n{:?}",
                self.page_id,
                slot,
                self.header().get_num_tuples(),
                unsafe { self.data.as_ref() }.unwrap().bytes
            )
        });

        let tuple_offset = slot.offset as usize;
        let tuple_size = slot.size as usize;

        let data = unsafe { self.data.as_ref().unwrap() };

        let tuple_data = &data.bytes[tuple_offset..(tuple_offset + tuple_size)];
        Tuple::from_bytes(tuple_data)
    }
}

impl<'a> From<&'a mut Page> for TablePage {
    fn from(page: &'a mut Page) -> TablePage {
        let data = &mut page.data as *mut PageData as *mut TablePageData;
        TablePage {
            data,
            page_id: page.get_page_id(),
            latch: page.latch.clone(),
            read_only: false,
        }
    }
}

impl<'a> From<&'a Page> for TablePage {
    fn from(page: &'a Page) -> TablePage {
        let data = &page.data as *const PageData as *mut TablePageData;
        TablePage {
            data,
            page_id: page.get_page_id(),
            latch: page.latch.clone(),
            read_only: true,
        }
    }
}

impl TablePageHeader {
    pub fn mark_dirty(&mut self) {
        self.is_dirty |= true;
    }

    pub fn set_next_page_id(&mut self, page: PageId) {
        self.next_page = page;
        self.mark_dirty();
    }

    pub fn get_next_page(&self) -> PageId {
        self.next_page
    }

    pub fn add_tuple(&mut self) {
        self.num_tuples += 1;
        self.mark_dirty();
    }

    pub fn get_num_tuples(&self) -> SlotId {
        self.num_tuples as SlotId
    }
}

#[repr(C)]
#[derive(Debug)]
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
        let bytes: [u8; SLOT_SIZE] = bytes.try_into().unwrap();
        unsafe { std::mem::transmute::<[u8; SLOT_SIZE], TablePageSlot>(bytes) }
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
        tuple::{
            constraints::Constraints,
            schema::{Field, Schema},
        },
        types::{Types, ValueFactory},
    };

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_lock_sharing() -> Result<()> {
        let page = &mut Page::new();

        let t1: TablePage = page.into();
        let t2: TablePage = page.into();

        t1.latch.try_wlock();

        assert!(t2.latch.is_locked());
        assert!(page.latch.is_locked());

        t1.latch.wunlock();

        assert!(!t2.latch.is_locked());
        assert!(!page.latch.is_locked());

        t1.latch.rlock();
        page.latch.upgradable_rlock();
        t2.latch.rlock();

        Ok(())
    }

    #[test]
    fn test_underlying_page_share() -> Result<()> {
        let page = &mut Page::new();
        let mut table_page: TablePage = page.into();
        let table_page_2: TablePage = page.into();

        let tuple = Tuple::new(
            vec![ValueFactory::from_string(&Types::UInt, "300")],
            &Schema::new(vec![Field::new(
                "a",
                Types::UInt,
                Constraints::nullable(false),
            )]),
        );

        table_page.insert_tuple(&tuple)?;

        assert_eq!(
            page.read_bytes(PAGE_SIZE - Types::UInt.size(), PAGE_SIZE),
            tuple.to_bytes()
        );
        assert!(page.is_dirty());
        assert!(table_page.header().is_dirty);
        assert!(table_page_2.header().is_dirty);

        assert_eq!(table_page_2.read_tuple(0).1.to_bytes(), tuple.to_bytes());

        Ok(())
    }
}
