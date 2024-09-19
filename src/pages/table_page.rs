use super::traits::Serialize;

use std::{mem, slice};
use super::{PAGE_SIZE, Page};

/// The table page data that persists on disk
/// all other fields are helpers (pointers and flags)
/// that are computed on the fly
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct TablePageData {
    header: TablePageHeader,
    data: [u8; PAGE_SIZE - mem::size_of::<TablePageHeader>()],
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TablePage {
    data: TablePageData,
    is_dirty: bool,
    last_tuple_offset: u8,
    last_slot_offset: u8,
}

#[allow(dead_code)]
impl TablePage {
    pub fn new() -> Self {
        Page::new().into()
    }

    pub fn header(&self) -> &TablePageHeader {
        &self.data.header
    }

    pub fn header_mut(&mut self) -> &mut TablePageHeader {
        &mut self.data.header
    }
}

impl Into<Page> for TablePage {
    fn into(self) -> Page {
        let mut page = Page::from_bytes(self.as_bytes());
        page.is_dirty = if self.is_dirty { 1 } else { 0 };
        page
    }
}

impl From<Page> for TablePage {
    fn from(page: Page) -> Self {
        assert_eq!(page.data.len(), PAGE_SIZE);
        let mut table_page = Self::from_bytes(page.as_bytes());
        table_page.is_dirty = page.is_dirty == 1;
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
            last_tuple_offset: 0,
            last_slot_offset: 0,
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
        unsafe {
            slice::from_raw_parts(
                (self as *const TablePageHeader) as *const u8,
                mem::size_of::<TablePageHeader>(),
            )
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), mem::size_of::<TablePageHeader>());
        unsafe { *(bytes.as_ptr() as *const TablePageHeader) }
    }
}

#[allow(dead_code)]
impl TablePageHeader {
    pub fn new() -> Self {
        TablePageHeader {
            next_page: -1,
            num_tuples: 0,
        }
    }

    pub fn set_next_page(&mut self, next_page: i32) {
        self.next_page = next_page;
    }

    pub fn get_next_page(&self) -> i32 {
        self.next_page
    }

    pub fn add_tuple(&mut self) {
        self.num_tuples += 1;
    }
}
