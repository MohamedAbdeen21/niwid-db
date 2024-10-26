pub(crate) mod index_page;
pub(crate) mod table_page;
pub(crate) mod traits;

use std::sync::Arc;

use traits::Serialize;

use crate::{disk_manager::DiskWritable, latch::Latch};

pub const PAGE_SIZE: usize = 4096; // 4 KBs
pub const INVALID_PAGE: PageId = 0;

pub type PageId = u32;
pub type SlotId = u16;

/// The data that is shared and modified between all page types
/// 3 padding bytes, dirty flag, and then the actual data
#[repr(C)]
#[derive(Debug, Clone)]
pub struct PageData {
    _padding: [u8; 3],
    is_dirty: bool,
    bytes: [u8; PAGE_SIZE],
}

/// A generic page with an underlying array of [`PAGE_SIZE`] bytes
/// Other pages must implement `From<Page>` and `Into<Page>` traits
#[repr(C)]
#[derive(Debug, Clone)]
pub struct Page {
    data: PageData,
    page_id: PageId,
    latch: Arc<Latch>,
}

impl Serialize for Page {
    fn to_bytes(&self) -> &[u8] {
        &self.data.bytes
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), PAGE_SIZE);
        let mut page = Page::new();
        page.data.bytes.copy_from_slice(bytes);
        page
    }
}

impl DiskWritable for Page {
    fn size() -> usize {
        PAGE_SIZE
    }

    fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    fn get_page_id(&self) -> PageId {
        self.page_id
    }
}

impl Default for Page {
    fn default() -> Self {
        Page::new()
    }
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: PageData {
                _padding: [0; 3],
                bytes: [0; PAGE_SIZE],
                is_dirty: false,
            },
            page_id: INVALID_PAGE,
            latch: Arc::new(Latch::new()),
        }
    }

    pub fn mark_clean(&mut self) {
        self.data.is_dirty = false;
    }

    pub fn mark_dirty(&mut self) {
        self.data.is_dirty = true;
    }

    pub fn is_dirty(&self) -> bool {
        self.data.is_dirty
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    pub fn read_bytes(&self, start: usize, end: usize) -> &[u8] {
        &self.data.bytes[start..end]
    }

    pub fn write_bytes(&mut self, start: usize, end: usize, bytes: &[u8]) {
        self.data.bytes[start..end].copy_from_slice(bytes);
        self.mark_dirty();
    }

    pub fn set_latch(&mut self, latch: Arc<Latch>) {
        self.latch = latch;
    }

    #[cfg(test)]
    pub fn get_latch(&self) -> &Arc<Latch> {
        &self.latch
    }
}
