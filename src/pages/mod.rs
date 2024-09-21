pub mod latch;
pub(crate) mod table_page;
pub(crate) mod traits;

use std::sync::Arc;

use latch::Latch;
use traits::Serialize;

use crate::disk_manager::DiskWritable;

pub const PAGE_SIZE: usize = 4096; // 4 KBs
pub const INVALID_PAGE: PageId = -1;

pub type PageId = i64;

/// A generic page with an underlying array of [`PAGE_SIZE`] bytes
/// Other pages must implement `From<Page>` and `Into<Page>` traits
#[repr(C)]
#[derive(Debug, Clone)]
pub struct Page {
    /// Underlying block of memory of size [`PAGE_SIZE`]
    /// first two bytes are the is_dirty flag as it's shared with
    /// other page types
    data: [u8; PAGE_SIZE],
    page_id: PageId,
    latch: Arc<Latch>,
}

impl Serialize for Page {
    fn to_bytes(&self) -> &[u8] {
        &self.data
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), PAGE_SIZE);
        let mut page = Page::new();
        page.data.copy_from_slice(bytes);
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
            data: [0; PAGE_SIZE],
            page_id: INVALID_PAGE,
            latch: Arc::new(Latch::new()),
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.data[0] == 1
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    pub fn read_bytes(&self, start: usize, end: usize) -> &[u8] {
        &self.data[start..end]
    }

    pub fn write_bytes(&mut self, start: usize, end: usize, bytes: &[u8]) {
        self.data[start..end].copy_from_slice(bytes);
        self.data[0] = 1;
    }

    pub fn set_latch(&mut self, latch: Arc<Latch>) {
        self.latch = latch;
    }

    #[cfg(test)]
    pub fn get_latch(&self) -> &Arc<Latch> {
        &self.latch
    }
}
