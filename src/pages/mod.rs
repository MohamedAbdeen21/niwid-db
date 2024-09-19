pub(crate) mod table_page;
pub(crate) mod traits;

use traits::Serialize;

pub const PAGE_SIZE: usize = 4096; // 4 KBs
pub const INVALID_PAGE: PageId = -1;

pub type PageId = isize;

/// A generic page with an underlying array of [`PAGE_SIZE`] bytes
/// Other pages must implement From<Page> and Into<Page> traits
#[repr(C, align(4))]
#[derive(Debug)]
pub struct Page {
    /// Underlying block of memory of size [`PAGE_SIZE`]
    data: [u8; PAGE_SIZE],
    is_dirty: bool,
    page_id: PageId,
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

impl Default for Page {
    fn default() -> Self {
        Page::new()
    }
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: [0u8; PAGE_SIZE],
            is_dirty: false,
            page_id: INVALID_PAGE,
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
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
        self.is_dirty = true;
    }
}
