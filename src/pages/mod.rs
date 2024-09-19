pub(crate) mod table_page;
pub(crate) mod table_page_iterator;
pub(crate) mod traits;

use traits::Serialize;

pub const PAGE_SIZE: usize = 4096; // 4 KBs
const INVALID_PAGE: i32 = -1;

/// A generic page with an underlying array of [`PAGE_SIZE`] bytes
/// Other pages must implement From<Page> and Into<Page> traits
#[repr(C, align(4))]
#[derive(Debug)]
pub struct Page {
    /// Underlying block of memory of size [`PAGE_SIZE`]
    data: [u8; PAGE_SIZE],
    is_dirty: bool,
    page_id: i32,
}

impl Serialize for Page {
    fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), PAGE_SIZE);
        let mut page = Page::new();
        page.data.copy_from_slice(bytes);
        page
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

    #[allow(unused)]
    pub fn is_dirty(&self) -> bool {
        return self.is_dirty;
    }

    pub fn page_id(&self) -> i32 {
        return self.page_id;
    }

    pub fn set_page_id(&mut self, page_id: i32) {
        self.page_id = page_id;
    }

    // pub fn read<T: Serialize + Sized>(&self, offset: usize) -> T {
    //     self.read_sized(offset, mem::size_of::<T>())
    // }
    //
    // pub fn write<T: Serialize + Sized>(&mut self, offset: usize, value: T) {
    //     self.write_sized(offset, mem::size_of::<T>(), value)
    // }
    //
    // pub fn read_sized<T: Serialize>(&self, offset: usize, size: usize) -> T {
    //     let slice: &[u8] = &self.data[offset..offset + size];
    //     T::from_bytes(slice)
    // }
    //
    // pub fn write_sized<T: Serialize>(&mut self, offset: usize, size: usize, value: T) {
    //     let bytes = value.as_bytes();
    //     self.data[offset..offset + size].copy_from_slice(bytes);
    // }
}
