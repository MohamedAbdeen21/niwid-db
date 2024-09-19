mod traits;
pub mod table_page;

use std::mem;
use traits::Serialize;

pub const PAGE_SIZE: usize = 4096; // 4 KBs

/// A generic page with an underlying array of [`PAGE_SIZE`] bytes
/// Other pages must implement From<Page> and Into<Page> traits
#[repr(C)]
#[derive(Debug)]
pub struct Page {
    /// Underlying block of memory of size [`PAGE_SIZE`]
    data: [u8; PAGE_SIZE],
    /// u32 instead of boolean to avoid alignment issues
    is_dirty: u32,
}

#[allow(dead_code)]
impl Page {
    pub fn new() -> Self {
        Page {
            data: [0u8; PAGE_SIZE],
            is_dirty: 0,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), PAGE_SIZE);
        let mut page = Page::new();
        page.data.copy_from_slice(bytes);
        page
    }

    pub fn read<T: Serialize + Sized>(&self, offset: usize) -> T {
        self.read_sized(offset, mem::size_of::<T>())
    }

    pub fn write<T: Serialize + Sized>(&mut self, offset: usize, value: T) {
        self.write_sized(offset, mem::size_of::<T>(), value)
    }

    pub fn read_sized<T: Serialize>(&self, offset: usize, size: usize) -> T {
        let slice: &[u8] = &self.data[offset..offset + size];
        T::from_bytes(slice)
    }

    pub fn write_sized<T: Serialize>(&mut self, offset: usize, size: usize, value: T) {
        let bytes = value.as_bytes();
        self.data[offset..offset + size].copy_from_slice(bytes);
    }
}

