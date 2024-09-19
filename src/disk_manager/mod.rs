use crate::pages::traits::Serialize;
use crate::pages::{PageId, INVALID_PAGE};
use anyhow::{anyhow, Result};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::path::Path;

pub const DISK_STORAGE: &str = "data/test";

#[derive(Debug)]
pub struct DiskManager {
    path: String,
}

/// Any type that can be written to disk must implement this trait
/// currently only implemented by Page, but maybe I'll need a Blob Page
/// type for strings > PAGE_SIZE (??)
pub trait DiskWritable: Serialize {
    /// how many bytes to be read from disk
    fn size() -> usize;
    /// used to correctly set page id after reading
    fn set_page_id(&mut self, page_id: PageId);
    /// used as filename when writing to disk
    fn get_page_id(&self) -> PageId;
}

// TODO: Find a way to do Direct IO
impl DiskManager {
    pub fn new(path: &str) -> Self {
        std::fs::create_dir_all(path).unwrap();
        Self {
            path: path.to_string(),
        }
    }

    pub fn write_to_file<T: DiskWritable>(&self, page: &T) -> Result<()> {
        if page.get_page_id() == INVALID_PAGE {
            return Err(anyhow!("Asked to write a page with invalid ID"));
        }

        let path = Path::join(
            Path::new(&self.path),
            Path::new(&page.get_page_id().to_string()),
        );

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false) // don't overwrite existing file
            .open(path)
            .expect("file opened successfully");

        file.write_all(page.to_bytes())
            .expect("file written successfully");
        Ok(())
    }

    pub fn read_from_file<T: DiskWritable>(&self, page_id: PageId) -> Result<T> {
        let path = Path::join(Path::new(&self.path), Path::new(&page_id.to_string()));

        let mut file = OpenOptions::new().read(true).open(path)?;

        let mut buffer = vec![0u8; T::size()];
        file.read_exact(&mut buffer)?;
        let mut page = T::from_bytes(&buffer);
        page.set_page_id(page_id);
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pages::Page;
    use crate::{pages::table_page::TablePage, tuple::Tuple, types::Str};
    use std::fs::remove_file;

    const TEST_PATH: &str = "data/test/";

    fn cleanup_disk(page: String) -> Result<()> {
        Ok(remove_file(format!("{}/{}", TEST_PATH, page))?)
    }

    #[test]
    fn test_write_then_read() -> Result<()> {
        let page_id = 9999;

        let mut page = Page::new();
        page.set_page_id(page_id);

        let disk = DiskManager::new(TEST_PATH);
        disk.write_to_file(&page)?;

        let read_page = disk.read_from_file::<Page>(page_id)?;

        assert_eq!(read_page.get_page_id(), page_id);
        assert_eq!(read_page.get_page_id(), page.get_page_id());
        assert_eq!(read_page.to_bytes(), page.to_bytes());

        cleanup_disk(page_id.to_string())?;

        Ok(())
    }

    #[test]
    fn test_write_bytes_then_read() -> Result<()> {
        let disk = DiskManager::new(TEST_PATH);
        let page_id = 8888;

        let page = &mut Page::new();
        page.set_page_id(page_id);
        let table_page: *mut TablePage = page.into();

        let tuple = Tuple::new(vec![Str("Hello!".to_string()).into()]);
        let (write_page_id, write_slot_id) =
            unsafe { table_page.as_mut().unwrap() }.insert_raw(&tuple)?;

        assert_eq!(page_id, write_page_id);

        disk.write_to_file(page)?;

        let page: *const TablePage = (&disk.read_from_file::<Page>(page_id)?).into();
        let read_tuple = unsafe { page.as_ref().unwrap() }.read_raw(write_slot_id);

        assert_eq!(read_tuple.get_data(), tuple.get_data());

        cleanup_disk(page_id.to_string())?;

        Ok(())
    }
}
