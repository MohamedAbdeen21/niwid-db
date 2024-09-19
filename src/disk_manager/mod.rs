use crate::pages::traits::Serialize;
use crate::pages::{Page, PageId, INVALID_PAGE, PAGE_SIZE};
use anyhow::{anyhow, Result};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::path::Path;

#[derive(Debug)]
pub struct DiskManager {
    path: String,
}

// TODO: Find a way to do Direct IO
impl DiskManager {
    pub fn new(path: &str) -> Self {
        std::fs::create_dir_all(path).unwrap();
        Self {
            path: path.to_string(),
        }
    }

    pub fn write_to_file(&self, page: &Page) -> Result<()> {
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
            .open(path)
            .expect("file opened successfully");

        file.write_all(page.to_bytes())
            .expect("file written successfully");
        Ok(())
    }

    pub fn read_from_file(&self, page_id: PageId) -> Result<Page> {
        let path = Path::join(Path::new(&self.path), Path::new(&page_id.to_string()));

        let mut file = OpenOptions::new().read(true).open(path)?;

        let mut buffer = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buffer)?;
        let mut page = Page::from_bytes(&buffer);
        page.set_page_id(page_id);
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pages::table_page::TablePage,
        tuple::{schema::Schema, Tuple},
        types::{Primitive, Str, Types},
    };
    use std::fs::remove_dir_all;

    const TEST_PATH: &str = "data/test/";

    fn cleanup_disk(dm: DiskManager) -> Result<()> {
        Ok(remove_dir_all(dm.path)?)
    }

    #[test]
    fn test_write_then_read() -> Result<()> {
        let page_id = 9999;

        let mut page = Page::new();
        page.set_page_id(page_id);

        let disk = DiskManager::new(TEST_PATH);
        disk.write_to_file(&page)?;

        let read_page = disk.read_from_file(page_id)?;

        assert_eq!(read_page.get_page_id(), page_id);
        assert_eq!(read_page.get_page_id(), page.get_page_id());
        assert_eq!(read_page.to_bytes(), page.to_bytes());

        cleanup_disk(disk)?;

        Ok(())
    }

    #[test]
    fn test_write_bytes_then_read() -> Result<()> {
        let disk = DiskManager::new(TEST_PATH);
        let page_id = 8888;

        let page = &mut Page::new();
        page.set_page_id(page_id);
        let table_page: *mut TablePage = page.into();

        let tuple_data = vec![Str("Hello!".to_string()).to_bytes()];
        let schema = Schema::new(vec!["a"], vec![Types::Str]);
        let tuple = Tuple::new(tuple_data, &schema);
        let (write_page_id, write_slot_id) =
            unsafe { table_page.as_mut().unwrap() }.insert_raw(&tuple)?;

        assert_eq!(page_id, write_page_id);

        disk.write_to_file(page)?;

        let page: TablePage = (&disk.read_from_file(page_id)?).into();
        let read_tuple = page.read_raw(write_slot_id);

        assert_eq!(read_tuple.get_data(), tuple.get_data());

        cleanup_disk(disk)?;

        Ok(())
    }
}
