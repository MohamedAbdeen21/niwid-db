mod shadow_page;

use crate::pages::traits::Serialize;
use crate::pages::{PageId, INVALID_PAGE};
use anyhow::{anyhow, Context, Result};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;

pub const DISK_STORAGE: &str = "data/data/";

#[cfg(test)]
pub fn test_path() -> String {
    use uuid::Uuid;

    let id = Uuid::new_v4(); // Generate a unique UUID
    format!("data/test/test_{}/", id)
}

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
        let path = Path::new(path);

        std::fs::create_dir_all(path).unwrap();

        Self {
            path: path.to_str().unwrap().to_string(),
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
            .open(path)?;

        file.write_all(page.to_bytes())
            .expect("file written successfully");

        Ok(())
    }

    pub fn read_from_file<T: DiskWritable>(&self, page_id: PageId) -> Result<T> {
        if page_id == INVALID_PAGE {
            return Err(anyhow!("Asked to read a page with invalid ID"));
        }

        let path = Path::join(Path::new(&self.path), Path::new(&page_id.to_string()));

        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .context("file opened for reading")?;

        let mut buffer = vec![0u8; T::size()];
        file.read_exact(&mut buffer)
            .expect("Failed to read buffer from disk");
        let mut page = T::from_bytes(&buffer);
        page.set_page_id(page_id);

        Ok(page)
    }

    #[allow(dead_code)]
    pub fn start_transaction(&self, transaction_id: u64) -> Result<()> {
        let trans_cache = Path::join(
            Path::new(&self.path),
            Path::new(&transaction_id.to_string()),
        );

        std::fs::create_dir_all(&trans_cache)?;

        Ok(())
    }

    #[allow(dead_code)]
    pub fn shadow_page<T: DiskWritable>(&self, transaction_id: u64, page_id: PageId) -> Result<T> {
        let trans_cache = Path::join(
            Path::new(&self.path),
            Path::new(&transaction_id.to_string()),
        );

        let to_path = Path::join(Path::new(&trans_cache), Path::new(&page_id.to_string()));
        let from_path = Path::join(Path::new(&self.path), Path::new(&page_id.to_string()));

        std::fs::copy(&from_path, &to_path)?;

        let mut file = OpenOptions::new()
            .read(true)
            .open(to_path)
            .context("file opened for reading")?;

        let mut buffer = vec![0u8; T::size()];
        file.read_exact(&mut buffer)
            .expect("Failed to read buffer from disk");
        let mut page = T::from_bytes(&buffer);
        page.set_page_id(page_id);

        Ok(page)
    }

    #[allow(dead_code)]
    pub fn commit_transaction(&self, transaction_id: u64) -> Result<()> {
        let trans_cache = Path::join(
            Path::new(&self.path),
            Path::new(&transaction_id.to_string()),
        );

        let trans_cache_committed = Path::join(
            Path::new(&self.path),
            Path::new(&format!("{}.committed", transaction_id)),
        );

        // should be atomic
        std::fs::rename(&trans_cache, &trans_cache_committed)?;

        std::fs::remove_dir_all(trans_cache_committed)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pages::Page;
    use crate::tuple::schema::Schema;
    use crate::types::Types;
    use crate::{pages::table_page::TablePage, tuple::Tuple, types::Str};
    use std::fs::remove_dir_all;

    #[test]
    fn test_write_then_read() -> Result<()> {
        let page_id = 9999;

        let mut page = Page::new();
        page.set_page_id(page_id);

        let path = test_path();

        let disk = DiskManager::new(&path);
        disk.write_to_file(&page)?;

        let read_page = disk.read_from_file::<Page>(page_id)?;

        assert_eq!(read_page.get_page_id(), page_id);
        assert_eq!(read_page.get_page_id(), page.get_page_id());
        assert_eq!(read_page.to_bytes(), page.to_bytes());

        remove_dir_all(path)?;

        Ok(())
    }

    #[test]
    fn test_write_bytes_then_read() -> Result<()> {
        let path = test_path();

        let disk = DiskManager::new(&path);
        let page_id = 8888;

        let page = &mut Page::new();
        page.set_page_id(page_id);
        let mut table_page: TablePage = page.into();

        let dummy_schema = Schema::new(vec!["str"], vec![Types::Str]);
        let tuple = Tuple::new(vec![Str("Hello!".to_string()).into()], &dummy_schema);
        let (write_page_id, write_slot_id) = table_page.insert_raw(&tuple)?;

        assert_eq!(page_id, write_page_id);

        disk.write_to_file(page)?;

        let page: TablePage = (&disk.read_from_file::<Page>(page_id)?).into();
        let read_tuple = page.read_raw(write_slot_id);

        assert_eq!(read_tuple.get_data(), tuple.get_data());

        remove_dir_all(path)?;

        Ok(())
    }
}
