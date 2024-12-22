use crate::errors::Error;
use crate::pages::traits::Serialize;
use crate::pages::{PageId, INVALID_PAGE};
use crate::printdbg;
use crate::txn_manager::TxnId;
use anyhow::{anyhow, bail, Context, Result};
use std::fs::{create_dir_all, read_dir, remove_dir_all, rename, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const DISK_STORAGE: &str = "/tmp/data";

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

        create_dir_all(path).unwrap();

        let disk = Self {
            path: path.to_str().unwrap().to_string(),
        };

        disk.recover_txns().unwrap();

        create_dir_all(disk.txn_dir()).unwrap();

        disk
    }

    fn recover_txns(&self) -> Result<()> {
        let txn_dir = read_dir(self.txn_dir());

        if txn_dir.is_err() {
            return Ok(());
        }

        for txn in txn_dir.unwrap() {
            if txn.is_err() {
                return Ok(());
            }

            let txn = txn.unwrap();
            if txn.file_name().to_str().unwrap().ends_with("committed") {
                read_dir(txn.path())?
                    .try_for_each(|page| -> Result<()> {
                        let page = page?;
                        let original = Path::join(
                            Path::new(&self.path),
                            Path::new(page.file_name().to_str().unwrap()),
                        );

                        rename(page.path(), &original)?;

                        Ok(())
                    })
                    .context("Recovery: Committing pages")?;
            }
        }

        // anything not committed can be safely removed
        remove_dir_all(self.txn_dir())?;

        Ok(())
    }

    pub fn write_to_file<T: DiskWritable>(&self, page: &T, txn_id: Option<TxnId>) -> Result<()> {
        if page.get_page_id() == INVALID_PAGE {
            bail!(Error::Internal(
                "Asked to write a page with invalid ID".into()
            ));
        }

        let root = match txn_id {
            None => Path::new(&self.path),
            Some(txn_id) => &Path::join(Path::new(&self.txn_dir()), Path::new(&txn_id.to_string())),
        };

        let path = Path::join(root, Path::new(&page.get_page_id().to_string()));

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
            return Err(anyhow!("Asked to read a page with invalid ID {}", page_id));
        }

        let path = Path::join(Path::new(&self.path), Path::new(&page_id.to_string()));

        let mut file = OpenOptions::new()
            .read(true)
            .open(path.clone())
            .context(format!("file {} can't open for reading", path.display()))?;

        let mut buffer = vec![0u8; T::size()];
        file.read_exact(&mut buffer)
            .expect("Failed to read buffer from disk");
        let mut page = T::from_bytes(&buffer);
        page.set_page_id(page_id);

        Ok(page)
    }

    pub fn start_txn(&self, txn_id: TxnId) -> Result<()> {
        let txn_cache = Path::join(&self.txn_dir(), Path::new(&txn_id.to_string()));

        std::fs::create_dir_all(&txn_cache)?;

        Ok(())
    }

    pub fn shadow_page<T: DiskWritable>(&self, txn_id: TxnId, page_id: PageId) -> Result<T> {
        printdbg!("DM: shadowing page {page_id} for {txn_id}");
        let trans_cache = Path::join(Path::new(&self.txn_dir()), Path::new(&txn_id.to_string()));

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

    fn txn_dir(&self) -> PathBuf {
        Path::join(Path::new(&self.path), Path::new("txn"))
    }

    pub fn rollback_txn(&self, txn_id: TxnId) -> Result<()> {
        remove_dir_all(Path::join(
            Path::new(&self.txn_dir()),
            Path::new(&txn_id.to_string()),
        ))?;

        Ok(())
    }

    pub fn commit_txn(&self, txn_id: TxnId) -> Result<()> {
        let txn_cache = Path::join(Path::new(&self.txn_dir()), Path::new(&txn_id.to_string()));

        let txn_committed = Path::join(
            Path::new(&self.txn_dir()),
            Path::new(&format!("{}.committed", txn_id)),
        );

        // should be atomic.
        // if we don't return from the query, data might stil be committed
        // but shouldn't be much of a problem
        rename(&txn_cache, &txn_committed)?;

        let pages = read_dir(&txn_committed)?;

        pages
            .into_iter()
            .try_for_each(|page| -> Result<()> {
                let page = page?;
                let original = Path::join(
                    Path::new(&self.path),
                    Path::new(page.file_name().to_str().unwrap()),
                );

                rename(page.path(), &original)?;

                Ok(())
            })
            .context("Committing pages")?;

        remove_dir_all(txn_committed)?;

        Ok(())
    }
}

#[cfg(test)]
impl Drop for DiskManager {
    fn drop(&mut self) {
        remove_dir_all(self.path.clone()).unwrap_or_default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pages::Page;
    use crate::tuple::constraints::Constraints;
    use crate::tuple::schema::{Field, Schema};
    use crate::types::{Types, ValueFactory};
    use crate::{pages::table_page::TablePage, tuple::Tuple};
    use std::fs::remove_dir_all;

    #[test]
    fn test_write_then_read() -> Result<()> {
        let page_id = 9999;

        let mut page = Page::new();
        page.set_page_id(page_id);

        let path = test_path();

        let disk = DiskManager::new(&path);
        disk.write_to_file(&page, None)?;

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

        let dummy_schema = Schema::new(vec![Field::new(
            "str",
            Types::Str,
            Constraints::nullable(false),
        )]);

        let tuple = Tuple::new(
            vec![ValueFactory::from_string(&Types::Str, "Hello!")],
            &dummy_schema,
        );
        let (write_page_id, write_slot_id) = table_page.insert_raw(&tuple)?;

        assert_eq!(page_id, write_page_id);

        disk.write_to_file(page, None)?;

        let page: TablePage = (&disk.read_from_file::<Page>(page_id)?).into();
        let read_tuple = page.read_raw(write_slot_id);

        assert_eq!(read_tuple.get_data(), tuple.get_data());

        remove_dir_all(path)?;

        Ok(())
    }

    #[test]
    fn test_start_commit_transactions() -> Result<()> {
        let path = test_path();

        // TODO: More tests with shadows
        let disk = DiskManager::new(&path);

        disk.start_txn(1)?;
        disk.start_txn(2)?;

        assert!(std::fs::read_dir(disk.txn_dir())?.next().is_some());

        let mut iter = std::fs::read_dir(disk.txn_dir())?;
        assert!(iter.next().is_some());
        assert!(iter.next().is_some());

        disk.commit_txn(1)?;
        disk.commit_txn(2)?;

        assert!(std::fs::read_dir(disk.txn_dir())?.next().is_none());

        Ok(())
    }

    #[test]
    fn test_shadow_page() -> Result<()> {
        let path = test_path();
        let page_id = 777;
        let txn_id = 2;

        let disk = DiskManager::new(&path);

        let mut page = Page::new();
        page.set_page_id(page_id);

        let data = "Hello, World!".as_bytes();
        let start = 2;
        let end = data.len() + start;
        page.write_bytes(start, end, data);

        disk.write_to_file::<Page>(&page, None)?;

        disk.start_txn(txn_id)?;

        let mut shadowed_page = disk.shadow_page::<Page>(txn_id, page_id)?;
        shadowed_page.write_bytes(end, end + 2, &[100, 50]);
        disk.write_to_file(&shadowed_page, Some(txn_id))?;

        let read = shadowed_page.read_bytes(start, end);

        assert_eq!(read, data);

        disk.commit_txn(txn_id)?;

        let committed_page = disk.read_from_file::<Page>(page_id)?;

        assert_eq!(committed_page.read_bytes(start, end), data);
        assert_eq!(committed_page.read_bytes(end, end + 2), [100, 50]);

        Ok(())
    }
}
