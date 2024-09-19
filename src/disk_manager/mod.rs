use crate::pages::traits::Serialize;
use crate::pages::{Page, PAGE_SIZE};
use anyhow::Result;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub struct DiskManager {
    path: String,
}

// standard value for most common unix systems
const O_DIRECT: i32 = 0o040000;

impl DiskManager {
    pub fn new(path: &str) -> Self {
        std::fs::create_dir_all(path).unwrap();
        Self {
            path: path.to_string(),
        }
    }

    pub fn write_to_file<T: Into<Page> + Copy>(&self, page: &T, file: &str) -> Result<()> {
        let page: Page = (*page).into();

        let file = file.to_string() + "_" + page.page_id().to_string().as_str();

        let path = Path::join(Path::new(&self.path), Path::new(&file));

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(path)?;

        file.write_all(page.as_bytes())?;
        Ok(())
    }

    pub fn read_from_file<T: From<Page>>(&self, file: &str, page_id: i32) -> Result<T> {
        let file = file.to_string() + "_" + page_id.to_string().as_str();

        let path = Path::join(Path::new(&self.path), Path::new(&file));

        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(O_DIRECT)
            .open(path)?;

        let mut buffer = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buffer)?;
        let mut page = Page::from_bytes(&buffer);
        page.set_page_id(page_id);
        Ok(page.into())
    }
}
