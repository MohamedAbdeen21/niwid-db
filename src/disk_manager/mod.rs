use crate::pages::traits::Serialize;
use crate::pages::{Page, PAGE_SIZE};
use anyhow::Result;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;

// standard value for most common unix systems
const O_DIRECT: i32 = 0o040000;

pub fn write_to_file<T: Into<Page> + Copy>(page: &T, path: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .custom_flags(O_DIRECT)
        .open(path)?;

    let page: Page = page.clone().into();
    file.write_all(page.as_bytes())?;
    Ok(())
}

pub fn read_from_file<T: From<Page>>(path: &str) -> Result<T> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(O_DIRECT)
        .open(path)?;

    let mut buffer = vec![0u8; PAGE_SIZE];
    file.read_exact(&mut buffer)?;
    Ok(Page::from_bytes(&buffer).into())
}
