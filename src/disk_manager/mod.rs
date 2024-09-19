use std::fs::File;
use std::io::{Read, Write};
use anyhow::Result;
use crate::pages::{Page, PAGE_SIZE};

pub fn write_to_file<T: Into<Page> + Copy>(page: &T, path: &str) -> Result<()> {
    let mut file = File::create(path)?;
    let page: Page = page.clone().into();
    file.write_all(page.as_bytes())?;
    Ok(())
}

pub fn read_from_file<T: From<Page>>(path: &str) -> Result<T> {
    let mut file = File::open(path)?;
    let mut buffer = vec![0u8; PAGE_SIZE];
    file.read_exact(&mut buffer)?;
    Ok(Page::from_bytes(&buffer).into())
}
