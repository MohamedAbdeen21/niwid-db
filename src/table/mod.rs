use crate::{
    buffer_pool::{BufferPool, BufferPoolManager},
    pages::table_page::{TablePage, TupleId},
    tuple::{Entry, Tuple},
};
use anyhow::Result;

pub mod table_iterator;

#[allow(unused)]
#[derive(Clone)]
pub struct Table {
    first_page: *mut TablePage,
    last_page: Option<*mut TablePage>,
    bpm: BufferPoolManager,
}

impl Table {
    pub fn new() -> Result<Self> {
        let bpm = BufferPool::new();

        Ok(Self {
            first_page: bpm
                .write()
                .unwrap()
                .new_page()?
                .write()
                .unwrap()
                .get_page_write()
                .into(),
            last_page: None,
            bpm,
        })
    }

    fn to_iter(self) -> table_iterator::TableIterator {
        table_iterator::TableIterator::new(self)
    }

    pub fn insert(&mut self, tuple: Tuple) -> Result<()> {
        let to_insert = unsafe {
            match self.last_page {
                Some(page) => page.as_mut().unwrap(),
                None => self.first_page.as_mut().unwrap(),
            }
        };

        if to_insert.insert_tuple(&tuple).is_ok() {
            return Ok(());
        }

        // page is full, add another page and link to table
        let mut bpm = self.bpm.write().unwrap();
        let mut frame = bpm.new_page()?.write().unwrap();
        let page = frame.get_page_write();

        to_insert.header_mut().set_next_page_id(page.get_page_id());

        self.last_page = Some(page.into());
        unsafe {
            self.last_page
                .unwrap()
                .as_mut()
                .unwrap()
                .insert_tuple(&tuple)?;
        }

        Ok(())
    }

    pub fn scan(&self, mut f: impl FnMut(&Entry)) {
        self.clone().to_iter().for_each(|entry| f(&entry))
    }

    pub fn delete(&mut self, id: TupleId) -> Result<()> {
        let (page_id, slot_id) = id;
        let mut bpm = self.bpm.write().unwrap();
        let mut frame = bpm.fetch_page(page_id)?.write().unwrap();
        let page = frame.get_page_write();

        let page: *mut TablePage = page.into();

        unsafe {
            page.as_mut().unwrap().delete_tuple(slot_id);
        }

        Ok(())
    }
}
