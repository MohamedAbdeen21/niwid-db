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
    last_page: *mut TablePage,
    bpm: BufferPoolManager,
}

impl Table {
    pub fn new() -> Result<Self> {
        let bpm = BufferPool::new();

        let page: *mut TablePage = bpm
            .write()
            .unwrap()
            .new_page()?
            .write()
            .unwrap()
            .get_page_write()
            .into();

        let page_id = unsafe { (*page).get_page_id() };

        // increment pin count
        let _ = bpm.write().unwrap().fetch_frame(page_id);

        Ok(Self {
            first_page: page,
            last_page: page,
            bpm,
        })
    }

    fn to_iter(&self) -> table_iterator::TableIterator {
        table_iterator::TableIterator::new(self)
    }

    pub fn insert(&mut self, tuple: Tuple) -> Result<()> {
        let last = unsafe { self.last_page.as_mut().unwrap() };

        if last.insert_tuple(&tuple).is_ok() {
            return Ok(());
        }

        // page is full, add another page and link to table
        let mut bpm = self.bpm.write().unwrap();
        let mut frame = bpm.new_page()?.write().unwrap();
        let page = frame.get_page_write();

        last.header_mut().set_next_page_id(page.get_page_id());

        self.last_page = page.into();

        unsafe {
            self.last_page.as_mut().unwrap().insert_tuple(&tuple)?;
        }

        Ok(())
    }

    pub fn scan(&mut self, mut f: impl FnMut(&Entry)) {
        self.to_iter().for_each(|entry| f(&entry))
    }

    pub fn delete(&mut self, id: TupleId) -> Result<()> {
        let (page_id, slot_id) = id;

        let page: *mut TablePage = self
            .bpm
            .write()
            .unwrap()
            .fetch_frame(page_id)?
            .write()
            .unwrap()
            .get_page_write()
            .into();

        unsafe {
            page.as_mut().unwrap().delete_tuple(slot_id);
        };

        let page_id = unsafe { page.as_ref().unwrap().get_page_id() };

        self.bpm.write().unwrap().unpin(&page_id);

        Ok(())
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let mut bpm = self.bpm.write().unwrap();

        bpm.unpin(&unsafe { self.first_page.as_ref().unwrap() }.get_page_id());
        bpm.unpin(&unsafe { self.last_page.as_ref().unwrap() }.get_page_id());
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        tuple::schema::Schema,
        types::{Primitive, Types, U16, U8},
    };

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_unpin_drop() -> Result<()> {
        let bpm = BufferPool::new();
        let mut table = Table::new()?;

        let schema = Schema::new(
            vec!["id".to_string(), "age".to_string()],
            vec![Types::U8, Types::U16],
        );

        let tuple_data = vec![U8(2).to_bytes(), U16(50000).to_bytes()];
        let tuple = Tuple::new(tuple_data, &schema);
        table.insert(tuple)?;

        let page_id = unsafe { table.first_page.as_ref().unwrap().get_page_id() };
        drop(table);
        assert_eq!(1, bpm.read().unwrap().get_pin_count(&page_id));

        Ok(())
    }
}
