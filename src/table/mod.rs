use crate::buffer_pool::{BufferPool, BufferPoolManager};
use crate::pages::{
    table_page::{TablePage, TupleExt, TupleId, META_SIZE, PAGE_END, SLOT_SIZE},
    traits::Serialize,
    PageId,
};
use crate::tuple::{schema::Schema, Entry, Tuple};
use crate::types::{Str, Types, STR_DELIMITER};
use anyhow::{anyhow, Result};

pub mod table_iterator;

pub struct Table {
    name: String,
    first_page: *mut TablePage,
    last_page: *mut TablePage,
    blob_page: *mut TablePage,
    bpm: BufferPoolManager,
    schema: Schema,
}

impl Table {
    pub fn new(name: String, schema: &Schema) -> Result<Self> {
        let bpm = BufferPool::new();

        let page: *mut TablePage = bpm.write().unwrap().new_page()?.get_page_write().into();

        let page_id = unsafe { (*page).get_page_id() };

        // increment pin count
        let _ = bpm.write().unwrap().fetch_frame(page_id);

        let blob_page: *mut TablePage = bpm.write().unwrap().new_page()?.get_page_write().into();

        Ok(Self {
            name,
            first_page: page,
            last_page: page,
            blob_page,
            bpm,
            schema: schema.clone(),
        })
    }

    pub fn fetch(
        name: String,
        schema: &Schema,
        first_page_id: PageId,
        last_page_id: PageId,
    ) -> Result<Self> {
        let bpm = BufferPool::new();

        let first_page: *mut TablePage = bpm
            .write()
            .unwrap()
            .fetch_frame(first_page_id)?
            .get_page_write()
            .into();

        let last_page: *mut TablePage = if last_page_id != first_page_id {
            bpm.write()
                .unwrap()
                .fetch_frame(last_page_id)?
                .get_page_write()
                .into()
        } else {
            // increment page pin count
            let _ = bpm.write().unwrap().fetch_frame(first_page_id);
            first_page
        };

        let blob_page: *mut TablePage = bpm.write().unwrap().new_page()?.get_page_write().into();

        Ok(Self {
            name,
            first_page,
            last_page,
            blob_page,
            bpm,
            schema: schema.clone(),
        })
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    #[allow(unused)]
    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_first_page_id(&self) -> PageId {
        unsafe { self.first_page.as_ref().unwrap().get_page_id() }
    }

    pub fn get_last_page_id(&self) -> PageId {
        unsafe { self.last_page.as_ref().unwrap().get_page_id() }
    }

    #[allow(unused)]
    pub fn get_blob_page_id(&self) -> PageId {
        unsafe { self.blob_page.as_ref().unwrap().get_page_id() }
    }

    fn to_iter(&self) -> table_iterator::TableIterator {
        table_iterator::TableIterator::new(self)
    }

    fn insert_string(&mut self, data: &[u8]) -> Result<TupleId> {
        if data.len() > PAGE_END - SLOT_SIZE {
            return Err(anyhow!("Tuple is too long"));
        }

        let schema = Schema::new(vec!["a"], vec![Types::Str]);
        let tuple = Tuple::new(vec![data.into()], &schema);

        let blob = unsafe { self.blob_page.as_mut().unwrap() };

        if let Ok(id) = blob.insert_raw(&tuple) {
            return Ok(id);
        }

        // page is full, add another page and link to table
        let blob_page: *mut TablePage = self
            .bpm
            .write()
            .unwrap()
            .new_page()?
            .get_page_write()
            .into();

        let last_page_id = unsafe { self.blob_page.as_ref().unwrap().get_page_id() };

        self.bpm.write().unwrap().unpin(&last_page_id);

        self.blob_page = blob_page;

        unsafe { self.blob_page.as_mut().unwrap().insert_raw(&tuple) }
    }

    fn insert_strings(&mut self, tuple: Tuple) -> Result<Tuple> {
        if !self.schema.types.contains(&Types::Str) {
            return Ok(tuple);
        }

        let mut string = "".to_string();
        let mut processed_data = Vec::with_capacity(tuple.len());
        let mut inside = false;

        let delimiter = STR_DELIMITER as u8;
        for byte in tuple.get_data().iter() {
            if inside && byte != &delimiter {
                string.push(*byte as char);
            } else if inside && byte == &delimiter {
                let tuple_id: TupleId = self.insert_string(string.as_bytes())?;
                string.clear();
                processed_data.extend(tuple_id.to_bytes());
                inside = false;
            } else if !inside && byte == &delimiter {
                inside = true;
            } else {
                processed_data.push(*byte);
            }
        }

        Ok(Tuple::from_bytes(&processed_data))
    }

    /// fetch the string from the tuple, takes TupleId bytes
    /// (page_id, slot_id)
    pub fn fetch_string(&self, tuple_id_data: &[u8]) -> Str {
        let (page, slot) = TupleId::from_bytes(tuple_id_data);
        let blob_page: *const TablePage = self
            .bpm
            .write()
            .unwrap()
            .fetch_frame(page)
            .unwrap()
            .get_page_read()
            .into();

        let tuple = unsafe { blob_page.as_ref().unwrap() }.read_raw(slot);
        let string = String::from_utf8(tuple.get_data().to_vec()).unwrap();
        Str(string)
    }

    pub fn insert(&mut self, tuple: Tuple) -> Result<TupleId> {
        if tuple.len() > PAGE_END - SLOT_SIZE - META_SIZE {
            return Err(anyhow!("Tuple is too long"));
        }

        let tuple = self.insert_strings(tuple)?;
        let last = unsafe { self.last_page.as_mut().unwrap() };

        if let Ok(id) = last.insert_tuple(&tuple) {
            return Ok(id);
        }

        // page is full, add another page and link to table
        let page: *mut TablePage = self
            .bpm
            .write()
            .unwrap()
            .new_page()?
            .get_page_write()
            .into();

        let page_id = unsafe { page.as_ref().unwrap().get_page_id() };

        last.set_next_page_id(page_id);

        let last_page_id = unsafe { self.last_page.as_ref().unwrap().get_page_id() };
        self.bpm.write().unwrap().unpin(&last_page_id);

        self.last_page = page;

        unsafe { self.last_page.as_mut().unwrap().insert_tuple(&tuple) }
    }

    pub fn scan(&self, mut f: impl FnMut(&(TupleId, Entry))) {
        self.to_iter().for_each(|entry| f(&entry))
    }

    pub fn delete(&mut self, id: TupleId) -> Result<()> {
        let (page_id, slot_id) = id;

        let page: *mut TablePage = self
            .bpm
            .write()
            .unwrap()
            .fetch_frame(page_id)?
            .get_page_write()
            .into();

        unsafe {
            page.as_mut().unwrap().delete_tuple(slot_id);
        };

        let page_id = unsafe { page.as_ref().unwrap().get_page_id() };

        self.bpm.write().unwrap().unpin(&page_id);

        Ok(())
    }

    #[allow(dead_code)]
    pub fn update(&mut self, old_tuple: Tuple, new_tuple: Tuple) -> Result<TupleId> {
        let mut tuple_id = None;
        self.scan(|(id, (_, tuple))| {
            if *tuple == old_tuple {
                tuple_id = Some(*id)
            }
        });

        if tuple_id.is_none() {
            return Err(anyhow!("Tuple not found"));
        }

        self.delete(tuple_id.unwrap())?;
        self.insert(new_tuple)
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let mut bpm = self.bpm.write().unwrap();

        bpm.unpin(&unsafe { self.first_page.as_ref().unwrap() }.get_page_id());
        bpm.unpin(&unsafe { self.last_page.as_ref().unwrap() }.get_page_id());
        bpm.unpin(&unsafe { self.blob_page.as_ref().unwrap() }.get_page_id());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use super::*;
    use crate::{tuple::schema::Schema, types::*};
    use anyhow::Result;

    fn test_table(size: usize, schema: &Schema) -> Result<Table> {
        let bpm = Arc::new(RwLock::new(BufferPool::init(size)));

        let page: *mut TablePage = bpm.write().unwrap().new_page()?.get_page_write().into();

        let page_id = unsafe { (*page).get_page_id() };

        // increment pin count
        let _ = bpm.write().unwrap().fetch_frame(page_id);

        let blob_page: *mut TablePage = bpm.write().unwrap().new_page()?.get_page_write().into();

        Ok(Table {
            name: "test".to_string(),
            first_page: page,
            last_page: page,
            blob_page,
            bpm,
            schema: schema.clone(),
        })
    }

    #[test]
    fn test_unpin_drop() -> Result<()> {
        let schema = Schema::new(vec!["id", "age"], vec![Types::U8, Types::U16]);

        let mut table = test_table(2, &schema)?;
        let bpm = table.bpm.clone();

        let tuple_data = vec![U8(2).to_bytes(), U16(50000).to_bytes()];
        let tuple = Tuple::new(tuple_data, &schema);
        table.insert(tuple)?;

        let page_id = unsafe { table.first_page.as_ref().unwrap().get_page_id() };

        drop(table);
        assert_eq!(1, bpm.read().unwrap().get_pin_count(&page_id).unwrap());

        Ok(())
    }

    #[test]
    fn test_multiple_pages() -> Result<()> {
        let schema = Schema::new(vec!["a"], vec![Types::U128]);

        let mut table = test_table(4, &schema)?;

        let first_id = table.get_first_page_id();
        let blob_id = table.get_blob_page_id();

        // entry size = 25 (9 header + 16 data)
        // slot size = 4
        // free page = 4080
        // 140 * 29 = 4080
        for i in 0..140 {
            let tuple_data = vec![U128(i).to_bytes()];
            let tuple = Tuple::new(tuple_data, &schema);
            table.insert(tuple)?;
        }

        assert_eq!(table.first_page, table.last_page);

        table.insert(Tuple::new(vec![U128(9999).to_bytes()], &schema))?;
        let second_id = table.get_last_page_id();

        assert_ne!(table.first_page, table.last_page);

        // add a third page, make sure that page 2 is unpinned
        for i in 0..140 {
            let tuple_data = vec![U128(i).to_bytes()];
            let tuple = Tuple::new(tuple_data, &schema);
            table.insert(tuple)?;
        }

        let third_id = table.get_last_page_id();

        assert_eq!(
            2,
            table.bpm.read().unwrap().get_pin_count(&first_id).unwrap()
        );
        assert_eq!(
            2,
            table.bpm.read().unwrap().get_pin_count(&blob_id).unwrap()
        );
        assert_eq!(
            1,
            table.bpm.read().unwrap().get_pin_count(&second_id).unwrap()
        );
        assert_eq!(
            2,
            table.bpm.read().unwrap().get_pin_count(&third_id).unwrap()
        );

        // get count of tuples
        let mut count = 0;
        table.scan(|_| count += 1);
        assert_eq!(281, count);

        Ok(())
    }

    #[test]
    fn test_insert_string() -> Result<()> {
        let s1 = "Hello, World!";
        let s2 = "Hello, Again";
        let schema = Schema::new(
            vec!["a", "str", "b"],
            vec![Types::U8, Types::Str, Types::Str, Types::U8],
        );

        let mut table = test_table(4, &schema)?;

        let tuple = Tuple::new(
            vec![
                U8(100).to_bytes(),
                Str(s1.to_string()).to_bytes(),
                U8(50).to_bytes(),
            ],
            &schema,
        );
        table.insert(tuple)?;

        let tuple = Tuple::new(
            vec![
                U8(20).to_bytes(),
                Str(s2.to_string()).to_bytes(),
                U8(10).to_bytes(),
            ],
            &schema,
        );
        table.insert(tuple)?;

        let mut counter = 0;

        let mut assert_strings = |(_, (_, tuple)): &(TupleId, Entry)| {
            let tuple = &tuple;
            let tuple_bytes = tuple.get_value::<U128>("str", &schema).unwrap();
            let string = table.fetch_string(&tuple_bytes.to_bytes());
            assert_eq!(
                string,
                if counter == 0 {
                    Str(s1.to_string())
                } else {
                    Str(s2.to_string())
                }
            );
            counter += 1;
        };

        table.scan(|entry| assert_strings(entry));

        Ok(())
    }
}
