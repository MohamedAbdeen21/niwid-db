use std::sync::RwLock;

use crate::{
    buffer_pool::{BufferPool, BufferPoolManager},
    pages::{
        table_page::{TablePage, TupleId},
        traits::Serialize,
    },
    tuple::{schema::Schema, Entry, Tuple},
    types::Types,
};
use anyhow::Result;

pub mod table_iterator;

#[allow(unused)]
pub struct Table {
    first_page: RwLock<*mut TablePage>,
    last_page: RwLock<*mut TablePage>,
    blob_page: RwLock<*mut TablePage>,
    bpm: BufferPoolManager,
    schema: Schema,
}

impl Table {
    pub fn new(schema: &Schema) -> Result<Self> {
        let bpm = BufferPool::new();

        let page: *mut TablePage = bpm.write().unwrap().new_page()?.get_page_write().into();

        let page_id = unsafe { (*page).get_page_id() };

        // increment pin count
        let _ = bpm.write().unwrap().fetch_frame(page_id);

        let blob_page: *mut TablePage = bpm.write().unwrap().new_page()?.get_page_write().into();

        Ok(Self {
            first_page: RwLock::new(page),
            last_page: RwLock::new(page),
            blob_page: RwLock::new(blob_page),
            bpm,
            schema: schema.clone(),
        })
    }

    fn to_iter(&self) -> table_iterator::TableIterator {
        table_iterator::TableIterator::new(self)
    }

    fn insert_string(&mut self, data: &[u8]) -> Result<TupleId> {
        let schema = Schema::new(vec!["a"], vec![Types::Str]);
        let tuple = Tuple::new(vec![data.into()], &schema);

        let blob = unsafe { self.blob_page.write().unwrap().as_mut().unwrap() };

        if let Ok(id) = blob.insert_tuple(&tuple) {
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

        let last_page_id = unsafe {
            self.blob_page
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .get_page_id()
        };

        self.bpm.write().unwrap().unpin(&last_page_id);

        self.blob_page = RwLock::new(blob_page);

        unsafe {
            self.blob_page
                .write()
                .unwrap()
                .as_mut()
                .unwrap()
                .insert_tuple(&tuple)
        }
    }

    fn insert_strings(&mut self, tuple: Tuple) -> Result<Tuple> {
        if !self.schema.types.contains(&Types::Str) {
            return Ok(tuple);
        }

        let mut string = "".to_string();
        let mut processed_data = Vec::with_capacity(tuple.len());
        let mut inside = false;

        for byte in tuple.get_data().iter() {
            if inside && byte != &b'\0' {
                string.push(*byte as char);
            } else if inside && byte == &b'\0' {
                let (page_id, slot_id) = self.insert_string(string.as_bytes())?;
                processed_data.extend_from_slice(&page_id.to_ne_bytes());
                processed_data.extend_from_slice(&slot_id.to_ne_bytes());
                string.clear();
                inside = false;
            } else if !inside && byte == &b'\0' {
                inside = true;
            } else {
                processed_data.push(*byte);
            }
        }

        Ok(Tuple::from_bytes(&processed_data))
    }

    pub fn insert(&mut self, tuple: Tuple) -> Result<()> {
        let tuple = self.insert_strings(tuple)?;
        let last = unsafe { self.last_page.write().unwrap().as_mut().unwrap() };

        if last.insert_tuple(&tuple).is_ok() {
            return Ok(());
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

        last.header_mut().set_next_page_id(page_id);

        let last_page_id = unsafe {
            self.last_page
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .get_page_id()
        };
        self.bpm.write().unwrap().unpin(&last_page_id);

        self.last_page = RwLock::new(page);

        unsafe {
            self.last_page
                .write()
                .unwrap()
                .as_mut()
                .unwrap()
                .insert_tuple(&tuple)?;
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

        bpm.unpin(&unsafe { self.first_page.read().unwrap().as_ref().unwrap() }.get_page_id());
        bpm.unpin(&unsafe { self.last_page.read().unwrap().as_ref().unwrap() }.get_page_id());
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
            first_page: RwLock::new(page),
            last_page: RwLock::new(page),
            blob_page: RwLock::new(blob_page),
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

        let page_id = unsafe {
            table
                .first_page
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .get_page_id()
        };

        drop(table);
        assert_eq!(1, bpm.read().unwrap().get_pin_count(&page_id).unwrap());

        Ok(())
    }

    #[test]
    fn test_multiple_pages() -> Result<()> {
        let schema = Schema::new(vec!["a"], vec![Types::U128]);

        let mut table = test_table(4, &schema)?;

        // entry size = 25 (9 header + 16 data)
        // slot size = 4
        // free page = 4080
        // 140 * 29 = 4080
        for i in 0..140 {
            let tuple_data = vec![U128(i).to_bytes()];
            let tuple = Tuple::new(tuple_data, &schema);
            table.insert(tuple)?;
        }

        assert_eq!(
            *table.first_page.read().unwrap(),
            *table.last_page.read().unwrap()
        );

        table.insert(Tuple::new(vec![U128(9999).to_bytes()], &schema))?;

        assert_ne!(
            *table.first_page.read().unwrap(),
            *table.last_page.read().unwrap()
        );

        // add a third page, make sure that page 2 is unpinned
        for i in 0..140 {
            let tuple_data = vec![U128(i).to_bytes()];
            let tuple = Tuple::new(tuple_data, &schema);
            table.insert(tuple)?;
        }

        assert_eq!(2, table.bpm.read().unwrap().get_pin_count(&1).unwrap());
        assert_eq!(2, table.bpm.read().unwrap().get_pin_count(&2).unwrap()); // blob page
        assert_eq!(1, table.bpm.read().unwrap().get_pin_count(&3).unwrap());
        assert_eq!(2, table.bpm.read().unwrap().get_pin_count(&4).unwrap());

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

        let blob_page = unsafe { table.blob_page.read().unwrap().as_ref().unwrap() };
        let mut counter = 0;

        let mut assert_strings = |entry: &Entry| {
            let tuple = &entry.1;
            let tuple_bytes = tuple.get_value::<U128>("str", &schema).unwrap();
            let (_, slot_id) = TupleId::from_bytes(&tuple_bytes.to_bytes());
            let (_, string) = blob_page.read_tuple(slot_id);
            assert_eq!(
                Str::from_bytes(string.to_bytes()),
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
