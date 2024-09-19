use crate::buffer_pool::{BufferPool, BufferPoolManager};
use crate::pages::{
    table_page::{TablePage, META_SIZE, PAGE_END, SLOT_SIZE},
    traits::Serialize,
    PageId,
};
use crate::tuple::{schema::Schema, Entry, Tuple};
use crate::tuple::{TupleExt, TupleId};
use crate::types::{AsBytes, Str, Types, U16};
use anyhow::{anyhow, Result};

pub mod table_iterator;

pub struct Table {
    name: String,
    first_page: TablePage,
    last_page: TablePage,
    blob_page: TablePage,
    bpm: BufferPoolManager,
    schema: Schema,
}

impl Table {
    pub fn new(name: String, schema: &Schema) -> Result<Self> {
        let bpm = BufferPool::new();

        let page: TablePage = bpm.lock().new_page()?.writer().into();

        let page_id = page.get_page_id();

        // increment pin count
        let _ = bpm.lock().fetch_frame(page_id);

        let blob_page: TablePage = bpm.lock().new_page()?.writer().into();

        Ok(Self {
            name,
            first_page: page.clone(),
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

        let first_page: TablePage = bpm.lock().fetch_frame(first_page_id)?.writer().into();

        let last_page: TablePage = if last_page_id != first_page_id {
            bpm.lock().fetch_frame(last_page_id)?.writer().into()
        } else {
            // increment page pin count
            let _ = bpm.lock().fetch_frame(first_page_id);
            first_page.clone()
        };

        let blob_page: TablePage = bpm.lock().new_page()?.writer().into();

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

    pub fn get_first_page_id(&self) -> PageId {
        self.first_page.get_page_id()
    }

    pub fn get_last_page_id(&self) -> PageId {
        self.last_page.get_page_id()
    }

    #[cfg(test)]
    pub fn get_blob_page_id(&self) -> PageId {
        self.blob_page.get_page_id()
    }

    fn iter(&self) -> table_iterator::TableIterator {
        table_iterator::TableIterator::new(self)
    }

    fn insert_string(&mut self, bytes: &[u8]) -> Result<TupleId> {
        if bytes.len() > PAGE_END - SLOT_SIZE {
            return Err(anyhow!("Tuple is too long"));
        }

        let tuple = Tuple::new(vec![Str::from_raw_bytes(bytes).into()], &Schema::default());

        if let Ok(id) = self.blob_page.insert_raw(&tuple) {
            return Ok(id);
        }

        // page is full, add another page and link to table
        let blob_page: TablePage = self.bpm.lock().new_page()?.writer().into();

        let last_page_id = self.blob_page.get_page_id();

        self.bpm.lock().unpin(&last_page_id);

        self.blob_page = blob_page;

        self.blob_page.insert_raw(&tuple)
    }

    fn insert_strings(&mut self, tuple: Tuple) -> Result<Tuple> {
        if !self.schema.types.contains(&Types::Str) {
            return Ok(tuple);
        }

        let mut offsets: Vec<_> = self
            .schema
            .types
            .iter()
            .scan(0, |acc, ty| {
                let size = if ty == &Types::Str {
                    let slice = &tuple.get_data()[*acc..*acc + 2];
                    U16::from_bytes(slice).0 as usize + 2
                } else {
                    ty.size()
                };
                *acc += size;
                Some(*acc)
            })
            .collect();

        offsets.insert(0, 0);

        let data = self
            .schema
            .types
            .clone()
            .iter()
            .zip(offsets.windows(2).map(|w| (w[0], w[1])))
            .flat_map(|(ty, (offset, size))| match ty {
                Types::Str => {
                    let str_bytes = &tuple.get_data()[offset..size];
                    self.insert_string(str_bytes).unwrap().to_bytes()
                }
                _ => tuple.get_data()[offset..size].to_vec(),
            })
            .collect::<Vec<_>>();

        let mut new_tuple = Tuple::from_bytes(&data);
        new_tuple._null_bitmap = tuple._null_bitmap;
        Ok(new_tuple)
    }

    /// fetch the string from the tuple, takes TupleId bytes
    /// (page_id, slot_id)
    pub fn fetch_string(&self, tuple_id_data: &[u8]) -> Str {
        let (page, slot) = TupleId::from_bytes(tuple_id_data);
        let blob_page: TablePage = self.bpm.lock().fetch_frame(page).unwrap().reader().into();

        let tuple = blob_page.read_raw(slot);
        Str::from_raw_bytes(tuple.get_data())
    }

    #[allow(unused)]
    pub fn fetch_tuple(&self, tuple_id: TupleId) -> Result<Entry> {
        let (page_id, slot) = tuple_id;
        let page: TablePage = self.bpm.lock().fetch_frame(page_id)?.reader().into();

        Ok(page.read_tuple(slot))
    }

    pub fn insert(&mut self, tuple: Tuple) -> Result<TupleId> {
        if tuple.len() > PAGE_END - (SLOT_SIZE + META_SIZE) {
            return Err(anyhow!("Tuple is too long"));
        }

        let tuple = self.insert_strings(tuple)?;
        let last = &mut self.last_page;

        if let Ok(id) = last.insert_tuple(&tuple) {
            return Ok(id);
        }

        // page is full, add another page and link to table
        let page: TablePage = self.bpm.lock().new_page()?.writer().into();

        let page_id = page.get_page_id();

        last.set_next_page_id(page_id);

        let last_page_id = self.last_page.get_page_id();
        self.bpm.lock().unpin(&last_page_id);

        self.last_page = page;

        self.last_page.insert_tuple(&tuple)
    }

    pub fn scan(&self, mut f: impl FnMut(&(TupleId, Entry)) -> Result<()>) -> Result<()> {
        self.iter().try_for_each(|entry| f(&entry))
    }

    pub fn delete(&mut self, id: TupleId) -> Result<()> {
        let (page_id, slot_id) = id;

        let mut page: TablePage = self.bpm.lock().fetch_frame(page_id)?.writer().into();

        page.delete_tuple(slot_id);

        let page_id = page.get_page_id();

        self.bpm.lock().unpin(&page_id);

        Ok(())
    }

    #[allow(dead_code)]
    pub fn update(&mut self, old_tuple: Tuple, new_tuple: Tuple) -> Result<TupleId> {
        let mut tuple_id = None;
        self.scan(|(id, (_, tuple))| {
            if *tuple == old_tuple {
                tuple_id = Some(*id)
            }
            Ok(())
        })?;

        if tuple_id.is_none() {
            return Err(anyhow!("Tuple not found"));
        }

        self.delete(tuple_id.unwrap())?;
        self.insert(new_tuple)
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let mut bpm = self.bpm.lock();

        bpm.unpin(&self.first_page.get_page_id());
        bpm.unpin(&self.last_page.get_page_id());
        bpm.unpin(&self.blob_page.get_page_id());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{tuple::schema::Schema, types::*};
    use anyhow::Result;
    use parking_lot::FairMutex;

    pub fn test_table(size: usize, schema: &Schema) -> Result<Table> {
        let bpm = Arc::new(FairMutex::new(BufferPool::init(size)));

        let mut guard = bpm.lock();

        let page: TablePage = guard.new_page()?.writer().into();

        // increment pin count as it's also the last page
        let _ = guard.fetch_frame(page.get_page_id());

        let blob_page: TablePage = guard.new_page()?.writer().into();

        drop(guard);

        Ok(Table {
            name: "test".to_string(),
            first_page: page.clone(),
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

        let tuple_data: Vec<Box<dyn AsBytes>> = vec![U8(2).into(), U16(50000).into()];
        let tuple = Tuple::new(tuple_data, &schema);
        table.insert(tuple)?;

        let page_id = table.first_page.get_page_id();

        drop(table);
        assert_eq!(1, bpm.lock().get_pin_count(&page_id).unwrap());

        Ok(())
    }

    #[test]
    fn test_multiple_pages() -> Result<()> {
        let schema = Schema::new(vec!["a"], vec![Types::U128]);

        let mut table = test_table(4, &schema)?;

        let first_id = table.get_first_page_id();
        let blob_id = table.get_blob_page_id();

        // entry size = 33 (17 meta + 16 data)
        // slot size = 4
        // free page = 4080
        // 110 * 37 ≈ 4080
        let tuples_per_page = 110;

        for i in 0..tuples_per_page {
            let tuple = Tuple::new(vec![U128(i).into()], &schema);
            table.insert(tuple)?;
        }

        assert_eq!(
            table.first_page.get_page_id(),
            table.last_page.get_page_id()
        );

        table.insert(Tuple::new(vec![U128(9999).into()], &schema))?;
        let second_id = table.get_last_page_id();

        assert_ne!(
            table.first_page.get_page_id(),
            table.last_page.get_page_id()
        );

        // add a third page, make sure that page 2 is unpinned
        for i in 0..tuples_per_page {
            let tuple = Tuple::new(vec![U128(i).into()], &schema);
            table.insert(tuple)?;
        }

        let third_id = table.get_last_page_id();

        assert_eq!(2, table.bpm.lock().get_pin_count(&first_id).unwrap());
        assert_eq!(2, table.bpm.lock().get_pin_count(&blob_id).unwrap());
        assert_eq!(1, table.bpm.lock().get_pin_count(&second_id).unwrap());
        assert_eq!(2, table.bpm.lock().get_pin_count(&third_id).unwrap());

        // get count of tuples
        let mut count = 0;
        table.scan(|_| {
            count += 1;
            Ok(())
        })?;
        assert_eq!(tuples_per_page * 2 + 1, count);

        Ok(())
    }

    #[test]
    fn test_insert_string() -> Result<()> {
        let s1 = "Hello, World!";
        let s2 = "Hello, Again";
        let schema = Schema::new(
            vec!["a", "str", "b"],
            vec![Types::U8, Types::Str, Types::U8],
        );

        let mut table = test_table(4, &schema)?;

        let tuple = Tuple::new(
            vec![U8(100).into(), Str(s1.to_string()).into(), U8(50).into()],
            &schema,
        );
        table.insert(tuple)?;

        let tuple = Tuple::new(
            vec![U8(20).into(), Str(s2.to_string()).into(), U8(10).into()],
            &schema,
        );
        table.insert(tuple)?;

        let mut counter = 0;

        let assert_strings = |(_, (_, tuple)): &(TupleId, Entry)| {
            let tuple = &tuple;
            let tuple_bytes = tuple.get_value_of::<U128>("str", &schema)?.unwrap();
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
            Ok(())
        };

        table.scan(assert_strings)?;

        Ok(())
    }

    #[test]
    fn test_multi_string() -> Result<()> {
        let s1 = "Hello, World!";
        let s2 = "Hello, Again";
        let schema = Schema::new(
            vec!["s1", "a", "s2"],
            vec![Types::Str, Types::U8, Types::Str],
        );

        let mut table = test_table(4, &schema)?;

        let tuple = Tuple::new(
            vec![
                Str(s1.to_string()).into(),
                U8(100).into(),
                Str(s2.to_string()).into(),
            ],
            &schema,
        );
        table.insert(tuple)?;

        let assert_strings = |(_, (_, tuple)): &(TupleId, Entry)| {
            let tuple = &tuple;
            let tuple_bytes = tuple.get_value_at::<U128>(0, &schema)?.unwrap();
            let read_s1 = table.fetch_string(&tuple_bytes.to_bytes());

            let a = tuple.get_value_at::<U8>(1, &schema)?.unwrap();

            let tuple_bytes = tuple.get_value_at::<U128>(2, &schema)?.unwrap();
            let read_s2 = table.fetch_string(&tuple_bytes.to_bytes());

            assert_eq!(read_s1.0, s1);
            assert_eq!(a.0, 100);
            assert_eq!(read_s2.0, s2);

            Ok(())
        };

        table.scan(assert_strings)?;

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let schema = Schema::new(
            vec!["a", "b", "c"],
            vec![Types::U128, Types::F64, Types::I8],
        );

        let mut table = test_table(4, &schema)?;

        let tuple = Tuple::new(
            vec![U128(10).into(), F64(10.0).into(), I8(10).into()],
            &schema,
        );
        let t1_id = table.insert(tuple)?;

        let tuple_data = vec![U128(20).into(), F64(20.0).into(), I8(20).into()];
        let tuple = Tuple::new(tuple_data, &schema);
        let t2_id = table.insert(tuple)?;

        table.delete(t1_id)?;

        let scanner_1 = |(_, (_, tuple)): &(TupleId, Entry)| {
            let values = tuple.get_values(&schema)?;
            assert_eq!(values[0].to_bytes(), U128(20).to_bytes());
            assert_eq!(values[1].to_bytes(), F64(20.0).to_bytes());
            assert_eq!(values[2].to_bytes(), I8(20).to_bytes());

            Ok(())
        };

        table.scan(scanner_1)?;

        table.delete(t2_id)?;

        let scanner_2 = |_: &(TupleId, Entry)| Err(anyhow!("Should not run")); // should never run

        table.scan(scanner_2)?;

        Ok(())
    }

    #[test]
    fn test_nulls() -> Result<()> {
        let schema = Schema::new(
            vec!["a", "b", "c"],
            vec![Types::U128, Types::Str, Types::I8],
        );

        let mut table = test_table(4, &schema)?;
        let tuple = Tuple::new(vec![Null().into(), Null().into(), Null().into()], &schema);
        table.insert(tuple)?;

        let validator = |(_, (meta, tuple)): &(TupleId, Entry)| {
            assert!(meta.is_null(0));
            assert!(meta.is_null(1));
            assert!(meta.is_null(2));

            tuple
                .get_values(&schema)?
                .iter()
                .for_each(|v| assert!(v.is_null()));

            Ok(())
        };

        table.scan(validator)?;

        Ok(())
    }
}
