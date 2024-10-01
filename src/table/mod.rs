use crate::buffer_pool::{ArcBufferPool, BufferPoolManager};
use crate::pages::table_page::{TablePage, META_SIZE, PAGE_END, SLOT_SIZE};
use crate::pages::traits::Serialize;
use crate::pages::PageId;
use crate::tuple::{schema::Schema, Entry, Tuple};
use crate::tuple::{TupleExt, TupleId};
use crate::txn_manager::{ArcTransactionManager, TransactionManager, TxnId};
use crate::types::{AsBytes, Str, Types, U16};
use anyhow::{anyhow, Result};

pub mod table_iterator;

pub struct Table {
    name: String,
    first_page: PageId,
    last_page: PageId,
    blob_page: PageId,
    bpm: ArcBufferPool,
    txn_manager: ArcTransactionManager,
    schema: Schema,
    active_txn: Option<TxnId>,
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Table {
    pub fn new(name: String, schema: &Schema) -> Result<Self> {
        let bpm = BufferPoolManager::get();
        let txn_manager = TransactionManager::get();

        let page_id = bpm.lock().new_page()?.reader().get_page_id();

        let blob_page = bpm.lock().new_page()?.reader().get_page_id();

        Ok(Self {
            name,
            first_page: page_id,
            last_page: page_id,
            blob_page,
            bpm,
            txn_manager,
            active_txn: None,
            schema: schema.clone(),
        })
    }

    pub fn fetch(
        name: String,
        schema: &Schema,
        first_page: PageId,
        last_page: PageId,
    ) -> Result<Self> {
        let bpm = BufferPoolManager::get();
        let txn_manager = TransactionManager::get();

        let blob_page = bpm.lock().new_page()?.reader().get_page_id();

        Ok(Self {
            name,
            first_page,
            last_page,
            blob_page,
            bpm,
            txn_manager,
            active_txn: None,
            schema: schema.clone(),
        })
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_first_page_id(&self) -> PageId {
        self.first_page
    }

    pub fn get_schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn get_last_page_id(&self) -> PageId {
        self.last_page
    }

    #[cfg(test)]
    pub fn get_blob_page_id(&self) -> PageId {
        self.blob_page
    }

    fn iter(&self) -> table_iterator::TableIterator {
        table_iterator::TableIterator::new(self)
    }

    fn insert_string(&mut self, bytes: &[u8]) -> Result<TupleId> {
        if bytes.len() > PAGE_END - SLOT_SIZE {
            return Err(anyhow!("Tuple is too long"));
        }

        let tuple = Tuple::new(vec![Str::from_raw_bytes(bytes).into()], &Schema::default());

        if let Some(id) = self.active_txn {
            self.txn_manager.lock().touch_page(id, self.blob_page)?;
        }

        let mut blob_page: TablePage = self
            .bpm
            .lock()
            .fetch_frame(self.blob_page, self.active_txn)?
            .writer()
            .into();

        if let Ok(id) = blob_page.insert_raw(&tuple) {
            return Ok(id);
        }

        // page is full, add another page
        let mut new_blob_page: TablePage = self.bpm.lock().new_page()?.writer().into();

        self.bpm.lock().unpin(&self.blob_page);

        self.blob_page = new_blob_page.get_page_id();

        if let Some(id) = self.active_txn {
            self.txn_manager.lock().touch_page(id, self.blob_page)?;
        }

        new_blob_page.insert_raw(&tuple)
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

    pub fn start_txn(&mut self, txn_id: TxnId) -> Result<()> {
        if self.active_txn.is_some() {
            Err(anyhow!("Another transaction is active"))
        } else {
            self.active_txn = Some(txn_id);
            Ok(())
        }
    }

    fn new_txn(&mut self) -> Result<TxnId> {
        let id = self.txn_manager.lock().start()?;
        self.active_txn = Some(id);
        Ok(id)
    }

    pub fn commit_txn(&mut self) -> Result<()> {
        if self.active_txn.is_some() {
            self.active_txn = None;
            Ok(())
        } else {
            Err(anyhow!("No active transaction"))
        }
    }

    #[allow(unused)]
    pub fn abort_txn(&self, txn_id: TxnId) -> Result<()> {
        self.txn_manager.lock().abort(txn_id)
    }

    /// fetch the string from the tuple, takes TupleId bytes
    /// (page_id, slot_id)
    pub fn fetch_string(&self, tuple_id_data: &[u8]) -> Str {
        let (page, slot) = TupleId::from_bytes(tuple_id_data);

        if let Some(id) = self.active_txn {
            self.txn_manager.lock().touch_page(id, page).unwrap();
        }

        let blob_page: TablePage = self
            .bpm
            .lock()
            .fetch_frame(page, self.active_txn)
            .unwrap()
            .reader()
            .into();

        let tuple = blob_page.read_raw(slot);
        self.bpm.lock().unpin(&page);
        Str::from_raw_bytes(tuple.get_data())
    }

    #[allow(unused)]
    pub fn fetch_tuple(&self, tuple_id: TupleId) -> Result<Entry> {
        let (page_id, slot) = tuple_id;
        let page: TablePage = self
            .bpm
            .lock()
            .fetch_frame(page_id, self.active_txn)?
            .reader()
            .into();

        let entry = page.read_tuple(slot);

        self.bpm.lock().unpin(&page_id);

        Ok(entry)
    }

    pub fn insert(&mut self, tuple: Tuple) -> Result<TupleId> {
        if tuple.len() > PAGE_END - (SLOT_SIZE + META_SIZE) {
            return Err(anyhow!("Tuple is too long"));
        }

        let tuple = self.insert_strings(tuple)?;

        if let Some(id) = self.active_txn {
            self.txn_manager.lock().touch_page(id, self.last_page)?;
        }

        let mut last_page: TablePage = self
            .bpm
            .lock()
            .fetch_frame(self.last_page, self.active_txn)?
            .writer()
            .into();

        if let Ok(id) = last_page.insert_tuple(&tuple) {
            self.bpm.lock().unpin(&self.last_page);
            return Ok(id);
        }

        // page is full, add another page and link to table
        let mut new_page: TablePage = self.bpm.lock().new_page()?.writer().into();

        let page_id = new_page.get_page_id();

        last_page.set_next_page_id(page_id);

        self.bpm.lock().unpin(&self.last_page);

        self.last_page = page_id;

        if let Some(id) = self.active_txn {
            self.txn_manager.lock().touch_page(id, self.last_page)?;
        }

        new_page.insert_tuple(&tuple)
    }

    pub fn scan(&self, mut f: impl FnMut(&(TupleId, Entry)) -> Result<()>) -> Result<()> {
        self.iter().try_for_each(|entry| f(&entry))
    }

    pub fn delete(&mut self, id: TupleId) -> Result<()> {
        let (page_id, slot_id) = id;

        let mut page: TablePage = self
            .bpm
            .lock()
            .fetch_frame(page_id, self.active_txn)?
            .writer()
            .into();

        page.delete_tuple(slot_id);

        let page_id = page.get_page_id();

        self.bpm.lock().unpin(&page_id);

        Ok(())
    }

    #[allow(dead_code)]
    pub fn update(&mut self, old_tuple: Tuple, new_tuple: Tuple) -> Result<TupleId> {
        // updates should run inside txns, if a txn is
        // already live use it else create a scoped txn here
        let self_txn = self.active_txn.is_none();

        if self_txn {
            self.new_txn()?;
        }

        let mut tuple_id = None;
        self.scan(|(id, (_, tuple))| {
            if *tuple == old_tuple {
                tuple_id = Some(*id)
            }
            Ok(())
        })?;

        match tuple_id {
            None => return Err(anyhow!("Tuple not found")),
            Some(id) => self.delete(id)?,
        }

        let tuple_id = self.insert(new_tuple)?;

        if self_txn {
            self.commit_txn()?
        }

        Ok(tuple_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::disk_manager::test_path;
    use crate::tuple::schema::Schema;
    use crate::types::*;
    use anyhow::Result;
    use parking_lot::FairMutex;

    pub fn test_table(size: usize, schema: &Schema) -> Result<Table> {
        let path = test_path();
        let bpm = Arc::new(FairMutex::new(BufferPoolManager::new(size, &path)));

        let mut guard = bpm.lock();

        let page = guard.new_page()?.reader().get_page_id();

        let blob_page = guard.new_page()?.reader().get_page_id();

        let txn_manager = Arc::new(FairMutex::new(TransactionManager::new()));

        drop(guard);

        Ok(Table {
            name: "test".to_string(),
            first_page: page,
            last_page: page,
            blob_page,
            bpm,
            txn_manager,
            active_txn: None,
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

        let page_id = table.first_page;

        drop(table);
        assert_eq!(0, bpm.lock().get_pin_count(&page_id).unwrap());

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

        assert_eq!(table.first_page, table.last_page);

        table.insert(Tuple::new(vec![U128(9999).into()], &schema))?;
        let second_id = table.get_last_page_id();

        assert_ne!(table.first_page, table.last_page);

        // add a third page, make sure that page 2 is unpinned
        for i in 0..tuples_per_page {
            let tuple = Tuple::new(vec![U128(i).into()], &schema);
            table.insert(tuple)?;
        }

        let third_id = table.get_last_page_id();

        assert_eq!(0, table.bpm.lock().get_pin_count(&first_id).unwrap());
        assert_eq!(0, table.bpm.lock().get_pin_count(&blob_id).unwrap());
        assert_eq!(0, table.bpm.lock().get_pin_count(&second_id).unwrap());
        assert_eq!(0, table.bpm.lock().get_pin_count(&third_id).unwrap());

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
