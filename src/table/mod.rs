use crate::buffer_pool::ArcBufferPool;
use crate::catalog::CATALOG_NAME;
use crate::errors::Error;
use crate::indexes::b_plus_tree::btree::BPlusTree;
use crate::pages::table_page::{TablePage, META_SIZE, PAGE_END, SLOT_SIZE};
use crate::pages::{PageId, INVALID_PAGE};
use crate::printdbg;
use crate::tuple::schema::Field;
use crate::tuple::{schema::Schema, Entry, Tuple};
use crate::tuple::{TupleExt, TupleId};
use crate::txn_manager::{ArcTransactionManager, TxnId};
use crate::types::{Str, StrAddr, Types, Value, ValueFactory};
use crate::wal::manager::ArcLogManager;
use crate::wal::record::{Record, RowOperation};
use anyhow::{bail, ensure, Result};

pub mod table_iterator;

/// The catalog row describing a table: everything needed to reopen it
pub struct CatalogRow {
    pub name: String,
    pub schema: Schema,
    pub first_page: PageId,
    pub last_page: PageId,
    pub index_page: Option<PageId>,
}

pub struct Table {
    pub name: String,
    pub first_page: PageId,
    pub last_page: PageId,
    blob_page: PageId,
    bpm: ArcBufferPool,
    txn_manager: ArcTransactionManager,
    schema: Schema,
    lm: ArcLogManager,
    active_txn: Option<TxnId>,
    /// Index to check uniqueness of columns, is None for tables that don't check
    /// uniqueness, such as the catalog
    index: Option<BPlusTree>,
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Table {
    pub fn new(
        bpm: ArcBufferPool,
        txn_manager: ArcTransactionManager,
        lm: ArcLogManager,
        name: String,
        schema: &Schema,
        txn: TxnId,
    ) -> Result<Self> {
        if !lm.recovering() {
            lm.lock()
                .append(txn, Record::CreateTable(name.clone(), schema.clone()));
        }

        let page_id = bpm.lock().new_page()?.reader().get_page_id();

        let blob_page = bpm.lock().new_page()?.reader().get_page_id();

        Ok(Self {
            name,
            first_page: page_id,
            last_page: page_id,
            blob_page,
            index: Some(BPlusTree::new(bpm.clone(), txn_manager.clone(), Some(txn))),
            bpm,
            txn_manager,
            lm,
            active_txn: None,
            schema: schema.clone(),
        })
    }

    pub fn drop(&self, txn: TxnId) {
        if !self.lm.recovering() {
            self.lm
                .lock()
                .append(txn, Record::DropTable(self.name.clone()));
        }
    }

    pub fn fetch(
        bpm: &mut ArcBufferPool,
        txn_manager: &mut ArcTransactionManager,
        lm: ArcLogManager,
        row: CatalogRow,
    ) -> Result<Self> {
        let CatalogRow {
            name,
            schema,
            first_page,
            last_page,
            index_page,
        } = row;

        let blob_page = bpm.lock().new_page()?.reader().get_page_id();

        let index = index_page.map(|id| BPlusTree::fetch(id, bpm.clone(), txn_manager.clone()));

        Ok(Self {
            name,
            first_page,
            last_page,
            blob_page,
            bpm: bpm.clone(),
            txn_manager: txn_manager.clone(),
            lm,
            active_txn: None,
            schema,
            index,
        })
    }

    pub fn get_tuple(&self, id: TupleId) -> Option<Tuple> {
        let (page, slot) = TupleId::from_bytes(&id.to_bytes());

        let page: TablePage = self
            .bpm
            .lock()
            .fetch_frame(page, self.active_txn)
            .unwrap()
            .reader()
            .into();

        let (meta, tuple) = page.read_tuple(slot);

        if meta.is_deleted() {
            None
        } else {
            Some(tuple)
        }
    }

    pub fn get_first_page_id(&self) -> PageId {
        self.first_page
    }

    pub fn get_schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn get_index(&self) -> &Option<BPlusTree> {
        &self.index
    }

    pub fn get_index_page_id(&self) -> PageId {
        self.index
            .as_ref()
            .map(|i| i.get_root_page_id())
            .unwrap_or(INVALID_PAGE)
    }

    pub fn get_last_page_id(&self) -> PageId {
        self.last_page
    }

    #[cfg(test)]
    pub fn get_blob_page_id(&self) -> PageId {
        self.blob_page
    }

    fn iter(&self, txn_id: Option<TxnId>) -> table_iterator::TableIterator {
        table_iterator::TableIterator::new(self, txn_id)
    }

    fn insert_string(&mut self, bytes: &[u8]) -> Result<TupleId> {
        if bytes.len() > PAGE_END - SLOT_SIZE {
            bail!(Error::TupleTooBig(PAGE_END - SLOT_SIZE, bytes.len()));
        }

        let tuple = Tuple::new(
            vec![ValueFactory::from_bytes(&Types::Str, bytes)],
            &Schema::default(),
        );

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
            self.bpm.lock().unpin(&self.blob_page, self.active_txn);
            return Ok(id);
        }

        // page is full, add another page
        let new_blob_page: TablePage = self.bpm.lock().new_page()?.reader().into();

        self.bpm.lock().unpin(&self.blob_page, self.active_txn);

        self.blob_page = new_blob_page.get_page_id();

        if let Some(id) = self.active_txn {
            self.txn_manager.lock().touch_page(id, self.blob_page)?;
        }

        self.insert_string(bytes)
    }

    fn insert_strings(&mut self, tuple: Tuple) -> Result<Tuple> {
        let types: Vec<Types> = self.schema.fields.iter().map(|f| f.ty.clone()).collect();

        if !types.contains(&Types::Str) {
            return Ok(tuple);
        }

        let fields: Vec<Field> = self
            .schema
            .fields
            .iter()
            .cloned()
            .map(|f| match f {
                Field {
                    ty: Types::Str,
                    name,
                    constraints,
                } => Field {
                    ty: Types::StrAddr,
                    name: name.clone(),
                    constraints,
                },
                e => e,
            })
            .collect();

        let mut offsets: Vec<_> = types
            .iter()
            .scan(0, |acc, ty| {
                let size = if *ty == Types::Str {
                    let slice = tuple.get_data()[*acc..*acc + 2].try_into().unwrap();
                    u16::from_ne_bytes(slice) as usize + 2
                } else {
                    ty.size()
                };
                *acc += size;
                Some(*acc)
            })
            .collect();

        offsets.insert(0, 0);

        let values = types
            .into_iter()
            .zip(offsets.windows(2).map(|w| (w[0], w[1])))
            .map(|(ty, (offset, size))| match ty {
                Types::Str => {
                    let str_bytes = &tuple.get_data()[offset..size];
                    let addr = &self.insert_string(str_bytes).unwrap().to_bytes();
                    ValueFactory::from_bytes(&Types::StrAddr, addr)
                }
                _ => ValueFactory::from_bytes(&ty, &tuple.get_data()[offset..size]),
            })
            .collect::<Vec<_>>();

        let mut new_tuple = Tuple::new(values, &Schema::new(fields));
        new_tuple._null_bitmap = tuple._null_bitmap;
        Ok(new_tuple)
    }

    pub fn start_txn(&mut self, txn_id: TxnId) -> Result<()> {
        printdbg!("Table {} starting txn {}", self.name, txn_id);
        if let Some(current) = self.active_txn {
            if txn_id != current {
                bail!(Error::TransactionActive);
            }
        } else {
            self.active_txn = Some(txn_id);
        }
        Ok(())
    }

    pub fn commit_txn(&mut self) -> Result<()> {
        if let Some(txn) = self.active_txn {
            printdbg!("Table {} Committing txn {}", self.name, txn);

            self.active_txn = None;
            Ok(())
        } else {
            bail!(Error::NoActiveTransaction);
        }
    }

    pub fn rollback_txn(&mut self) -> Result<()> {
        if let Some(txn) = self.active_txn {
            printdbg!("Table {} rolling back txn {}", self.name, txn);

            self.active_txn = None;
            Ok(())
        } else {
            bail!(Error::NoActiveTransaction);
        }
    }

    /// fetch the string from the tuple, takes TupleId bytes
    /// (page_id, slot_id)
    pub fn fetch_string(&self, str_pointer: StrAddr) -> Str {
        let (page, slot) = TupleId::from_bytes(&str_pointer.to_bytes());

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
        self.bpm.lock().unpin(&page, self.active_txn);
        Str::from_raw_bytes(tuple.get_data())
    }

    /// unpack a tuple into its values, dereferencing string addresses
    /// into the actual strings, so the result carries no references
    /// to this node's blob pages
    pub fn get_portable_values(&self, tuple: &Tuple) -> Result<Vec<Value>> {
        Ok(tuple
            .get_values(&self.schema)?
            .into_iter()
            .map(|v| match v {
                Value::StrAddr(addr) => Value::Str(self.fetch_string(addr)),
                v => v,
            })
            .collect())
    }

    fn check_nullability(&self, tuple: &Tuple) -> Result<()> {
        for (i, field) in self.schema.fields.iter().enumerate() {
            if !field.constraints.nullable
                && tuple.get_value_at(i as u8, &self.schema).unwrap().is_null()
            {
                bail!(Error::NullNotAllowed(field.name.clone()));
            }
        }
        Ok(())
    }

    /// Returns None if no uniqueness is defined for the schema.
    /// Or Some(Key) if the tuple is unique, where Key is the unique value.
    pub fn check_uniqueness(&self, tuple: &Tuple) -> Result<Option<Value>> {
        for (i, field) in self.schema.fields.iter().enumerate() {
            if field.constraints.unique {
                // unwrap on option because nullability is checked first and
                // uniquness disallows null values
                // also, schema forces unique columns to be castable to u32 (int, uint, float)
                let key = tuple.get_value_at(i as u8, &self.schema)?;

                return match self
                    .index
                    .as_ref()
                    .unwrap()
                    .search(self.active_txn, key.as_u32())
                {
                    None => Ok(Some(key)),
                    Some(_) => bail!(Error::DuplicateValue(format!("{key}"), field.name.clone())),
                };
            }
        }
        Ok(None)
    }

    pub fn insert(&mut self, values: Vec<Value>) -> Result<TupleId> {
        let txn = self.active_txn.ok_or(Error::NoActiveTransaction)?;

        let tuple = Tuple::new(values.clone(), &self.schema);

        if tuple.len() > PAGE_END - (SLOT_SIZE + META_SIZE) {
            bail!(Error::TupleTooBig(
                PAGE_END - (SLOT_SIZE + META_SIZE),
                tuple.len()
            ));
        }

        self.check_nullability(&tuple)?;
        let key = self.check_uniqueness(&tuple)?;

        let tuple = self.insert_strings(tuple)?;

        loop {
            self.txn_manager.lock().touch_page(txn, self.last_page)?;

            let mut last_page: TablePage = self
                .bpm
                .lock()
                .fetch_frame(self.last_page, self.active_txn)?
                .writer()
                .into();

            let inserted_tuple_id = last_page.insert_tuple(&tuple);

            self.bpm.lock().unpin(&self.last_page, self.active_txn);

            if let Ok(id) = inserted_tuple_id {
                if let Some(key) = key {
                    self.index
                        .as_mut()
                        .unwrap()
                        .insert(self.active_txn, key.as_u32(), id)?;
                };

                // catalog rows are rebuilt from CreateTable/DropTable records;
                // logging them too would double-apply on replay
                if !self.lm.recovering() && self.name != CATALOG_NAME {
                    self.lm.lock().append(
                        txn,
                        Record::Operation(RowOperation::Insert(self.name.clone(), values)),
                    );
                }

                return Ok(id);
            }

            // page is full, add another page and link to table
            let page_id = self.bpm.lock().new_page()?.reader().get_page_id();

            last_page.set_next_page_id(page_id);

            self.last_page = page_id;
        }
    }

    pub fn scan(
        &self,
        txn_id: Option<TxnId>,
        mut f: impl FnMut(&(TupleId, Entry)) -> Result<()>,
    ) -> Result<()> {
        self.iter(txn_id).try_for_each(|entry| f(&entry))
    }

    pub fn delete(&mut self, id: TupleId) -> Result<()> {
        let txn = self.active_txn.ok_or(Error::NoActiveTransaction)?;
        let (page_id, slot_id) = id;

        self.txn_manager.lock().touch_page(txn, page_id)?;

        let tuple = self.get_tuple(id).unwrap();

        if self.name != CATALOG_NAME && !self.lm.recovering() {
            self.lm.lock().append(
                txn,
                Record::Operation(RowOperation::Delete(
                    self.name.clone(),
                    self.get_portable_values(&tuple)?,
                )),
            );
        }

        let mut page: TablePage = self
            .bpm
            .lock()
            .fetch_frame(page_id, self.active_txn)?
            .writer()
            .into();

        page.delete_tuple(slot_id);

        if let Some(id) = self.get_unique_column_id() {
            let key = tuple.get_value_at(id, &self.schema)?.as_u32();
            self.index.as_mut().unwrap().delete(self.active_txn, key)?;
        }

        self.bpm.lock().unpin(&page_id, self.active_txn);

        Ok(())
    }

    #[inline]
    fn get_unique_column_id(&self) -> Option<u8> {
        self.schema
            .fields
            .iter()
            .position(|field| field.constraints.unique)
            .map(|i| i as u8)
    }

    pub fn update(&mut self, tuple_id: Option<TupleId>, new_values: Vec<Value>) -> Result<TupleId> {
        ensure!(
            self.active_txn.is_some(),
            Error::Internal("Table: No active transaction".into())
        );

        let new_tuple = Tuple::new(new_values.clone(), &self.schema);

        let id = tuple_id.unwrap(); //TODO: Handle None

        self.check_nullability(&new_tuple)?;
        let key = self.check_uniqueness(&new_tuple);

        // value is not unique, does it collide with
        // the old (to be deleted) tuple or an existing tuple?
        if key.is_err() {
            let unique_column_id = self.get_unique_column_id().unwrap();

            let new_key = new_tuple.get_value_at(unique_column_id, &self.schema)?;
            let old_tuple = self.get_tuple(id).unwrap();
            let old_key = old_tuple.get_value_at(unique_column_id, &self.schema)?;

            let unique_column_changed = old_key != new_key;

            // collided with a different tuple
            if unique_column_changed {
                let field_name = self
                    .schema
                    .fields
                    .get(unique_column_id as usize)
                    .unwrap()
                    .name
                    .clone();

                bail!(Error::DuplicateValue(format!("{new_key}"), field_name))
            }
        }

        self.delete(id)?;
        let tuple_id = self.insert(new_values)?;

        Ok(tuple_id)
    }

    /// Needs to return a duplicate because of how catalog handles ownership
    pub fn truncate(&self, txn: TxnId) -> Result<Table> {
        if !self.lm.recovering() {
            self.lm
                .lock()
                .append(txn, Record::Truncate(self.name.clone()));
        }

        let first_page = self.bpm.lock().new_page()?.reader().get_page_id();
        let last_page = first_page;
        let index = BPlusTree::new(self.bpm.clone(), self.txn_manager.clone(), self.active_txn);

        Ok(Self {
            name: self.name.clone(),
            first_page,
            last_page,
            blob_page: self.blob_page,
            bpm: self.bpm.clone(),
            txn_manager: self.txn_manager.clone(),
            lm: self.lm.clone(),
            active_txn: self.active_txn,
            schema: self.schema.clone(),
            index: Some(index),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::tests::test_arc_bpm;
    use crate::tuple::constraints::Constraints;
    use crate::tuple::schema::{Field, Schema};
    use crate::txn_manager::tests::test_arc_transaction_manager;
    use crate::{lit, types::*};
    use anyhow::{anyhow, Result};

    pub fn begin(table: &mut Table) -> Result<TxnId> {
        let txn = table.txn_manager.lock().start()?;
        table.start_txn(txn)?;
        Ok(txn)
    }

    pub fn commit(table: &mut Table, txn: TxnId) -> Result<()> {
        table.txn_manager.lock().commit(txn)?;
        table.commit_txn()
    }

    pub fn test_table(size: usize, schema: &Schema) -> Result<Table> {
        let bpm = test_arc_bpm(size);

        let mut guard = bpm.lock();

        let page = guard.new_page()?.reader().get_page_id();

        let blob_page = guard.new_page()?.reader().get_page_id();

        let txn_manager = test_arc_transaction_manager(bpm.clone());
        let lm = crate::wal::manager::LogManagerHandle::new(&crate::disk_manager::test_path());

        drop(guard);

        Ok(Table {
            name: "test".to_string(),
            first_page: page,
            last_page: page,
            blob_page,
            index: Some(BPlusTree::new(bpm.clone(), txn_manager.clone(), None)),
            bpm,
            txn_manager,
            lm,
            active_txn: None,
            schema: schema.clone(),
        })
    }

    #[test]
    fn test_unpin_drop() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", Types::UInt, Constraints::nullable(false)),
            Field::new("age", Types::UInt, Constraints::nullable(false)),
        ]);

        let mut table = test_table(2, &schema)?;

        let bpm = table.bpm.clone();

        let txn = begin(&mut table)?;
        let tuple_data: Vec<Value> = vec![lit!(UInt, "2")?, lit!(UInt, "50000")?];
        table.insert(tuple_data)?;
        commit(&mut table, txn)?;

        let page_id = table.first_page;

        drop(table);
        assert_eq!(0, bpm.lock().get_pin_count(&page_id).unwrap());

        Ok(())
    }

    #[test]
    fn test_multiple_pages() -> Result<()> {
        let schema = Schema::new(vec![Field::new(
            "a",
            Types::UInt,
            Constraints::nullable(false),
        )]);
        let mut table = test_table(8, &schema)?;

        let first_id = table.get_first_page_id();
        let blob_id = table.get_blob_page_id();

        // entry size = 22 (18 meta + 4 data)
        // slot size = 4
        // free page = 4090
        // 4090 / 24 ≈ 157
        let tuples_per_page = PAGE_END / (META_SIZE + SLOT_SIZE + 4);

        let txn = begin(&mut table)?;

        for i in 0..tuples_per_page {
            table.insert(vec![lit!(UInt, i.to_string())?])?;
        }

        assert_eq!(table.first_page, table.last_page);

        table.insert(vec![lit!(UInt, "99999")?])?;
        let second_id = table.get_last_page_id();

        assert_ne!(table.first_page, table.last_page);

        // add a third page, make sure that page 2 is unpinned
        for i in 0..tuples_per_page {
            table.insert(vec![lit!(UInt, i.to_string())?])?;
        }

        let third_id = table.get_last_page_id();

        commit(&mut table, txn)?;

        assert_eq!(0, table.bpm.lock().get_pin_count(&first_id).unwrap());
        assert_eq!(0, table.bpm.lock().get_pin_count(&blob_id).unwrap());
        assert_eq!(0, table.bpm.lock().get_pin_count(&second_id).unwrap());
        assert_eq!(0, table.bpm.lock().get_pin_count(&third_id).unwrap());

        // get count of tuples
        let mut count = 0;
        table.scan(None, |_| {
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
        let schema = Schema::new(vec![
            Field::new("a", Types::UInt, Constraints::nullable(false)),
            Field::new("str", Types::Str, Constraints::nullable(false)),
            Field::new("b", Types::UInt, Constraints::nullable(false)),
        ]);

        let mut table = test_table(4, &schema)?;

        let txn = begin(&mut table)?;
        table.insert(vec![lit!(UInt, "100")?, lit!(Str, s1)?, lit!(UInt, "50")?])?;
        table.insert(vec![lit!(UInt, "20")?, lit!(Str, s2)?, lit!(UInt, "10")?])?;
        commit(&mut table, txn)?;

        let mut counter = 0;

        let assert_strings = |(_, (_, tuple)): &(TupleId, Entry)| {
            let values = table.get_portable_values(tuple)?;
            assert_eq!(values[1].str(), if counter == 0 { s1 } else { s2 });
            counter += 1;
            Ok(())
        };

        table.scan(None, assert_strings)?;

        Ok(())
    }

    #[test]
    fn test_multi_string() -> Result<()> {
        let s1 = "Hello, World!";
        let s2 = "Hello, Again";
        let schema = Schema::new(vec![
            Field::new("s1", Types::Str, Constraints::nullable(false)),
            Field::new("a", Types::UInt, Constraints::nullable(false)),
            Field::new("s2", Types::Str, Constraints::nullable(false)),
        ]);

        let mut table = test_table(4, &schema)?;

        let txn = begin(&mut table)?;
        table.insert(vec![lit!(Str, s1)?, lit!(UInt, "100")?, lit!(Str, s2)?])?;
        commit(&mut table, txn)?;

        let assert_strings = |(_, (_, tuple)): &(TupleId, Entry)| {
            let values = table.get_portable_values(tuple)?;

            assert_eq!(values[0].str(), s1);
            assert_eq!(UInt::from_bytes(&values[1].to_bytes()).0, 100);
            assert_eq!(values[2].str(), s2);

            Ok(())
        };

        table.scan(None, assert_strings)?;

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", Types::UInt, Constraints::nullable(false)),
            Field::new("b", Types::Float, Constraints::nullable(false)),
            Field::new("c", Types::Int, Constraints::nullable(false)),
        ]);

        let mut table = test_table(4, &schema)?;

        let txn = begin(&mut table)?;

        let t1_id = table.insert(vec![
            lit!(UInt, "10")?,
            lit!(Float, "10.0")?,
            lit!(Int, "10")?,
        ])?;

        let tuple_data = vec![lit!(UInt, "20")?, lit!(Float, "20.0")?, lit!(Int, "20")?];
        let t2_id = table.insert(tuple_data.clone())?;

        table.delete(t1_id)?;

        commit(&mut table, txn)?;

        let scanner_1 = |(_, (_, tuple)): &(TupleId, Entry)| {
            let values = tuple.get_values(&schema)?;
            assert_eq!(values[0].to_bytes(), tuple_data[0].to_bytes());
            assert_eq!(values[1].to_bytes(), tuple_data[1].to_bytes());
            assert_eq!(values[2].to_bytes(), tuple_data[2].to_bytes());

            Ok(())
        };

        table.scan(None, scanner_1)?;

        let txn = begin(&mut table)?;
        table.delete(t2_id)?;
        commit(&mut table, txn)?;

        let scanner_2 = |_: &(TupleId, Entry)| Err(anyhow!("Should not run")); // should never run

        table.scan(None, scanner_2)?;

        Ok(())
    }

    #[test]
    fn test_nulls() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", Types::UInt, Constraints::nullable(true)),
            Field::new("b", Types::Str, Constraints::nullable(true)),
            Field::new("c", Types::Int, Constraints::nullable(true)),
        ]);

        let mut table = test_table(4, &schema)?;

        let txn = begin(&mut table)?;
        table.insert(vec![Value::Null, Value::Null, Value::Null])?;
        commit(&mut table, txn)?;

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

        table.scan(None, validator)?;

        Ok(())
    }

    #[test]
    fn test_nullability() -> Result<()> {
        let schema = Schema::new(vec![Field::new(
            "a",
            Types::Int,
            Constraints::nullable(false),
        )]);

        let mut table = test_table(4, &schema)?;

        begin(&mut table)?;
        assert!(table.insert(vec![Value::Null]).is_err());

        Ok(())
    }

    #[test]
    fn test_uniqueness() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", Types::UInt, Constraints::unique(true)),
            Field::new("b", Types::UInt, Constraints::nullable(false)),
        ]);

        let mut table = test_table(5, &schema)?;

        let txn = begin(&mut table)?;

        table.insert(vec![lit!(UInt, "10")?, lit!(UInt, "20")?])?;

        assert!(table
            .insert(vec![lit!(UInt, "10")?, lit!(UInt, "30")?])
            .is_err());

        commit(&mut table, txn)?;

        let scanner_1 = |(_, (_, tuple)): &(TupleId, Entry)| {
            let values = tuple.get_values(&schema)?;
            assert_eq!(values[0], lit!(UInt, "10")?);
            assert_eq!(values[1], lit!(UInt, "20")?);

            Ok(())
        };

        table.scan(None, scanner_1)?;

        Ok(())
    }
}
