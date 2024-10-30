use crate::buffer_pool::ArcBufferPool;
use crate::indexes::b_plus_tree::btree::BPlusTree;
use crate::pages::indexes::b_plus_tree::Key;
use crate::pages::table_page::{TablePage, META_SIZE, PAGE_END, SLOT_SIZE};
use crate::pages::PageId;
use crate::printdbg;
use crate::tuple::schema::Field;
use crate::tuple::{schema::Schema, Entry, Tuple};
use crate::tuple::{TupleExt, TupleId};
use crate::txn_manager::{ArcTransactionManager, TxnId};
use crate::types::{Str, StrAddr, Types, ValueFactory};
use anyhow::{anyhow, Result};

pub mod table_iterator;

pub struct Table {
    name: String,
    pub first_page: PageId,
    pub last_page: PageId,
    blob_page: PageId,
    bpm: ArcBufferPool,
    txn_manager: ArcTransactionManager,
    schema: Schema,
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
        name: String,
        schema: &Schema,
    ) -> Result<Self> {
        let page_id = bpm.lock().new_page()?.reader().get_page_id();

        let blob_page = bpm.lock().new_page()?.reader().get_page_id();

        Ok(Self {
            name,
            first_page: page_id,
            last_page: page_id,
            blob_page,
            index: Some(BPlusTree::new(bpm.clone(), txn_manager.clone())),
            bpm,
            txn_manager,
            active_txn: None,
            schema: schema.clone(),
        })
    }

    pub fn fetch(
        bpm: &mut ArcBufferPool,
        txn_manager: &mut ArcTransactionManager,
        name: String,
        schema: &Schema,
        first_page: PageId,
        last_page: PageId,
        index_page: Option<PageId>,
    ) -> Result<Self> {
        let blob_page = bpm.lock().new_page()?.reader().get_page_id();

        let index = index_page.map(|id| BPlusTree::fetch(id, bpm.clone(), txn_manager.clone()));

        Ok(Self {
            name,
            first_page,
            last_page,
            blob_page,
            bpm: bpm.clone(),
            txn_manager: txn_manager.clone(),
            active_txn: None,
            schema: schema.clone(),
            index,
        })
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

    fn iter(&self, txn_id: Option<TxnId>) -> table_iterator::TableIterator {
        table_iterator::TableIterator::new(self, txn_id)
    }

    fn insert_string(&mut self, bytes: &[u8]) -> Result<TupleId> {
        if bytes.len() > PAGE_END - SLOT_SIZE {
            return Err(anyhow!("Tuple is too long"));
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
            if self.active_txn.is_none() {
                self.bpm.lock().flush(Some(self.blob_page))?;
            }
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
        printdbg!("Table {} Starting txn {}", self.name, txn_id);
        if let Some(current) = self.active_txn {
            if txn_id != current {
                return Err(anyhow!("Another transaction is active"));
            }
        } else {
            self.active_txn = Some(txn_id);
        }
        Ok(())
    }

    pub fn commit_txn(&mut self) -> Result<()> {
        if self.active_txn.is_some() {
            printdbg!(
                "Table {} Committing txn {}",
                self.name,
                self.active_txn.unwrap()
            );
            self.active_txn = None;
            Ok(())
        } else {
            Err(anyhow!("Table: No active transaction"))
        }
    }

    #[allow(dead_code)]
    pub fn rollback_txn(&mut self) -> Result<()> {
        self.commit_txn()
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

    fn check_nullability(&self, tuple: &Tuple) -> Result<()> {
        for (i, field) in self.schema.fields.iter().enumerate() {
            if !field.constraints.nullable
                && tuple.get_value_at(i as u8, &self.schema).unwrap().is_none()
            {
                return Err(anyhow!("Null value in non-nullable field {}", field.name));
            }
        }
        Ok(())
    }

    pub fn check_uniqueness(&mut self, tuple: &Tuple) -> Result<Option<Key>> {
        for (i, field) in self.schema.fields.iter().enumerate() {
            if field.constraints.unique {
                // TODO:
                // unwrap on option because nullability is checked first and
                // uniquness disallows null values
                // also, schema forces unique columns to be of type u32
                let key = tuple.get_value_at(i as u8, &self.schema)?.unwrap().u32();

                return match self.index.as_ref().unwrap().search(self.active_txn, key) {
                    None => Ok(Some(key)),
                    Some(_) => Err(anyhow!("Duplicate value in unique field {}", field.name)),
                };
            }
        }
        Ok(None)
    }

    pub fn insert(&mut self, tuple: Tuple) -> Result<TupleId> {
        if tuple.len() > PAGE_END - (SLOT_SIZE + META_SIZE) {
            return Err(anyhow!("Tuple is too long"));
        }

        self.check_nullability(&tuple)?;
        let key = self.check_uniqueness(&tuple)?;

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

        let id = last_page.insert_tuple(&tuple);

        self.bpm.lock().unpin(&self.last_page, self.active_txn);

        if let Ok(id) = id {
            if let Some(key) = key {
                self.index
                    .as_mut()
                    .unwrap()
                    .insert(self.active_txn, key, id)?;
            };
            if self.active_txn.is_none() {
                self.bpm.lock().flush(Some(self.last_page))?;
            }
            return Ok(id);
        }

        // page is full, add another page and link to table
        let page_id = self.bpm.lock().new_page()?.reader().get_page_id();

        last_page.set_next_page_id(page_id);

        self.last_page = page_id;

        self.insert(tuple)
    }

    pub fn scan(
        &self,
        txn_id: Option<TxnId>,
        mut f: impl FnMut(&(TupleId, Entry)) -> Result<()>,
    ) -> Result<()> {
        self.iter(txn_id).try_for_each(|entry| f(&entry))
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

        if self.active_txn.is_none() {
            self.bpm.lock().flush(Some(page_id))?;
        }

        self.bpm.lock().unpin(&page_id, self.active_txn);

        Ok(())
    }

    pub fn update(&mut self, tuple_id: Option<TupleId>, new_tuple: Tuple) -> Result<TupleId> {
        if self.active_txn.is_none() {
            panic!("Table: No active transaction");
        }

        match tuple_id {
            None => todo!(),
            Some(id) => self.delete(id)?,
        }

        let tuple_id = self.insert(new_tuple)?;

        Ok(tuple_id)
    }

    /// Needs to return a duplicate because of how catalog handles ownership
    pub fn truncate(&self) -> Result<Table> {
        let first_page = self.bpm.lock().new_page()?.reader().get_page_id();
        let last_page = first_page;
        let index = BPlusTree::new(self.bpm.clone(), self.txn_manager.clone());

        Ok(Self {
            name: self.name.clone(),
            first_page,
            last_page,
            blob_page: self.blob_page,
            bpm: self.bpm.clone(),
            txn_manager: self.txn_manager.clone(),
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
    use crate::{types::*, value};
    use anyhow::Result;

    pub fn test_table(size: usize, schema: &Schema) -> Result<Table> {
        let bpm = test_arc_bpm(size);

        let mut guard = bpm.lock();

        let page = guard.new_page()?.reader().get_page_id();

        let blob_page = guard.new_page()?.reader().get_page_id();

        let txn_manager = test_arc_transaction_manager(bpm.clone());

        drop(guard);

        Ok(Table {
            name: "test".to_string(),
            first_page: page,
            last_page: page,
            blob_page,
            index: Some(BPlusTree::new(bpm.clone(), txn_manager.clone())),
            bpm,
            txn_manager,
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

        let tuple_data: Vec<Value> = vec![
            ValueFactory::from_string(&Types::UInt, "2"),
            ValueFactory::from_string(&Types::UInt, "50000"),
        ];
        let tuple = Tuple::new(tuple_data, &schema);
        table.insert(tuple)?;

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
        let mut table = test_table(5, &schema)?;

        let first_id = table.get_first_page_id();
        let blob_id = table.get_blob_page_id();

        // entry size = 22 (18 meta + 4 data)
        // slot size = 4
        // free page = 4090
        // 4090 / 24 ≈ 157
        let tuples_per_page = PAGE_END / (META_SIZE + SLOT_SIZE + 4);

        for i in 0..tuples_per_page {
            let tuple = Tuple::new(
                vec![ValueFactory::from_string(&Types::UInt, i.to_string())],
                &schema,
            );
            table.insert(tuple)?;
        }

        assert_eq!(table.first_page, table.last_page);

        table.insert(Tuple::new(
            vec![ValueFactory::from_string(&Types::UInt, "99999")],
            &schema,
        ))?;
        let second_id = table.get_last_page_id();

        assert_ne!(table.first_page, table.last_page);

        // add a third page, make sure that page 2 is unpinned
        for i in 0..tuples_per_page {
            let tuple = Tuple::new(
                vec![ValueFactory::from_string(&Types::UInt, i.to_string())],
                &schema,
            );
            table.insert(tuple)?;
        }

        let third_id = table.get_last_page_id();

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

        let tuple = Tuple::new(
            vec![
                ValueFactory::from_string(&Types::UInt, "100"),
                ValueFactory::from_string(&Types::Str, s1),
                ValueFactory::from_string(&Types::UInt, "50"),
            ],
            &schema,
        );
        table.insert(tuple)?;

        let tuple = Tuple::new(
            vec![
                ValueFactory::from_string(&Types::UInt, "20"),
                ValueFactory::from_string(&Types::Str, s2),
                ValueFactory::from_string(&Types::UInt, "10"),
            ],
            &schema,
        );
        table.insert(tuple)?;

        let mut counter = 0;

        let assert_strings = |(_, (_, tuple)): &(TupleId, Entry)| {
            let tuple_bytes = tuple.get_value_of("str", &schema)?.unwrap();
            let string = table.fetch_string(tuple_bytes.str_addr());
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

        let tuple = Tuple::new(
            vec![
                ValueFactory::from_string(&Types::Str, s1),
                ValueFactory::from_string(&Types::UInt, "100"),
                ValueFactory::from_string(&Types::Str, s2),
            ],
            &schema,
        );
        table.insert(tuple)?;

        let assert_strings = |(_, (_, tuple)): &(TupleId, Entry)| {
            let values = tuple.get_values(&schema)?;
            let read_s1 = table.fetch_string(values[0].str_addr());

            let a = UInt::from_bytes(&values[1].to_bytes()).0;

            let read_s2 = table.fetch_string(values[2].str_addr());

            assert_eq!(read_s1.0, s1);
            assert_eq!(a, 100);
            assert_eq!(read_s2.0, s2);

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

        let tuple = Tuple::new(
            vec![
                ValueFactory::from_string(&Types::UInt, "10"),
                ValueFactory::from_string(&Types::Float, "10.0"),
                ValueFactory::from_string(&Types::Int, "10"),
            ],
            &schema,
        );
        let t1_id = table.insert(tuple)?;

        let tuple_data = vec![
            ValueFactory::from_string(&Types::UInt, "20"),
            ValueFactory::from_string(&Types::Float, "20.0"),
            ValueFactory::from_string(&Types::Int, "20"),
        ];
        let tuple = Tuple::new(tuple_data.clone(), &schema);
        let t2_id = table.insert(tuple)?;

        table.delete(t1_id)?;

        let scanner_1 = |(_, (_, tuple)): &(TupleId, Entry)| {
            let values = tuple.get_values(&schema)?;
            assert_eq!(values[0].to_bytes(), tuple_data[0].to_bytes());
            assert_eq!(values[1].to_bytes(), tuple_data[1].to_bytes());
            assert_eq!(values[2].to_bytes(), tuple_data[2].to_bytes());

            Ok(())
        };

        table.scan(None, scanner_1)?;

        table.delete(t2_id)?;

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

        let tuple = Tuple::new(vec![Value::Null, Value::Null, Value::Null], &schema);
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

        let tuple = Tuple::new(vec![Value::Null], &schema);

        assert!(table.insert(tuple).is_err());

        Ok(())
    }

    #[test]
    fn test_uniqunes() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", Types::UInt, Constraints::unique(true)),
            Field::new("b", Types::UInt, Constraints::nullable(false)),
        ]);

        let mut table = test_table(5, &schema)?;

        let t1 = Tuple::new(vec![value!(UInt, "10"), value!(UInt, "20")], &schema);
        let t2 = Tuple::new(vec![value!(UInt, "10"), value!(UInt, "30")], &schema);

        table.insert(t1)?;
        assert!(table.insert(t2).is_err());

        let scanner_1 = |(_, (_, tuple)): &(TupleId, Entry)| {
            let values = tuple.get_values(&schema)?;
            assert_eq!(values[0], value!(UInt, "10"));
            assert_eq!(values[1], value!(UInt, "20"));

            Ok(())
        };

        table.scan(None, scanner_1)?;

        Ok(())
    }
}
