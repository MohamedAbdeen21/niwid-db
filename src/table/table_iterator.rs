use crate::buffer_pool::ArcBufferPool;
use crate::pages::table_page::TablePage;
use crate::pages::{PageId, SlotId, INVALID_PAGE};
use crate::tuple::{Entry, TupleId};
use crate::txn_manager::TxnId;

use super::Table;

// TODO: try to iterate over pages not tuples
pub(super) struct TableIterator {
    page: TablePage,
    current_slot: SlotId,
    next_page: PageId,
    bpm: ArcBufferPool,
    num_tuples: SlotId,
    active_txn: Option<TxnId>,
}

impl TableIterator {
    pub fn new(table: &Table, txn_id: Option<TxnId>) -> Self {
        let bpm = table.bpm.clone();
        let page: TablePage = bpm
            .lock()
            .fetch_frame(table.first_page, txn_id)
            .unwrap()
            .reader()
            .into();

        let header = page.header();

        TableIterator {
            current_slot: 0,
            next_page: header.get_next_page(),
            num_tuples: header.get_num_tuples(),
            page,
            bpm,
            active_txn: txn_id,
        }
    }
}

impl Iterator for TableIterator {
    type Item = (TupleId, Entry);

    fn next(&mut self) -> Option<Self::Item> {
        // current page is done, drop it
        if self.current_slot >= self.num_tuples {
            let page_id = self.page.get_page_id();
            self.bpm.lock().unpin(&page_id, self.active_txn);
        }

        if self.current_slot >= self.num_tuples
            && (self.next_page == INVALID_PAGE || self.next_page == 0)
        {
            return None;
        }

        if self.current_slot >= self.num_tuples {
            self.page = self
                .bpm
                .lock()
                .fetch_frame(self.next_page, self.active_txn)
                .unwrap()
                .reader()
                .into();

            self.current_slot = 0;
            let header = self.page.header();
            self.next_page = header.get_next_page();
            self.num_tuples = header.get_num_tuples();
            return self.next();
        }

        let (meta, tuple) = self.page.read_tuple(self.current_slot);
        self.current_slot += 1;

        // TODO: Can run into stack overflows because of recursion
        // I hate how rust doesn't have tail recursion
        if meta.is_deleted() {
            return self.next();
        }

        let entry = (meta, tuple);

        let page_id = self.page.get_page_id();

        Some(((page_id, self.current_slot - 1), entry))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::pages::table_page::{TablePage, META_SIZE, PAGE_END, SLOT_SIZE};
    use crate::table::tests::test_table;
    use crate::tuple::constraints::Constraints;
    use crate::tuple::schema::{Field, Schema};
    use crate::tuple::{Entry, Tuple, TupleId};
    use crate::types::{Types, Value, ValueFactory};

    use super::TableIterator;

    #[test]
    fn test_skip_deleted() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", Types::UInt, Constraints::nullable(false)),
            Field::new("age", Types::UInt, Constraints::nullable(false)),
        ]);
        let mut table = test_table(3, &schema)?;

        let t1 = Tuple::new(
            vec![
                ValueFactory::from_string(&Types::UInt, "2"),
                ValueFactory::from_string(&Types::UInt, "3"),
            ],
            &schema,
        );
        table.insert(t1)?;

        let t2 = Tuple::new(
            vec![
                ValueFactory::from_string(&Types::UInt, "4"),
                ValueFactory::from_string(&Types::UInt, "5"),
            ],
            &schema,
        );
        let t2_id = table.insert(t2)?;

        table.delete(t2_id)?;

        let t3 = Tuple::new(
            vec![
                ValueFactory::from_string(&Types::UInt, "6"),
                ValueFactory::from_string(&Types::UInt, "7"),
            ],
            &schema,
        );
        table.insert(t3)?;

        let mut counter = 0;
        let scanner = |(_, (meta, _)): (TupleId, Entry)| -> Result<()> {
            counter += 1;
            assert!(!meta.is_deleted());
            Ok(())
        };

        TableIterator::new(&table, None).try_for_each(scanner)?;

        assert_eq!(counter, 2);

        Ok(())
    }

    #[test]
    fn test_multiple_pages_iter() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", Types::Int, Constraints::nullable(true)),
            Field::new("b", Types::Int, Constraints::nullable(true)),
        ]);

        let tuples_per_page = PAGE_END / (META_SIZE + SLOT_SIZE + 8);

        let mut table = test_table(3, &schema)?;

        for i in 0..tuples_per_page {
            let tuple = Tuple::new(
                vec![
                    ValueFactory::from_string(&Types::Int, i.to_string()),
                    ValueFactory::from_string(&Types::Int, i.to_string()),
                ],
                &schema,
            );
            table.insert(tuple)?;
        }

        let first_page: TablePage = table
            .bpm
            .lock()
            .fetch_frame(table.first_page, None)?
            .reader()
            .into();

        assert_eq!(table.first_page, table.last_page);

        assert!(first_page.is_dirty());

        let tuple = Tuple::new(vec![Value::Null, Value::Null], &schema);
        table.insert(tuple)?;

        assert_ne!(table.first_page, table.last_page);

        table.bpm.lock().unpin(&table.first_page, None);

        let mut counter = 0;
        let scanner = |_: (TupleId, Entry)| -> Result<()> {
            counter += 1;
            Ok(())
        };

        assert_eq!(
            table
                .bpm
                .lock()
                .get_pin_count(&table.get_first_page_id())
                .unwrap(),
            0
        );

        TableIterator::new(&table, None).try_for_each(scanner)?;

        assert_eq!(counter, tuples_per_page + 1);

        Ok(())
    }
}
