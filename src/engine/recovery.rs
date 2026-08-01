use std::collections::HashSet;

use anyhow::Result;

use crate::engine::Engine;
use crate::errors::Error;
use crate::txn_manager::TxnId;
use crate::wal::record::{Record, RowOperation};
use crate::wal::Lsn;

impl Engine {
    pub(crate) fn recover(&self) -> Result<()> {
        let start = self.checkpoint_lsn();
        let mut log = self.log_manager.lock();

        let committed: HashSet<TxnId> = log
            .iter_from(start)
            .filter(|record| matches!(record.record_type, Record::Commit))
            .map(|record| record.txn_id)
            .collect();

        drop(log);

        if committed.is_empty() {
            return Ok(());
        }

        self.log_manager.set_recovering(true);
        let replayed = self.replay(start, &committed);
        self.log_manager.set_recovering(false);

        replayed?;

        // the replayed state is in memory only; recording it now keeps the
        // next startup from applying the same records again
        self.checkpoint()
    }

    fn replay(&self, start: Lsn, committed: &HashSet<TxnId>) -> Result<()> {
        let mut log = self.log_manager.lock();

        let records: Vec<_> = log
            .iter_from(start)
            .filter(|record| committed.contains(&record.txn_id))
            .map(|record| record.record_type)
            .collect();

        drop(log);

        let txn = self.txn_manager.lock().start()?;

        for record in records {
            self.apply(record, txn)?;
        }

        self.txn_manager.lock().commit(txn)?;
        self.catalog.write().commit(txn)?;

        Ok(())
    }

    fn apply(&self, record: Record, txn: TxnId) -> Result<()> {
        let mut catalog = self.catalog.write();

        match record {
            Record::Commit | Record::Abort => {}
            Record::CreateTable(name, schema) => {
                catalog.add_table(&name, &schema, true, txn)?;
            }
            Record::DropTable(name) => {
                catalog
                    .drop_table(&name, true, txn)
                    .ok_or(Error::TableNotFound(name))?;
            }
            Record::Truncate(name) => {
                catalog.truncate_table(&name, txn)?;
            }
            Record::Operation(RowOperation::Insert(name, values)) => {
                catalog
                    .get_table_mut(&name, Some(txn))
                    .ok_or(Error::TableNotFound(name))??
                    .insert(values)?;
            }
            Record::Operation(RowOperation::Delete(name, values)) => {
                let table = catalog
                    .get_table_mut(&name, Some(txn))
                    .ok_or(Error::TableNotFound(name.clone()))??;

                let id = table
                    .find_by_values(&values)?
                    .ok_or(Error::Internal(format!(
                        "Replay: no row to delete in {name}"
                    )))?;

                table.delete(id)?;
            }
            Record::Operation(RowOperation::Update {
                table: name,
                old_values,
                new_values,
            }) => {
                let table = catalog
                    .get_table_mut(&name, Some(txn))
                    .ok_or(Error::TableNotFound(name.clone()))??;

                let id = table
                    .find_by_values(&old_values)?
                    .ok_or(Error::Internal(format!(
                        "Replay: no row to update in {name}"
                    )))?;

                table.update(Some(id), new_values)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::disk_manager::test_path;
    use crate::engine::Engine;
    use anyhow::Result;

    #[test]
    fn reopening_replays_committed_rows_once() -> Result<()> {
        let dir = test_path();

        // kept alive: the test-only Drop impls delete the data dir
        let first = Engine::with_pool_size(&dir, 50);
        let mut ctx = first.context();
        ctx.execute_sql("CREATE TABLE t (a int)")?;
        ctx.execute_sql("INSERT INTO t VALUES (1)")?;

        let second = Engine::with_pool_size(&dir, 50);
        let rows = second
            .context()
            .execute_sql("SELECT * FROM t")?
            .rows()
            .len();

        assert_eq!(rows, 1, "row was applied twice");

        Ok(())
    }
}
