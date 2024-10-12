use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::execution::result_set::ResultSet;
use crate::sql::logical_plan::build_initial_plan;
use crate::sql::logical_plan::optimizer::optimize_logical_plan;
use crate::sql::parser::parse;
use crate::table::Table;
use crate::tuple::schema::Schema;
use crate::txn_manager::{ArcTransactionManager, TransactionManager, TxnId};
use anyhow::Result;

pub struct Context {
    pub catalog: Catalog,
    txn_manager: ArcTransactionManager,
    active_txn: Option<TxnId>,
    txn_tables: Vec<String>,
    catalog_changed: bool,
}

impl Context {
    pub fn new() -> Result<Self> {
        Ok(Self {
            catalog: Catalog::new()?,
            txn_manager: TransactionManager::get(),
            active_txn: None,
            txn_tables: vec![],
            catalog_changed: false,
        })
    }

    pub fn add_table(
        &mut self,
        name: &str,
        schema: &Schema,
        ignore_if_exists: bool,
    ) -> Result<&mut Table> {
        if let Some(txn_id) = self.active_txn {
            self.catalog.table.start_txn(txn_id)?;
            self.catalog_changed = true;
        }

        let table = self.catalog.add_table(name, schema, ignore_if_exists)?;

        Ok(table)
    }

    pub fn start_txn(&mut self) -> Result<()> {
        if self.active_txn.is_some() {
            return Ok(());
        }

        let id = self.txn_manager.lock().start()?;
        self.active_txn = Some(id);

        Ok(())
    }

    pub fn commit_txn(&mut self) -> Result<()> {
        if self.active_txn.is_none() {
            return Ok(());
        }

        self.txn_manager.lock().commit(self.active_txn.unwrap())?;

        for table in self.txn_tables.iter_mut() {
            self.catalog.get_table(table).unwrap().commit_txn()?;
        }

        if self.catalog_changed {
            self.catalog.table.commit_txn()?;
            self.catalog_changed = false;
        }

        self.txn_tables.clear();
        self.active_txn = None;

        Ok(())
    }

    pub fn execute_sql(&mut self, sql: impl Into<String>) -> Result<ResultSet> {
        let statment = parse(sql)?;

        let plan = build_initial_plan(statment)?;
        let plan = optimize_logical_plan(plan);

        plan.execute()
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        // static objects don't call drop, need to make
        // sure that frames persist on disk
        BufferPoolManager::get()
            .lock()
            .flush(None)
            .expect("Shutdown: Flushing buffer pool failed");
    }
}
