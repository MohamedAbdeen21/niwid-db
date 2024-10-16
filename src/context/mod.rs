use crate::buffer_pool::BufferPoolManager;
use crate::catalog::{ArcCatalog, Catalog};
use crate::execution::result_set::ResultSet;
use crate::sql::logical_plan::build_initial_plan;
use crate::sql::logical_plan::optimizer::optimize_logical_plan;
use crate::sql::parser::parse;
use crate::tuple::schema::Schema;
use crate::txn_manager::{ArcTransactionManager, TransactionManager, TxnId};
use anyhow::Result;

pub struct Context {
    catalog: ArcCatalog,
    txn_manager: ArcTransactionManager,
    active_txn: Option<TxnId>,
    txn_tables: Vec<String>,
    catalog_changed: bool,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            catalog: Catalog::get(),
            txn_manager: TransactionManager::get(),
            active_txn: None,
            txn_tables: vec![],
            catalog_changed: false,
        }
    }
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn truncate_table(&mut self, table_name: String) -> Result<()> {
        self.catalog
            .lock()
            .truncate_table(table_name, self.active_txn)
    }

    pub fn add_table(
        &mut self,
        name: String,
        schema: &Schema,
        ignore_if_exists: bool,
    ) -> Result<()> {
        if let Some(txn_id) = self.active_txn {
            self.catalog.lock().table().start_txn(txn_id)?;
            self.catalog_changed = true;
        }

        self.catalog
            .lock()
            .add_table(name, schema, ignore_if_exists, self.active_txn)
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

        for table in self.txn_tables.drain(..) {
            self.catalog
                .lock()
                .get_table(&table, self.active_txn)
                .unwrap()
                .commit_txn()?;
        }

        if self.catalog_changed {
            self.catalog.lock().table().commit_txn()?;
            self.catalog_changed = false;
        }

        self.catalog.lock().tables.commit(self.active_txn.unwrap());
        self.active_txn = None;

        Ok(())
    }

    pub fn abort_txn(&mut self) -> Result<()> {
        if self.active_txn.is_none() {
            return Ok(());
        }

        self.txn_manager.lock().abort(self.active_txn.unwrap())?;

        for table in self.txn_tables.iter_mut() {
            self.catalog
                .lock()
                .get_table(table, self.active_txn)
                .unwrap()
                .abort_txn()?;
        }

        self.txn_tables.clear();
        self.catalog.lock().tables.abort(self.active_txn.unwrap());
        self.active_txn = None;

        Ok(())
    }

    pub fn execute_sql(&mut self, sql: impl Into<String>) -> Result<ResultSet> {
        let statment = parse(sql)?;

        // println!("SQL: {:?}", statment);

        let plan = build_initial_plan(statment, self.active_txn)?;
        let plan = optimize_logical_plan(plan);

        plan.execute(self.active_txn)
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
