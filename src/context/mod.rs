use crate::catalog::{ArcCatalog, Catalog};
use crate::execution::result_set::ResultSet;
use crate::sql::logical_plan::build_initial_plan;
use crate::sql::logical_plan::optimizer::optimize_logical_plan;
use crate::sql::parser::parse;
use crate::txn_manager::{ArcTransactionManager, TransactionManager, TxnId};
use anyhow::{anyhow, Result};

pub struct Context {
    catalog: ArcCatalog,
    txn_manager: ArcTransactionManager,
    active_txn: Option<TxnId>,
    catalog_changed: bool,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            catalog: Catalog::get(),
            txn_manager: TransactionManager::get(),
            active_txn: None,
            catalog_changed: false,
        }
    }
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_active_txn(&self) -> Option<TxnId> {
        self.active_txn
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
            return Err(anyhow!("Context: No active transaction"));
        }

        self.txn_manager.lock().commit(self.active_txn.unwrap())?;

        if self.catalog_changed {
            self.catalog.lock().table().commit_txn()?;
            self.catalog_changed = false;
        }

        self.catalog.lock().commit(self.active_txn.unwrap())?;
        self.active_txn = None;

        Ok(())
    }

    pub fn rollback_txn(&mut self) -> Result<()> {
        if self.active_txn.is_none() {
            return Ok(());
        }

        self.txn_manager.lock().rollback(self.active_txn.unwrap())?;

        self.catalog
            .lock()
            .tables
            .rollback(self.active_txn.unwrap());
        self.active_txn = None;

        Ok(())
    }

    pub fn execute_sql(&mut self, sql: impl Into<String>) -> Result<ResultSet> {
        let statment = parse(sql)?;

        // println!("SQL: {:?}", statment);

        let plan = build_initial_plan(statment, self.active_txn)?;
        let plan = optimize_logical_plan(plan);

        plan.execute(self)
    }
}
