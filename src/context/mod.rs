use crate::catalog::{ArcCatalog, Catalog};
use crate::execution::result_set::ResultSet;
use crate::sql::logical_plan::optimizer::optimize_logical_plan;
use crate::sql::logical_plan::LogicalPlanBuilder;
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
        let catalog = Catalog::get();
        let txn_manager = TransactionManager::get();

        Self::new(catalog, txn_manager)
    }
}

impl Context {
    pub fn new(catalog: ArcCatalog, txn_manager: ArcTransactionManager) -> Self {
        Self {
            catalog,
            txn_manager,
            active_txn: None,
            catalog_changed: false,
        }
    }

    pub fn get_catalog(&self) -> ArcCatalog {
        self.catalog.clone()
    }

    pub fn get_active_txn(&self) -> Option<TxnId> {
        self.active_txn
    }

    pub fn start_txn(&mut self) -> Result<TxnId> {
        if let Some(id) = self.active_txn {
            return Ok(id);
        }

        let id = self.txn_manager.lock().start()?;
        self.active_txn = Some(id);

        Ok(id)
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
            return Err(anyhow!("Context: No active transaction"));
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

        let plan_builder = LogicalPlanBuilder::new(self.catalog.clone());

        let plan = plan_builder.build_initial_plan(statment, self.active_txn)?;
        let plan = optimize_logical_plan(plan);

        plan.execute(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::tests::test_arc_bpm;
    use crate::catalog::tests::test_arc_catalog;
    use crate::txn_manager::tests::test_arc_transaction_manager;
    use anyhow::Result;

    #[test]
    fn test_context() -> Result<()> {
        let test_bpm = test_arc_bpm(50);
        let test_catalog = test_arc_catalog(test_bpm.clone());
        let test_txn_mngr = test_arc_transaction_manager(test_bpm);
        let mut ctx = Context::new(test_catalog, test_txn_mngr);
        ctx.execute_sql("CREATE TABLE t (a int, b int)")?;
        Ok(())
    }
}
