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

    #[cfg(test)]
    fn clone_context(&self) -> Self {
        Self::new(self.catalog.clone(), self.txn_manager.clone())
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
    use crate::types::Types;
    use crate::types::ValueFactory;
    use crate::value;
    use anyhow::Result;

    fn assert_result_sample(result: &ResultSet) {
        let rows = result.rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], value!(Int, "1"));
        assert_eq!(rows[0][1], value!(Int, "2"));
        assert_eq!(rows[1][0], value!(Int, "3"));
        assert_eq!(rows[1][1], value!(Int, "4"));
    }

    fn test_context() -> Context {
        let test_bpm = test_arc_bpm(50);
        let test_txn_mngr = test_arc_transaction_manager(test_bpm.clone());
        let test_catalog = test_arc_catalog(test_bpm.clone(), test_txn_mngr.clone());
        Context::new(test_catalog, test_txn_mngr)
    }

    #[test]
    fn test_single_txn_per_context() -> Result<()> {
        let mut ctx = test_context();
        let id = ctx.start_txn()?;

        assert_eq!(id, ctx.start_txn()?);

        ctx.commit_txn()?;

        assert_ne!(id, ctx.start_txn()?);

        Ok(())
    }

    #[test]
    fn test_manage_txns_with_sql() -> Result<()> {
        let first_txn_id = 0;

        let mut ctx = test_context();
        ctx.execute_sql("begin")?;
        assert_eq!(ctx.start_txn()?, first_txn_id);
        ctx.execute_sql("commit")?;
        assert_ne!(ctx.start_txn()?, first_txn_id);

        Ok(())
    }

    #[test]
    fn test_simple_sql() -> Result<()> {
        let mut ctx = test_context();
        ctx.execute_sql("CREATE TABLE test (a int, b int)")?;
        ctx.execute_sql("INSERT INTO test VALUES (1, 2), (3, 4)")?;
        let result = ctx.execute_sql("SELECT * FROM test")?;

        assert_result_sample(&result);

        Ok(())
    }

    #[test]
    fn test_isolation() -> Result<()> {
        let mut ctx1 = test_context();
        let mut ctx2 = ctx1.clone_context();

        ctx1.execute_sql("BEGIN")?;
        ctx1.execute_sql("CREATE TABLE test (a int, b int);")?;
        let catalog = ctx1.execute_sql("SELECT * FROM __CATALOG__")?;
        assert!(!catalog.is_empty()); // TODO: Check actual values
        ctx1.execute_sql("INSERT INTO test VALUES (1, 2), (3, 4);")?;

        let result = ctx1.execute_sql("SELECT * FROM test;")?;

        assert_result_sample(&result);

        // doesn't exist for ctx2
        assert!(ctx2.execute_sql("SELECT * FROM test").is_err());
        let catalog = ctx2.execute_sql("SELECT * FROM __CATALOG__")?;
        assert!(catalog.is_empty());

        ctx1.execute_sql("COMMIT")?;

        let result = ctx2.execute_sql("SELECT * FROM test")?;

        assert_result_sample(&result);

        Ok(())
    }

    #[test]
    fn test_txn_rollback() -> Result<()> {
        let mut ctx = test_context();
        ctx.execute_sql("BEGIN")?;
        ctx.execute_sql("CREATE TABLE test (a int, b int);")?;
        ctx.execute_sql("INSERT INTO test VALUES (1, 2), (3, 4);")?;
        let result = ctx.execute_sql("SELECT * FROM test;")?;

        assert_result_sample(&result);

        ctx.execute_sql("ROLLBACK")?;

        // create was rolled back, so table should be not found
        assert!(ctx.execute_sql("SELECT * FROM test;").is_err());
        let catalog = ctx.execute_sql("SELECT * FROM __CATALOG__")?;
        assert!(catalog.is_empty());

        Ok(())
    }

    #[test]
    fn test_joins() -> Result<()> {
        let mut ctx = test_context();
        ctx.execute_sql("CREATE TABLE t1 (a int, b int);")?;
        ctx.execute_sql("INSERT INTO t1 VALUES (1, 3), (2, 4);")?;

        ctx.execute_sql("CREATE TABLE t2 (c int, d int);")?;
        ctx.execute_sql("INSERT INTO t2 VALUES (1, 5), (2, 6);")?;

        let result = ctx.execute_sql("SELECT b, d FROM t1 JOIN t2 ON a = c;")?;

        let rows = result.rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], value!(Int, "3"));
        assert_eq!(rows[0][1], value!(Int, "5"));
        assert_eq!(rows[1][0], value!(Int, "4"));
        assert_eq!(rows[1][1], value!(Int, "6"));

        Ok(())
    }

    #[test]
    fn test_qualified_joins() -> Result<()> {
        let mut ctx = test_context();
        ctx.execute_sql("CREATE TABLE t1 (a int, b int);")?;
        ctx.execute_sql("INSERT INTO t1 VALUES (1, 3), (2, 4);")?;

        ctx.execute_sql("CREATE TABLE t2 (a int, b int);")?;
        ctx.execute_sql("INSERT INTO t2 VALUES (1, 5), (2, 6);")?;

        let result = ctx.execute_sql("SELECT t1.b, t2.b FROM t1 JOIN t2 ON t1.a = t2.a;")?;

        let rows = result.rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], value!(Int, "3"));
        assert_eq!(rows[0][1], value!(Int, "5"));
        assert_eq!(rows[1][0], value!(Int, "4"));
        assert_eq!(rows[1][1], value!(Int, "6"));

        Ok(())
    }

    #[test]
    fn test_force_qualifying_joins() -> Result<()> {
        let mut ctx = test_context();
        ctx.execute_sql("CREATE TABLE t1 (a int, b int);")?;
        ctx.execute_sql("INSERT INTO t1 VALUES (1, 3), (2, 4);")?;

        ctx.execute_sql("CREATE TABLE t2 (a int, c int);")?;
        ctx.execute_sql("INSERT INTO t2 VALUES (1, 5), (2, 6);")?;

        // force that selection be qualified even though the selected col is not ambiguous
        assert!(ctx
            .execute_sql("SELECT c FROM t1 JOIN t2 ON t1.a = t2.a;")
            .is_err());

        Ok(())
    }

    #[test]
    fn test_txns_with_sql() -> Result<()> {
        let mut ctx = test_context();
        ctx.execute_sql("BEGIN")?;
        ctx.execute_sql("CREATE TABLE test (a uint unique, b int not null);")?;
        ctx.execute_sql("INSERT INTO test VALUES (1, 2), (3, 4);")?;
        assert!(ctx.execute_sql("INSERT INTO test VALUES (1, 5);").is_err());
        assert!(ctx
            .execute_sql("INSERT INTO test VALUES (8, null);")
            .is_err());
        let _result = ctx.execute_sql("SELECT * FROM test;")?;
        ctx.execute_sql("COMMIT")?;

        Ok(())
    }
}
