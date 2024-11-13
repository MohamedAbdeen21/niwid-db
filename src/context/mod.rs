use crate::catalog::{ArcCatalog, Catalog};
use crate::errors::Error;
use crate::execution::result_set::ResultSet;
use crate::sql::logical_plan::optimizer::optimize_logical_plan;
use crate::sql::logical_plan::LogicalPlanBuilder;
use crate::sql::parser::parse;
use crate::txn_manager::{ArcTransactionManager, TransactionManager, TxnId};
use anyhow::{anyhow, ensure, Result};

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
            self.catalog.write().table().commit_txn()?;
            self.catalog_changed = false;
        }

        self.catalog.write().commit(self.active_txn.unwrap())?;
        self.active_txn = None;

        Ok(())
    }

    pub fn rollback_txn(&mut self) -> Result<()> {
        ensure!(self.active_txn.is_some(), Error::NoActiveTransaction);

        self.txn_manager.lock().rollback(self.active_txn.unwrap())?;

        self.catalog
            .write()
            .tables
            .rollback(self.active_txn.unwrap());
        self.active_txn = None;

        Ok(())
    }

    pub fn execute_sql(&mut self, sql: impl Into<String>) -> Result<ResultSet> {
        let statment = parse(sql)?;

        println!("SQL: {:?}", statment);

        let plan_builder = LogicalPlanBuilder::new(self.catalog.clone());

        let plan = plan_builder.build_initial_plan(statment, self.active_txn)?;
        let plan = optimize_logical_plan(plan);

        plan.execute(self)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::buffer_pool::tests::test_arc_bpm;
    use crate::catalog::tests::test_arc_catalog;
    use crate::lit;
    use crate::txn_manager::tests::test_arc_transaction_manager;
    use crate::types::Types;
    use crate::types::ValueFactory;
    use anyhow::Result;

    fn assert_result_sample(result: &ResultSet) {
        let rows = result.rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], lit!(Int, "1"));
        assert_eq!(rows[0][1], lit!(Int, "2"));
        assert_eq!(rows[1][0], lit!(Int, "3"));
        assert_eq!(rows[1][1], lit!(Int, "4"));
    }

    fn assert_plan(result: &ResultSet, plan: &str) {
        // skip the execution time
        assert_eq!(
            result
                .get_info()
                .lines()
                .skip(1)
                .collect::<Vec<_>>()
                .join("\n"),
            plan
        )
    }

    pub fn test_context() -> Context {
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

        // let result = ctx.execute_sql("SELECT * FROM test WHERE a != 2")?;
        assert_result_sample(&result);
        //
        // let result = ctx.execute_sql("SELECT * FROM test WHERE a = 1")?;
        assert_eq!(result.rows()[0][0], lit!(Int, "1"));

        ctx.execute_sql("DELETE FROM test WHERE a = 1")?;
        let result = ctx.execute_sql("SELECT * FROM test")?;
        assert_eq!(result.rows()[0][0], lit!(Int, "3"));
        assert_eq!(result.rows()[0][1], lit!(Int, "4"));

        ctx.execute_sql("EXPLAIN ANALYZE UPDATE test SET b = 5 WHERE a = 3")?;
        let result = ctx.execute_sql("SELECT * FROM test")?;
        assert_eq!(result.rows()[0][0], lit!(Int, "3"));
        assert_eq!(result.rows()[0][1], lit!(Int, "5"));

        Ok(())
    }

    #[test]
    fn test_empty_result() -> Result<()> {
        let mut ctx = test_context();
        ctx.execute_sql("CREATE TABLE test (a int, b int)")?;
        ctx.execute_sql("INSERT INTO test VALUES (1, 2), (3, 4)")?;
        let result = ctx.execute_sql("SELECT * FROM test")?;
        assert_result_sample(&result);

        let result = ctx.execute_sql("SELECT * FROM test WHERE a == 2")?;
        assert_eq!(result.rows().len(), 0);

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
        assert_eq!(rows[0][0], lit!(Int, "3"));
        assert_eq!(rows[0][1], lit!(Int, "5"));
        assert_eq!(rows[1][0], lit!(Int, "4"));
        assert_eq!(rows[1][1], lit!(Int, "6"));

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
        assert_eq!(rows[0][0], lit!(Int, "3"));
        assert_eq!(rows[0][1], lit!(Int, "5"));
        assert_eq!(rows[1][0], lit!(Int, "4"));
        assert_eq!(rows[1][1], lit!(Int, "6"));

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
    fn test_nullability() -> Result<()> {
        let mut ctx = test_context();
        ctx.execute_sql("CREATE TABLE test (a int not null, b int);")?;
        ctx.execute_sql("INSERT INTO test VALUES (1, 2), (3, 4), (4, null);")?;
        assert_eq!(
            ctx.execute_sql("INSERT INTO test VALUES (null, 10)")
                .unwrap_err()
                .to_string(),
            "Null value in non-nullable field a"
        );
        Ok(())
    }

    #[test]
    fn test_txns_with_sql() -> Result<()> {
        let mut ctx = test_context();
        ctx.execute_sql("BEGIN")?;
        ctx.execute_sql("CREATE TABLE test (a uint unique, b int not null);")?;
        ctx.execute_sql("INSERT INTO test VALUES (1, 2), (3, 4);")?;
        // duplicate in unqiue column
        assert_eq!(
            ctx.execute_sql("INSERT INTO test VALUES (1, 5);")
                .unwrap_err()
                .to_string(),
            "Duplicate value in unique field a"
        );
        // null in not null column
        assert_eq!(
            ctx.execute_sql("INSERT INTO test VALUES (8, null)")
                .unwrap_err()
                .to_string(),
            "Null value in non-nullable field b"
        );
        let result = ctx.execute_sql("SELECT * FROM test;")?;

        assert_result_sample(&result);
        ctx.execute_sql("COMMIT")?;

        Ok(())
    }

    #[test]
    fn test_use_index_in_selects() -> Result<()> {
        let mut ctx = test_context();
        ctx.execute_sql("CREATE TABLE test (a uint unique, b int);")?;
        ctx.execute_sql("INSERT INTO test VALUES (1, 2), (3, 4);")?;

        let expected_plan = r#"Logical Plan:
-- Projection: [#a,#b]
---- IndexScan: test Scan( a range [1,1] ) [#a,#b]"#;

        let result = ctx.execute_sql("EXPLAIN ANALYZE SELECT * FROM test PREWHERE a = 1;")?;
        assert_plan(&result, expected_plan);
        assert_eq!(result.rows()[0][0], lit!(UInt, "1"));

        let expected_plan = r#"Logical Plan:
-- Projection: [#a,#b]
---- IndexScan: test Scan( a range (,3) ) [#a,#b]"#;

        let result = ctx.execute_sql("EXPLAIN ANALYZE SELECT * FROM test PREWHERE a < 3;")?;
        assert_plan(&result, expected_plan);
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], lit!(UInt, "1"));

        let expected_plan = r#"Logical Plan:
-- Projection: [#a,#b]
---- IndexScan: test Scan( a range (1,) ) [#a,#b]"#;

        let result = ctx.execute_sql("EXPLAIN ANALYZE SELECT * FROM test PREWHERE a > 1;")?;
        assert_plan(&result, expected_plan);
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], lit!(UInt, "3"));

        let expected_plan = r#"Logical Plan:
-- Projection: [#a,#b]
---- Filter: #a <> 3
------ IndexScan: test Scan( a range [1,) ) [#a,#b]"#;

        let result =
            ctx.execute_sql("EXPLAIN ANALYZE SELECT * FROM test PREWHERE a >= 1 WHERE a != 3")?;
        assert_plan(&result, expected_plan);
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], lit!(UInt, "1"));

        let expected_plan = r#"Logical Plan:
-- Projection: [#a,#b]
---- IndexScan: test Scan( a range [1,3] ) [#a,#b]"#;

        let result =
            ctx.execute_sql("EXPLAIN ANALYZE SELECT * FROM test PREWHERE (a BETWEEN 1 AND 3)")?;
        assert_plan(&result, expected_plan);
        assert_result_sample(&result);

        Ok(())
    }
}
