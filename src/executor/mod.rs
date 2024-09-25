use crate::catalog::Catalog;
use crate::txn_manager::{ArcTransactionManager, TransactionManager};
use anyhow::Result;

#[allow(dead_code)]
pub struct Executor {
    catalog: Catalog,
    txn_manager: ArcTransactionManager,
}

#[allow(dead_code)]
impl Executor {
    pub fn new() -> Result<Self> {
        Ok(Self {
            catalog: Catalog::new()?,
            txn_manager: TransactionManager::get(),
        })
    }

    pub fn execute_sql(_sql: &str) -> ResultSet {
        todo!()
    }
}

pub struct ResultSet {}
