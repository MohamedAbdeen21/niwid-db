mod versioned_map;

use crate::buffer_pool::ArcBufferPool;
use crate::errors::Error;
use crate::pages::{PageId, INVALID_PAGE};
use crate::printdbg;
use crate::table::Table;
use crate::tuple::constraints::Constraints;
use crate::tuple::schema::{Field, Schema};
use crate::tuple::{Entry, TupleId};
use crate::txn_manager::{ArcTransactionManager, TxnId};
use crate::types::{Types, Value, ValueFactory};
use crate::wal::manager::ArcLogManager;
use anyhow::{bail, Result};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use versioned_map::VersionedMap;

// preserve page_id 1 for catalog, bpm starts assigning at 2
pub const CATALOG_PAGE: PageId = 2;
pub const CATALOG_NAME: &str = "__CATALOG__";

/// One row of the catalog table: everything needed to reopen a Table.
/// The column layout is defined here and nowhere else.
pub struct CatalogRow {
    pub name: String,
    pub schema: Schema,
    pub first_page: PageId,
    pub last_page: PageId,
    pub index_page: Option<PageId>,
}

impl CatalogRow {
    /// Schema of the catalog table itself
    pub fn catalog_schema() -> Schema {
        Schema::new(vec![
            Field::new("table_name", Types::Str, Constraints::nullable(false)),
            Field::new("first_page", Types::UInt, Constraints::nullable(false)),
            Field::new("last_page", Types::UInt, Constraints::nullable(false)),
            Field::new("index_root", Types::UInt, Constraints::nullable(false)),
            Field::new("schema", Types::Str, Constraints::nullable(false)),
        ])
    }

    pub fn to_values(&self) -> Result<Vec<Value>> {
        let serialized_schema = String::from_utf8(self.schema.to_bytes().to_vec())?;

        Ok(vec![
            ValueFactory::from_string(&Types::Str, &self.name)?,
            ValueFactory::from_string(&Types::UInt, self.first_page.to_string())?,
            ValueFactory::from_string(&Types::UInt, self.last_page.to_string())?,
            ValueFactory::from_string(
                &Types::UInt,
                self.index_page.unwrap_or(INVALID_PAGE).to_string(),
            )?,
            ValueFactory::from_string(&Types::Str, &serialized_schema)?,
        ])
    }

    /// values must be portable (strings resolved), i.e. from get_portable_values
    pub fn from_values(values: &[Value]) -> Self {
        CatalogRow {
            name: values[0].str(),
            first_page: values[1].u32(),
            last_page: values[2].u32(),
            index_page: Some(values[3].u32()),
            schema: Schema::from_bytes(values[4].str().as_bytes()),
        }
    }
}

pub type ArcCatalog = Arc<RwLock<Catalog>>;

pub struct Catalog {
    pub tables_map: VersionedMap<String, (TupleId, Table)>,
    txn_tables: HashMap<TxnId, HashSet<String>>,
    bpm: ArcBufferPool,
    txn_manager: ArcTransactionManager,
    lm: ArcLogManager,
}

impl Catalog {
    /// Catalog is a table itself, this gives access to the underlying table
    pub fn table(&mut self) -> &mut Table {
        // No need to track version for catalog, catalog always has the same
        // tuple_id and can never be deleted
        self.tables_map
            .get_mut(None, &CATALOG_NAME.to_string())
            .map(|(_, t)| t)
            .unwrap()
    }

    fn build_catalog(
        bpm: &mut ArcBufferPool,
        txn_manager: &mut ArcTransactionManager,
        lm: &ArcLogManager,
        table: Table,
    ) -> VersionedMap<String, (TupleId, Table)> {
        let mut tables = VersionedMap::new();

        let table_builder = |(id, (_, tuple)): &(TupleId, Entry)| {
            let row = CatalogRow::from_values(&table.get_portable_values(tuple)?);
            let name = row.name.clone();

            let table = Table::fetch(bpm, txn_manager, lm.clone(), row).expect("Fetch failed");

            tables.insert(None, name, (*id, table));

            Ok(())
        };

        table
            .scan(None, table_builder)
            .expect("Catalog scan failed");

        tables.insert(None, CATALOG_NAME.to_string(), ((CATALOG_PAGE, 0), table));

        tables
    }

    #[allow(clippy::new_without_default)]
    pub fn new(bpm: ArcBufferPool, txn_manager: ArcTransactionManager, lm: ArcLogManager) -> Self {
        let mut bpm = bpm.clone();
        let mut txn_manager = txn_manager.clone();

        let schema = CatalogRow::catalog_schema();

        let table = Table::fetch(
            &mut bpm,
            &mut txn_manager,
            lm.clone(),
            CatalogRow {
                name: CATALOG_NAME.to_string(),
                schema,
                first_page: CATALOG_PAGE,
                last_page: CATALOG_PAGE,
                index_page: None,
            },
        )
        .expect("Catalog fetch failed");

        let tables = Self::build_catalog(&mut bpm, &mut txn_manager, &lm, table);

        Catalog {
            tables_map: tables,
            txn_tables: HashMap::new(),
            bpm,
            txn_manager,
            lm,
        }
    }

    pub fn add_table(
        &mut self,
        table_name: &String,
        schema: &Schema,
        ignore_if_exists: bool,
        txn: TxnId,
    ) -> Result<bool> {
        let exists = self.get_table(table_name, Some(txn)).is_some();
        if exists && ignore_if_exists {
            return Ok(false);
        } else if exists {
            bail!(Error::TableExists(table_name.clone()));
        }

        let mut table = Table::new(
            self.bpm.clone(),
            self.txn_manager.clone(),
            self.lm.clone(),
            table_name.to_string(),
            schema,
            txn,
        )?;
        let row = CatalogRow {
            name: table_name.to_string(),
            schema: schema.clone(),
            first_page: table.get_first_page_id(),
            last_page: table.get_last_page_id(),
            index_page: Some(table.get_index_page_id()),
        };

        table.start_txn(txn)?;
        self.table().start_txn(txn)?;
        self.txn_tables
            .entry(txn)
            .or_default()
            .insert(CATALOG_NAME.to_string());

        let tuple_id = self.table().insert(row.to_values()?)?;

        self.tables_map
            .insert(Some(txn), table_name.to_string(), (tuple_id, table));

        Ok(true)
    }

    pub fn get_schema(&self, table_name: &str, txn: Option<TxnId>) -> Option<Schema> {
        self.tables_map
            .get(txn, &table_name.to_string())
            .map(|(_, table)| table.get_schema())
    }

    pub fn get_table_mut(
        &mut self,
        table_name: &str,
        txn: Option<TxnId>,
    ) -> Option<Result<&mut Table>> {
        if table_name == CATALOG_NAME {
            // Catalog table should be accessed through table() method
            // this should limit direct operations on the catalog
            return None;
        }

        match self.tables_map.get_mut(txn, &table_name.to_string()) {
            Some((_, table)) => {
                if let Some(txn_id) = txn {
                    if let Err(e) = table.start_txn(txn_id) {
                        Some(Err(e))
                    } else {
                        self.txn_tables
                            .entry(txn_id)
                            .or_default()
                            .insert(table_name.to_string());
                        Some(Ok(table))
                    }
                } else {
                    Some(Ok(table))
                }
            }
            None => None,
        }
    }

    pub fn get_table(&self, table_name: &str, txn: Option<TxnId>) -> Option<&Table> {
        self.tables_map
            .get(txn, &table_name.to_string())
            .map(|(_, table)| table)
    }

    pub fn commit(&mut self, txn: TxnId) -> Result<()> {
        // tables changed during the txn
        let mut committed_keys = self.txn_tables.remove(&txn).unwrap_or_default();
        // tables made/deleted during the txn
        committed_keys.extend(self.tables_map.commit(txn));

        printdbg!("Txn {} committed tables {:?}", txn, committed_keys);

        committed_keys
            .iter()
            .try_for_each(|key| self.tables_map.get_mut(None, key).unwrap().1.commit_txn())?;

        Ok(())
    }

    pub fn rollback(&mut self, txn: TxnId) -> Result<()> {
        let rolledback_keys = self.txn_tables.remove(&txn).unwrap_or_default();
        self.tables_map.rollback(txn);

        rolledback_keys
            .iter()
            .try_for_each(|key| match self.tables_map.get_mut(None, key) {
                Some((_, table)) => table.rollback_txn(),
                None => Ok(()),
            })?;

        Ok(())
    }

    pub fn truncate_table(&mut self, table_name: &String, txn: TxnId) -> Result<()> {
        let table = match self.get_table_mut(table_name, Some(txn)) {
            Some(table) => table,
            None => bail!(Error::TableNotFound(table_name.clone())),
        };

        let dup = table?.truncate(txn)?;
        let tuple_id = self.tables_map.get_mut(Some(txn), table_name).unwrap().0;
        self.tables_map
            .insert(Some(txn), table_name.to_string(), (tuple_id, dup));

        Ok(())
    }

    pub fn drop_table(
        &mut self,
        table_name: &String,
        ignore_if_exists: bool,
        txn: TxnId,
    ) -> Option<()> {
        let tuple_id = match self.tables_map.get(Some(txn), table_name) {
            Some((tuple_id, table)) => {
                table.drop(txn);
                *tuple_id
            }
            None => return if ignore_if_exists { Some(()) } else { None },
        };

        self.table().start_txn(txn).ok()?;
        self.txn_tables
            .entry(txn)
            .or_default()
            .insert(CATALOG_NAME.to_string());

        self.table().delete(tuple_id).ok()?;

        self.tables_map.remove(Some(txn), table_name);

        Some(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn test_arc_catalog(bpm: ArcBufferPool, txn_manager: ArcTransactionManager) -> ArcCatalog {
        use crate::disk_manager::test_path;
        use crate::wal::manager::LogManagerHandle;

        Arc::new(RwLock::new(Catalog::new(
            bpm,
            txn_manager,
            LogManagerHandle::new(&test_path()),
        )))
    }
}
