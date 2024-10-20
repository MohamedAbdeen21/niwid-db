mod versioned_map;

use crate::pages::PageId;
use crate::printdbg;
use crate::table::Table;
use crate::tuple::schema::{Field, Schema};
use crate::tuple::{Entry, Tuple, TupleId};
use crate::txn_manager::TxnId;
use crate::types::{AsBytes, Types, Value, ValueFactory};
use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use versioned_map::VersionedMap;

// preserve page_id 1 for catalog, bpm starts assigning at 2
pub const CATALOG_PAGE: PageId = 2;
pub const CATALOG_NAME: &str = "__CATALOG__";

pub type ArcCatalog = Arc<Mutex<Catalog>>;

lazy_static! {
    static ref CATALOG: ArcCatalog = Arc::new(Mutex::new(Catalog::new()));
}

// FIXME: Catalog is shared between contexts.
// For other tables, all changes happen inside BPM, who manages the txns
// but for catalog, we have the tables hashmap, so the catalog has to manage
// txns on its own
pub struct Catalog {
    pub tables: VersionedMap<String, (TupleId, Table)>, // TODO: handle ownership
    schema: Schema,                                     // A catalog is itself a table
    txn_tables: HashMap<TxnId, HashSet<String>>,
}

impl Catalog {
    pub fn get() -> ArcCatalog {
        CATALOG.clone()
    }

    /// Catalog is a table itself, this gives access to the underlying table
    pub fn table(&mut self) -> &mut Table {
        // No need to track version for catalog, catalog always has the same
        // tuple_id and can never be deleted (TODO:)
        self.tables
            .get_mut(None, &CATALOG_NAME.to_string())
            .map(|(_, t)| t)
            .unwrap()
    }

    fn build_catalog(table: Table, schema: &Schema) -> VersionedMap<String, (TupleId, Table)> {
        let mut tables = VersionedMap::new();

        let table_builder = |(id, (_, tuple)): &(TupleId, Entry)| {
            let values = tuple.get_values(schema)?;
            let name = table.fetch_string(values[0].str_addr()).0;
            let first_page_id = ValueFactory::from_bytes(&Types::UInt, &values[1].to_bytes()).u32();
            let last_page_id = ValueFactory::from_bytes(&Types::UInt, &values[2].to_bytes()).u32();
            let schema = table.fetch_string(values[3].str_addr());
            let schema = Schema::from_bytes(schema.0.to_string().as_bytes());

            let table = Table::fetch(name.clone(), &schema, first_page_id, last_page_id)
                .expect("Fetch failed");

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
    pub fn new() -> Self {
        let schema = Schema::new(vec![
            Field::new("table_name", Types::Str, false),
            Field::new("first_page", Types::UInt, false),
            Field::new("last_page", Types::UInt, false),
            Field::new("schema", Types::Str, false),
        ]);

        let table = Table::fetch(
            CATALOG_NAME.to_string(),
            &schema,
            CATALOG_PAGE,
            CATALOG_PAGE,
        )
        .expect("Catalog fetch failed");

        let tables = Self::build_catalog(table, &schema);

        Catalog {
            tables,
            schema,
            txn_tables: HashMap::new(),
        }
    }

    pub fn add_table(
        &mut self,
        table_name: String,
        schema: &Schema,
        ignore_if_exists: bool,
        txn: Option<TxnId>,
    ) -> Result<()> {
        let exists = self.get_table_mut(&table_name, txn).is_some();
        if exists && ignore_if_exists {
            return Ok(());
        } else if exists {
            return Err(anyhow!("Table {} already exists", table_name));
        }

        let mut table = Table::new(table_name.to_string(), schema)?;
        let serialized_schema = String::from_utf8(schema.to_bytes().to_vec())?;
        let tuple_data: Vec<Value> = vec![
            ValueFactory::from_string(&Types::Str, &table_name),
            ValueFactory::from_string(&Types::UInt, table.get_first_page_id().to_string()),
            ValueFactory::from_string(&Types::UInt, table.get_last_page_id().to_string()),
            ValueFactory::from_string(&Types::Str, &serialized_schema),
        ];
        let tuple = Tuple::new(tuple_data, &self.schema);
        let tuple_id = self.table().insert(tuple)?;

        if let Some(txn) = txn {
            table.start_txn(txn)?;
        }

        self.tables
            .insert(txn, table_name.to_string(), (tuple_id, table));

        Ok(())
    }

    pub fn get_schema(&self, table_name: &str, txn: Option<TxnId>) -> Option<Schema> {
        self.tables
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

        match self.tables.get_mut(txn, &table_name.to_string()) {
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

    pub fn get_table(&mut self, table_name: &str, txn: Option<TxnId>) -> Option<&Table> {
        self.tables
            .get(txn, &table_name.to_string())
            .map(|(_, table)| table)
    }

    pub fn commit(&mut self, txn: TxnId) -> Result<()> {
        let mut committed_keys = self.txn_tables.remove(&txn).unwrap_or_default();
        committed_keys.extend(self.tables.commit(txn));

        printdbg!("Txn {} Committed keys {:?}", txn, committed_keys);

        committed_keys
            .iter()
            .try_for_each(|key| self.tables.get_mut(None, key).unwrap().1.commit_txn())?;

        Ok(())
    }

    pub fn truncate_table(&mut self, table_name: String, txn: Option<TxnId>) -> Result<()> {
        let table = match self.get_table_mut(&table_name, txn) {
            Some(table) => table,
            None => return Err(anyhow!("Table {} doesn't exist", table_name)),
        };

        let dup = table?.truncate()?;
        let tuple_id = self.tables.get_mut(txn, &table_name).unwrap().0;
        self.tables
            .insert(txn, table_name.to_string(), (tuple_id, dup));

        Ok(())
    }

    pub fn drop_table(
        &mut self,
        table_name: String,
        ignore_if_exists: bool,
        txn: Option<TxnId>,
    ) -> Option<()> {
        let tuple_id = match self.tables.get(txn, &table_name) {
            Some((tuple_id, _)) => *tuple_id,
            None => return if ignore_if_exists { Some(()) } else { None },
        };

        self.table().delete(tuple_id).ok()?;

        self.tables.remove(txn, &table_name);

        Some(())
    }
}
