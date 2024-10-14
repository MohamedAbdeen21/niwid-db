use std::collections::HashMap;
use std::sync::Arc;

use crate::pages::PageId;
use crate::table::Table;
use crate::tuple::schema::{Field, Schema};
use crate::tuple::{Entry, Tuple, TupleId};
use crate::types::{AsBytes, Types, Value, ValueFactory};
use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use parking_lot::Mutex;

// preserve page_id 1 for catalog, bpm starts assigning at 2
pub const CATALOG_PAGE: PageId = 2;
pub const CATALOG_NAME: &str = "__CATALOG__";

pub type ArcCatalog = Arc<Mutex<Catalog>>;

lazy_static! {
    static ref CATALOG: ArcCatalog = Arc::new(Mutex::new(Catalog::new()));
}

pub struct Catalog {
    pub table: Table,
    tables: HashMap<String, (TupleId, Table)>, // TODO: handle ownership
    #[allow(unused)]
    schema: Schema,       // A catalog is itself a table
}

impl Catalog {
    pub fn get() -> ArcCatalog {
        CATALOG.clone()
    }

    fn table() -> (Table, Schema) {
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

        (table, schema)
    }

    fn build_catalog() -> HashMap<String, (TupleId, Table)> {
        let (table, schema) = Self::table();

        let mut tables = HashMap::new();

        let table_builder = |(id, (_, tuple)): &(TupleId, Entry)| {
            let values = tuple.get_values(&schema)?;
            let name = table.fetch_string(values[0].str_addr()).0;
            let first_page_id = ValueFactory::from_bytes(&Types::UInt, &values[1].to_bytes()).u32();
            let last_page_id = ValueFactory::from_bytes(&Types::UInt, &values[2].to_bytes()).u32();
            let schema = table.fetch_string(values[3].str_addr());
            let schema = Schema::from_bytes(schema.0.to_string().as_bytes());

            let table = Table::fetch(name.clone(), &schema, first_page_id, last_page_id)
                .expect("Fetch failed");

            tables.insert(name, (*id, table));

            Ok(())
        };

        table.scan(table_builder).expect("Catalog scan failed");

        tables
    }

    pub fn new() -> Self {
        let (table, schema) = Self::table();
        let tables = Self::build_catalog();

        Catalog {
            table,
            tables,
            schema,
        }
    }

    pub fn add_table(
        &mut self,
        table_name: &str,
        schema: &Schema,
        ignore_if_exists: bool,
    ) -> Result<()> {
        if self.get_table(table_name).is_some() {
            if ignore_if_exists {
                return Ok(());
            }
            return Err(anyhow!("Table {} already exists", table_name));
        }

        let table = Table::new(table_name.to_string(), schema)?;
        let serialized_schema = String::from_utf8(schema.to_bytes().to_vec())?;
        let tuple_data: Vec<Value> = vec![
            ValueFactory::from_string(&Types::Str, table_name),
            ValueFactory::from_string(&Types::UInt, &table.get_first_page_id().to_string()),
            ValueFactory::from_string(&Types::UInt, &table.get_last_page_id().to_string()),
            ValueFactory::from_string(&Types::Str, &serialized_schema),
        ];
        let tuple = Tuple::new(tuple_data, &self.schema);
        let tuple_id = self.table.insert(tuple)?;

        self.tables
            .insert(table_name.to_string(), (tuple_id, table));

        Ok(())
    }

    pub fn get_schema(&self, table_name: &str) -> Option<Schema> {
        self.tables
            .get(table_name)
            .map(|(_, table)| table.get_schema())
    }

    pub fn get_table(&mut self, table_name: &str) -> Option<&mut Table> {
        self.tables.get_mut(table_name).map(|(_, table)| table)
    }

    #[allow(unused)]
    pub fn drop_table(&mut self, table_name: &str) -> Option<()> {
        let tuple_id = self.tables.get(table_name)?.0;

        self.table.delete(tuple_id).ok()?;

        self.tables.remove(table_name);

        Some(())
    }

    pub fn update_last_page(&mut self, table_name: String) -> Result<()> {
        let (tuple_id, table) = self.tables.get_mut(&table_name).unwrap();

        let schema = table.get_schema();

        let serialized_schema = String::from_utf8(schema.to_bytes().to_vec())?;
        let tuple_data: Vec<Value> = vec![
            ValueFactory::from_string(&Types::Str, &table_name),
            ValueFactory::from_string(&Types::UInt, &table.get_first_page_id().to_string()),
            ValueFactory::from_string(&Types::UInt, &table.get_last_page_id().to_string()),
            ValueFactory::from_string(&Types::Str, &serialized_schema),
        ];
        let tuple = Tuple::new(tuple_data, &self.schema);
        let new_tuple_id = self.table.update(Some(*tuple_id), tuple)?;

        self.tables.get_mut(&table_name).unwrap().0 = new_tuple_id;

        Ok(())
    }
}
