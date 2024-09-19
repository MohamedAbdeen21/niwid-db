use std::sync::RwLock;

use crate::buffer_pool::{BufferPool, BufferPoolManager};
use crate::pages::table_page::{TupleExt, TupleId};
use crate::pages::PageId;
use crate::table::Table;
use crate::tuple::schema::Schema;
use crate::tuple::{Entry, Tuple};
use crate::types::{Primitive, Str, Types, I128, I64};
use anyhow::Result;

// preserve page_id 0 for catalog, bpm starts assigning at 1
#[allow(unused)]
const CATALOG_PAGE: PageId = 0;
const CATALOG_NAME: &str = "__CATALOG__";

#[allow(unused)]
pub struct Catalog {
    table: Table,               // first page of the catalog
    tables: Vec<RwLock<Table>>, // TODO: handle ownership
    schema: Schema,             // A catalog is itself a table
    bpm: BufferPoolManager,
}

#[allow(unused)]
impl Catalog {
    pub fn new() -> Result<Self> {
        let bpm = BufferPool::new();
        let schema = Schema::new(
            vec!["table_name", "first_page", "last_page", "schema"],
            vec![Types::Str, Types::I64, Types::I64, Types::Str],
        );

        let table = Table::fetch(
            CATALOG_NAME.to_string(),
            &schema,
            CATALOG_PAGE,
            CATALOG_PAGE,
        )?;

        let mut tables: Vec<RwLock<Table>> = vec![];
        let mut table_builder = |(_, tuple): &Entry| {
            let name_bytes = tuple.get_value::<I128>("table_name", &schema).unwrap();
            let str_id = TupleId::from_bytes(&name_bytes.to_bytes());
            let name = table.fetch_string(str_id);
            let first_page_id = tuple.get_value::<I64>("first_page", &schema).unwrap().0 as PageId;
            let last_page_id = tuple.get_value::<I64>("last_page", &schema).unwrap().0 as PageId;

            tables.push(RwLock::new(
                Table::fetch(name.0, &schema, first_page_id, last_page_id).expect("Fetch failed"),
            ))
        };

        table.scan(table_builder);

        Ok(Catalog {
            table,
            tables,
            schema,
            bpm,
        })
    }

    pub fn add_table(&mut self, table_name: &str, schema: &Schema) -> Result<&RwLock<Table>> {
        let mut table = Table::new(table_name.to_string(), &schema)?;
        let tuple_data = vec![
            Str(table_name.to_string()).to_bytes(),
            I64(table.get_first_page_id() as i64).to_bytes(),
            I64(table.get_last_page_id() as i64).to_bytes(),
            // Str(schema.to_string()).to_bytes(), // TODO: Handle schema serialization
        ];
        let tuple = Tuple::new(tuple_data, &self.schema);
        println!(
            "Inserting tuple {:?}: {:?}: {:?}",
            tuple,
            table.get_first_page_id(),
            table.get_last_page_id()
        );
        self.table.insert(tuple)?;

        self.tables.push(RwLock::new(table));

        Ok(self.tables.last().unwrap())
    }

    pub fn get_table(&self, table_name: &str) -> Option<&RwLock<Table>> {
        self.tables
            .iter()
            .find(|table| table.read().unwrap().get_name() == table_name)
    }
}
