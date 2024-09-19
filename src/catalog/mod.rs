use crate::buffer_pool::{BufferPool, BufferPoolManager};
use crate::pages::table_page::TupleId;
use crate::pages::PageId;
use crate::table::Table;
use crate::tuple::schema::Schema;
use crate::tuple::{Entry, Tuple};
use crate::types::{Primitive, Str, Types, I128, I64};
use anyhow::{anyhow, Result};

// preserve page_id 0 for catalog, bpm starts assigning at 1
#[allow(unused)]
const CATALOG_PAGE: PageId = 0;
const CATALOG_NAME: &str = "__CATALOG__";

#[allow(unused)]
pub struct Catalog {
    table: Table,       // first page of the catalog
    tables: Vec<Table>, // TODO: handle ownership
    schema: Schema,     // A catalog is itself a table
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

        let mut tables: Vec<Table> = vec![];
        let mut table_builder = |(_, (_, tuple)): &(TupleId, Entry)| {
            let name_bytes = tuple.get_value::<I128>("table_name", &schema).unwrap();
            let name = table.fetch_string(&name_bytes.to_bytes());
            let first_page_id = tuple.get_value::<I64>("first_page", &schema).unwrap().0 as PageId;
            let last_page_id = tuple.get_value::<I64>("last_page", &schema).unwrap().0 as PageId;

            tables.push(
                Table::fetch(name.0, &schema, first_page_id, last_page_id).expect("Fetch failed"),
            )
        };

        table.scan(table_builder);

        Ok(Catalog {
            table,
            tables,
            schema,
            bpm,
        })
    }

    pub fn add_table(
        &mut self,
        table_name: &str,
        schema: &Schema,
        ignore_if_exists: bool,
    ) -> Result<&mut Table> {
        if self.get_table(table_name).is_some() {
            if ignore_if_exists {
                return Ok(self.get_table(table_name).unwrap());
            }
            return Err(anyhow!("Table {} already exists", table_name));
        }

        let mut table = Table::new(table_name.to_string(), schema)?;
        let tuple_data = vec![
            Str(table_name.to_string()).to_bytes(),
            I64(table.get_first_page_id()).to_bytes(),
            I64(table.get_last_page_id()).to_bytes(),
            // Str(schema.to_string()).to_bytes(), // TODO: Handle schema serialization
        ];
        let tuple = Tuple::new(tuple_data, &self.schema);
        self.table.insert(tuple)?;

        self.tables.push(table);

        Ok(self.tables.last_mut().unwrap())
    }

    pub fn get_table(&mut self, table_name: &str) -> Option<&mut Table> {
        self.tables
            .iter_mut()
            .find(|table| table.get_name() == table_name)
    }

    pub fn drop_table(&mut self, table_name: &str) -> Option<()> {
        let mut tuple_id = None;
        self.table.scan(|(id, (_, tuple))| {
            let name_bytes = tuple.get_value::<I128>("table_name", &self.schema).unwrap();
            let name = self.table.fetch_string(&name_bytes.to_bytes()).0;
            if name == table_name {
                tuple_id = Some(*id);
            }
        });

        self.table.delete(tuple_id?).ok()?;

        let index = self
            .tables
            .iter()
            .enumerate()
            .inspect(|(i, table)| println!("{i}: {:?}", table.get_name()))
            .position(|(_, table)| table.get_name() == table_name)?;

        println!("index: {index}");
        self.tables.remove(index);

        self.tables
            .iter()
            .for_each(|table| println!("{:?}", table.get_name()));

        Some(())
    }
}

// impl Drop for Catalog {
//     fn drop(&mut self) {
//         self.bpm.unpin(&self.table.get());
//     }
// }
