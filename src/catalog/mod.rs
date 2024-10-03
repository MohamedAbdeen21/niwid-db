use crate::pages::PageId;
use crate::table::Table;
use crate::tuple::schema::{Field, Schema};
use crate::tuple::{Entry, Tuple, TupleId};
use crate::types::{AsBytes, Str, StrAddr, Types, I64};
use anyhow::{anyhow, Result};

// preserve page_id 1 for catalog, bpm starts assigning at 2
pub const CATALOG_PAGE: PageId = 1;
pub const CATALOG_NAME: &str = "__CATALOG__";

pub struct Catalog {
    pub table: Table,
    tables: Vec<(TupleId, Table)>, // TODO: handle ownership
    #[allow(unused)]
    schema: Schema, // A catalog is itself a table
}

impl Catalog {
    pub fn new() -> Result<Self> {
        let schema = Schema::new(vec![
            Field::new("table_name", Types::Str, false),
            Field::new("first_page", Types::I64, false),
            Field::new("last_page", Types::I64, false),
            Field::new("schema", Types::Str, false),
        ]);

        let table = Table::fetch(
            CATALOG_NAME.to_string(),
            &schema,
            CATALOG_PAGE,
            CATALOG_PAGE,
        )?;

        let mut tables = vec![];
        let table_builder = |(id, (_, tuple)): &(TupleId, Entry)| {
            let values = tuple.get_values(&schema)?;
            let name = table.fetch_string(&*values[0]);
            let first_page_id = I64::from_bytes(&values[1].to_bytes()).0;
            let last_page_id = I64::from_bytes(&values[2].to_bytes()).0;
            let schema = table.fetch_string(&*values[3]);
            let schema = Schema::from_bytes(schema.0.to_string().as_bytes());

            tables.push((
                *id,
                Table::fetch(name.0, &schema, first_page_id, last_page_id).expect("Fetch failed"),
            ));

            Ok(())
        };

        table.scan(table_builder)?;

        Ok(Catalog {
            table,
            tables,
            schema,
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

        let table = Table::new(table_name.to_string(), schema)?;
        let serialized_schema = String::from_utf8(schema.to_bytes().to_vec())?;
        let tuple_data: Vec<Box<dyn AsBytes>> = vec![
            Str(table_name.to_string()).into(),
            I64(table.get_first_page_id()).into(),
            I64(table.get_last_page_id()).into(),
            Str(serialized_schema).into(),
        ];
        let tuple = Tuple::new(tuple_data, schema);
        let tuple_id = self.table.insert(tuple)?;

        self.tables.push((tuple_id, table));

        Ok(&mut self.tables.last_mut().unwrap().1)
    }

    pub fn get_table(&mut self, table_name: &str) -> Option<&mut Table> {
        self.tables
            .iter_mut()
            .find(|(_, table)| table.get_name() == table_name)
            .map(|(_, table)| table)
    }

    #[allow(unused)]
    pub fn drop_table(&mut self, table_name: &str) -> Option<()> {
        let mut tuple_id = None;
        self.table
            .scan(|(id, (_, tuple))| {
                let name_bytes = tuple
                    .get_value_of::<StrAddr>("table_name", &self.schema)?
                    .unwrap();
                let name = self.table.fetch_string(&name_bytes).0;

                if name == table_name {
                    tuple_id = Some(*id);
                }

                Ok(())
            })
            .ok()?;

        self.table.delete(tuple_id?).ok()?;

        let index = self
            .tables
            .iter()
            .position(|(_, table)| table.get_name() == table_name)?;

        self.tables.remove(index);

        Some(())
    }
}

impl Drop for Catalog {
    // TODO: update each table's last_page
    fn drop(&mut self) {}
}
