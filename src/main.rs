mod buffer_pool;
mod catalog;
mod disk_manager;
mod pages;
mod table;
mod tuple;
mod types;

use anyhow::Result;
use catalog::Catalog;
use tuple::{schema::Schema, Tuple};
use types::{Primitive, Types, U16, U8};

fn main() -> Result<()> {
    let mut catalog = Catalog::new()?;

    let schema = Schema::new(vec!["id", "age"], vec![Types::U8, Types::U16]);
    let table = catalog.add_table("users", &schema, true)?;

    let tuple_data = vec![U8(2).to_bytes(), U16(3).to_bytes()];
    let tuple = Tuple::new(tuple_data, &schema);
    table.insert(tuple)?;

    drop(catalog);

    let mut catalog = Catalog::new()?;
    let table = catalog.get_table("users").unwrap();

    table.scan(|entry| println!("{:?}", entry));

    Ok(())
    //
    // let tuple_data = vec![U8(4).to_bytes(), U16(5).to_bytes()];
    // let tuple = Tuple::new(tuple_data, &schema);
    // table.insert(tuple)?;
    //
    // table.scan(|entry| println!("{:?}", entry));
    //
    // table.delete((1, 0))?;
    //
    // table.scan(|entry| println!("{:?}", entry));
    //
    // drop(table);
    //
    // let schema = Schema::new(vec!["id", "age"], vec![Types::U16, Types::U16]);
    //
    // let mut table2 = Table::new(&schema)?;
    //
    // let tuple_data = vec![U16(10000).to_bytes(), U16(50000).to_bytes()];
    // let tuple = Tuple::new(tuple_data, &schema);
    // table2.insert(tuple)?;
    // table2.scan(|entry| println!("{:?}", entry));
    //
    // Ok(())
}
