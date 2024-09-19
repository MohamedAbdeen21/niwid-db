mod buffer_pool;
mod catalog;
mod disk_manager;
mod pages;
mod table;
mod tuple;
mod types;

use anyhow::Result;
use table::Table;
use tuple::{schema::Schema, Tuple};
use types::{Primitive, Types, U16, U8};

fn main() -> Result<()> {
    let mut table = Table::new()?;

    let schema = Schema::new(
        vec!["id".to_string(), "age".to_string()],
        vec![Types::U8, Types::U16],
    );

    let tuple_data = vec![U8(2).to_bytes(), U16(50000).to_bytes()];
    let tuple = Tuple::new(tuple_data, &schema);
    table.insert(tuple)?;

    let tuple_data = vec![U8(4).to_bytes(), U16(5).to_bytes()];
    let tuple = Tuple::new(tuple_data, &schema);
    table.insert(tuple)?;

    table.scan(|entry| println!("{:?}", entry));

    table.delete((1, 0))?;

    table.scan(|entry| println!("{:?}", entry));

    drop(table);

    let mut table2 = Table::new()?;
    let schema = Schema::new(
        vec!["id".to_string(), "age".to_string()],
        vec![Types::U16, Types::U16],
    );

    let tuple_data = vec![U16(10000).to_bytes(), U16(50000).to_bytes()];
    let tuple = Tuple::new(tuple_data, &schema);
    table2.insert(tuple)?;
    table2.scan(|entry| println!("{:?}", entry));

    Ok(())
}

// fn insert_write_read(table: &mut Table) -> Result<()> {
// println!("Data after write: {:?}", table_page);
//
// bp.write_to_file(table_page, path)?;
//
// let mut loaded_data = disk.read_from_file::<TablePage>(path, 0)?;
//
// println!("Data after write: {:?}", loaded_data);
//
// let t1 = loaded_data.read_tuple(0);
// println!("Data after write: {:?}", t1);
//
// let t2 = loaded_data.read_tuple(1);
// println!("Data after write: {:?}", t2);
//
// // let t3 = loaded_data.read_tuple(2);
// // println!("Data after write: {:?}", t3);
//
// loaded_data.delete_tuple(0);
//
// let t1 = loaded_data.read_tuple(0);
// println!("Data after write: {:?}", t1);
//
// println!("{}", t1.1.get_value::<U16>("age", &schema)?.0);
//
// table.delete(1);
//
// // std::fs::remove_file(path)?;

//     Ok(())
// }
