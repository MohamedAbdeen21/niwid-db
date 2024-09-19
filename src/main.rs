mod disk_manager;
mod pages;
mod tuple;
mod types;

use anyhow::Result;
use disk_manager::DiskManager;
use pages::table_page::TablePage;
use tuple::{schema::Schema, Tuple};
use types::{Primitive, Types, U16, U8};

fn main() -> Result<()> {
    let path = "my_struct.bin";
    let disk = DiskManager::new("data/");

    let mut table_page = TablePage::new(0);
    insert_write_read(disk, &mut table_page, path)?;

    let iter = table_page.to_iter();

    iter.filter(|(meta, _)| !meta.is_deleted())
        .for_each(|(meta, data)| {
            println!("{:?}, {:?}", data, meta);
        });

    Ok(())
}

fn insert_write_read(disk: DiskManager, table_page: &mut TablePage, path: &str) -> Result<()> {
    let schema = Schema::new(
        vec!["id".to_string(), "age".to_string()],
        vec![Types::U8, Types::U16],
    );

    let tuple_data = vec![U8(2).to_bytes(), U16(3).to_bytes()];
    let tuple = Tuple::new(tuple_data, &schema);
    table_page.insert_tuple(tuple)?;

    let tuple_data = vec![U8(4).to_bytes(), U16(5).to_bytes()];
    let tuple = Tuple::new(tuple_data, &schema);
    table_page.insert_tuple(tuple)?;

    // let tuple = Tuple::new(&[b'a', b'b', b'c', b'd']);
    // table_page.insert_tuple(tuple)?;

    disk.write_to_file(table_page, path)?;

    let mut loaded_data = disk.read_from_file::<TablePage>(path, 0)?;

    // println!("Data after write: {:?}", loaded_data);

    let t1 = loaded_data.read_tuple(0);
    println!("Data after write: {:?}", t1);

    let t2 = loaded_data.read_tuple(1);
    println!("Data after write: {:?}", t2);

    // let t3 = loaded_data.read_tuple(2);
    // println!("Data after write: {:?}", t3);

    loaded_data.delete_tuple(0);

    let t1 = loaded_data.read_tuple(0);
    println!("Data after write: {:?}", t1);

    println!("{}", t1.1.get_value::<U16>("age", &schema)?.0);

    table_page.delete_tuple(1);

    // std::fs::remove_file(path)?;

    Ok(())
}
