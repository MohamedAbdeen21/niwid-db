mod disk_manager;
mod pages;
mod tuple;

use anyhow::Result;
use disk_manager::DiskManager;
use pages::table_page::TablePage;
use tuple::Tuple;

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
    let tuple = Tuple::new(&[2, 3]);
    table_page.insert_tuple(tuple)?;

    let tuple = Tuple::new(&[4, 5]);
    table_page.insert_tuple(tuple)?;

    let tuple = Tuple::new(&[b'a', b'b', b'c', b'd']);
    table_page.insert_tuple(tuple)?;

    disk.write_to_file(table_page, path)?;

    let mut loaded_data = disk.read_from_file::<TablePage>(path)?;

    let t1 = loaded_data.read_tuple(0);
    println!("Data after write: {:?}", t1);

    let t2 = loaded_data.read_tuple(1);
    println!("Data after write: {:?}", t2);

    let t3 = loaded_data.read_tuple(2);
    println!("Data after write: {:?}", t3);

    loaded_data.delete_tuple(0);

    let t1 = loaded_data.read_tuple(0);
    println!("Data after write: {:?}", t1);

    table_page.delete_tuple(1);

    // std::fs::remove_file(path)?;

    Ok(())
}
