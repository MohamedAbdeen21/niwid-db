mod disk_manager;
mod pages;
mod tuple;

use anyhow::Result;
use disk_manager::DiskManager;
use pages::table_page::TablePage;

fn main() -> Result<()> {
    let path = "my_struct.bin";
    let disk = DiskManager::new("data/");

    let mut table_page = TablePage::new();
    table_page.insert_tuple(&[2, 3])?;
    table_page.insert_tuple(&[4, 5])?;
    // string as a byte array
    table_page.insert_tuple(&[b'a', b'b', b'c', b'd'])?;

    disk.write_to_file(&table_page, path)?;

    let mut loaded_data = disk.read_from_file::<TablePage>(path)?;

    println!("{:?}", loaded_data);

    let t1 = loaded_data.read_tuple(0);
    println!("Data after write: {:?}", t1);

    let t2 = loaded_data.read_tuple(1);
    println!("Data after write: {:?}", t2);

    let t3 = loaded_data.read_tuple(2);
    println!("Data after write: {:?}", t3);

    loaded_data.delete_tuple(0);

    let t1 = loaded_data.read_tuple(0);
    println!("Data after write: {:?}", t1);

    // std::fs::remove_file(path)?;

    Ok(())
}
