mod disk_manager;
mod pages;

use anyhow::Result;
use disk_manager::{read_from_file, write_to_file};
use pages::table_page::TablePage;

fn main() -> Result<()> {
    let path = "my_struct.bin";

    let mut table_page = TablePage::new();
    table_page.insert_tuple(&[2, 3])?;
    table_page.insert_tuple(&[4, 5])?;

    write_to_file(&table_page, path)?;

    let loaded_data = read_from_file::<TablePage>(path)?;

    println!("{:?}", loaded_data);

    let header = loaded_data.read_tuple(0);
    println!("Data after write: {:?}", header);

    let header = loaded_data.read_tuple(1);
    println!("Data after write: {:?}", header);

    std::fs::remove_file(path)?;

    Ok(())
}
