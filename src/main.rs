mod pages;
mod disk_manager;

use disk_manager::{write_to_file, read_from_file};
use pages::Page;
use pages::table_page::TablePage;
use anyhow::Result;

fn main() -> Result<()> {
    let path = "my_struct.bin";

    let my_data = Page::new();
    let mut table_page: TablePage = my_data.into();
    table_page.header_mut().add_tuple();


    write_to_file(&table_page, path)?;

    let mut loaded_data = read_from_file::<TablePage>(path)?;

    let header = loaded_data.header_mut();
    println!("Header after write: {:?}", header);

    header.add_tuple();
    let header = loaded_data.header();
    println!("Header after direct modification: {:?}", header);

    //remove the file
    std::fs::remove_file(path)?;

    Ok(())
}
