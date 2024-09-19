use crate::buffer_pool::BufferPoolManager;
use crate::pages::PageId;
use crate::table::Table;
use crate::tuple::schema::Schema;

// preserve page_id 0 for catalog, bpm starts assigning at 1
#[allow(dead_code)]
const CATALOG_PAGE: PageId = 0;

#[allow(dead_code)]
pub struct Catalog {
    first_page: Table,  // first page of the catalog
    tables: Vec<Table>, // TODO: handle ownership
    schema: Schema,     // A catalog is itself a table
    bpm: BufferPoolManager,
}
