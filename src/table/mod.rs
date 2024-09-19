use crate::pages::table_page::TablePage;

pub mod table_iterator;

#[allow(unused)]
pub struct Table {
    pub first_page: TablePage,
    pub last_page: TablePage,
}

impl Table {
    pub fn new(first_page: TablePage, last_page: TablePage) -> Self {
        Self {
            first_page,
            last_page,
        }
    }

    pub fn to_iter(self) -> table_iterator::TableIterator {
        table_iterator::TableIterator::new(self)
    }
}
