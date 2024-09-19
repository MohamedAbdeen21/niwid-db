use std::sync::RwLock;

use crate::buffer_pool::{get_buffer_pool, BufferPool};
use crate::pages::table_page::TablePage;
use crate::pages::INVALID_PAGE;
use crate::tuple::Entry;

use super::Table;

// TODO: try to iterate over pages not tuples
pub struct TableIterator {
    page: TablePage,
    current: usize,
    next_page: i32,
    buffer_pool: &'static RwLock<BufferPool>,
}

#[allow(unused)]
impl TableIterator {
    pub fn new(table: Table) -> Self {
        let page = table.first_page.clone();
        TableIterator {
            page,
            current: 0,
            next_page: page.header().get_next_page(),
            buffer_pool: get_buffer_pool(),
        }
    }
}

impl Iterator for TableIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        if self.current >= self.page.header().get_num_tuples() && self.next_page == INVALID_PAGE {
            return None;
        }

        if self.current >= self.page.header().get_num_tuples() {
            self.page = self
                .buffer_pool
                .read()
                .unwrap()
                .get_frame(self.next_page)
                .read()
                .unwrap()
                .get_page()
                .into();

            println!("{}", "we out");
            self.current = 0;
            self.next_page = self.page.header().get_next_page();
            return self.next();
        }

        let entry = self.page.read_tuple(self.current);
        self.current += 1;
        Some(entry)
    }
}
