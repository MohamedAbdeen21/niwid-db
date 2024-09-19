use crate::buffer_pool::{BufferPool, BufferPoolManager};
use crate::pages::table_page::TablePage;
use crate::pages::{PageId, INVALID_PAGE};
use crate::tuple::Entry;

use super::Table;

// TODO: try to iterate over pages not tuples
pub struct TableIterator {
    page: TablePage,
    current: usize,
    next_page: PageId,
    buffer_pool: BufferPoolManager,
}

#[allow(unused)]
impl TableIterator {
    pub fn new(table: Table) -> Self {
        let page: TablePage = unsafe { *(table.first_page) };
        TableIterator {
            current: 0,
            next_page: page.header().get_next_page(),
            buffer_pool: BufferPool::new(),
            page,
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
                .write()
                .unwrap()
                .fetch_page(self.next_page)
                .ok()? // TODO: idk
                .read()
                .unwrap()
                .get_page_read()
                .into();

            self.current = 0;
            self.next_page = self.page.header().get_next_page();
            return self.next();
        }

        let entry = self.page.read_tuple(self.current);
        self.current += 1;
        Some(entry)
    }
}
