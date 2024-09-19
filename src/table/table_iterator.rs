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
    pub fn new(table: &Table) -> Self {
        let bpm = BufferPool::new();
        let page: TablePage = bpm
            .write()
            .unwrap()
            .fetch_frame(unsafe { table.first_page.as_ref().unwrap() }.get_page_id())
            .unwrap()
            .read()
            .unwrap()
            .get_page_read()
            .into();

        TableIterator {
            current: 0,
            next_page: page.header().get_next_page(),
            buffer_pool: bpm,
            page,
        }
    }
}

impl Iterator for TableIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        // current page is done, drop it
        if self.current >= self.page.header().get_num_tuples() {
            self.buffer_pool
                .write()
                .unwrap()
                .unpin(&self.page.get_page_id());
        }

        if self.current >= self.page.header().get_num_tuples() && self.next_page == INVALID_PAGE {
            return None;
        }

        if self.current >= self.page.header().get_num_tuples() {
            self.page = self
                .buffer_pool
                .write()
                .unwrap()
                .fetch_frame(self.next_page)
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
