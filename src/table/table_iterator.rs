use crate::buffer_pool::BufferPoolManager;
use crate::pages::table_page::TablePage;
use crate::pages::{PageId, INVALID_PAGE};
use crate::tuple::schema::Schema;
use crate::tuple::{Entry, TupleId};

use super::Table;

// TODO: try to iterate over pages not tuples
pub struct TableIterator {
    page: *const TablePage,
    current_slot: usize,
    next_page: PageId,
    bpm: BufferPoolManager,
    #[allow(unused)]
    schema: Schema,
    num_tuples: usize,
}

#[allow(unused)]
impl TableIterator {
    pub fn new(table: &Table) -> Self {
        let bpm = table.bpm.clone();
        let page: *const TablePage = bpm
            .write()
            .unwrap()
            .fetch_frame(unsafe { table.first_page.as_ref().unwrap() }.get_page_id())
            .unwrap()
            .get_page_read()
            .into();

        let header = unsafe { page.as_ref().unwrap() }.header();

        TableIterator {
            current_slot: 0,
            next_page: header.get_next_page(),
            num_tuples: header.get_num_tuples(),
            page,
            bpm,
            schema: table.schema.clone(),
        }
    }
}

impl Iterator for TableIterator {
    type Item = (TupleId, Entry);

    fn next(&mut self) -> Option<Self::Item> {
        // current page is done, drop it
        if self.current_slot >= self.num_tuples {
            let page_id = unsafe { self.page.as_ref().unwrap() }.get_page_id();
            self.bpm.write().unwrap().unpin(&page_id);
        }

        if self.current_slot >= self.num_tuples && self.next_page == INVALID_PAGE {
            return None;
        }

        if self.current_slot >= self.num_tuples {
            self.page = self
                .bpm
                .write()
                .unwrap()
                .fetch_frame(self.next_page)
                .ok()? // TODO: idk
                .get_page_read()
                .into();

            self.current_slot = 0;
            let header = unsafe { self.page.as_ref().unwrap() }.header();
            self.next_page = header.get_next_page();
            self.num_tuples = header.get_num_tuples();
            return self.next();
        }

        let entry = unsafe { self.page.as_ref().unwrap() }.read_tuple(self.current_slot);
        self.current_slot += 1;

        if entry.0.is_deleted() {
            return self.next();
        }

        let page_id = unsafe { self.page.as_ref().unwrap() }.get_page_id();

        Some(((page_id, self.current_slot - 1), entry))
    }
}
