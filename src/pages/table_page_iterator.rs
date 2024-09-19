use crate::pages::INVALID_PAGE;
use crate::tuple::Entry;

use super::table_page::TablePage;

pub struct TablePageIterator {
    page: TablePage,
    current: usize,
    next_page: i32,
}

#[allow(unused)]
impl TablePageIterator {
    pub fn new(page: TablePage) -> Self {
        TablePageIterator {
            page,
            current: 0,
            next_page: page.header().get_next_page(),
        }
    }
}

impl Iterator for TablePageIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        if self.current >= self.page.header().get_num_tuples() && self.next_page == INVALID_PAGE {
            return None;
        }

        if self.current >= self.page.header().get_num_tuples() {
            // TODO: Ask for next page
            return None;
        }

        let entry = self.page.read_tuple(self.current);
        self.current += 1;
        Some(entry)
    }
}
