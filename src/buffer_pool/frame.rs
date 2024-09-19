use crate::pages::Page;

use super::FrameId;

#[allow(unused)]
pub struct Frame {
    id: FrameId,
    page: Page,
    counter: u16,
    history: i64, // TODO: LRU history, will need a vec for other replacers
}

#[allow(unused)]
impl Frame {
    pub fn new(id: FrameId) -> Self {
        Self {
            id,
            page: Page::new(),
            // TODO: Fix this. Dropping the bpm drops the frames and therefore
            // decreases counter below 0
            counter: 1,
            history: 0,
        }
    }

    pub(super) fn pin(&mut self) {
        self.counter += 1;
    }

    fn unpin(&mut self) {
        self.counter -= 1;
    }

    pub fn get_pin_count(&self) -> u16 {
        self.counter
    }

    pub(super) fn set_page(&mut self, page: Page) {
        self.page = page;
    }

    pub fn get_page(&self) -> &Page {
        &self.page
    }

    pub fn record_access(&mut self, timestamp: i64) {
        self.history = timestamp;
    }
}

impl Drop for Frame {
    fn drop(&mut self) {
        self.unpin();
    }
}
