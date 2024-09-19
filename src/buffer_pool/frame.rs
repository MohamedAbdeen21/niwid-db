use crate::pages::Page;

use super::FrameId;

#[allow(unused)]
pub struct Frame {
    id: FrameId,
    page: Page,
    counter: u16,
}

#[allow(unused)]
impl Frame {
    pub fn new(id: FrameId) -> Self {
        Self {
            id,
            page: Page::new(),
            counter: 1,
        }
    }

    pub fn get_frame_id(&self) -> FrameId {
        self.id
    }

    pub(super) fn pin(&mut self) {
        self.counter += 1;
    }

    pub(super) fn unpin(&mut self) {
        self.counter -= 1;
    }

    pub(super) fn get_pin_count(&self) -> u16 {
        self.counter
    }

    pub(super) fn set_page(&mut self, page: Page) {
        self.page = page;
    }

    pub fn get_page_write(&mut self) -> &mut Page {
        unsafe { &mut *(&mut self.page as *mut Page) }
    }

    pub fn get_page_read(&self) -> &Page {
        &self.page
    }
}
