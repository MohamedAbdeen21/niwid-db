use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};

use crate::pages::{latch::Latch, Page, PageId};

pub struct Frame {
    page: Page,
    counter: AtomicU16,
    latch: Arc<Latch>,
}

impl Default for Frame {
    fn default() -> Self {
        Self::new()
    }
}

impl Frame {
    pub fn new() -> Self {
        Self {
            page: Page::new(),
            counter: AtomicU16::new(0),
            latch: Arc::new(Latch::new()),
        }
    }

    pub(super) fn pin(&mut self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn unpin(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }

    pub(super) fn get_pin_count(&self) -> u16 {
        self.counter.load(Ordering::Relaxed)
    }

    pub(super) fn set_page(&mut self, page: Page) {
        self.page = page;
        self.page.set_latch(self.latch.clone());
    }

    pub(super) fn get_page_id(&self) -> PageId {
        self.page.get_page_id()
    }

    pub(super) fn move_page(&mut self, frame: Self) {
        assert!(self.get_latch().is_locked());
        self.set_page(frame.page);
    }

    #[allow(unused)]
    pub fn get_latch(&self) -> &Arc<Latch> {
        &self.latch
    }

    pub fn writer(&mut self) -> &mut Page {
        &mut self.page
    }

    pub fn reader(&self) -> &Page {
        &self.page
    }
}
