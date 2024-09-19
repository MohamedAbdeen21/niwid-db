use std::sync::atomic::{AtomicU16, Ordering};

use crate::pages::Page;

pub struct Frame {
    page: Page,
    counter: AtomicU16,
}

impl Frame {
    pub fn new() -> Self {
        Self {
            page: Page::new(),
            counter: AtomicU16::new(1),
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
    }

    pub fn writer(&mut self) -> &mut Page {
        unsafe { &mut *(&mut self.page as *mut Page) }
    }

    pub fn reader(&self) -> &Page {
        &self.page
    }
}
