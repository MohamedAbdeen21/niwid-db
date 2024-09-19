mod frame;
mod replacer;

use crate::disk_manager::{DiskManager, DISK_STORAGE};
use crate::pages::{Page, PageId, INVALID_PAGE};
use anyhow::{anyhow, Result};
use frame::Frame;
use lazy_static::lazy_static;
use parking_lot::FairMutex;
use std::collections::{HashMap, LinkedList};
use std::sync::Arc;

const BUFFER_POOL_SIZE: usize = 10_000;

type FrameId = usize;
pub type BufferPoolManager = Arc<FairMutex<BufferPool>>;

pub struct BufferPool {
    free_frames: LinkedList<FrameId>,
    frames: Box<[Frame]>,
    page_table: HashMap<PageId, FrameId>,
    replacer: Box<dyn replacer::Replacer>,
    disk_manager: DiskManager,
    next_page_id: Page,
}

impl BufferPool {
    pub fn new() -> BufferPoolManager {
        get_buffer_pool()
    }

    #[allow(unused)]
    pub fn inspect(&self) {
        println!("Free Frames: {:?}", self.free_frames);
        println!("Page Table: {:?}", self.page_table);
    }

    pub fn init(size: usize) -> Self {
        // takes a few seconds if bp size is too large, can be parallelized.
        let frames = (0..size).map(|_| Frame::new()).collect::<Vec<_>>();

        let disk_manager = DiskManager::new(DISK_STORAGE);

        // make sure catalog page can also be fetched
        if disk_manager.read_from_file::<Page>(0).is_err() {
            let mut catalog_page = Page::new();
            catalog_page.set_page_id(0);
            disk_manager.write_to_file(&catalog_page).unwrap();
        }

        // buffer pool data that must persist on disk e.g. next page id
        let next_page_id = match disk_manager.read_from_file(PageId::MAX) {
            Ok(page) => page,
            Err(_) => {
                let mut page = Page::new();
                page.set_page_id(PageId::MAX);
                page
            }
        };

        Self {
            free_frames: LinkedList::from_iter(0..size),
            frames: frames.into_boxed_slice(),
            page_table: HashMap::new(),
            replacer: Box::new(replacer::LRU::new(size)),
            disk_manager,
            // page_id 0 is preserved for catalog
            next_page_id,
        }
    }

    pub fn increment_page_id(&mut self) -> Result<PageId> {
        let id = PageId::from_ne_bytes(self.next_page_id.read_bytes(2, 10).try_into().unwrap());
        self.next_page_id
            .write_bytes(2, 10, &(id + 1).to_ne_bytes());
        self.disk_manager.write_to_file(&self.next_page_id)?;
        Ok(id + 1)
    }

    pub fn fetch_frame(&mut self, page_id: PageId) -> Result<&mut Frame> {
        let frame_id = if let Some(frame_id) = self.page_table.get(&page_id) {
            *frame_id
        } else {
            let page = self.disk_manager.read_from_file(page_id)?;
            let frame_id = if !self.free_frames.is_empty() {
                self.free_frames.pop_front().unwrap()
            } else if self.replacer.can_evict() {
                self.evict_frame()
            } else {
                return Err(anyhow!("no free frames to evict"));
            };

            self.frames[frame_id].set_page(page);
            self.page_table.insert(page_id, frame_id);

            frame_id
        };

        let frame = &mut self.frames[frame_id];
        frame.pin();
        self.replacer.record_access(frame_id);

        Ok(frame)
    }

    pub fn new_page(&mut self) -> Result<&mut Frame> {
        let frame_id = if !self.free_frames.is_empty() {
            self.free_frames.pop_front().unwrap()
        } else if self.replacer.can_evict() {
            self.evict_frame()
        } else {
            return Err(anyhow!("no free frames to evict"));
        };

        let page_id = self.increment_page_id()?;

        let frame = &mut self.frames[frame_id];
        frame.pin();
        self.replacer.record_access(frame_id);

        let mut page = Page::new();
        page.set_page_id(page_id);
        self.disk_manager.write_to_file(&page)?;

        frame.set_page(page);
        self.page_table.insert(page_id, frame_id);

        Ok(frame)
    }

    pub fn evict_frame(&mut self) -> FrameId {
        let frame_id = self.replacer.evict();
        let frame = &mut self.frames[frame_id];
        assert!(frame.get_pin_count() == 1);
        let page = frame.writer();

        self.page_table.remove(&page.get_page_id());

        frame_id
    }

    pub fn unpin(&mut self, page_id: &PageId) {
        let frame_id = *self.page_table.get(page_id).unwrap();
        let frame = &mut self.frames[frame_id];
        frame.unpin();

        if frame.get_pin_count() == 1 {
            self.replacer.set_evictable(frame_id, true);
        }

        if frame.reader().is_dirty() {
            self.disk_manager.write_to_file(frame.writer()).unwrap();
        }
    }

    #[cfg(test)]
    pub fn get_pin_count(&self, page_id: &PageId) -> Option<u16> {
        let frame_id = *self.page_table.get(page_id)?;
        Some(self.frames[frame_id].get_pin_count())
    }

    #[allow(dead_code)]
    pub fn shadow_page(&mut self, _page_id: PageId, _page: Page) {
        todo!()
    }

    #[allow(dead_code)]
    pub fn get_page(&self, _page_id: PageId) -> Option<&Page> {
        todo!()
    }
}

fn get_buffer_pool() -> BufferPoolManager {
    lazy_static! {
        static ref BUFFER_POOL: BufferPoolManager =
            Arc::new(FairMutex::new(BufferPool::init(BUFFER_POOL_SIZE)));
    }

    BUFFER_POOL.clone()
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let pages: Vec<&mut Page> = self
            .frames
            .iter_mut()
            .filter(|f| f.reader().get_page_id() != INVALID_PAGE)
            .map(|f| f.writer())
            .collect();

        for page in pages {
            self.disk_manager.write_to_file(page).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pages::table_page::TablePage;

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_dont_evict_pinned() -> Result<()> {
        let mut bpm = BufferPool::init(2);

        let p1: TablePage = bpm.new_page()?.reader().into();

        let _: TablePage = bpm.new_page()?.reader().into();

        assert!(bpm.new_page().is_err());

        bpm.unpin(&p1.get_page_id());

        let _: TablePage = bpm.new_page()?.reader().into();

        assert!(bpm.new_page().is_err());

        Ok(())
    }
}
