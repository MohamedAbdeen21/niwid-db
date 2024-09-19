mod frame;
mod replacer;

use crate::disk_manager::DiskManager;
use crate::pages::{Page, PageId};
use anyhow::{anyhow, Result};
use frame::Frame;
use lazy_static::lazy_static;
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, RwLock};

const BUFFER_POOL_SIZE: usize = 10_000;

type FrameId = usize;
pub type BufferPoolManager = Arc<RwLock<BufferPool>>;

pub struct BufferPool {
    free_frames: LinkedList<FrameId>,
    frames: Box<[Frame]>,
    page_table: HashMap<PageId, FrameId>,
    replacer: Box<dyn replacer::Replacer>,
    disk_manager: DiskManager,
    next_page_id: PageId,
}

impl BufferPool {
    pub fn new() -> BufferPoolManager {
        get_buffer_pool()
    }

    #[allow(unused)]
    pub fn inspect(&self) -> () {
        println!("Free Frames: {:?}", self.free_frames);
        println!("Page Table: {:?}", self.page_table);
    }

    pub fn init(size: usize) -> Self {
        let mut frames = Vec::with_capacity(size);
        for i in 0..size {
            frames.push(Frame::new(i));
        }

        Self {
            free_frames: LinkedList::from_iter(0..size),
            frames: frames.into_boxed_slice(),
            page_table: HashMap::new(),
            replacer: Box::new(replacer::LRU::new(size)),
            disk_manager: DiskManager::new("data/test.db"),
            // page_id 0 is preserved for catalog
            next_page_id: 1,
        }
    }

    pub fn fetch_frame(&mut self, page_id: PageId) -> Result<&mut Frame> {
        println!("fetching frame {}", page_id);
        let frame_id = if let Some(frame_id) = self.page_table.get(&page_id) {
            *frame_id
        } else {
            println!("reading frame {}", page_id);
            let page = self.disk_manager.read_from_file(page_id).unwrap();
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

        let frame = &mut self.frames[frame_id];
        frame.pin();
        self.replacer.record_access(frame_id);

        let page_id = self.next_page_id;
        self.next_page_id += 1;

        let mut page = Page::new();
        page.set_page_id(page_id);
        frame.set_page(page);
        self.page_table.insert(page_id, frame_id);

        Ok(frame)
    }

    pub fn evict_frame(&mut self) -> FrameId {
        let frame_id = self.replacer.evict();
        let frame = &mut self.frames[frame_id];
        assert!(frame.get_pin_count() == 1);
        let page = frame.get_page_write();

        self.page_table.remove(&page.get_page_id());

        if page.is_dirty() {
            self.disk_manager.write_to_file(&page).unwrap();
        }

        frame_id
    }

    pub fn unpin(&mut self, page_id: &PageId) {
        let frame_id = *self.page_table.get(page_id).unwrap();
        let frame = &mut self.frames[frame_id];
        frame.unpin();

        if frame.get_pin_count() == 1 {
            self.replacer.set_evictable(frame_id, true);
        }
    }

    #[allow(unused)]
    pub fn get_pin_count(&self, page_id: &PageId) -> Option<u16> {
        let frame_id = *self.page_table.get(page_id)?;
        Some(self.frames[frame_id].get_pin_count())
    }
}

fn get_buffer_pool() -> BufferPoolManager {
    lazy_static! {
        static ref BUFFER_POOL: Arc<RwLock<BufferPool>> =
            Arc::new(RwLock::new(BufferPool::init(BUFFER_POOL_SIZE)));
    }

    BUFFER_POOL.clone()
}

#[cfg(test)]
mod tests {
    use crate::pages::table_page::TablePage;

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_dont_evict_pinned() -> Result<()> {
        let bpm = RwLock::new(BufferPool::init(2));
        let mut bpmw = bpm.write().unwrap();

        let p1: TablePage = bpmw.new_page()?.get_page_read().into();

        let _: TablePage = bpmw.new_page()?.get_page_read().into();

        assert_eq!(true, bpmw.new_page().is_err());

        bpmw.unpin(&p1.get_page_id());

        let _: TablePage = bpmw.new_page()?.get_page_read().into();

        assert_eq!(true, bpmw.new_page().is_err());

        Ok(())
    }
}
