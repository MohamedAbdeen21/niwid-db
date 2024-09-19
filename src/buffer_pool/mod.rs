mod frame;
mod replacer;

use crate::disk_manager::DiskManager;
use crate::pages::{Page, PageId};
use anyhow::{anyhow, Result};
use frame::Frame;
use lazy_static::lazy_static;
use std::collections::{HashMap, LinkedList};
use std::sync::RwLock;

const BUFFER_POOL_SIZE: usize = 10_000;

type FrameId = usize;
pub type BufferPoolManager = &'static RwLock<BufferPool>;

pub struct BufferPool {
    free_frames: LinkedList<FrameId>,
    frames: Box<[RwLock<Frame>]>,
    page_table: HashMap<PageId, FrameId>,
    replacer: Box<dyn replacer::Replacer>,
    disk_manager: DiskManager,
}

impl BufferPool {
    pub fn new() -> BufferPoolManager {
        let bpm = get_buffer_pool();

        bpm.write().unwrap().frames.iter_mut().for_each(|frame| {
            frame.write().unwrap().bpm = Some(bpm);
        });

        bpm
    }

    fn init() -> Self {
        let mut frames = Vec::with_capacity(BUFFER_POOL_SIZE);
        for i in 0..BUFFER_POOL_SIZE {
            frames.push(RwLock::new(Frame::new(i)));
        }

        Self {
            free_frames: LinkedList::from_iter(0..BUFFER_POOL_SIZE),
            frames: frames.into_boxed_slice(),
            page_table: HashMap::new(),
            replacer: Box::new(replacer::LRU::new()),
            disk_manager: DiskManager::new("data/test.db"),
        }
    }

    pub fn fetch_page(&mut self, page_id: PageId) -> Result<&mut RwLock<Frame>> {
        let frame_id = if self.page_table.contains_key(&page_id) {
            *self.page_table.get(&page_id).unwrap()
        } else {
            let page = self.disk_manager.read_from_file(page_id).unwrap();
            let frame_id = if self.free_frames.is_empty() {
                self.free_frames.pop_back().unwrap()
            } else if self.replacer.can_evict() {
                self.evict_frame()
            } else {
                return Err(anyhow!("no free frames to evict"));
            };

            self.frames[frame_id].write().unwrap().set_page(page);
            self.page_table.insert(page_id, frame_id);

            frame_id
        };

        let frame = &mut self.frames[frame_id];
        frame.write().unwrap().pin();
        self.replacer.record_access(frame_id);

        Ok(frame)
    }

    pub fn new_page(&mut self) -> Result<&RwLock<Frame>> {
        if self.free_frames.is_empty() && !self.replacer.can_evict() {
            return Err(anyhow!("no free frames to evict"));
        }

        let frame_id = if self.free_frames.is_empty() {
            self.evict_frame()
        } else {
            self.free_frames.pop_back().unwrap()
        };

        let frame = &self.frames[frame_id];
        frame.write().unwrap().pin();
        self.replacer.record_access(frame_id);

        let page_id = 1;
        let mut page = Page::new();
        page.set_page_id(page_id);
        frame.write().unwrap().set_page(page);
        self.page_table.insert(page_id, frame_id);

        Ok(frame)
    }

    pub fn evict_frame(&mut self) -> FrameId {
        let frame_id = self.replacer.evict();
        let frame = &mut self.frames[frame_id].write().unwrap();
        assert!(frame.get_pin_count() == 1);
        let page = frame.get_page_write();

        if page.is_dirty() {
            self.disk_manager.write_to_file(&page).unwrap();
        }

        self.free_frames.push_back(frame_id);

        frame_id
    }

    pub fn set_evictable(&mut self, frame_id: FrameId) {
        assert!(self.frames[frame_id].read().unwrap().get_pin_count() == 1);
        self.replacer.set_evictable(frame_id, true);
    }
}

lazy_static! {
    static ref BUFFER_POOL: RwLock<BufferPool> = RwLock::new(BufferPool::init());
}

fn get_buffer_pool() -> BufferPoolManager {
    &BUFFER_POOL
}
