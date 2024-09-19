mod frame;
mod replacer;

use crate::disk_manager::DiskManager;
use crate::pages::Page;
use anyhow::{anyhow, Result};
use frame::Frame;
use lazy_static::lazy_static;
use std::collections::{HashMap, LinkedList};
use std::sync::RwLock;

const BUFFER_POOL_SIZE: usize = 10_000;

type FrameId = usize;

pub struct BufferPool {
    timestamp: u64,
    free_frames: LinkedList<FrameId>,
    frames: Box<[RwLock<Frame>]>,
    page_table: HashMap<i32, FrameId>,
    replacer: Box<dyn replacer::Replacer>,
    disk_manager: DiskManager,
}

impl BufferPool {
    pub fn new() -> Self {
        let mut frames = Vec::with_capacity(BUFFER_POOL_SIZE);
        for i in 0..BUFFER_POOL_SIZE {
            frames.push(RwLock::new(Frame::new(i)));
        }

        Self {
            timestamp: 0,
            free_frames: LinkedList::from_iter(0..BUFFER_POOL_SIZE),
            frames: frames.into_boxed_slice(),
            page_table: HashMap::new(),
            replacer: Box::new(replacer::LRU {}),
            disk_manager: DiskManager::new("data/"),
        }
    }

    pub fn get_frame(&self, page_id: i32) -> &RwLock<Frame> {
        let timestamp = chrono::Utc::now().timestamp_millis();

        if self.page_table.contains_key(&page_id) {
            let frame_id = self.page_table.get(&page_id).unwrap();

            let frame = &self.frames[*frame_id];
            let mut wframe = frame.write().unwrap();
            wframe.pin();
            wframe.record_access(timestamp);

            frame
        } else {
            todo!();
        }
    }

    pub fn new_page(&mut self) -> Result<&RwLock<Frame>> {
        let timestamp = chrono::Utc::now().timestamp_millis();

        if self.free_frames.is_empty() && !self.replacer.can_evict() {
            return Err(anyhow!("no free frames to evict"));
        }

        let frame_id = if self.free_frames.is_empty() {
            self.evict_frame()
        } else {
            self.free_frames.pop_back().unwrap()
        };

        let frame = &self.frames[frame_id];
        let mut wframe = frame.write().unwrap();
        wframe.pin();
        wframe.record_access(timestamp);

        let page_id = 1;
        let mut page = Page::new();
        page.set_page_id(page_id);
        wframe.set_page(page);

        self.page_table.insert(page_id, frame_id);

        Ok(frame)
    }

    pub fn evict_frame(&mut self) -> FrameId {
        todo!()
    }
}

lazy_static! {
    static ref BUFFER_POOL: RwLock<BufferPool> = RwLock::new(BufferPool::new());
}

pub fn get_buffer_pool() -> &'static RwLock<BufferPool> {
    &BUFFER_POOL
}
