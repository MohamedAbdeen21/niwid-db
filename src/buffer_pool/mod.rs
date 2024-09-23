mod frame;
mod replacer;

use crate::catalog::CATALOG_PAGE;
use crate::disk_manager::{DiskManager, DISK_STORAGE};
use crate::pages::{Page, PageId, INVALID_PAGE};
use crate::txn_manager::TxnId;
use anyhow::{anyhow, Result};
use frame::Frame;
use lazy_static::lazy_static;
use parking_lot::FairMutex;
use std::collections::{HashMap, HashSet, LinkedList};
use std::mem::take;
use std::sync::Arc;

const BUFFER_POOL_SIZE: usize = 10_000;
const BUFFER_POOL_PAGE: PageId = 0;

type FrameId = usize;
pub type ArcBufferPool = Arc<FairMutex<BufferPoolManager>>;

pub struct BufferPoolManager {
    disk_manager: DiskManager,

    free_frames: LinkedList<FrameId>,
    page_table: HashMap<PageId, FrameId>,
    frames: Vec<Frame>,

    txn_table: HashMap<TxnId, HashSet<FrameId>>,

    replacer: Box<dyn replacer::Replacer>,

    next_page_id: Page,
}

impl BufferPoolManager {
    pub fn get() -> ArcBufferPool {
        BUFFER_POOL.clone()
    }

    #[allow(unused)]
    pub fn inspect(&self) {
        println!("Free Frames: {:?}", self.free_frames);
        println!("Page Table: {:?}", self.page_table);
    }

    pub fn new(size: usize, path: &str) -> Self {
        // takes a few seconds if bp size is too large, can be parallelized.
        let frames = (0..size).map(|_| Frame::new()).collect::<Vec<_>>();

        let disk_manager = DiskManager::new(path);

        // make sure catalog page can also be fetched
        if disk_manager.read_from_file::<Page>(CATALOG_PAGE).is_err() {
            let mut catalog_page = Page::new();
            catalog_page.set_page_id(CATALOG_PAGE);
            disk_manager.write_to_file(&catalog_page, None).unwrap();
        }

        // buffer pool data that must persist on disk e.g. next page id
        let next_page_id = match disk_manager.read_from_file(BUFFER_POOL_PAGE) {
            Ok(page) => page,
            Err(_) => {
                let mut page = Page::new();
                page.set_page_id(BUFFER_POOL_PAGE);
                page.write_bytes(2, 10, &1_i64.to_ne_bytes());
                page
            }
        };

        Self {
            free_frames: LinkedList::from_iter(0..size),
            frames,
            page_table: HashMap::new(),
            replacer: Box::new(replacer::LRU::new(size)),
            disk_manager,
            // page_id 0 is preserved for bp [`BUFFER_POOL_PAGE`], and 1 for catalog [`CATALOG_PAGE`]
            next_page_id,
            txn_table: HashMap::new(),
        }
    }

    pub fn increment_page_id(&mut self) -> Result<PageId> {
        let id = PageId::from_ne_bytes(self.next_page_id.read_bytes(2, 10).try_into().unwrap());
        self.next_page_id
            .write_bytes(2, 10, &(id + 1).to_ne_bytes());
        self.disk_manager.write_to_file(&self.next_page_id, None)?;
        Ok(id + 1)
    }

    fn find_free_frame(&mut self) -> Result<FrameId> {
        if let Some(frame) = self.free_frames.pop_front() {
            Ok(frame)
        } else if self.replacer.can_evict() {
            Ok(self.evict_frame())
        } else {
            return Err(anyhow!("no free frames to evict"));
        }
    }

    pub fn fetch_frame(&mut self, page_id: PageId, txn_id: Option<TxnId>) -> Result<&mut Frame> {
        let frame_id = if let Some(id) = txn_id {
            // I don't like this
            self.txn_table
                .get(&id)
                .unwrap()
                .iter()
                .find(|f| self.frames[**f].get_page_id() == page_id)
                .copied()
                .ok_or_else(|| anyhow!("page not found"))?
        } else if let Some(frame_id) = self.page_table.get(&page_id) {
            *frame_id
        } else {
            let page = self.disk_manager.read_from_file(page_id)?;
            let frame_id = self.find_free_frame()?;

            self.frames[frame_id].set_page(page);
            self.page_table.insert(page_id, frame_id);

            frame_id
        };

        let frame = &mut self.frames[frame_id];
        frame.pin();
        self.replacer.record_access(frame_id);

        Ok(frame)
    }

    // TODO: txn id
    pub fn new_page(&mut self) -> Result<&mut Frame> {
        let frame_id = self.find_free_frame()?;

        let page_id = self.increment_page_id()?;

        let frame = &mut self.frames[frame_id];
        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, true);

        let mut page = Page::new();
        page.set_page_id(page_id);
        self.disk_manager.write_to_file(&page, None)?;

        frame.set_page(page);
        self.page_table.insert(page_id, frame_id);

        Ok(frame)
    }

    pub fn evict_frame(&mut self) -> FrameId {
        let frame_id = self.replacer.evict();
        let frame = &mut self.frames[frame_id];
        assert!(frame.get_pin_count() == 0);
        let page = frame.writer();

        self.page_table.remove(&page.get_page_id());

        if page.is_dirty() {
            self.disk_manager.write_to_file(page, None).unwrap();
        }

        frame_id
    }

    pub fn unpin(&mut self, page_id: &PageId) {
        let frame_id = *self.page_table.get(page_id).unwrap();
        let frame = &mut self.frames[frame_id];
        frame.unpin();

        if frame.get_pin_count() == 0 {
            self.replacer.set_evictable(frame_id, true);
        }
    }

    #[cfg(test)]
    pub fn get_pin_count(&self, page_id: &PageId) -> Option<u16> {
        let frame_id = *self.page_table.get(page_id)?;
        Some(self.frames[frame_id].get_pin_count())
    }

    pub fn start_txn(&mut self, txn_id: TxnId) -> Result<()> {
        // don't worry about atomicity, bpm is shared behind a mutex
        self.txn_table.insert(txn_id, HashSet::new());

        self.disk_manager.start_txn(txn_id)?;

        Ok(())
    }

    pub fn shadow_page(&mut self, txn_id: TxnId, page_id: PageId) -> Result<()> {
        let frame = self.fetch_frame(page_id, None)?;

        let shadowed_page = frame.reader().clone();

        let shadow_frame_id = self.find_free_frame()?;
        let shadow_frame = &mut self.frames[shadow_frame_id];

        shadow_frame.set_page(shadowed_page);

        self.txn_table
            .get_mut(&txn_id)
            .unwrap()
            .insert(shadow_frame_id);

        Ok(())
    }

    pub fn commit_txn(&mut self, txn_id: TxnId) -> Result<()> {
        // commit shadowed pages to txn cache, this is for durability and atomicity
        for frame_id in self.txn_table.get(&txn_id).unwrap() {
            let page = self.frames[*frame_id].reader();
            self.disk_manager.write_to_file(page, Some(txn_id))?;
        }

        self.disk_manager.commit_txn(txn_id)?;

        // empty txns are generally okay, this is just for debugging
        if self.txn_table.get(&txn_id).unwrap().is_empty() {
            Err(anyhow!("Committing empty txn"))?;
        }

        for frame_id in self.txn_table.remove(&txn_id).unwrap() {
            let shadow_frame = take(&mut self.frames[frame_id]);

            let shadow_page_id = shadow_frame.reader().get_page_id();

            let old_frame_id = self.page_table[&shadow_page_id];
            let old_frame = &mut self.frames[old_frame_id];

            if !old_frame.get_latch().is_locked() {
                panic!(
                    "txn {} frame {} page {} is not write-locked before commit",
                    txn_id, old_frame_id, shadow_page_id
                );
            }

            old_frame.move_page(shadow_frame);

            self.unpin(&shadow_page_id);

            self.free_frames.push_back(frame_id);
        }

        Ok(())
    }

    pub fn abort_txn(&mut self, _txn_id: TxnId) -> Result<()> {
        todo!()
    }
}

lazy_static! {
    static ref BUFFER_POOL: ArcBufferPool = Arc::new(FairMutex::new(BufferPoolManager::new(
        BUFFER_POOL_SIZE,
        DISK_STORAGE
    )));
}

impl Drop for BufferPoolManager {
    fn drop(&mut self) {
        // TODO: do we need to check txns?
        self.frames
            .iter_mut()
            .filter(|f| f.reader().get_page_id() != INVALID_PAGE)
            .map(|f| f.writer())
            .for_each(|p| self.disk_manager.write_to_file(p, None).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        disk_manager::test_path,
        pages::table_page::{TablePage, PAGE_END},
    };
    use anyhow::Result;

    fn cleanup(bpm: BufferPoolManager, path: &str) -> Result<()> {
        drop(bpm);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn test_dont_evict_pinned() -> Result<()> {
        let path = test_path();

        let mut bpm = BufferPoolManager::new(2, &path);

        let p1 = bpm.new_page()?.reader().get_page_id();
        let p2 = bpm.new_page()?.reader().get_page_id();

        // pin the page
        let _ = bpm.fetch_frame(p1, None);
        let _ = bpm.fetch_frame(p2, None);

        assert!(bpm.new_page().is_err());

        bpm.unpin(&p1);

        assert!(bpm.new_page().is_ok());

        let _ = bpm.fetch_frame(p1, None);

        assert!(bpm.new_page().is_err());

        cleanup(bpm, &path)?;

        Ok(())
    }

    #[test]
    fn test_shared_latch() -> Result<()> {
        let path = test_path();

        let mut bpm = BufferPoolManager::new(2, &path);

        let frame = bpm.new_page()?;
        let page = frame.writer();
        let table_page: TablePage = page.into();

        page.get_latch().wlock();

        assert!(frame.get_latch().is_locked());
        assert!(table_page.get_latch().is_locked());

        frame.get_latch().wunlock();

        assert!(!frame.get_latch().is_locked());
        assert!(!table_page.get_latch().is_locked());

        cleanup(bpm, &path)?;

        Ok(())
    }

    #[test]
    fn test_shadow_pages() -> Result<()> {
        let path = test_path();

        let mut bpm = BufferPoolManager::new(2, &path);

        let txn_id = 1;

        bpm.start_txn(1)?;

        let page = bpm.new_page()?.writer();
        let lock = page.get_latch().clone();
        lock.wlock();

        let page_id = page.get_page_id();
        bpm.shadow_page(txn_id, page_id)?;
        let shadow_page = bpm.fetch_frame(page_id, Some(txn_id))?.writer();

        shadow_page.get_latch().wlock();

        let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        shadow_page.write_bytes(PAGE_END - data.len(), PAGE_END, &data);

        // shadow allocates a new frame
        // pins first page and doesn't record access to shadow page
        // effectively temporarily "pining" it.
        assert!(bpm.new_page().is_err());

        bpm.commit_txn(txn_id)?;

        // frame and page are sharing lock
        let new_page = bpm.fetch_frame(page_id, None)?.writer();
        assert!(new_page.get_latch().is_locked());

        assert_eq!(new_page.read_bytes(PAGE_END - data.len(), PAGE_END), data);

        lock.wunlock();

        assert!(bpm.new_page().is_ok());

        cleanup(bpm, &path)?;

        Ok(())
    }
}
