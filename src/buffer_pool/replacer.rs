use super::FrameId;

pub(super) trait Replacer: Send + Sync {
    fn can_evict(&self) -> bool;
    fn evict(&mut self) -> Option<FrameId>;
}

pub(super) struct LRU {}

impl Replacer for LRU {
    fn can_evict(&self) -> bool {
        todo!()
    }

    fn evict(&mut self) -> Option<FrameId> {
        todo!()
    }
}
