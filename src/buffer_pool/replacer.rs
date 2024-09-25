use core::fmt::Debug;
use std::collections::HashMap;

use priority_queue::PriorityQueue;

use super::FrameId;

pub(super) trait Replacer: Send + Sync + Debug {
    fn record_access(&mut self, frame_id: FrameId);
    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool);
    fn can_evict(&self) -> bool;
    fn evict(&mut self) -> FrameId;
    #[cfg(test)]
    fn peek(&self) -> Option<FrameId>;
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug)]
pub(super) struct LRU {
    timestamp: i64,
    heap: PriorityQueue<FrameId, i64>,
    /// Store the last access when the frame is set no unevictable.
    /// Restores the ts when the frame is marked as evictable again.
    last_access: HashMap<FrameId, i64>,
}

impl LRU {
    pub fn new(size: usize) -> Self {
        Self {
            timestamp: 0,
            heap: PriorityQueue::with_capacity(size),
            last_access: HashMap::with_capacity(size),
        }
    }
}

impl Replacer for LRU {
    /// Record Frame access timestamp
    /// Sets the frame to unevictable
    /// make sure to call [`self.set_evictable`] with `true`
    /// when the frame is no longer in use
    fn record_access(&mut self, frame_id: FrameId) {
        self.timestamp += 1;
        self.heap.push_decrease(frame_id, -self.timestamp);
        self.set_evictable(frame_id, false);
    }

    /// Check if a frame can be evicted
    /// Must check before calling [`self.evict`]
    /// as evict just unwraps the value
    fn can_evict(&self) -> bool {
        !self.heap.is_empty()
    }

    /// Get the LRU frame to evict.
    /// Removes frame from heap, effectively resetting its
    /// access history
    fn evict(&mut self) -> FrameId {
        self.heap.pop().unwrap().0
    }

    #[cfg(test)]
    fn peek(&self) -> Option<FrameId> {
        self.heap.peek().map(|(frame_id, _)| *frame_id)
    }

    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        if evictable {
            let ts = self.last_access.remove(&frame_id).unwrap();
            self.heap.push(frame_id, ts);
        } else {
            let (frame_id, ts) = self.heap.remove(&frame_id).unwrap();
            self.last_access.insert(frame_id, ts);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_replace_lru() -> Result<()> {
        let mut replacer = LRU::new(3);
        replacer.record_access(1);
        replacer.record_access(2);
        replacer.record_access(3);
        replacer.set_evictable(1, true);
        assert_eq!(replacer.peek(), Some(1));
        replacer.record_access(1);
        // no evictable
        assert!(!replacer.can_evict());
        assert!(replacer.peek().is_none());
        replacer.record_access(2);
        replacer.set_evictable(3, true);
        assert!(replacer.can_evict());
        replacer.set_evictable(2, true);
        replacer.set_evictable(1, true);
        assert_eq!(replacer.evict(), 3);
        assert_eq!(replacer.evict(), 1);
        assert_eq!(replacer.evict(), 2);
        Ok(())
    }
}
