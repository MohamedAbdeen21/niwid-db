use std::sync::Arc;

use crate::{latch::Latch, tuple::TupleId};

use super::PageId;

#[allow(dead_code)]
enum PageType {
    Root,
    Leaf,
    Internal,
}

#[allow(dead_code)]
struct IndexPageData {
    _padding: [u8; 3],
    is_dirty: bool,
    level: u8,
    slots: u16,
    prev: PageId,
    next: PageId,
}

#[allow(dead_code)]
struct IndexLeafPage {
    data: IndexPageData,
    keys: [u64; 200],
    values: [TupleId; 200],
}

#[allow(dead_code)]
struct IndexPage {
    data: *mut IndexPageData,
    page_id: PageId,
    latch: Arc<Latch>,
}
