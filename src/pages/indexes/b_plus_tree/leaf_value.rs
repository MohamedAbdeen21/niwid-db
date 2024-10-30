use crate::{
    pages::{PageId, SlotId},
    tuple::{TupleExt, TupleId},
};

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct LeafValue {
    pub page_id: PageId,
    pub slot_id: SlotId,
    pub is_deleted: bool,
}

impl LeafValue {
    pub fn new(page_id: PageId, slot_id: SlotId) -> Self {
        Self {
            page_id,
            slot_id,
            is_deleted: false,
        }
    }

    pub fn tuple_id(&self) -> (PageId, SlotId) {
        (self.page_id, self.slot_id)
    }
}

impl TupleExt for LeafValue {
    fn from_bytes(bytes: &[u8]) -> Self {
        let (page_id, slot_id) = TupleId::from_bytes(bytes[..6].try_into().unwrap());
        let is_deleted = bytes[6] == 1;
        Self {
            page_id,
            slot_id,
            is_deleted,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let deleted: u8 = if self.is_deleted { 1 } else { 0 };
        let mut bytes = (self.page_id, self.slot_id).to_bytes();
        bytes.push(deleted);
        bytes
    }

    fn from_string(_s: &str) -> Self {
        unreachable!()
    }
}
