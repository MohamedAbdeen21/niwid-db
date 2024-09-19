pub mod schema;
pub mod types;

use std::{mem, slice};

use crate::pages::traits::Serialize;
use crate::tuple::schema::Schema;
use anyhow::{anyhow, Result};

pub type Entry = (TupleMetaData, Tuple);

#[repr(C)]
#[derive(Debug)]
pub struct Tuple {
    data: Box<[u8]>,
}

impl Tuple {
    pub fn new(data: &[u8], schema: &Schema) -> Self {
        let size = schema.types.iter().fold(0, |acc, t| acc + t.size());
        if data.len() != size {
            panic!("data length mismatch");
        }
        Self::from_bytes(data)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn get_value(&self, field: &str, schema: &Schema) -> Result<&[u8]> {
        let field_id = schema
            .fields
            .iter()
            .position(|f| f == field)
            .ok_or(anyhow!("field not found"))?;

        if field_id >= schema.types.len() {
            return Err(anyhow!("field id out of bounds"));
        }

        let offset = schema
            .types
            .iter()
            .take(field_id as usize)
            .fold(0, |acc, t| acc + t.size());

        let size = schema.types[field_id].size();

        Ok(&self.data[offset..offset + size])
    }
}

impl Serialize for Tuple {
    fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            data: bytes.to_vec().into_boxed_slice(),
        }
    }
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct TupleMetaData {
    is_deleted: bool,
    null_bitmap: u64, // yes, tables are limited to 64 fields
}

impl Serialize for TupleMetaData {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(
                (self as *const TupleMetaData) as *const u8,
                mem::size_of::<TupleMetaData>(),
            )
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), mem::size_of::<TupleMetaData>());
        unsafe { *(bytes.as_ptr() as *const TupleMetaData) }
    }
}

#[allow(unused)]
impl TupleMetaData {
    pub fn new() -> Self {
        Self {
            null_bitmap: 0,
            is_deleted: false,
        }
    }

    pub fn mark_deleted(&mut self) {
        self.is_deleted = true;
    }

    pub fn is_null(&self, field_id: u8) -> bool {
        (self.null_bitmap >> field_id) & 1 == 1
    }

    pub fn set_null(&mut self, field_id: u8) {
        self.null_bitmap |= 1 << field_id;
    }

    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }
}
