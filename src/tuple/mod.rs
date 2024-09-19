pub mod schema;

use std::{mem, slice};

use crate::tuple::schema::Schema;
use crate::{pages::traits::Serialize, types::Primitive};
use anyhow::{anyhow, Result};

pub type Entry = (TupleMetaData, Tuple);

#[repr(C)]
#[derive(Debug, Eq, PartialEq)]
pub struct Tuple {
    data: Box<[u8]>,
}

impl Tuple {
    pub fn new(data: Vec<Box<[u8]>>, _schema: &Schema) -> Self {
        // let strings = schema
        //     .types
        //     .iter()
        //     .enumerate()
        //     .filter(|(_, t)| t == &&Types::Str);
        //
        // let _size = schema.types.iter().fold(0, |acc, t| acc + t.size());

        let data = data
            .iter()
            .flat_map(|b| b.iter())
            .cloned()
            .collect::<Vec<u8>>();

        // TODO: validate input and careful with strings
        // if data.len() != size {
        //     panic!("data length mismatch");
        // }

        Self::from_bytes(data.as_slice())
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn get_value<T: Primitive>(&self, field: &str, schema: &Schema) -> Result<T> {
        let field_id = schema
            .fields
            .iter()
            .position(|f| f == field)
            .ok_or(anyhow!("field not found"))?;

        if field_id >= schema.types.len() {
            return Err(anyhow!("field id out of bounds"));
        }

        let dtype = &schema.types[field_id];

        let offset = schema
            .types
            .iter()
            .take(field_id as usize)
            .fold(0, |acc, t| acc + t.size());

        let slice = &self.data[offset..offset + dtype.size()];
        Ok(T::from_bytes(slice))
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }
}

impl Serialize for Tuple {
    fn to_bytes(&self) -> &[u8] {
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
    fn to_bytes(&self) -> &[u8] {
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

impl Default for TupleMetaData {
    fn default() -> Self {
        Self::new()
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
