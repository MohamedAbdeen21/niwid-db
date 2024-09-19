pub mod schema;

use crate::pages::PageId;
use crate::tuple::schema::Schema;
use crate::types::{AsBytes, *};
use crate::{pages::traits::Serialize, types::Types};
use anyhow::{anyhow, Result};
use std::{mem, slice};

/// Tuple Meta Data + the Tuple itself
pub type Entry = (TupleMetaData, Tuple);
/// Page Id and slot Id
pub type TupleId = (PageId, usize);

#[repr(C)]
#[derive(Debug, Eq, PartialEq)]
pub struct Tuple {
    data: Box<[u8]>,
}

impl Tuple {
    pub fn new(values: Vec<Box<dyn AsBytes>>, schema: &Schema) -> Self {
        let has_nulls = values.iter().any(|t| t.is_null());
        let data = if has_nulls {
            values
                .into_iter()
                .zip(schema.types.iter())
                .map(|(t, ty)| -> Box<dyn AsBytes> {
                    if t.is_null() {
                        match ty {
                            Types::Str => Str("".to_string()).into(),
                            Types::I64 => I64(0).into(),
                            Types::I128 => I128(0).into(),
                            Types::U64 => U64(0).into(),
                            Types::U128 => U128(0).into(),
                            Types::F64 => F64(0.0).into(),
                            Types::F32 => F32(0.0).into(),
                            Types::Bool => Bool(false).into(),
                            Types::I8 => I8(0).into(),
                            Types::I16 => I16(0).into(),
                            Types::I32 => I32(0).into(),
                            Types::U8 => U8(0).into(),
                            Types::U16 => U16(0).into(),
                            Types::U32 => U32(0).into(),
                            Types::Char => Char('0').into(),
                        }
                    } else {
                        t
                    }
                })
                .collect()
        } else {
            values
        };

        let x = data.iter().flat_map(|t| t.to_bytes()).collect::<Vec<u8>>();

        Self::from_bytes(&x)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn get_value<T: AsBytes>(&self, field: &str, schema: &Schema) -> Result<T> {
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

pub trait TupleExt {
    fn from_bytes(bytes: &[u8]) -> Self;
    #[allow(unused)]
    fn to_bytes(&self) -> Vec<u8>;
}

impl TupleExt for TupleId {
    fn from_bytes(bytes: &[u8]) -> Self {
        let page_offset = std::mem::size_of::<PageId>();
        let slot_size = std::mem::size_of::<usize>();
        let page_id = PageId::from_ne_bytes(bytes[0..page_offset].try_into().unwrap());
        let slot_id = usize::from_le_bytes(
            bytes[page_offset..page_offset + slot_size]
                .try_into()
                .unwrap(),
        );
        (page_id, slot_id)
    }

    fn to_bytes(&self) -> Vec<u8> {
        let page_id_size = std::mem::size_of::<PageId>();
        let slot_id_size = std::mem::size_of::<usize>();
        let mut bytes = Vec::with_capacity(page_id_size + slot_id_size);
        bytes.extend_from_slice(&self.0.to_ne_bytes());
        bytes.extend_from_slice(&self.1.to_ne_bytes());

        bytes
    }
}
