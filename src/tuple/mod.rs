pub mod constraints;
pub mod schema;

use crate::pages::{PageId, SlotId};
use crate::tuple::schema::Schema;
use crate::types::{AsBytes, Types, Value};
use crate::{pages::traits::Serialize, types::ValueFactory};
use anyhow::{anyhow, Result};
use std::{mem, slice};

/// Tuple Meta Data + the Tuple itself
pub type Entry = (TupleMetaData, Tuple);
/// Page Id and slot Id
pub type TupleId = (PageId, SlotId);
pub const TUPLE_ID_SIZE: usize = 6;

#[repr(C)]
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Tuple {
    /// NOT WRITTEN TO DISK, transferred to metadata during insertion
    /// which eventaully gets written to disk.
    /// here just for convenience
    pub(super) _null_bitmap: u64,
    data: Box<[u8]>,
}

impl Tuple {
    pub fn new(mut values: Vec<Value>, schema: &Schema) -> Self {
        let mut nulls = 0;
        if values.iter().any(|v| v.is_null()) {
            values = values
                .into_iter()
                .zip(schema.fields.iter().map(|f| f.ty.clone()))
                .enumerate()
                .map(|(i, (value, type_))| {
                    if value.is_null() {
                        nulls |= 1 << i;
                        ValueFactory::default(&type_)
                    } else {
                        value
                    }
                })
                .collect::<Vec<_>>();
        }

        let x = values
            .iter()
            .flat_map(|t| t.to_bytes())
            .collect::<Vec<u8>>();

        let mut tuple = Self::from_bytes(&x);
        tuple._null_bitmap = nulls;
        tuple
    }

    pub fn from_sql(ident: Vec<Option<String>>, schema: Schema) -> Self {
        let values = ident
            .iter()
            .zip(schema.fields.iter().map(|f| f.ty.clone()))
            .map(|(v, ty)| match v {
                None => ValueFactory::null(),
                Some(v) => ValueFactory::from_string(&ty, v),
            })
            .collect();

        Tuple::new(values, &schema)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn get_value_of(&self, field: &str, schema: &Schema) -> Result<Option<Value>> {
        let field_id = schema
            .fields
            .iter()
            .position(|f| f.name == field)
            .ok_or(anyhow!("field not found"))?;

        self.get_value_at(field_id as u8, schema)
    }

    pub fn get_values(&self, schema: &Schema) -> Result<Vec<Value>> {
        let mut values = vec![];

        let mut offset = 0;
        for (i, mut type_) in schema.fields.iter().map(|f| &f.ty).enumerate() {
            if matches!(type_, Types::Str) {
                type_ = &Types::StrAddr; // size of tuple_id
            }
            let size = type_.size();
            let value = ValueFactory::from_bytes(type_, &self.get_data()[offset..offset + size]);
            offset += size;
            if (self._null_bitmap >> i) & 1 == 1 {
                values.push(Value::Null);
            } else {
                values.push(value);
            }
        }

        Ok(values)
    }

    pub fn get_value_at(&self, id: u8, schema: &Schema) -> Result<Option<Value>> {
        if id as usize >= schema.fields.len() {
            return Err(anyhow!("field id out of bounds"));
        }

        if (self._null_bitmap >> id) & 1 == 1 {
            return Ok(None);
        }

        let types: Vec<_> = schema.fields.iter().map(|f| &f.ty).collect();

        let dtype = match types[id as usize] {
            Types::Str => &Types::StrAddr,
            e => e,
        };

        let offset = types
            .iter()
            .take(id as usize)
            .fold(0, |acc, t| acc + t.size());

        let slice = &self.data[offset..offset + dtype.size()];
        let value = ValueFactory::from_bytes(dtype, slice);
        Ok(Some(value))
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
            _null_bitmap: 0,
            data: bytes.to_vec().into_boxed_slice(),
        }
    }
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct TupleMetaData {
    _padding: [u8; 1],
    is_deleted: bool,
    timestamp: u64,
    null_bitmap: u64,
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
        unsafe { *(bytes.as_ptr() as *mut TupleMetaData) }
    }
}

impl Default for TupleMetaData {
    fn default() -> Self {
        Self::new(0)
    }
}

impl TupleMetaData {
    pub fn new(nulls: u64) -> Self {
        Self {
            _padding: [0; 1],
            timestamp: 0,
            null_bitmap: nulls,
            is_deleted: false,
        }
    }

    pub fn mark_deleted(&mut self) {
        self.is_deleted = true;
    }

    #[cfg(test)]
    pub fn is_null(&self, field_id: u8) -> bool {
        (self.null_bitmap >> field_id) & 1 == 1
    }

    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    pub fn get_null_bitmap(&self) -> u64 {
        self.null_bitmap
    }
}

pub trait TupleExt {
    fn from_string(s: &str) -> Self;
    fn from_bytes(bytes: &[u8]) -> Self;
    fn to_bytes(&self) -> Vec<u8>;
}

impl TupleExt for TupleId {
    fn from_string(_s: &str) -> Self {
        unreachable!()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let page_offset = std::mem::size_of::<PageId>();
        let slot_size = std::mem::size_of::<SlotId>();
        assert_eq!(bytes.len(), page_offset + slot_size);
        let page_id = PageId::from_ne_bytes(bytes[0..page_offset].try_into().unwrap());
        let slot_id = SlotId::from_le_bytes(
            bytes[page_offset..page_offset + slot_size]
                .try_into()
                .unwrap(),
        );
        (page_id, slot_id)
    }

    fn to_bytes(&self) -> Vec<u8> {
        let page_id_size = std::mem::size_of::<PageId>();
        let slot_id_size = std::mem::size_of::<SlotId>();
        let mut bytes = Vec::with_capacity(page_id_size + slot_id_size);
        bytes.extend_from_slice(&self.0.to_ne_bytes());
        bytes.extend_from_slice(&self.1.to_ne_bytes());

        assert_eq!(bytes.len(), page_id_size + slot_id_size);
        bytes
    }
}
