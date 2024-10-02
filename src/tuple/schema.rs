use crate::types::Types;
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub ty: Types,
    pub nullable: bool,
}

impl Default for Field {
    fn default() -> Self {
        Self {
            name: String::new(),
            ty: Types::U8,
            nullable: false,
        }
    }
}

impl Field {
    pub fn new(name: &str, ty: Types, nullable: bool) -> Self {
        Self {
            name: name.to_string(),
            ty,
            nullable,
        }
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }
}

impl Schema {
    pub fn to_bytes(&self) -> Box<[u8]> {
        let x = serialize(self).unwrap();
        x.into_boxed_slice()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        deserialize(bytes).unwrap()
    }
}
