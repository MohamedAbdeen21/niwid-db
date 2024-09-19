use crate::types::Types;
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<String>,
    pub types: Vec<Types>,
}

impl Schema {
    pub fn new(fields: Vec<&str>, types: Vec<Types>) -> Self {
        if fields.len() != types.len() {
            panic!("fields and types must be the same length");
        }
        Self {
            fields: fields.iter().map(|s| s.to_string()).collect(),
            types,
        }
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
