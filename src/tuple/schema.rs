use crate::types::Types;

#[derive(Clone)]
pub struct Schema {
    pub fields: Vec<String>,
    pub types: Vec<Types>,
}

impl Schema {
    pub fn new(fields: Vec<String>, types: Vec<Types>) -> Self {
        Self { fields, types }
    }
}
