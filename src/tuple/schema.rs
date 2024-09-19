use crate::types::Types;

#[derive(Clone)]
pub struct Schema {
    pub fields: Vec<String>,
    pub types: Vec<Types>,
}

impl Schema {
    pub fn new(fields: Vec<&str>, types: Vec<Types>) -> Self {
        Self {
            fields: fields.iter().map(|s| s.to_string()).collect(),
            types,
        }
    }
}
