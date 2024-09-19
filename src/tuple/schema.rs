use super::types::Types;

pub struct Schema {
    pub fields: Vec<String>,
    pub types: Vec<Types>,
}

#[allow(unused)]
impl Schema {
    pub fn new(fields: Vec<String>, types: Vec<Types>) -> Self {
        Self { fields, types }
    }
}
