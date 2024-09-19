#[allow(unused)]
pub enum Types {
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,
    F32,
    F64,
    Bool,
}

impl Types {
    pub fn size(&self) -> usize {
        match self {
            Types::U8 => 1,
            Types::U16 => 2,
            Types::U32 => 4,
            Types::U64 => 8,
            Types::U128 => 16,
            Types::I8 => 1,
            Types::I16 => 2,
            Types::I32 => 4,
            Types::I64 => 8,
            Types::I128 => 16,
            Types::F32 => 4,
            Types::F64 => 8,
            Types::Bool => 1,
        }
    }
}
