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
    Char,
}

impl Types {
    pub fn size(&self) -> usize {
        match self {
            Types::U8 | Types::Char | Types::Bool | Types::I8 => 1,
            Types::U16 | Types::I16 => 2,
            Types::U32 | Types::I32 | Types::F32 => 4,
            Types::U64 | Types::I64 | Types::F64 => 8,
            Types::U128 | Types::I128 => 16,
        }
    }
}

#[allow(unused)]
pub trait Primitive {
    fn add(self, other: Self) -> Self;
    fn subtract(self, other: Self) -> Self;
    fn multiply(self, other: Self) -> Self;
    fn divide(self, other: Self) -> Self;
    fn to_bytes(&self) -> Box<[u8]>;
    fn from_bytes(bytes: &[u8]) -> Self;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct U8(pub u8);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct U16(pub u16);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct U32(pub u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct U64(pub u64);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct U128(pub u128);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct I8(pub i8);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct I16(pub i16);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct I32(pub i32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct I64(pub i64);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct I128(pub i128);
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct F32(pub f32);
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct F64(pub f64);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bool(pub bool);

impl Primitive for U8 {
    fn add(self, other: Self) -> Self {
        U8(self.0.wrapping_add(other.0))
    }
    fn subtract(self, other: Self) -> Self {
        U8(self.0.wrapping_sub(other.0))
    }
    fn multiply(self, other: Self) -> Self {
        U8(self.0.wrapping_mul(other.0))
    }
    fn divide(self, other: Self) -> Self {
        U8(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        U8(bytes[0])
    }
}

impl Primitive for U16 {
    fn add(self, other: Self) -> Self {
        U16(self.0.wrapping_add(other.0))
    }
    fn subtract(self, other: Self) -> Self {
        U16(self.0.wrapping_sub(other.0))
    }
    fn multiply(self, other: Self) -> Self {
        U16(self.0.wrapping_mul(other.0))
    }
    fn divide(self, other: Self) -> Self {
        U16(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        U16(u16::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for U32 {
    fn add(self, other: Self) -> Self {
        U32(self.0.wrapping_add(other.0))
    }
    fn subtract(self, other: Self) -> Self {
        U32(self.0.wrapping_sub(other.0))
    }
    fn multiply(self, other: Self) -> Self {
        U32(self.0.wrapping_mul(other.0))
    }
    fn divide(self, other: Self) -> Self {
        U32(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        U32(u32::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for U64 {
    fn add(self, other: Self) -> Self {
        U64(self.0.wrapping_add(other.0))
    }
    fn subtract(self, other: Self) -> Self {
        U64(self.0.wrapping_sub(other.0))
    }
    fn multiply(self, other: Self) -> Self {
        U64(self.0.wrapping_mul(other.0))
    }
    fn divide(self, other: Self) -> Self {
        U64(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        U64(u64::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for U128 {
    fn add(self, other: Self) -> Self {
        U128(self.0.wrapping_add(other.0))
    }
    fn subtract(self, other: Self) -> Self {
        U128(self.0.wrapping_sub(other.0))
    }
    fn multiply(self, other: Self) -> Self {
        U128(self.0.wrapping_mul(other.0))
    }
    fn divide(self, other: Self) -> Self {
        U128(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        U128(u128::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for I8 {
    fn add(self, other: Self) -> Self {
        I8(self.0.wrapping_add(other.0))
    }
    fn subtract(self, other: Self) -> Self {
        I8(self.0.wrapping_sub(other.0))
    }
    fn multiply(self, other: Self) -> Self {
        I8(self.0.wrapping_mul(other.0))
    }
    fn divide(self, other: Self) -> Self {
        I8(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        I8(i8::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for I16 {
    fn add(self, other: Self) -> Self {
        I16(self.0.wrapping_add(other.0))
    }
    fn subtract(self, other: Self) -> Self {
        I16(self.0.wrapping_sub(other.0))
    }
    fn multiply(self, other: Self) -> Self {
        I16(self.0.wrapping_mul(other.0))
    }
    fn divide(self, other: Self) -> Self {
        I16(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        I16(i16::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for I32 {
    fn add(self, other: Self) -> Self {
        I32(self.0.wrapping_add(other.0))
    }
    fn subtract(self, other: Self) -> Self {
        I32(self.0.wrapping_sub(other.0))
    }
    fn multiply(self, other: Self) -> Self {
        I32(self.0.wrapping_mul(other.0))
    }
    fn divide(self, other: Self) -> Self {
        I32(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        I32(i32::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for I64 {
    fn add(self, other: Self) -> Self {
        I64(self.0.wrapping_add(other.0))
    }
    fn subtract(self, other: Self) -> Self {
        I64(self.0.wrapping_sub(other.0))
    }
    fn multiply(self, other: Self) -> Self {
        I64(self.0.wrapping_mul(other.0))
    }
    fn divide(self, other: Self) -> Self {
        I64(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        I64(i64::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for I128 {
    fn add(self, other: Self) -> Self {
        I128(self.0.wrapping_add(other.0))
    }
    fn subtract(self, other: Self) -> Self {
        I128(self.0.wrapping_sub(other.0))
    }
    fn multiply(self, other: Self) -> Self {
        I128(self.0.wrapping_mul(other.0))
    }
    fn divide(self, other: Self) -> Self {
        I128(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        I128(i128::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for F32 {
    fn add(self, other: Self) -> Self {
        F32(self.0 + other.0)
    }
    fn subtract(self, other: Self) -> Self {
        F32(self.0 - other.0)
    }
    fn multiply(self, other: Self) -> Self {
        F32(self.0 * other.0)
    }
    fn divide(self, other: Self) -> Self {
        F32(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        F32(f32::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for F64 {
    fn add(self, other: Self) -> Self {
        F64(self.0 + other.0)
    }
    fn subtract(self, other: Self) -> Self {
        F64(self.0 - other.0)
    }
    fn multiply(self, other: Self) -> Self {
        F64(self.0 * other.0)
    }
    fn divide(self, other: Self) -> Self {
        F64(self.0 / other.0) // Simple division without checking for division by zero
    }
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        F64(f64::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for Bool {
    fn add(self, _other: Self) -> Self {
        unimplemented!()
    }
    fn subtract(self, _other: Self) -> Self {
        unimplemented!()
    }
    fn multiply(self, other: Self) -> Self {
        Bool(self.0 && other.0)
    }
    fn divide(self, _other: Self) -> Self {
        unimplemented!()
    }
    fn to_bytes(&self) -> Box<[u8]> {
        vec![self.0 as u8].into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        Bool(bytes[0] != 0)
    }
}
