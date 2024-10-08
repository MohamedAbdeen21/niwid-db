use std::fmt::Debug;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::tuple::TupleId;

#[allow(unused)]
#[derive(PartialEq, Eq, Clone, Debug)] // others
#[derive(Serialize, Deserialize)] // for schema serde
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
    /// string is stored as a [`TupleId`] to a blob page
    Str,
    StrAddr,
}

impl Types {
    pub fn size(&self) -> usize {
        match self {
            Types::U8 | Types::Char | Types::Bool | Types::I8 => 1,
            Types::U16 | Types::I16 => 2,
            Types::U32 | Types::I32 | Types::F32 => 4,
            Types::U64 | Types::I64 | Types::F64 => 8,
            Types::U128 | Types::I128 => 16,
            Types::Str | Types::StrAddr => std::mem::size_of::<TupleId>(),
        }
    }

    pub fn to_sql(&self) -> String {
        match self {
            Types::U8 => "TINYINT UNSIGNED".to_string(),
            Types::U16 => "SMALLINT UNSIGNED".to_string(),
            Types::U32 => "INT UNSIGNED".to_string(),
            // TODO: check this
            Types::U64 => "BIGINT UNSIGNED".to_string(),
            Types::U128 => "BIGINT UNSIGNED".to_string(),
            Types::I8 => "TINYINT".to_string(),
            Types::I16 => "SMALLINT".to_string(),
            Types::I32 => "INT".to_string(),
            // TODO: check this
            Types::I64 => "BIGINT".to_string(),
            Types::I128 => "BIGINT".to_string(),
            Types::F32 => "FLOAT".to_string(),
            Types::F64 => "DOUBLE".to_string(),
            Types::Bool => "BOOLEAN".to_string(),
            Types::Char => "CHAR".to_string(),
            Types::Str => "VARCHAR".to_string(),
            Types::StrAddr => unreachable!(),
        }
    }

    #[allow(unreachable_patterns)]
    pub fn from_sql(s: &str) -> Self {
        match s {
            "TINYINT UNSIGNED" => Types::U8,
            "SMALLINT UNSIGNED" => Types::U16,
            "INT UNSIGNED" => Types::U32,
            "BIGINT UNSIGNED" => Types::U64,
            "BIGINT UNSIGNED" => Types::U128,
            "TINYINT" => Types::I8,
            "SMALLINT" => Types::I16,
            "INT" => Types::I32,
            "BIGINT" => Types::I64,
            "BIGINT" => Types::I128,
            "FLOAT" => Types::F32,
            "DOUBLE" => Types::F64,
            "BOOLEAN" => Types::Bool,
            "CHAR" => Types::Char,
            "VARCHAR" => Types::Str,
            _ => panic!("Unsupported type: {}", s),
        }
    }
}
macro_rules! impl_value_methods {
    ($($variant:ident($ty:ident)),+ $(,)?) => {
        impl Value {
            $(
                pub fn $ty(&self) -> $ty {
                    if let Value::$variant(v) = self {
                        v.0.clone()
                    } else {
                        panic!("forced conversion error: {:?} => {}", self, stringify!($variant))
                    }
                }
            )*
        }
    };
}

impl Value {
    pub fn str_addr(&self) -> StrAddr {
        if let Value::Str(v) = self {
            U128(ValueFactory::from_bytes(&Types::U128, &v.to_bytes()).u128())
        } else if let Value::StrAddr(v) = self {
            *v
        } else {
            panic!("forced conversion error: {:?} => StrAddr", self)
        }
    }
}

impl Value {
    pub fn str(&self) -> String {
        unreachable!("strings are stored as pointers, use str_addr() instead")
    }
}

impl_value_methods!(
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    F32(f32),
    F64(f64),
    Bool(bool),
    Char(char),
);

#[derive(Debug, Clone)]
pub enum Value {
    U8(U8),
    U16(U16),
    U32(U32),
    U64(U64),
    U128(U128),
    I8(I8),
    I16(I16),
    I32(I32),
    I64(I64),
    I128(I128),
    F32(F32),
    F64(F64),
    Bool(Bool),
    Char(Char),
    Str(Str),
    StrAddr(StrAddr),
    Null,
}

impl AsBytes for Value {
    fn to_bytes(&self) -> Box<[u8]> {
        match self {
            Value::U8(v) => v.to_bytes(),
            Value::U16(v) => v.to_bytes(),
            Value::U32(v) => v.to_bytes(),
            Value::U64(v) => v.to_bytes(),
            Value::U128(v) => v.to_bytes(),
            Value::I8(v) => v.to_bytes(),
            Value::I16(v) => v.to_bytes(),
            Value::I32(v) => v.to_bytes(),
            Value::I64(v) => v.to_bytes(),
            Value::I128(v) => v.to_bytes(),
            Value::F32(v) => v.to_bytes(),
            Value::F64(v) => v.to_bytes(),
            Value::Bool(v) => v.to_bytes(),
            Value::Char(v) => v.to_bytes(),
            Value::Str(v) => v.to_bytes(),
            Value::StrAddr(v) => v.to_bytes(),
            Value::Null => unreachable!("can't convert null to bytes"),
        }
    }

    fn from_bytes(_bytes: &[u8]) -> Self
    where
        Self: Sized,
    {
        unimplemented!("Use ValueFactory instead");
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::U8(v) => write!(f, "{:?}", v.0),
            Value::U16(v) => write!(f, "{:?}", v.0),
            Value::U32(v) => write!(f, "{:?}", v.0),
            Value::U64(v) => write!(f, "{:?}", v.0),
            Value::U128(v) => write!(f, "{:?}", v.0),
            Value::I8(v) => write!(f, "{:?}", v.0),
            Value::I16(v) => write!(f, "{:?}", v.0),
            Value::I32(v) => write!(f, "{:?}", v.0),
            Value::I64(v) => write!(f, "{:?}", v.0),
            Value::I128(v) => write!(f, "{:?}", v.0),
            Value::F32(v) => write!(f, "{:?}", v.0),
            Value::F64(v) => write!(f, "{:?}", v.0),
            Value::Bool(v) => write!(f, "{:?}", v.0),
            Value::Char(v) => write!(f, "{:?}", v.0),
            Value::Str(v) => write!(f, "{:?}", v.0),
            Value::StrAddr(v) => write!(f, "{:?}", v.0),
        }
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

pub trait AsBytes: Debug + 'static + Display {
    fn is_null(&self) -> bool {
        false
    }
    fn to_bytes(&self) -> Box<[u8]>;
    fn from_bytes(bytes: &[u8]) -> Self
    where
        Self: Sized;
}

pub trait Primitive {
    fn default() -> Self;
    fn from_string(s: &str) -> Self;
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Str(pub String);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Char(pub char);

impl Primitive for U8 {
    fn default() -> Self {
        U8(0)
    }
    fn from_string(s: &str) -> Self {
        U8(s.parse().unwrap())
    }
}

impl AsBytes for U8 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        U8(u8::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for U16 {
    fn default() -> Self {
        U16(0)
    }
    fn from_string(s: &str) -> Self {
        U16(s.parse().unwrap())
    }
}

impl AsBytes for U16 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        U16(u16::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for U32 {
    fn default() -> Self {
        U32(0)
    }

    fn from_string(s: &str) -> Self {
        U32(s.parse().unwrap())
    }
}

impl AsBytes for U32 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        U32(u32::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for U64 {
    fn default() -> Self {
        U64(0)
    }
    fn from_string(s: &str) -> Self {
        U64(s.parse().unwrap())
    }
}

impl AsBytes for U64 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        U64(u64::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for U128 {
    fn default() -> Self {
        U128(0)
    }

    fn from_string(s: &str) -> Self {
        U128(s.parse().unwrap())
    }
}

impl AsBytes for U128 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        U128(u128::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for I8 {
    fn default() -> Self {
        I8(0)
    }

    fn from_string(s: &str) -> Self {
        I8(s.parse().unwrap())
    }
}

impl AsBytes for I8 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        I8(i8::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for I16 {
    fn default() -> Self {
        I16(0)
    }

    fn from_string(s: &str) -> Self {
        I16(s.parse().unwrap())
    }
}

impl AsBytes for I16 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        I16(i16::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for I32 {
    fn default() -> Self {
        I32(0)
    }

    fn from_string(s: &str) -> Self {
        I32(s.parse().unwrap())
    }
}

impl AsBytes for I32 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        I32(i32::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for I64 {
    fn default() -> Self {
        I64(0)
    }

    fn from_string(s: &str) -> Self {
        I64(s.parse().unwrap())
    }
}

impl AsBytes for I64 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        I64(i64::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for I128 {
    fn default() -> Self {
        I128(0)
    }

    fn from_string(s: &str) -> Self {
        I128(s.parse().unwrap())
    }
}

impl AsBytes for I128 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        I128(i128::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for F32 {
    fn default() -> Self {
        F32(0.0)
    }

    fn from_string(s: &str) -> Self {
        F32(s.parse().unwrap())
    }
}

impl AsBytes for F32 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        F32(f32::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for F64 {
    fn default() -> Self {
        F64(0.0)
    }

    fn from_string(s: &str) -> Self {
        F64(s.parse().unwrap())
    }
}

impl AsBytes for F64 {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        F64(f64::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for Bool {
    fn default() -> Self {
        Bool(false)
    }

    fn from_string(s: &str) -> Self {
        if s == "true" {
            Bool(true)
        } else {
            Bool(false)
        }
    }
}
impl AsBytes for Bool {
    fn to_bytes(&self) -> Box<[u8]> {
        vec![self.0 as u8].into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        Bool(bytes[0] != 0)
    }
}

impl Primitive for Char {
    fn default() -> Self {
        Char('\0')
    }

    fn from_string(s: &str) -> Self {
        if s.len() != 1 {
            panic!("Invalid input to char: {}", s);
        }
        Char(s.chars().next().unwrap())
    }
}

impl AsBytes for Char {
    fn to_bytes(&self) -> Box<[u8]> {
        vec![self.0 as u8].into_boxed_slice()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        Char(bytes[0] as char)
    }
}

pub type StrAddr = U128;

impl Primitive for Str {
    fn default() -> Self {
        Str(String::new())
    }
    fn from_string(s: &str) -> Self {
        Str(s.to_string())
    }
}

impl AsBytes for Str {
    /// prepend size (2 bytes) + string bytes
    fn to_bytes(&self) -> Box<[u8]> {
        let size = U16(self.0.len() as u16);
        size.to_bytes()
            .iter()
            .chain(self.0.as_bytes())
            .cloned()
            .collect::<Vec<u8>>()
            .into_boxed_slice()
    }

    /// interpret bytes as size (2 bytes) + string
    fn from_bytes(bytes: &[u8]) -> Self {
        let (_, str) = (
            U16::from_bytes(&bytes[0..2]),
            String::from_utf8(bytes[2..].to_vec()).unwrap(),
        );

        Str(str)
    }
}

impl Str {
    /// Interpret bytes as string
    pub fn from_raw_bytes(bytes: &[u8]) -> Self {
        Str(String::from_utf8(bytes[2..].to_vec()).unwrap())
    }
}

macro_rules! impl_fn {
    ($var:ident, $method:ident $(, $arg:expr)?) => {
        match $var {
            Types::Str => Value::Str(Str::$method($($arg)?)),
            Types::I64 => Value::I64(I64::$method($($arg)?)),
            Types::I128 => Value::I128(I128::$method($($arg)?)),
            Types::U64 => Value::U64(U64::$method($($arg)?)),
            Types::U128 => Value::U128(U128::$method($($arg)?)),
            Types::F64 => Value::F64(F64::$method($($arg)?)),
            Types::F32 => Value::F32(F32::$method($($arg)?)),
            Types::Bool => Value::Bool(Bool::$method($($arg)?)),
            Types::I8 => Value::I8(I8::$method($($arg)?)),
            Types::I16 => Value::I16(I16::$method($($arg)?)),
            Types::I32 => Value::I32(I32::$method($($arg)?)),
            Types::U8 => Value::U8(U8::$method($($arg)?)),
            Types::U16 => Value::U16(U16::$method($($arg)?)),
            Types::U32 => Value::U32(U32::$method($($arg)?)),
            Types::Char => Value::Char(Char::$method($($arg)?)),
            Types::StrAddr => Value::StrAddr(StrAddr::$method($($arg)?)),
        }
    };
}

macro_rules! impl_display {
    ($type:ty) => {
        impl Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

impl_display!(U8);
impl_display!(U16);
impl_display!(U32);
impl_display!(U64);
impl_display!(U128);
impl_display!(I8);
impl_display!(I16);
impl_display!(I32);
impl_display!(I64);
impl_display!(I128);
impl_display!(F32);
impl_display!(F64);
impl_display!(Bool);
impl_display!(Str);
impl_display!(Char);

pub struct ValueFactory {}

impl ValueFactory {
    pub fn default(t: &Types) -> Value {
        impl_fn!(t, default)
    }

    pub fn from_bytes(t: &Types, bytes: &[u8]) -> Value {
        impl_fn!(t, from_bytes, bytes)
    }

    pub fn from_string(t: &Types, s: &str) -> Value {
        impl_fn!(t, from_string, s)
    }

    pub fn null() -> Value {
        Value::Null
    }
}
