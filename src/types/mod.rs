use anyhow::bail;
use anyhow::Result;
use std::fmt::Debug;
use std::fmt::Display;
use std::num::ParseFloatError;
use std::num::ParseIntError;

use serde::{Deserialize, Serialize};

use crate::errors::Error;
use crate::tuple::TupleExt;
use crate::tuple::TupleId;
use crate::tuple::TUPLE_ID_SIZE;

#[derive(PartialEq, Eq, Clone, Debug)] // others
#[derive(Serialize, Deserialize)] // for schema serde
pub enum Types {
    UInt,
    Int,
    Float,
    Bool,
    /// string is stored as a [`TupleId`] to a blob page
    Str,
    StrAddr,
    /// used only by the query engine to determine compatibility, mapped to correct type
    /// during tuple creation
    Null,
}

impl Types {
    pub fn size(&self) -> usize {
        match self {
            Types::Null => unreachable!("Nulls should be mapped correctly during Tuple creation"),
            Types::Bool => 1,
            Types::Str | Types::StrAddr => TUPLE_ID_SIZE,
            Types::UInt | Types::Int | Types::Float => 4,
        }
    }

    pub fn to_sql(&self) -> String {
        match self {
            Types::UInt => "UINT".to_string(),
            Types::Int => "INT".to_string(),
            Types::Float => "FLOAT".to_string(),
            Types::Bool => "BOOLEAN".to_string(),
            Types::Str => "TEXT".to_string(),
            Types::StrAddr | Types::Null => unreachable!(),
        }
    }

    // used when checking inserted rows for compatibility
    // it is the user's responsibility to ensure that values match
    // the table's schema (inserting UINT value in an INT column)
    pub fn is_compatible(&self, other: &Types) -> bool {
        matches!(
            (self, other),
            (Types::UInt, Types::UInt)
                | (Types::Int, Types::Int)
                | (Types::Float, Types::Float)
                | (Types::Bool, Types::Bool)
                | (Types::Str, Types::Str)
                | (Types::Int, Types::UInt)
                | (Types::StrAddr, Types::StrAddr)
                | (Types::Null, _)
                | (_, Types::Null)
        )
    }

    pub fn from_sql(s: &str) -> Result<Self> {
        Ok(match s.to_uppercase().as_str() {
            "UINT" | "INT UNSIGNED" => Types::UInt,
            "INT" => Types::Int,
            "FLOAT" => Types::Float,
            "BOOLEAN" | "BOOL" => Types::Bool,
            "VARCHAR" | "TEXT" => Types::Str,
            _ => bail!(Error::Unsupported(format!("Unsupported type: {}", s))),
        })
    }
}

macro_rules! impl_cast_to_u32 {
    ($($variant:ident),+ $(,)?) => {
        impl Value {
            pub fn as_u32(&self) -> u32 {
                match self {
                    $(
                        Value::$variant(v) => v.0 as u32,
                    )+
                    _ => panic!(
                        "Internal Error: forced cast error: {:?} => u32",
                        self,
                    ),
                }
            }
        }
    };
}

macro_rules! impl_value_methods {
    ($($variant:ident($ty:ident)),+ $(,)?) => {
        impl Value {
            $(
                pub fn $ty(&self) -> $ty {
                    if let Value::$variant(v) = self {
                        v.0.clone()
                    } else {
                        panic!("Internal Error: forced conversion error: {:?} => {}", self, stringify!($variant))
                    }
                }
            )*
        }
    };
}

impl Value {
    pub fn str_addr(&self) -> StrAddr {
        if let Value::Str(v) = self {
            TupleId::from_bytes((*v.to_bytes()).try_into().unwrap())
        } else if let Value::StrAddr(v) = self {
            *v
        } else {
            panic!(
                "Internal Error: forced conversion error: {:?} => StrAddr",
                self
            )
        }
    }
}

impl Value {
    pub fn str(&self) -> String {
        if let Value::Str(v) = self {
            v.0.clone()
        } else {
            panic!("Internal Error: forced conversion error: {:?} => Str", self)
        }
    }
}

impl_value_methods!(Int(i32), Float(f32), UInt(u32), Bool(bool));
impl_cast_to_u32!(Int, Float, UInt);

pub type StrAddr = TupleId;

#[derive(Debug, Clone)]
pub enum Value {
    UInt(UInt),
    Int(Int),
    Float(Float),
    Bool(Bool),
    Str(Str),
    StrAddr(StrAddr),
    Null,
}

impl Value {
    pub fn to_string_unquoted(&self) -> String {
        match self {
            Value::Float(_) => format!("{}", self), // print the exact value, without truncation
            Value::Int(v) => v.to_string(),
            Value::Bool(v) => v.to_string(),
            Value::UInt(v) => v.to_string(),
            Value::Str(v) => v.to_string(),
            Value::Null => "null".to_string(),
            Value::StrAddr(_) => unreachable!(),
        }
    }

    pub fn add(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::UInt(UInt(l)), Value::UInt(UInt(r))) => Ok(Value::UInt(UInt(l + r))),
            (Value::Int(Int(l)), Value::Int(Int(r))) => Ok(Value::Int(Int(l + r))),
            (Value::Float(Float(l)), Value::Float(Float(r))) => Ok(Value::Float(Float(l + r))),
            (Value::UInt(UInt(l)), Value::Int(Int(r))) => Ok(Value::Int(Int(*l as i32 + r))),
            (Value::Int(Int(l)), Value::UInt(UInt(r))) => Ok(Value::Int(Int(l + *r as i32))),
            (Value::Int(Int(l)), Value::Float(Float(r))) => Ok(Value::Float(Float(*l as f32 + r))),
            (Value::Float(Float(l)), Value::Int(Int(r))) => Ok(Value::Float(Float(l + *r as f32))),
            (l, r) => bail!(Error::Unimplemented(format!("{} + {}", l, r))),
        }
    }

    pub fn sub(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::UInt(UInt(l)), Value::UInt(UInt(r))) => Ok(Value::UInt(UInt(l - r))),
            (Value::Int(Int(l)), Value::Int(Int(r))) => Ok(Value::Int(Int(l - r))),
            (Value::Float(Float(l)), Value::Float(Float(r))) => Ok(Value::Float(Float(l - r))),
            (Value::UInt(UInt(l)), Value::Int(Int(r))) => Ok(Value::Int(Int(*l as i32 - r))),
            (Value::Int(Int(l)), Value::UInt(UInt(r))) => Ok(Value::Int(Int(l - *r as i32))),
            (Value::Int(Int(l)), Value::Float(Float(r))) => Ok(Value::Float(Float(*l as f32 - r))),
            (Value::Float(Float(l)), Value::Int(Int(r))) => Ok(Value::Float(Float(l - *r as f32))),
            (l, r) => bail!(Error::Unimplemented(format!("{} - {}", l, r))),
        }
    }

    pub fn mul(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::UInt(UInt(l)), Value::UInt(UInt(r))) => Ok(Value::UInt(UInt(l * r))),
            (Value::Int(Int(l)), Value::Int(Int(r))) => Ok(Value::Int(Int(l * r))),
            (Value::Float(Float(l)), Value::Float(Float(r))) => Ok(Value::Float(Float(l * r))),
            (Value::UInt(UInt(l)), Value::Int(Int(r))) => Ok(Value::Int(Int(*l as i32 * r))),
            (Value::Int(Int(l)), Value::UInt(UInt(r))) => Ok(Value::Int(Int(l * *r as i32))),
            (Value::Int(Int(l)), Value::Float(Float(r))) => Ok(Value::Float(Float(*l as f32 * r))),
            (Value::Float(Float(l)), Value::Int(Int(r))) => Ok(Value::Float(Float(l * *r as f32))),
            (Value::UInt(UInt(l)), Value::Float(r)) => Ok(Value::Float(Float(*l as f32 * r.0))),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Float(Float(l)), Value::UInt(UInt(r))) => {
                Ok(Value::Float(Float(l * *r as f32)))
            }
            (l, r) => bail!(Error::Unimplemented(format!("{} * {}", l, r))),
        }
    }

    pub fn div(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (_, Value::UInt(UInt(0))) | (_, Value::Int(Int(0))) | (_, Value::Float(Float(0.0))) => {
                bail!(Error::DivisionByZero)
            }
            (Value::UInt(UInt(l)), Value::UInt(UInt(r))) => Ok(Value::UInt(UInt(l / r))),
            (Value::Int(Int(l)), Value::Int(Int(r))) => Ok(Value::Int(Int(l / r))),
            (Value::Float(Float(l)), Value::Float(Float(r))) => Ok(Value::Float(Float(l / r))),
            (Value::UInt(UInt(l)), Value::Int(Int(r))) => Ok(Value::Int(Int(*l as i32 / r))),
            (Value::Int(Int(l)), Value::UInt(UInt(r))) => Ok(Value::Int(Int(l / *r as i32))),
            (Value::Int(Int(l)), Value::Float(Float(r))) => Ok(Value::Float(Float(*l as f32 / r))),
            (Value::Float(Float(l)), Value::Int(Int(r))) => Ok(Value::Float(Float(l / *r as f32))),
            (l, r) => bail!(Error::Unimplemented(format!("{} / {}", l, r))),
        }
    }

    pub fn and(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::Bool(Bool(l)), Value::Bool(Bool(r))) => Ok(Value::Bool(Bool(*l && *r))),
            (l, r) => bail!(Error::Unimplemented(format!("{} && {}", l, r))),
        }
    }

    pub fn or(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::Bool(Bool(l)), Value::Bool(Bool(r))) => Ok(Value::Bool(Bool(*l || *r))),
            (l, r) => bail!(Error::Unimplemented(format!("{} || {}", l, r))),
        }
    }

    pub fn equ(&self, other: &Self) -> Result<bool> {
        match (self, other) {
            (Value::Null, Value::Null) => Ok(true),
            (_, Value::Null) | (Value::Null, _) => Ok(false),
            (Value::UInt(l), Value::UInt(r)) => Ok(l == r),
            (Value::Int(l), Value::Int(r)) => Ok(l == r),
            (Value::Float(l), Value::Float(r)) => Ok(l == r),
            (Value::Bool(l), Value::Bool(r)) => Ok(l == r),
            (Value::Str(l), Value::Str(r)) => Ok(l == r),
            (Value::Int(Int(l)), Value::UInt(UInt(r))) => Ok(*l as u32 == *r),
            (Value::UInt(UInt(l)), Value::Int(Int(r))) => Ok(*l == *r as u32),
            (l, r) => bail!(Error::Unimplemented(format!("{} = {}", l, r))),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.equ(other).unwrap()
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::UInt(l), Value::UInt(r)) => l.partial_cmp(r),
            (Value::Int(l), Value::Int(r)) => l.partial_cmp(r),
            (Value::Float(l), Value::Float(r)) => l.partial_cmp(r),
            (Value::Bool(l), Value::Bool(r)) => l.partial_cmp(r),
            (Value::Str(l), Value::Str(r)) => l.partial_cmp(r),
            (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
            (Value::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Value::Null) => Some(std::cmp::Ordering::Greater),
            (Value::Int(Int(l)), Value::UInt(UInt(r))) => l.partial_cmp(&(*r as i32)),
            (Value::UInt(UInt(l)), Value::Int(Int(r))) => (*l as i32).partial_cmp(r),
            (Value::UInt(UInt(l)), Value::Float(Float(r))) => (*l as f32).partial_cmp(r),
            (Value::Float(Float(l)), Value::UInt(UInt(r))) => l.partial_cmp(&(*r as f32)),
            (Value::Int(Int(l)), Value::Float(Float(r))) => (*l as f32).partial_cmp(r),
            (Value::Float(Float(l)), Value::Int(Int(r))) => l.partial_cmp(&(*r as f32)),
            _ => None,
        }
    }
}

impl Value {
    pub fn get_type(&self) -> Types {
        match self {
            Value::Bool(_) => Types::Bool,
            Value::Str(_) => Types::Str,
            Value::StrAddr(_) => Types::StrAddr,
            Value::UInt(_) => Types::UInt,
            Value::Int(_) => Types::Int,
            Value::Float(_) => Types::Float,
            Value::Null => Types::Null,
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Bool(Bool(v)) => *v,
            Value::Null => false,
            Value::UInt(UInt(v)) => *v != 0,
            Value::Int(Int(v)) => *v != 0,
            Value::Float(Float(v)) => *v != 0.0,
            Value::Str(Str(v)) => !v.is_empty(),
            Value::StrAddr(_) => unreachable!(),
        }
    }
}

impl AsBytes for Value {
    fn to_bytes(&self) -> Box<[u8]> {
        match self {
            Value::Bool(v) => v.to_bytes(),
            Value::Str(v) => v.to_bytes(),
            Value::StrAddr(v) => v.to_bytes().into_boxed_slice(),
            Value::UInt(v) => v.to_bytes(),
            Value::Int(v) => v.to_bytes(),
            Value::Float(v) => v.to_bytes(),
            Value::Null => unreachable!("can't convert null to bytes"),
        }
    }

    fn from_bytes(_bytes: &[u8]) -> Self
    where
        Self: Sized,
    {
        unreachable!("This is a stub method. Use ValueFactory, or `lit!` macro instead");
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Int(v) => write!(f, "{:?}", v.0),
            Value::UInt(v) => write!(f, "{:?}", v.0),
            Value::Float(v) => write!(f, "{:?}", v.0),
            Value::Bool(v) => write!(f, "{:?}", v.0),
            Value::Str(v) => write!(f, "{:?}", v.0),
            Value::StrAddr(v) => write!(f, "{:?}", v),
        }
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

pub trait AsBytes: Debug + 'static + Display {
    fn to_bytes(&self) -> Box<[u8]>;
    fn from_bytes(bytes: &[u8]) -> Self
    where
        Self: Sized;
}

pub trait Primitive: Sized {
    fn default() -> Self;
    fn from_string(s: &str) -> Result<Self>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct UInt(pub u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Int(pub i32);
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Float(pub f32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bool(pub bool);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Str(pub String);

impl Primitive for UInt {
    fn default() -> Self {
        UInt(0)
    }

    fn from_string(s: &str) -> Result<Self> {
        Ok(UInt(s.parse().map_err(|e: ParseIntError| {
            Error::ParseFailed(s.to_string(), Types::UInt, e.to_string())
        })?))
    }
}

impl AsBytes for UInt {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        UInt(u32::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for Int {
    fn default() -> Self {
        Int(0)
    }

    fn from_string(s: &str) -> Result<Self> {
        Ok(Int(s.parse().map_err(|e: ParseIntError| {
            Error::ParseFailed(s.to_string(), Types::Int, e.to_string())
        })?))
    }
}

impl AsBytes for Int {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        Int(i32::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for Float {
    fn default() -> Self {
        Float(0.0)
    }

    fn from_string(s: &str) -> Result<Self> {
        Ok(Float(s.parse().map_err(|e: ParseFloatError| {
            Error::ParseFailed(s.to_string(), Types::Float, e.to_string())
        })?))
    }
}

impl AsBytes for Float {
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_ne_bytes().to_vec().into_boxed_slice()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        Float(f32::from_ne_bytes(bytes.try_into().unwrap()))
    }
}

impl Primitive for Bool {
    fn default() -> Self {
        Bool(false)
    }

    fn from_string(s: &str) -> Result<Self> {
        Ok(Bool(s == "true"))
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

impl Primitive for Str {
    fn default() -> Self {
        Str(String::new())
    }
    fn from_string(s: &str) -> Result<Self> {
        Ok(Str(s.to_string()))
    }
}

impl AsBytes for Str {
    /// prepend size (2 bytes) + string bytes
    fn to_bytes(&self) -> Box<[u8]> {
        let size = self.0.len() as u16;
        size.to_ne_bytes()
            .iter()
            .chain(self.0.as_bytes())
            .cloned()
            .collect::<Vec<u8>>()
            .into_boxed_slice()
    }

    /// interpret bytes as size (2 bytes) + string
    fn from_bytes(bytes: &[u8]) -> Self {
        let (_, str) = (
            u16::from_ne_bytes(bytes[0..2].try_into().unwrap()),
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
            Types::Float => Value::Float(Float::$method($($arg)?)),
            Types::Bool => Value::Bool(Bool::$method($($arg)?)),
            Types::UInt => Value::UInt(UInt::$method($($arg)?)),
            Types::Int => Value::Int(Int::$method($($arg)?)),
            Types::StrAddr => Value::StrAddr(TupleId::$method($($arg)?)),
            Types::Null => unreachable!(),
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

impl_display!(Float);
impl_display!(UInt);
impl_display!(Int);
impl_display!(Bool);
impl_display!(Str);

#[macro_export]
macro_rules! lit {
    ($t:ident, $s:expr) => {
        ValueFactory::from_string(&Types::$t, $s)
    };
}

pub struct ValueFactory {}

impl ValueFactory {
    pub fn default(t: &Types) -> Value {
        impl_fn!(t, default)
    }

    pub fn from_bytes(t: &Types, bytes: &[u8]) -> Value {
        impl_fn!(t, from_bytes, bytes)
    }

    pub fn from_string(t: &Types, s: impl Into<String>) -> Result<Value> {
        let v = match t {
            Types::Str => Value::Str(Str::from_string(&s.into())?),
            Types::Float => Value::Float(Float::from_string(&s.into())?),
            Types::UInt => Value::UInt(UInt::from_string(&s.into())?),
            Types::Int => Value::Int(Int::from_string(&s.into())?),
            Types::Bool => Value::Bool(Bool::from_string(&s.into())?),
            Types::Null | Types::StrAddr => unreachable!(),
        };

        Ok(v)
    }

    pub fn null() -> Value {
        Value::Null
    }
}

#[cfg(test)]
mod test {
    use super::Types;
    use sqllogictest::ColumnType;

    impl ColumnType for Types {
        fn from_char(value: char) -> Option<Self> {
            match value {
                'I' => Some(Types::Int),
                'S' => Some(Types::Str),
                'U' => Some(Types::UInt),
                'B' => Some(Types::Bool),
                'F' => Some(Types::Float),
                _ => None,
            }
        }

        fn to_char(&self) -> char {
            match self {
                Types::Int => 'I',
                Types::Str => 'S',
                Types::UInt => 'U',
                Types::Bool => 'B',
                Types::Float => 'F',
                Types::StrAddr | Types::Null => unreachable!(),
            }
        }
    }
}
