use std::fmt::Debug;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::tuple::TupleExt;
use crate::tuple::TupleId;
use crate::tuple::TUPLE_ID_SIZE;

#[allow(unused)]
#[derive(PartialEq, Eq, Clone, Debug)] // others
#[derive(Serialize, Deserialize)] // for schema serde
pub enum Types {
    UInt,
    Int,
    Float,
    Bool,
    Char,
    /// string is stored as a [`TupleId`] to a blob page
    Str,
    StrAddr,
}

impl Types {
    pub fn size(&self) -> usize {
        match self {
            Types::Char | Types::Bool => 1,
            Types::Str | Types::StrAddr => TUPLE_ID_SIZE,
            Types::UInt | Types::Int | Types::Float => 4,
        }
    }

    pub fn to_sql(&self) -> String {
        match self {
            Types::UInt => "INT UNSIGNED".to_string(),
            Types::Int => "INT".to_string(),
            Types::Float => "FlOAT".to_string(),
            Types::Bool => "BOOLEAN".to_string(),
            Types::Char => "CHAR".to_string(),
            Types::Str => "VARCHAR".to_string(),
            Types::StrAddr => unreachable!(),
        }
    }

    #[allow(unreachable_patterns)]
    pub fn from_sql(s: &str) -> Self {
        match s {
            "UINT" | "INT UNSIGNED" => Types::UInt,
            "INT" => Types::Int,
            "FLOAT" => Types::Float,
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
            TupleId::from_bytes((*v.to_bytes()).try_into().unwrap())
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

impl_value_methods!(Int(i32), Float(f32), UInt(u32), Bool(bool), Char(char),);

pub type StrAddr = TupleId;

#[derive(Debug, Clone)]
pub enum Value {
    UInt(UInt),
    Int(Int),
    Float(Float),
    Bool(Bool),
    Char(Char),
    Str(Str),
    StrAddr(StrAddr),
    Null,
}

impl Value {
    pub fn get_type(&self) -> Types {
        match self {
            Value::Bool(_) => Types::Bool,
            Value::Char(_) => Types::Char,
            Value::Str(_) => Types::Str,
            Value::StrAddr(_) => Types::StrAddr,
            Value::UInt(_) => Types::UInt,
            Value::Int(_) => Types::Int,
            Value::Float(_) => Types::Float,
            Value::Null => Types::Char, // FIXME:
        }
    }
}

impl AsBytes for Value {
    fn to_bytes(&self) -> Box<[u8]> {
        match self {
            Value::Bool(v) => v.to_bytes(),
            Value::Char(v) => v.to_bytes(),
            Value::Str(v) => v.to_bytes(),
            Value::StrAddr(v) => v.to_bytes().into_boxed_slice(),
            Value::Null => unreachable!("can't convert null to bytes"),
            Value::UInt(v) => v.to_bytes(),
            Value::Int(v) => v.to_bytes(),
            Value::Float(v) => v.to_bytes(),
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
            Value::Int(v) => write!(f, "{:?}", v.0),
            Value::UInt(v) => write!(f, "{:?}", v.0),
            Value::Float(v) => write!(f, "{:?}", v.0),
            Value::Bool(v) => write!(f, "{:?}", v.0),
            Value::Char(v) => write!(f, "{:?}", v.0),
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

trait FromString {
    fn from_string(s: &str) -> Self;
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Char(pub char);

impl Primitive for UInt {
    fn default() -> Self {
        UInt(0)
    }

    fn from_string(s: &str) -> Self {
        UInt(s.parse().unwrap())
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

    fn from_string(s: &str) -> Self {
        Int(s.parse().unwrap())
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

    fn from_string(s: &str) -> Self {
        Float(s.parse().unwrap())
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
            Types::Char => Value::Char(Char::$method($($arg)?)),
            Types::StrAddr => Value::StrAddr(TupleId::$method($($arg)?)),
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
