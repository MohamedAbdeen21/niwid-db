use sqlparser::ast::BinaryOperator; // just use sqlparser operators instead of writing our own

use crate::{
    tuple::schema::{Field, Schema},
    types::Value,
};

#[allow(unused)]
#[derive(Clone, Debug)]
pub enum LogicalExpr {
    Literal(Value),
    Column(String),
}

impl LogicalExpr {
    pub fn print(&self) -> String {
        match self {
            LogicalExpr::Literal(v) => format!("{}", v),
            LogicalExpr::Column(v) => format!("#{}", v),
        }
    }

    pub fn to_field(&self, schema: &Schema) -> Field {
        match self {
            LogicalExpr::Literal(v) => Field::new(&format!("{}", v), v.get_type(), true),
            LogicalExpr::Column(v) => schema.fields.iter().find(|f| f.name == *v).unwrap().clone(),
        }
    }
}

#[allow(unused)]
pub struct BinaryExpr {
    left: LogicalExpr,
    op: BinaryOperator,
    right: LogicalExpr,
}

impl BinaryExpr {
    #[allow(unused)]
    pub fn new(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> Self {
        Self { left, op, right }
    }
}

pub struct BooleanBinaryExpr {
    left: LogicalExpr,
    op: BinaryOperator,
    right: LogicalExpr,
}

impl BooleanBinaryExpr {
    #[allow(unused)]
    pub fn new(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> Self {
        Self { left, op, right }
    }

    pub fn print(&self) -> String {
        format!("{} {} {}", self.left.print(), self.op, self.right.print())
    }
}
