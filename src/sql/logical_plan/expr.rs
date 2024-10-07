// just use sqlparser operators instead of writing our own
use sqlparser::ast::BinaryOperator;

use crate::types::Value;

#[allow(unused)]
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
}

#[allow(unused)]
pub struct BinaryExpr {
    left: LogicalExpr,
    op: BinaryOperator,
    right: LogicalExpr,
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
