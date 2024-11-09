use sqlparser::ast::BinaryOperator;

use crate::{
    tuple::schema::{Field, Schema},
    types::Value,
};

#[derive(Clone, Debug)]
pub enum LogicalExpr {
    Literal(Value),
    Column(String),
    BinaryExpr(Box<BinaryExpr>),
    AliasedExpr(Box<LogicalExpr>, String),
}

impl LogicalExpr {
    pub fn print(&self) -> String {
        match self {
            LogicalExpr::Literal(v) => format!("{}", v),
            LogicalExpr::Column(v) => format!("#{}", v),
            LogicalExpr::BinaryExpr(binary_expr) => {
                let inner_expr = binary_expr.print();
                if inner_expr.starts_with('(') && inner_expr.ends_with(')') {
                    inner_expr
                } else {
                    format!("({})", inner_expr)
                }
            }
            LogicalExpr::AliasedExpr(expr, alias) => format!("{} AS {}", expr.print(), alias),
        }
    }

    pub fn to_field(&self, schema: &Schema) -> Field {
        match self {
            LogicalExpr::Literal(v) => Field::new(&format!("{}", v), v.get_type(), true),
            LogicalExpr::Column(v) => schema.fields.iter().find(|f| f.name == *v).unwrap().clone(),
            LogicalExpr::BinaryExpr(e) => e.to_field(schema),
            LogicalExpr::AliasedExpr(e, alias) => {
                let field = e.to_field(schema);
                Field::new(alias, field.ty, field.nullable)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct BinaryExpr {
    pub left: LogicalExpr,
    pub op: BinaryOperator,
    pub right: LogicalExpr,
}

impl BinaryExpr {
    pub fn new(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> Self {
        Self { left, op, right }
    }

    pub fn print(&self) -> String {
        format!("({} {} {})", self.left.print(), self.op, self.right.print())
    }

    fn to_field(&self, schema: &Schema) -> Field {
        let left = self.left.to_field(schema);
        let right = self.right.to_field(schema);

        Field::new(
            &format!("{} {} {}", left.name, self.op, right.name),
            left.ty,
            left.nullable,
        )
    }
}

pub struct BooleanBinaryExpr {
    pub left: LogicalExpr,
    pub op: BinaryOperator,
    pub right: LogicalExpr,
}

impl BooleanBinaryExpr {
    pub fn new(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> Self {
        Self { left, op, right }
    }

    pub fn print(&self) -> String {
        format!("{} {} {}", self.left.print(), self.op, self.right.print())
    }
}
