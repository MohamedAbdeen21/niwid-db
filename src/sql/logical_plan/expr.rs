use sqlparser::ast::BinaryOperator;

pub enum LogicalExpr {
    Literal(String),
    Column(String),
}

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
