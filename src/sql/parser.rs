use anyhow::Result;
use sqlparser::ast::{
    BinaryOperator, CreateTable, Expr, Insert, Query, SelectItem, SetExpr, Statement, TableFactor,
    Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::catalog::Catalog;
use crate::sql::logical_plan::expr::{BooleanBinaryExpr, LogicalExpr};
use crate::sql::logical_plan::plan::Filter;
use crate::types::{Types, ValueFactory};

use super::logical_plan::plan::Scan;
use super::logical_plan::plan::{LogicalPlan, Projection};

pub fn parse(sql: &str) -> Result<LogicalPlan> {
    let statment = Parser::new(&GenericDialect)
        .try_with_sql(&sql)?
        .parse_statement()?;

    match statment {
        Statement::Insert(Insert {
            table_name, source, ..
        }) => parse_insert(),
        Statement::Query(query) => {
            let Query { body, limit, .. } = *query;
            parse_select(body, limit)
        }
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            ..
        } => parse_update(),
        Statement::CreateTable(CreateTable {
            name,
            columns,
            if_not_exists,
            ..
        }) => parse_create(),
        _ => unimplemented!(),
    }
}

pub fn parse_query(_query: Option<Box<Query>>) -> Result<Option<LogicalPlan>> {
    todo!()
}
pub fn parse_insert() -> Result<LogicalPlan> {
    todo!()
}
pub fn parse_update() -> Result<LogicalPlan> {
    todo!()
}
pub fn parse_create() -> Result<LogicalPlan> {
    todo!()
}
pub fn parse_select(body: Box<SetExpr>, _limit: Option<Expr>) -> Result<LogicalPlan> {
    let select = match *body {
        SetExpr::Select(ref select) => select,
        _ => unimplemented!(),
    };

    let table_name = match &select.from.first().unwrap().relation {
        TableFactor::Table { name, .. } => name.0.first().unwrap().value.clone(),
        _ => todo!(),
    };

    // TODO: Singleton catalog to avoid re-building whole catalog
    let schema = Catalog::new()?.get_table(&table_name).unwrap().get_schema();

    let mut root = LogicalPlan::Scan(Scan::new(table_name, schema.clone()));

    let columns = &select
        .projection
        .iter()
        .flat_map(|e| match e {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => vec![ident.value.clone()],
            SelectItem::Wildcard(_) => schema.fields.iter().map(|f| f.name.clone()).collect(),
            _ => todo!(),
        })
        .collect::<Vec<_>>();

    root = LogicalPlan::Projection(Box::new(Projection::new(root, columns.clone())));

    let filters = select.selection.clone().map(|e| match e {
        Expr::BinaryOp { left, right, op } => parse_boolean_expr(*left, op, *right),
        _ => todo!(),
    });

    if let Some(filter) = filters {
        root = LogicalPlan::Filter(Box::new(Filter::new(root, filter?)));
    }

    println!("{}", root.print());

    Ok(root)
}

fn parse_boolean_expr(left: Expr, op: BinaryOperator, right: Expr) -> Result<BooleanBinaryExpr> {
    if !matches!(
        op,
        BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
    ) {
        return Err(anyhow::anyhow!("Not a binary Operator: {:?}", op));
    }

    Ok(BooleanBinaryExpr::new(left.into(), op, right.into()))
}

// convenience method
impl From<Expr> for LogicalExpr {
    fn from(expr: Expr) -> LogicalExpr {
        match expr {
            Expr::Identifier(ident) => LogicalExpr::Column(ident.to_string()),
            Expr::Value(SqlValue::Number(v, _)) => {
                LogicalExpr::Literal(ValueFactory::from_string(&Types::U128, &v))
            }
            _ => unimplemented!(),
        }
    }
}
