pub mod expr;
pub mod optimizer;
pub mod plan;

use expr::{BooleanBinaryExpr, LogicalExpr};
use plan::{CreateTable, Explain, Filter, Insert, LogicalPlan, Projection, Scan};
use sqlparser::ast::{
    BinaryOperator, ColumnDef, CreateTable as SqlCreateTable, Expr, Ident, Insert as SqlInsert,
    ObjectName, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    Value as SqlValue,
};

use anyhow::{anyhow, Result};

use crate::{
    catalog::Catalog,
    tuple::schema::Schema,
    types::{Types, ValueFactory},
};

pub fn build_initial_plan(statement: Statement) -> Result<LogicalPlan> {
    match statement {
        Statement::Explain {
            statement, analyze, ..
        } => build_explain(*statement, analyze),
        Statement::Insert(SqlInsert {
            table_name,
            source,
            columns,
            returning,
            ..
        }) => build_insert(table_name, source, columns, returning),
        Statement::Query(query) => {
            let Query { body, limit, .. } = *query;
            build_select(body, limit)
        }
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            ..
        } => build_update(),
        Statement::CreateTable(SqlCreateTable {
            name,
            columns,
            if_not_exists,
            ..
        }) => build_create(name, columns, if_not_exists),
        _ => unimplemented!(),
    }
}

fn build_explain(statement: Statement, analyze: bool) -> Result<LogicalPlan> {
    let root = build_initial_plan(statement)?;

    Ok(LogicalPlan::Explain(Box::new(Explain::new(root, analyze))))
}

#[allow(unused)]
fn build_query(query: Option<Box<Query>>) -> Result<Option<LogicalPlan>> {
    if query.is_none() {
        return Ok(None);
    }

    let Query { body, limit, .. } = *query.unwrap();

    let input = match *body {
        SetExpr::Select(_) => build_select(body, limit)?,
        SetExpr::Values(_) => todo!(),
        _ => unimplemented!(),
    };

    Ok(Some(input))
}

fn build_insert(
    table_name: ObjectName,
    source: Option<Box<Query>>,
    _columns: Vec<Ident>,
    _returning: Option<Vec<SelectItem>>,
) -> Result<LogicalPlan> {
    let input = build_query(source)?.unwrap();
    let table_name = table_name.0.first().unwrap().value.clone();

    let root = LogicalPlan::Insert(Box::new(Insert::new(input, table_name, Schema::default())));

    Ok(root)
}
fn build_update() -> Result<LogicalPlan> {
    todo!()
}
fn build_create(
    name: ObjectName,
    columns: Vec<ColumnDef>,
    if_not_exists: bool,
) -> Result<LogicalPlan> {
    let root = LogicalPlan::default();

    let create = CreateTable::new(
        root,
        name.0.first().unwrap().value.clone(),
        Schema::from_sql(columns),
        if_not_exists,
    );

    Ok(LogicalPlan::CreateTable(Box::new(create)))
}

fn build_select(body: Box<SetExpr>, _limit: Option<Expr>) -> Result<LogicalPlan> {
    let select = match *body {
        SetExpr::Select(ref select) => select,
        _ => unimplemented!(),
    };

    let mut root = match &select.from.first() {
        None => LogicalPlan::Empty,
        Some(TableWithJoins { relation, .. }) => {
            let name = match relation {
                TableFactor::Table { name, .. } => name.0.first().unwrap().value.clone(),
                _ => todo!(),
            };

            let schema = Catalog::get().lock().get_table(&name).unwrap().get_schema();

            LogicalPlan::Scan(Scan::new(name, schema.clone()))
        }
    };

    let filters = select.selection.clone().map(|e| match e {
        Expr::BinaryOp { left, right, op } => parse_boolean_expr(*left, op, *right),
        Expr::Value(SqlValue::Boolean(b)) => Ok(BooleanBinaryExpr::new(
            b.into(),
            BinaryOperator::Eq,
            true.into(),
        )),
        _ => todo!(),
    });

    if let Some(filter) = filters {
        root = LogicalPlan::Filter(Box::new(Filter::new(root, filter?)));
    }

    for projection in select.projection.iter() {
        if matches!(root, LogicalPlan::Empty)
            && matches!(projection, SelectItem::UnnamedExpr(Expr::Identifier(_)))
        {
            return Err(anyhow!("Can't select col without a table"));
        }
    }

    let columns = &select
        .projection
        .iter()
        .flat_map(|e| match e {
            SelectItem::UnnamedExpr(Expr::Value(SqlValue::Number(s, _))) => {
                vec![LogicalExpr::Literal(ValueFactory::from_string(
                    &Types::UInt,
                    &s,
                ))]
            }
            SelectItem::UnnamedExpr(Expr::Value(SqlValue::SingleQuotedString(s))) => {
                vec![LogicalExpr::Literal(ValueFactory::from_string(
                    &Types::Str,
                    &s,
                ))]
            }
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                vec![LogicalExpr::Column(ident.value.clone())]
            }
            SelectItem::Wildcard(_) => root
                .schema()
                .fields
                .iter()
                .map(|f| f.name.clone())
                .map(|c| LogicalExpr::Column(c))
                .collect(),
            SelectItem::UnnamedExpr(Expr::Tuple(fields)) => {
                if let Some(expr) = fields.iter().find(|e| !matches!(e, Expr::Value(_))) {
                    unimplemented!("{:?}", expr);
                };

                fields
                    .iter()
                    .map(|e| match e {
                        Expr::Value(SqlValue::Number(s, _)) => {
                            LogicalExpr::Literal(ValueFactory::from_string(&Types::UInt, s))
                        }
                        Expr::Value(SqlValue::SingleQuotedString(s)) => {
                            LogicalExpr::Literal(ValueFactory::from_string(&Types::Str, s))
                        }
                        e => todo!("{}", e),
                    })
                    .collect()
            }
            e => todo!("{}", e),
        })
        .collect::<Vec<_>>();

    root = LogicalPlan::Projection(Box::new(Projection::new(root, columns.clone())));

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
            Expr::Value(value) => {
                let (ty, v) = match value {
                    SqlValue::Number(v, _) => (Types::UInt, v),
                    SqlValue::SingleQuotedString(s) => (Types::Str, s),
                    _ => unimplemented!(),
                };

                LogicalExpr::Literal(ValueFactory::from_string(&ty, &v))
            }

            _ => unimplemented!(),
        }
    }
}

impl From<bool> for LogicalExpr {
    fn from(b: bool) -> LogicalExpr {
        LogicalExpr::Literal(ValueFactory::from_string(&Types::Bool, &b.to_string()))
    }
}
