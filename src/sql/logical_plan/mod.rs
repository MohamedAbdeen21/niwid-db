pub mod expr;
pub mod optimizer;
pub mod plan;

use expr::{BinaryExpr, BooleanBinaryExpr, LogicalExpr};
use plan::{
    CreateTable, DropTables, Explain, Filter, Insert, LogicalPlan, Projection, Scan, Truncate,
    Values,
};
use sqlparser::ast::{
    BinaryOperator, ColumnDef, CreateTable as SqlCreateTable, Expr, Ident, Insert as SqlInsert,
    ObjectName, ObjectType, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    TruncateTableTarget, Value as SqlValue, Values as SqlValues,
};

use anyhow::{anyhow, Result};

use crate::{
    catalog::Catalog,
    tuple::schema::Schema,
    txn_manager::TxnId,
    types::{Types, ValueFactory},
};

#[macro_export]
macro_rules! value {
    ($t:ident, $s:expr) => {
        ValueFactory::from_string(&Types::$t, &$s)
    };
}

pub fn build_initial_plan(statement: Statement, txn_id: Option<TxnId>) -> Result<LogicalPlan> {
    match statement {
        Statement::Explain {
            statement, analyze, ..
        } => build_explain(*statement, analyze, txn_id),
        Statement::Insert(SqlInsert {
            table_name,
            source,
            columns,
            returning,
            ..
        }) => build_insert(table_name, source, columns, returning, txn_id),
        Statement::Query(query) => {
            let Query { body, limit, .. } = *query;
            build_select(body, limit, txn_id)
        }
        Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => build_drop(object_type, if_exists, names, txn_id),
        #[allow(unused)]
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
        Statement::Truncate {
            table_names, table, ..
        } => build_truncate(table_names, table, txn_id),
        e => unimplemented!("{}", e),
    }
}

fn build_truncate(
    table_names: Vec<TruncateTableTarget>,
    table: bool,
    txn_id: Option<TxnId>,
) -> Result<LogicalPlan> {
    if !table {
        return Err(anyhow!("Did you mean 'TRUNCATE TABLE'?"));
    }

    // TODO: handle multiple tables
    let table_name = table_names
        .first()
        .unwrap()
        .name
        .0
        .first()
        .unwrap()
        .value
        .clone();

    if Catalog::get()
        .lock()
        .get_table(&table_name, txn_id)
        .is_none()
    {
        return Err(anyhow!("Table {} does not exist", table_name));
    }

    Ok(LogicalPlan::Truncate(Truncate::new(table_name)))
}

fn build_drop(
    object_type: ObjectType,
    if_exists: bool,
    names: Vec<ObjectName>,
    txn_id: Option<TxnId>,
) -> Result<LogicalPlan> {
    if object_type != ObjectType::Table {
        return Err(anyhow!("Unsupported object type: {:?}", object_type));
    }

    let names: Vec<_> = names
        .iter()
        .map(|n| n.0.first().unwrap().value.clone())
        .collect();

    println!("names: {:?}", names);

    if !if_exists {
        let non_existant: Vec<String> = names
            .iter()
            .filter(|&name| Catalog::get().lock().get_table(name, txn_id).is_none())
            .cloned()
            .collect();

        if !non_existant.is_empty() {
            return Err(anyhow!("Table(s) {} don't exist", non_existant.join(", ")));
        }
    }

    Ok(LogicalPlan::DropTables(DropTables::new(names, if_exists)))
}

fn build_explain(
    statement: Statement,
    analyze: bool,
    txn_id: Option<TxnId>,
) -> Result<LogicalPlan> {
    let root = build_initial_plan(statement, txn_id)?;

    Ok(LogicalPlan::Explain(Box::new(Explain::new(root, analyze))))
}

fn build_expr(expr: Expr) -> Result<LogicalExpr> {
    match expr {
        Expr::Value(SqlValue::Number(n, _)) => Ok(LogicalExpr::Literal(value!(UInt, n))),
        Expr::Value(SqlValue::SingleQuotedString(s)) => Ok(LogicalExpr::Literal(value!(Str, s))),
        Expr::Identifier(Ident { value, .. }) => Ok(LogicalExpr::Column(value)),
        Expr::BinaryOp { left, op, right } => Ok(LogicalExpr::BinaryExpr(Box::new(
            BinaryExpr::new(build_expr(*left)?, op, build_expr(*right)?),
        ))),
        Expr::Nested(e) => build_expr(*e),
        e => todo!("{:?}", e),
    }
}

fn build_values(rows: Vec<Vec<Expr>>) -> Result<LogicalPlan> {
    let rows = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|expr| match expr {
                    Expr::Value(_) => build_expr(expr),
                    e => todo!("{}", e),
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    let fields = rows
        .first()
        .unwrap()
        .iter()
        .map(|e| e.to_field(&Schema::default()))
        .collect();

    Ok(LogicalPlan::Values(Values::new(rows, Schema::new(fields))))
}

fn build_query(query: Option<Box<Query>>, txn_id: Option<TxnId>) -> Result<Option<LogicalPlan>> {
    if query.is_none() {
        return Ok(None);
    }

    let Query { body, limit, .. } = *query.unwrap();

    let input = match *body {
        SetExpr::Select(_) => build_select(body, limit, txn_id)?,
        SetExpr::Values(SqlValues { rows, .. }) => build_values(rows)?,
        _ => unimplemented!(),
    };

    Ok(Some(input))
}

fn build_insert(
    table_name: ObjectName,
    source: Option<Box<Query>>,
    _columns: Vec<Ident>,
    _returning: Option<Vec<SelectItem>>,
    txn_id: Option<TxnId>,
) -> Result<LogicalPlan> {
    let input = build_query(source, txn_id)?.unwrap();
    let table_name = table_name.0.first().unwrap().value.clone();
    let schema = Catalog::get()
        .lock()
        .get_schema(&table_name, txn_id)
        .unwrap();

    let input_schema = input.schema();
    let input_types: Vec<_> = input_schema.fields.iter().map(|f| &f.ty).collect();
    let table_types: Vec<_> = schema.fields.iter().map(|f| &f.ty).collect();

    if input_types != table_types {
        return Err(anyhow!(
            "Schema mismatch: {:?} vs {:?}",
            input_types,
            table_types
        ));
    }

    let root = LogicalPlan::Insert(Box::new(Insert::new(
        input,
        table_name,
        schema,
        Schema::default(),
    )));

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

fn build_select(
    body: Box<SetExpr>,
    _limit: Option<Expr>,
    txn_id: Option<TxnId>,
) -> Result<LogicalPlan> {
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

            let schema = Catalog::get()
                .lock()
                .get_table(&name, txn_id)
                .unwrap()
                .get_schema();

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
                vec![Ok(LogicalExpr::Literal(value!(UInt, *s)))]
            }
            SelectItem::UnnamedExpr(Expr::Value(SqlValue::SingleQuotedString(s))) => {
                vec![Ok(LogicalExpr::Literal(value!(Str, *s)))]
            }
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let name = ident.value.clone();
                if !root.schema().fields.iter().any(|f| f.name == name) {
                    vec![Err(anyhow!("Column {} doesn't exist", name))]
                } else {
                    vec![Ok(LogicalExpr::Column(ident.value.clone()))]
                }
            }
            SelectItem::Wildcard(_) => root
                .schema()
                .fields
                .iter()
                .map(|f| f.name.clone())
                .map(|c| Ok(LogicalExpr::Column(c)))
                .collect(),
            SelectItem::UnnamedExpr(Expr::Tuple(fields)) => fields
                .iter()
                .map(|e| match e {
                    Expr::Value(SqlValue::Number(s, _)) => {
                        Ok(LogicalExpr::Literal(value!(UInt, *s)))
                    }
                    Expr::Value(SqlValue::SingleQuotedString(s)) => {
                        Ok(LogicalExpr::Literal(value!(Str, *s)))
                    }
                    e => todo!("{}", e),
                })
                .collect(),
            SelectItem::UnnamedExpr(Expr::BinaryOp { left, right, op }) => {
                let left = match build_expr(*left.clone()) {
                    Ok(expr) => expr,
                    Err(e) => return vec![Err(e)],
                };
                let right = match build_expr(*right.clone()) {
                    Ok(expr) => expr,
                    Err(e) => return vec![Err(e)],
                };
                vec![Ok(LogicalExpr::BinaryExpr(Box::new(BinaryExpr::new(
                    left,
                    op.clone(),
                    right,
                ))))]
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = match build_expr(expr.clone()) {
                    Ok(expr) => expr,
                    Err(e) => return vec![Err(e)],
                };

                vec![Ok(LogicalExpr::AliasedExpr(
                    Box::new(expr),
                    alias.value.clone(),
                ))]
            }

            e => todo!("{:?}", e),
        })
        .collect::<Result<Vec<_>>>()?;

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
        LogicalExpr::Literal(value!(Bool, b.to_string()))
    }
}
