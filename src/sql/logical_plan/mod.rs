pub mod expr;
pub mod optimizer;
pub mod plan;

use expr::{BinaryExpr, BooleanBinaryExpr, LogicalExpr};
use plan::{
    CreateTable, Delete, DropTables, Explain, Filter, IndexScan, Insert, Join, LogicalPlan,
    Projection, Scan, Truncate, Update, Values,
};
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ColumnDef, CreateTable as SqlCreateTable,
    Delete as SqlDelete, Expr, FromTable, Ident, Insert as SqlInsert, Join as SqlJoin,
    JoinConstraint, JoinOperator, ObjectName, ObjectType, Query, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, TruncateTableTarget, Value as SqlValue, Values as SqlValues,
};

use anyhow::{anyhow, bail, Result};

use crate::catalog::ArcCatalog;
use crate::errors::Error;
use crate::lit;
use crate::tuple::schema::Schema;
use crate::txn_manager::TxnId;
use crate::types::{Types, Value, ValueFactory};

pub struct LogicalPlanBuilder {
    catalog: ArcCatalog,
}

impl LogicalPlanBuilder {
    pub fn new(catalog: ArcCatalog) -> Self {
        Self { catalog }
    }
}

impl LogicalPlanBuilder {
    pub fn build_initial_plan(
        &self,
        statement: Statement,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        match statement {
            Statement::Explain {
                statement, analyze, ..
            } => self.build_explain(*statement, analyze, txn_id),
            Statement::Insert(SqlInsert {
                table_name,
                source,
                columns,
                returning,
                ..
            }) => self.build_insert(table_name, source, columns, returning, txn_id),
            Statement::Query(query) => {
                let Query { body, limit, .. } = *query;
                self.build_select(body, limit, txn_id)
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => self.build_drop(object_type, if_exists, names, txn_id),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.build_update(table, assignments, selection, txn_id),
            Statement::CreateTable(SqlCreateTable {
                name,
                columns,
                if_not_exists,
                ..
            }) => self.build_create(name, columns, if_not_exists),
            Statement::Truncate {
                table_names, table, ..
            } => self.build_truncate(table_names, table, txn_id),
            Statement::StartTransaction { .. } => self.build_start_transaction(),
            Statement::Commit { .. } => self.build_commit_transaction(),
            Statement::Rollback { .. } => self.build_rollback_transaction(),
            Statement::Delete(SqlDelete {
                from, selection, ..
            }) => self.build_delete(from, selection, txn_id),
            e => unimplemented!("{}", e),
        }
    }

    fn build_delete(
        &self,
        from: FromTable,
        selection: Option<Expr>,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        let filter = match selection {
            Some(expr) => build_expr(&expr)?,
            None => {
                return Err(anyhow!(
                    "Delete statement must have a where clause, else use truncate"
                ))
            }
        };

        let tables = match from {
            FromTable::WithoutKeyword(tables) => tables,
            FromTable::WithFromKeyword(tables) => tables,
        };

        if tables.len() != 1 {
            return Err(anyhow!("Delete statement must have a single table"));
        }

        let table_name = match &tables.first().unwrap().relation {
            TableFactor::Table { name, .. } => name.0.first().unwrap().value.clone(),
            _ => bail!(anyhow!(Error::Unimplemented("Joins".into()))),
            // e => todo!("{:?}", e),
        };

        let schema = self
            .catalog
            .lock()
            .get_schema(&table_name, txn_id)
            .ok_or_else(|| anyhow!("Table {} does not exist", table_name))?;

        let root = LogicalPlan::Scan(Scan::new(table_name.clone(), schema));

        let root = LogicalPlan::Delete(Box::new(Delete::new(root, table_name, filter)));

        Ok(root)
    }

    fn build_index_scan(
        &self,
        table_name: String,
        schema: Schema,
        expr: BinaryExpr,
    ) -> Result<LogicalPlan> {
        let BinaryExpr { left, op, right } = expr;
        let (col, value, lhs) = match (left, right)  {
            (LogicalExpr::Column(col), LogicalExpr::Literal(value)) => (col, value, true) ,
            (LogicalExpr::Literal(value), LogicalExpr::Column(col)) => (col, value, false),
            _ => return Err(anyhow!("Invalid index scan, must be of form {{col}} {{op}} {{value}} or {{value}} {{op}} {{col}}"))
        };

        if !matches!(value, Value::UInt(_)) {
            return Err(anyhow!("Index scan only supported on UInt"));
        }

        if let Some(field) = schema.fields.iter().find(|f| f.name == col) {
            if !field.constraints.unique {
                return Err(anyhow!("Index scan only supported on unique fields"));
            }
        } else {
            return Err(anyhow!("Column {} not found in table {}", col, table_name));
        }

        let value = value.u32();

        let (left, lincl, right, rincl) = match op {
            BinaryOperator::Eq => (Some(value), true, Some(value), true),
            // col > value or value < col
            BinaryOperator::Gt if lhs => (Some(value), false, None, false),
            BinaryOperator::Lt if !lhs => (Some(value), false, None, false),
            // value > col or col < value
            BinaryOperator::Gt if !lhs => (None, false, Some(value), false),
            BinaryOperator::Lt if lhs => (None, false, Some(value), false),
            // col >= value or value =< col
            BinaryOperator::GtEq if lhs => (Some(value), true, None, false),
            BinaryOperator::LtEq if !lhs => (Some(value), true, None, false),
            // value >= col or col <= value
            BinaryOperator::GtEq if !lhs => (None, false, Some(value), true),
            BinaryOperator::LtEq if lhs => (None, false, Some(value), true),
            e => todo!("not supported {:?}", e),
        };

        Ok(LogicalPlan::IndexScan(IndexScan::new(
            table_name, schema, col, left, lincl, right, rincl,
        )))
    }

    fn build_start_transaction(&self) -> Result<LogicalPlan> {
        Ok(LogicalPlan::StartTxn)
    }

    fn build_rollback_transaction(&self) -> Result<LogicalPlan> {
        Ok(LogicalPlan::RollbackTxn)
    }

    fn build_commit_transaction(&self) -> Result<LogicalPlan> {
        Ok(LogicalPlan::CommitTxn)
    }

    fn build_truncate(
        &self,
        table_names: Vec<TruncateTableTarget>,
        table: bool,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        if !table {
            return Err(anyhow!("Did you mean 'TRUNCATE TABLE'?"));
        }

        if table_names.len() != 1 {
            return Err(anyhow!("Only single table can be truncated"));
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

        if self.catalog.lock().get_table(&table_name, txn_id).is_none() {
            return Err(anyhow!("Table {} does not exist", table_name));
        }

        Ok(LogicalPlan::Truncate(Truncate::new(table_name)))
    }

    fn build_drop(
        &self,
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

        if !if_exists {
            let non_existant: Vec<String> = names
                .iter()
                .filter(|&name| self.catalog.lock().get_table(name, txn_id).is_none())
                .cloned()
                .collect();

            if !non_existant.is_empty() {
                return Err(anyhow!("Table(s) {} don't exist", non_existant.join(", ")));
            }
        }

        Ok(LogicalPlan::DropTables(DropTables::new(names, if_exists)))
    }

    fn build_explain(
        &self,
        statement: Statement,
        analyze: bool,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        let root = self.build_initial_plan(statement, txn_id)?;

        Ok(LogicalPlan::Explain(Box::new(Explain::new(root, analyze))))
    }

    fn build_values(&self, rows: Vec<Vec<Expr>>) -> Result<LogicalPlan> {
        let rows = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|expr| match expr {
                        Expr::Value(_) => build_expr(&expr),
                        e => Err(anyhow!("Unsupported expression in VALUES: {:?}", e)),
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

    fn build_query(
        &self,
        query: Option<Box<Query>>,
        txn_id: Option<TxnId>,
    ) -> Result<Option<LogicalPlan>> {
        if query.is_none() {
            return Ok(None);
        }

        let Query { body, limit, .. } = *query.unwrap();

        let input = match *body {
            SetExpr::Select(_) => self.build_select(body, limit, txn_id)?,
            SetExpr::Values(SqlValues { rows, .. }) => self.build_values(rows)?,
            _ => unimplemented!(),
        };

        Ok(Some(input))
    }

    fn build_insert(
        &self,
        table_name: ObjectName,
        source: Option<Box<Query>>,
        _columns: Vec<Ident>,
        _returning: Option<Vec<SelectItem>>,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        let input = self.build_query(source, txn_id)?.unwrap();
        let table_name = table_name.0.first().unwrap().value.clone();
        let schema = self.catalog.lock().get_schema(&table_name, txn_id).unwrap();

        let input_schema = input.schema();
        let input_types: Vec<_> = input_schema.fields.iter().map(|f| &f.ty).collect();
        let table_types: Vec<_> = schema.fields.iter().map(|f| &f.ty).collect();

        if input_types
            .iter()
            .zip(table_types.iter())
            .any(|(a, b)| !a.is_compatible(b))
        {
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

    fn build_update(
        &self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        if assignments.len() > 1 {
            return Err(anyhow!("Multiple assignments are not supported"));
        };

        let filter = match selection {
            Some(expr) => build_expr(&expr)?,
            None => LogicalExpr::Literal(lit!(Bool, "true".to_string())),
        };

        let table_name = match table.relation {
            TableFactor::Table { name, .. } => name.0.first().unwrap().value.clone(),
            e => todo!("{:?}", e),
        };

        let assignments = self.build_assignemnt(assignments.into_iter().next().unwrap())?;

        let schema = self
            .catalog
            .lock()
            .get_schema(&table_name, txn_id)
            .ok_or_else(|| anyhow!("Table {} does not exist", table_name))?;

        if !schema.fields.iter().any(|f| *f.name == assignments.0) {
            return if schema.is_qualified() {
                Err(anyhow!(
                    "Please use qualified column names {}.{}",
                    table_name,
                    assignments.0
                ))
            } else {
                Err(anyhow!(
                    "Column {} does not exist in table {}",
                    assignments.0,
                    table_name
                ))
            };
        }

        let root = LogicalPlan::Scan(Scan::new(table_name.clone(), schema));

        let root =
            LogicalPlan::Update(Box::new(Update::new(root, table_name, assignments, filter)));

        Ok(root)
    }

    fn build_assignemnt(&self, assignment: Assignment) -> Result<(String, LogicalExpr)> {
        let Assignment { target, value } = assignment;

        let col = match target {
            AssignmentTarget::ColumnName(col) => col.0.first().unwrap().value.clone(),
            AssignmentTarget::Tuple(_) => todo!(),
        };

        Ok((col, build_expr(&value)?))
    }

    fn build_create(
        &self,
        name: ObjectName,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    ) -> Result<LogicalPlan> {
        let root = LogicalPlan::default();

        let create = CreateTable::new(
            root,
            name.0.first().unwrap().value.clone(),
            Schema::from_sql(columns)?,
            if_not_exists,
        );

        Ok(LogicalPlan::CreateTable(Box::new(create)))
    }

    fn build_source(
        &self,
        table: Option<&TableWithJoins>,
        prewhere: Option<Expr>,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        match &table {
            None => Ok(LogicalPlan::Empty),
            Some(TableWithJoins { relation, joins }) => {
                let left_name = match relation {
                    TableFactor::Table { name, .. } => name.0.first().unwrap().value.clone(),
                    e => bail!(Error::Unsupported(e.to_string())),
                };

                if joins.len() > 1 {
                    bail!(Error::Unsupported("Multiple joins".into()));
                }

                let mut left_schema = self
                    .catalog
                    .lock()
                    .get_schema(&left_name, txn_id)
                    .ok_or(anyhow!("Table {} does not exist", left_name))?;

                let root = if let Some(pre) = prewhere {
                    let expr = build_expr(&pre)?;
                    match expr {
                        LogicalExpr::BinaryExpr(expr) => {
                            self.build_index_scan(left_name.clone(), left_schema.clone(), *expr)
                        }
                        _ => return Err(anyhow!("Prewhere must be a binary expression")),
                    }?
                } else {
                    LogicalPlan::Scan(Scan::new(left_name.clone(), left_schema.clone()))
                };

                let root = match &joins.first() {
                    Some(SqlJoin {
                        relation,
                        join_operator,
                        ..
                    }) => match relation {
                        TableFactor::Table { name, .. } => {
                            let right_name = name.0.first().unwrap().value.clone();
                            let mut right_schema = self
                                .catalog
                                .lock()
                                .get_schema(&right_name, txn_id)
                                .ok_or(anyhow!("Table {} does not exist", right_name))?;

                            let right_scan = LogicalPlan::Scan(Scan::new(
                                right_name.clone(),
                                right_schema.clone(),
                            ));

                            let operator = match join_operator {
                                JoinOperator::Inner(e) => match e {
                                    JoinConstraint::On(expr) => match build_expr(expr)? {
                                        LogicalExpr::BinaryExpr(expr) => expr,
                                        _ => bail!(Error::Unsupported(
                                                "Only Binary Expressions are supported in join conditions".into()
                                        )),
                                    },
                                    JoinConstraint::Using(col) => {
                                        if col.len() != 1 {
                                            bail!(Error::Unsupported(
                                                "Using clause must have a single column".into()
                                            ))
                                        }
                                        let col = col.first().unwrap().value.clone();
                                        Box::new(BinaryExpr::new(
                                            LogicalExpr::Column(col.clone()),
                                            BinaryOperator::Eq,
                                            LogicalExpr::Column(col),
                                        ))
                                    }
                                    JoinConstraint::None => {
                                        bail!(Error::Expected("None".into(), "ON or USING".into()))
                                    }
                                    JoinConstraint::Natural => {
                                        bail!(Error::Expected(
                                            "Natural".into(),
                                            "ON or USING".into()
                                        ))
                                    }
                                },
                                _ => bail!(Error::Unsupported("Only supports inner joins".into())),
                            };

                            let join_schema = match left_schema.join(right_schema.clone()) {
                                Ok(s) => s,
                                Err(_) => {
                                    left_schema = left_schema.add_qualifier(&left_name);
                                    right_schema = right_schema.add_qualifier(&right_name);
                                    left_schema.join(right_schema).unwrap()
                                }
                            };

                            LogicalPlan::Join(Box::new(Join::new(
                                root,
                                right_scan,
                                *operator,
                                join_schema,
                            )))
                        }
                        _ => todo!(),
                    },
                    None => root,
                };

                Ok(root)
            }
        }
    }

    fn build_select(
        &self,
        body: Box<SetExpr>,
        _limit: Option<Expr>,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        let select = match *body {
            SetExpr::Select(select) => select,
            _ => unimplemented!(),
        };

        let mut root = self.build_source(select.from.first(), select.prewhere, txn_id)?;

        let filters = select.selection.clone().map(|e| match e {
            Expr::BinaryOp { left, right, op } => self.parse_boolean_expr(*left, op, *right),
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

        let columns = select
            .projection
            .into_iter()
            .flat_map(|e| match e {
                SelectItem::UnnamedExpr(Expr::Value(SqlValue::Number(s, _))) => {
                    vec![Ok(LogicalExpr::Literal(lit!(UInt, s)))]
                }
                SelectItem::UnnamedExpr(Expr::Value(SqlValue::SingleQuotedString(s))) => {
                    vec![Ok(LogicalExpr::Literal(lit!(Str, s)))]
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let name = ident.value.clone();

                    if !root.schema().fields.iter().any(|f| f.name == name) {
                        vec![if root.schema().is_qualified() {
                            Err(anyhow!("Please use qualified column names {}", name))
                        } else {
                            Err(anyhow!("Column {} does not exist in table", name))
                        }]
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
                            Ok(LogicalExpr::Literal(lit!(UInt, s)))
                        }
                        Expr::Value(SqlValue::SingleQuotedString(s)) => {
                            Ok(LogicalExpr::Literal(lit!(Str, s)))
                        }
                        e => todo!("{}", e),
                    })
                    .collect(),
                SelectItem::UnnamedExpr(Expr::BinaryOp { left, right, op }) => {
                    let left = match build_expr(&left) {
                        Ok(expr) => expr,
                        Err(e) => return vec![Err(e)],
                    };
                    let right = match build_expr(&right) {
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
                    let expr = match build_expr(&expr) {
                        Ok(expr) => expr,
                        Err(e) => return vec![Err(e)],
                    };

                    vec![Ok(LogicalExpr::AliasedExpr(
                        Box::new(expr),
                        alias.value.clone(),
                    ))]
                }
                SelectItem::UnnamedExpr(expr) => {
                    let expr = build_expr(&expr);
                    match expr {
                        Ok(expr) => {
                            let name = match expr {
                                LogicalExpr::Column(ref name) => name,
                                _ => unreachable!(),
                            };
                            if !root.schema().fields.iter().any(|f| &f.name == name) {
                                vec![Err(anyhow!("Column {} doesn't exist", name))]
                            } else {
                                vec![Ok(expr)]
                            }
                        }
                        Err(e) => vec![Err(e)],
                    }
                }
                e => todo!("{:?}", e),
            })
            .collect::<Result<Vec<_>>>()?;

        root = LogicalPlan::Projection(Box::new(Projection::new(root, columns)));

        Ok(root)
    }

    fn parse_boolean_expr(
        &self,
        left: Expr,
        op: BinaryOperator,
        right: Expr,
    ) -> Result<BooleanBinaryExpr> {
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
        LogicalExpr::Literal(lit!(Bool, b.to_string()))
    }
}

// moved outside the impl to satisfy Clippy
// https://rust-lang.github.io/rust-clippy/master/index.html#only_used_in_recursion
fn build_expr(expr: &Expr) -> Result<LogicalExpr> {
    match expr {
        Expr::Value(SqlValue::Number(n, _)) => Ok(LogicalExpr::Literal(lit!(UInt, n))),
        Expr::Value(SqlValue::SingleQuotedString(s)) => Ok(LogicalExpr::Literal(lit!(Str, s))),
        Expr::Identifier(Ident { value, .. }) => Ok(LogicalExpr::Column(value.clone())),
        Expr::BinaryOp { left, op, right } => Ok(LogicalExpr::BinaryExpr(Box::new(
            BinaryExpr::new(build_expr(left)?, op.clone(), build_expr(right)?),
        ))),
        Expr::Nested(e) => build_expr(e),
        Expr::CompoundIdentifier(i) => {
            if i.len() > 2 {
                bail!(Error::Unsupported(
                    "Please use table.column or column".into()
                ))
            }

            Ok(LogicalExpr::Column(
                i.iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
            ))
        }
        Expr::Value(SqlValue::Null) => Ok(LogicalExpr::Literal(Value::Null)),
        e => bail!(Error::Unsupported(format!("{}", e))),
    }
}
