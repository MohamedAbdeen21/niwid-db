pub mod expr;
pub mod optimizer;
pub mod plan;

use expr::{BinaryExpr, BooleanBinaryExpr, LogicalExpr};
use plan::{
    CreateTable, Delete, DropTables, Explain, Filter, IndexScan, Insert, Join, Limit, LogicalPlan,
    Projection, Scan, Truncate, Union, Update, Values,
};
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ColumnDef, CreateTable as SqlCreateTable,
    Delete as SqlDelete, Expr, FromTable, Ident, Insert as SqlInsert, Join as SqlJoin,
    JoinConstraint, JoinOperator, ObjectName, ObjectType, Offset, OffsetRows, Query, SelectItem,
    SetExpr, SetOperator, SetQuantifier, Statement, TableFactor, TableWithJoins,
    TruncateTableTarget, UnaryOperator, Value as SqlValue, Values as SqlValues,
};

use anyhow::{anyhow, bail, ensure, Result};

use crate::catalog::ArcCatalog;
use crate::errors::Error;
use crate::tuple::schema::Schema;
use crate::txn_manager::TxnId;
use crate::types::{Types, Value, ValueFactory};
use crate::{is_boolean_op, lit, printdbg};

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
            Statement::Query(query) => self.build_query(query, txn_id),
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
            e => bail!(Error::Unimplemented(format!("Statement: {:?}", e))),
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
                bail!(Error::Unsupported(
                    "DELETE must contain a WHERE, else use TRUNCATE".into()
                ))
            }
        };

        let tables = match from {
            FromTable::WithoutKeyword(tables) => tables,
            FromTable::WithFromKeyword(tables) => tables,
        };

        if tables.len() != 1 {
            bail!(Error::Unsupported(
                "Only one table per DELETE statement".into()
            ));
        }

        let table_name = match &tables.first().unwrap().relation {
            TableFactor::Table { name, .. } => name.0.first().unwrap().value.clone(),
            _ => bail!(Error::Unsupported(
                "Anything other than `DELETE FROM table_name [condition];`".into()
            )),
        };

        let schema = self
            .catalog
            .read()
            .get_schema(&table_name, txn_id)
            .ok_or(Error::TableNotFound(table_name.clone()))?;

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

        if !is_boolean_op!(op) {
            bail!(Error::Unsupported(
                "Only supports boolean binary operators".into()
            ));
        };

        let (col, lvalue, rvalue, lhs) = match (left, right)  {
            (LogicalExpr::Column(col), LogicalExpr::Literal(value)) => (col, value, None, true) ,
            (LogicalExpr::Literal(value), LogicalExpr::Column(col)) => (col, value, None, false),
            // BETWEEN
            (LogicalExpr::BinaryExpr(lexpr), LogicalExpr::BinaryExpr(rexpr)) if matches!(op, BinaryOperator::And) => {
                let column = match lexpr.left {
                    LogicalExpr::Column(col) => col,
                    _ => unreachable!()
                };
                let left = match lexpr.right {
                    LogicalExpr::Literal(value) => value,
                    _ => unreachable!()
                };

                let right = match rexpr.right {
                    LogicalExpr::Literal(value) => value,
                    _ => unreachable!()
                };
                ( column, left, Some(right), true)
            }
            _ => bail!(Error::Unsupported(
                "Invalid index scan, must be of form {{col}} {{op}} {{value}} or {{value}} {{op}} {{col}} or {{col}} BETWEEN {{expr}}".into()
            )),
        };

        if let Some(ref rv) = rvalue {
            if !matches!(rv, Value::UInt(_) | Value::Int(_) | Value::Float(_)) {
                bail!(Error::Unsupported(
                    "Index scan only supported on UInt".into()
                ));
            }
        }

        if !matches!(lvalue, Value::UInt(_) | Value::Int(_) | Value::Float(_)) {
            bail!(Error::Unsupported(
                "Index scan only supported on UInt".into()
            ));
        }

        if let Some(field) = schema.fields.iter().find(|f| f.name == col) {
            if !field.constraints.unique {
                bail!(Error::Unsupported(
                    "Index scan only supported on unique fields".into()
                ));
            }
        } else {
            bail!(Error::ColumnNotFound(col));
        }

        let lvalue = Some(lvalue.as_u32());
        let rvalue = rvalue.map(|v| v.as_u32());

        let (left, lincl, right, rincl) = match op {
            BinaryOperator::Eq => (lvalue, true, lvalue, true),
            // col > value or value < col
            BinaryOperator::Gt if lhs => (lvalue, false, None, false),
            BinaryOperator::Lt if !lhs => (lvalue, false, None, false),
            // value > col or col < value
            BinaryOperator::Gt if !lhs => (None, false, lvalue, false),
            BinaryOperator::Lt if lhs => (None, false, lvalue, false),
            // col >= value or value =< col
            BinaryOperator::GtEq if lhs => (lvalue, true, None, false),
            BinaryOperator::LtEq if !lhs => (lvalue, true, None, false),
            // value >= col or col <= value
            BinaryOperator::GtEq if !lhs => (None, false, lvalue, true),
            BinaryOperator::LtEq if lhs => (None, false, lvalue, true),

            BinaryOperator::And => (lvalue, true, rvalue, true),

            e => bail!(Error::Unsupported(format!(
                "Operator {} in PREWHERE clause",
                e
            ))),
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
        _table: bool,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        let catalog = self.catalog.read();

        let names = table_names
            .into_iter()
            .map(|t| t.name.0.first().unwrap().value.clone())
            .map(|name| {
                catalog
                    .get_table(&name, txn_id)
                    .ok_or(Error::TableNotFound(name).into())
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|t| t.name.clone())
            .collect();

        Ok(LogicalPlan::Truncate(Truncate::new(names)))
    }

    fn build_drop(
        &self,
        object_type: ObjectType,
        if_exists: bool,
        names: Vec<ObjectName>,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        if object_type != ObjectType::Table {
            bail!(Error::Unsupported(format!(
                "Object type {} not supported in drop",
                object_type
            )));
        }

        let names: Vec<_> = names
            .iter()
            .map(|n| n.0.first().unwrap().value.clone())
            .collect();

        let catalog = self.catalog.read();

        if !if_exists {
            let non_existant: Vec<String> = names
                .iter()
                .filter(|&name| catalog.get_table(name, txn_id).is_none())
                .cloned()
                .collect();

            if !non_existant.is_empty() {
                bail!("Table(s) {} don't exist", non_existant.join(", "));
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

        printdbg!("Initial plan: {}", root.print());

        Ok(LogicalPlan::Explain(Box::new(Explain::new(root, analyze))))
    }

    fn build_values(&self, rows: Vec<Vec<Expr>>) -> Result<LogicalPlan> {
        let rows = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|expr| match expr {
                        Expr::Value(_) | Expr::UnaryOp { .. } => build_expr(&expr),
                        e => bail!(Error::Unsupported(format!(
                            "Unsupported expression in VALUES: {:?}",
                            e
                        ))),
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        // should never happen because sqlparser doesn't allow empty rows
        if rows.is_empty() {
            bail!(Error::Expected(
                "VALUES to have at least one row".into(),
                "No rows".into(),
            ))
        }

        let fields = rows
            .first()
            .unwrap()
            .iter()
            .map(|e| e.to_field(&Schema::default()))
            .collect();

        Ok(LogicalPlan::Values(Values::new(rows, Schema::new(fields))))
    }

    fn build_query(&self, query: Box<Query>, txn_id: Option<TxnId>) -> Result<LogicalPlan> {
        let Query {
            body,
            limit,
            offset,
            ..
        } = *query;

        let input = match *body {
            SetExpr::Select(_) => self.build_select(body, limit, offset, txn_id)?,
            SetExpr::Values(SqlValues { rows, .. }) => self.build_values(rows)?,
            SetExpr::SetOperation {
                op: SetOperator::Union,
                left,
                right,
                set_quantifier,
            } => self.build_union(left, right, set_quantifier, txn_id)?,
            e => bail!(Error::Unsupported(format!("Query: {}", e))),
        };

        Ok(input)
    }

    fn build_union(
        &self,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
        quantifier: SetQuantifier,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        if quantifier == SetQuantifier::All {
            bail!(Error::Unsupported("UNION ALL not supported".into()));
        }

        let left = match *left {
            SetExpr::Select(_) => self.build_select(left, None, None, txn_id)?,
            SetExpr::SetOperation {
                op: SetOperator::Union,
                left,
                right,
                ..
            } => self.build_union(left, right, quantifier, txn_id)?,
            query => bail!(Error::Unsupported(format!("UNION with query: {}", query))),
        };

        let right = match *right {
            SetExpr::Select(_) => self.build_select(right, None, None, txn_id)?,
            SetExpr::SetOperation {
                op: SetOperator::Union,
                left,
                right,
                ..
            } => self.build_union(left, right, quantifier, txn_id)?,
            query => bail!(Error::Unsupported(format!("UNION with query: {}", query))),
        };

        let left_types: Vec<_> = left.schema().fields.iter().map(|f| f.ty.clone()).collect();
        let right_types: Vec<_> = right.schema().fields.iter().map(|f| f.ty.clone()).collect();

        if left_types
            .iter()
            .zip(right_types.iter())
            .any(|(a, b)| !a.is_compatible(b))
        {
            bail!(Error::TypeMismatch(left_types, right_types));
        }

        Ok(LogicalPlan::Union(Box::new(Union::new(left, right))))
    }

    fn build_insert(
        &self,
        table_name: ObjectName,
        source: Option<Box<Query>>,
        columns: Vec<Ident>,
        _returning: Option<Vec<SelectItem>>,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        if source.is_none() {
            bail!(Error::Expected(
                "INSERT statement to have input".into(),
                "Nothing".into(),
            ));
        }

        let input = self.build_query(source.unwrap(), txn_id)?;
        let input_schema = input.schema();

        let table_name = table_name.0.first().unwrap().value.clone();
        let schema = self
            .catalog
            .read()
            .get_schema(&table_name, txn_id)
            .ok_or(Error::TableNotFound(table_name.clone()))?;

        let columns = if columns.is_empty() {
            schema.fields.iter().map(|f| f.name.clone()).collect()
        } else {
            let names: Vec<_> = columns.into_iter().map(|i| i.value).collect();
            let non_existant: Vec<_> = names
                .iter()
                .filter(|i| !schema.fields.iter().any(|f| f.name == **i))
                .collect();

            if !non_existant.is_empty() {
                bail!(Error::ColumnsNotFound(
                    non_existant.into_iter().cloned().collect()
                ));
            }

            names
        };

        let insert = Insert::new(
            input,
            columns,
            table_name,
            schema.clone(),
            Schema::default(),
        );

        let input_types =
            insert.reorder(input_schema.fields.iter().map(|f| f.ty.clone()).collect())?;
        let table_types: Vec<_> = schema.fields.iter().map(|f| f.ty.clone()).collect();

        if table_types
            .iter()
            .zip(input_types.iter())
            .any(|(a, b)| !a.is_compatible(b))
        {
            bail!(Error::TypeMismatch(table_types, input_types));
        }

        let root = LogicalPlan::Insert(Box::new(insert));

        Ok(root)
    }

    fn build_update(
        &self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        let filter = match selection {
            Some(expr) => build_expr(&expr)?,
            None => LogicalExpr::Literal(lit!(Bool, "true".to_string())?),
        };

        let table_name = match table.relation {
            TableFactor::Table { name, .. } => name.0.first().unwrap().value.clone(),
            _ => bail!(Error::Unsupported(
                "Anything other than `UPDATE table_name ...;`".into()
            )),
        };

        let assignments = assignments
            .into_iter()
            .map(|a| self.build_assignemnt(a))
            .collect::<Result<Vec<_>>>()?;

        let schema = self
            .catalog
            .read()
            .get_schema(&table_name, txn_id)
            .ok_or(Error::TableNotFound(table_name.clone()))?;

        for (col, _) in assignments.iter() {
            if !schema.fields.iter().any(|f| &f.name == col) {
                if schema.is_qualified() {
                    bail!(Error::ColumnNotFound(format!(
                        "Please use qualified column names {}.{}",
                        table_name, col
                    )))
                } else {
                    bail!(Error::ColumnNotFound(format!(
                        "Column {} does not exist in table {}",
                        col, table_name
                    )))
                };
            }
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
                    .read()
                    .get_schema(&left_name, txn_id)
                    .ok_or(Error::TableNotFound(left_name.clone()))?;

                let root = if let Some(pre) = prewhere {
                    let expr = build_expr(&pre)?;
                    match expr {
                        LogicalExpr::BinaryExpr(expr) => {
                            self.build_index_scan(left_name.clone(), left_schema.clone(), *expr)
                        }
                        _ => bail!(Error::Unsupported(
                            "Prewhere must be a binary expression".into()
                        )),
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
                                .read()
                                .get_schema(&right_name, txn_id)
                                .ok_or(Error::TableNotFound(right_name.clone()))?;

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
                                        bail!(Error::Expected("ON or USING".into(), "None".into()))
                                    }
                                    JoinConstraint::Natural => {
                                        bail!(Error::Expected(
                                            "ON or USING".into(),
                                            "Natural".into()
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
                        _ => bail!(Error::Unsupported("Only supports tables with joins".into())),
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
        limit: Option<Expr>,
        offset: Option<Offset>,
        txn_id: Option<TxnId>,
    ) -> Result<LogicalPlan> {
        let select = match *body {
            SetExpr::Select(select) => select,
            e => unreachable!("Should only be called on a select query, got: {:?}", e),
        };

        let mut root = self.build_source(select.from.first(), select.prewhere, txn_id)?;

        let filters = select.selection.clone().map(|e| match e {
            Expr::BinaryOp { left, right, op } => self.parse_boolean_expr(*left, op, *right),
            Expr::Value(SqlValue::Boolean(b)) => Ok(BooleanBinaryExpr::new(
                b.into(),
                BinaryOperator::Eq,
                true.into(),
            )),
            Expr::Identifier(Ident { value, .. })
                if root
                    .schema()
                    .fields
                    .iter()
                    .find(|field| field.name == value)
                    .map(|f| matches!(f.ty, Types::Bool))
                    .unwrap_or(false) =>
            {
                Ok(BooleanBinaryExpr::new(
                    LogicalExpr::Column(value.clone()),
                    BinaryOperator::Eq,
                    true.into(),
                ))
            }
            Expr::CompoundIdentifier(idents) => {
                let name = idents
                    .into_iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<String>>()
                    .join(".");

                if !root.schema().fields.iter().any(|f| f.name == name) {
                    bail!(Error::ColumnNotFound(name.clone()));
                }

                Ok(BooleanBinaryExpr::new(
                    LogicalExpr::Column(name),
                    BinaryOperator::Eq,
                    true.into(),
                ))
            }
            e => bail!(Error::Unimplemented(format!("Expr: {:?}", e))),
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

        let offset = match offset {
            Some(offset) => {
                ensure!(
                    offset.rows == OffsetRows::None,
                    "OFFSET [ROWS|ROW] is not supported"
                );
                Some(offset.value)
            }
            None => None,
        };

        root = self.build_limit(root, limit, offset)?;

        let projections = self.build_projections(select.projection, root.schema())?;

        root = LogicalPlan::Projection(Box::new(Projection::new(root, projections)));

        Ok(root)
    }

    fn build_limit(
        &self,
        root: LogicalPlan,
        limit: Option<Expr>,
        offset: Option<Expr>,
    ) -> Result<LogicalPlan> {
        match limit {
            Some(limit) => {
                let limit = match limit {
                    Expr::Value(SqlValue::Number(s, _)) => match build_number(&s, false)? {
                        Value::UInt(u) => Ok(u.0),
                        e => Err(Error::Expected(
                            "LIMIT to be an unsigned integer".into(),
                            format!("{e}"),
                        )),
                    },
                    e => Err(Error::Expected(
                        "LIMIT to be an unsigned integer".into(),
                        format!("{e}"),
                    )),
                }?;

                let offset = match offset {
                    Some(offset) => match offset {
                        Expr::Value(SqlValue::Number(s, _)) => match build_number(&s, false)? {
                            Value::UInt(u) => Ok(u.0),
                            e => Err(Error::Expected(
                                "OFFSET to be an unsigned integer".into(),
                                format!("{e}"),
                            )),
                        },
                        e => Err(Error::Expected(
                            "OFFSET to be an unsigned integer".into(),
                            format!("{e}"),
                        )),
                    },
                    None => Ok(0),
                }?;

                Ok(LogicalPlan::Limit(Box::new(Limit::new(
                    root, limit, offset,
                ))))
            }
            None if offset.is_some() => {
                bail!(Error::Unsupported("OFFSET without LIMIT".into()));
            }
            None => Ok(root),
        }
    }

    fn build_projections(
        &self,
        projections: Vec<SelectItem>,
        schema: Schema,
    ) -> Result<Vec<LogicalExpr>> {
        let mut projs = vec![];

        projections.into_iter().try_for_each(|e| {
            let exprs = match e {
                SelectItem::UnnamedExpr(Expr::Value(SqlValue::Number(s, _))) => {
                    vec![LogicalExpr::Literal(build_number(&s, false)?)]
                }
                SelectItem::UnnamedExpr(Expr::Value(SqlValue::SingleQuotedString(s))) => {
                    vec![LogicalExpr::Literal(lit!(Str, s)?)]
                }
                SelectItem::UnnamedExpr(Expr::Value(SqlValue::Boolean(b))) => {
                    vec![LogicalExpr::Literal(lit!(Bool, b.to_string())?)]
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let name = ident.value.clone();

                    // Error handling if the column name is not found
                    if !schema.fields.iter().any(|f| f.name == name) {
                        if schema.is_qualified() {
                            bail!("Please use qualified column names {}", name);
                        } else {
                            bail!(Error::ColumnNotFound(name));
                        }
                    } else {
                        vec![LogicalExpr::Column(ident.value.clone())]
                    }
                }
                SelectItem::Wildcard(_) => schema
                    .fields
                    .iter()
                    .map(|f| f.name.clone())
                    .map(LogicalExpr::Column)
                    .collect(),
                SelectItem::UnnamedExpr(Expr::Tuple(fields)) => {
                    fields.iter().map(build_expr).collect::<Result<Vec<_>>>()?
                }
                SelectItem::UnnamedExpr(Expr::BinaryOp { left, right, op }) => {
                    let left = build_expr(&left)?;
                    let right = build_expr(&right)?;
                    vec![LogicalExpr::BinaryExpr(Box::new(BinaryExpr::new(
                        left,
                        op.clone(),
                        right,
                    )))]
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr = build_expr(&expr)?;
                    vec![LogicalExpr::AliasedExpr(
                        Box::new(expr),
                        alias.value.clone(),
                    )]
                }
                SelectItem::UnnamedExpr(expr) => {
                    let expr = build_expr(&expr)?;
                    match expr {
                        LogicalExpr::Column(ref name) => {
                            if !schema.fields.iter().any(|f| &f.name == name) {
                                bail!(Error::ColumnNotFound(name.clone()))
                            } else {
                                vec![expr]
                            }
                        }
                        LogicalExpr::Literal(_) => vec![expr],
                        e => bail!(Error::Unsupported(format!("Select Item: {:?}", e))),
                    }
                }
                // For unsupported SelectItems
                e => bail!(Error::Unsupported(format!("Select Item: {}", e))),
            };

            projs.extend(exprs);
            Ok(())
        })?;

        Ok(projs)
    }

    fn parse_boolean_expr(
        &self,
        left: Expr,
        op: BinaryOperator,
        right: Expr,
    ) -> Result<BooleanBinaryExpr> {
        if !is_boolean_op!(op) {
            bail!(Error::Unsupported(format!(
                "Expected a boolean binary operator, got: {:?}",
                op
            )));
        }

        Ok(BooleanBinaryExpr::new(
            left.try_into()?,
            op,
            right.try_into()?,
        ))
    }
}

// convenience method
impl TryFrom<Expr> for LogicalExpr {
    type Error = anyhow::Error;

    fn try_from(expr: Expr) -> Result<LogicalExpr> {
        match expr {
            Expr::Identifier(ident) => Ok(LogicalExpr::Column(ident.to_string())),
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => {
                if let Expr::Value(SqlValue::Number(n, _)) = *expr.clone() {
                    Ok(LogicalExpr::Literal(build_number(&n, true)?))
                } else {
                    bail!(Error::Unsupported("Unsupported value type".into()))
                }
            }
            Expr::Value(value) => {
                match value {
                    SqlValue::Number(s, _) => {
                        return Ok(LogicalExpr::Literal(build_number(&s, false)?))
                    }
                    SqlValue::Null => return Ok(LogicalExpr::Literal(Value::Null)),
                    _ => (),
                }

                let (ty, v) = match value {
                    SqlValue::SingleQuotedString(s) => (Types::Str, s),
                    SqlValue::Boolean(b) => (Types::Bool, b.to_string()),
                    e => bail!(Error::Unsupported(format!(
                        "Unsupported value in Expr: {}",
                        e
                    ))),
                };

                Ok(LogicalExpr::Literal(ValueFactory::from_string(&ty, &v)?))
            }
            Expr::BinaryOp { left, right, op } => {
                let l: LogicalExpr = (*left).try_into()?;
                let r: LogicalExpr = (*right).try_into()?;
                Ok(LogicalExpr::BinaryExpr(Box::new(BinaryExpr::new(l, op, r))))
            }
            e => bail!(Error::Unimplemented(format!(
                "Casting Expr {:?} to LogicalExpr",
                e
            ))),
        }
    }
}

impl From<bool> for LogicalExpr {
    fn from(b: bool) -> LogicalExpr {
        LogicalExpr::Literal(lit!(Bool, b.to_string()).unwrap())
    }
}

// moved outside the impl to satisfy Clippy
// https://rust-lang.github.io/rust-clippy/master/index.html#only_used_in_recursion
fn build_expr(expr: &Expr) -> Result<LogicalExpr> {
    match expr {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            if let Expr::Value(SqlValue::Number(n, _)) = *expr.clone() {
                Ok(LogicalExpr::Literal(build_number(&n, true)?))
            } else {
                bail!(Error::Unsupported("Expected a number".into()))
            }
        }
        Expr::Value(SqlValue::Number(n, _)) => Ok(LogicalExpr::Literal(build_number(n, false)?)),
        Expr::Value(SqlValue::SingleQuotedString(s)) => Ok(LogicalExpr::Literal(lit!(Str, s)?)),
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
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            if *negated {
                bail!(Error::Unsupported("NOT BETWEEN in index search".into()));
            };
            let column = build_expr(expr)?;
            let left = LogicalExpr::BinaryExpr(Box::new(BinaryExpr::new(
                column.clone(),
                BinaryOperator::GtEq,
                build_expr(low)?,
            )));
            let right = LogicalExpr::BinaryExpr(Box::new(BinaryExpr::new(
                column.clone(),
                BinaryOperator::LtEq,
                build_expr(high)?,
            )));
            Ok(LogicalExpr::BinaryExpr(Box::new(BinaryExpr::new(
                left,
                BinaryOperator::And,
                right,
            ))))
        }
        Expr::Value(SqlValue::Boolean(b)) => Ok(LogicalExpr::Literal(lit!(Bool, b.to_string())?)),
        e => bail!(Error::Unsupported(format!("Expr: {}", e))),
    }
}

fn build_number(num: &str, neg: bool) -> Result<Value> {
    let mut st = num.to_owned();

    if neg {
        st.insert(0, '-');
    };

    if st.contains('.') {
        lit!(Float, st)
    } else if st.contains('-') {
        lit!(Int, st)
    } else {
        lit!(UInt, st)
    }
}
