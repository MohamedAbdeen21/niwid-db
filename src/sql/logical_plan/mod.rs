pub mod expr;
pub mod optimizer;
pub mod plan;

use expr::{BinaryExpr, BooleanBinaryExpr, LogicalExpr};
use plan::{
    CreateTable, DropTables, Explain, Filter, Insert, LogicalPlan, Projection, Scan, Truncate,
    Update, Values,
};
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ColumnDef, CreateTable as SqlCreateTable, Expr,
    Ident, Insert as SqlInsert, ObjectName, ObjectType, Query, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, TruncateTableTarget, Value as SqlValue, Values as SqlValues,
};

use anyhow::{anyhow, Result};

use crate::catalog::ArcCatalog;
use crate::tuple::schema::Schema;
use crate::txn_manager::TxnId;
use crate::types::{Types, ValueFactory};

#[macro_export]
macro_rules! value {
    ($t:ident, $s:expr) => {
        ValueFactory::from_string(&Types::$t, &$s)
    };
}

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
            #[allow(unused)]
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
            e => unimplemented!("{}", e),
        }
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

    fn build_expr(&self, expr: Expr) -> Result<LogicalExpr> {
        match expr {
            Expr::Value(SqlValue::Number(n, _)) => Ok(LogicalExpr::Literal(value!(UInt, n))),
            Expr::Value(SqlValue::SingleQuotedString(s)) => {
                Ok(LogicalExpr::Literal(value!(Str, s)))
            }
            Expr::Identifier(Ident { value, .. }) => Ok(LogicalExpr::Column(value)),
            Expr::BinaryOp { left, op, right } => Ok(LogicalExpr::BinaryExpr(Box::new(
                BinaryExpr::new(self.build_expr(*left)?, op, self.build_expr(*right)?),
            ))),
            Expr::Nested(e) => self.build_expr(*e),
            e => todo!("{:?}", e),
        }
    }

    fn build_values(&self, rows: Vec<Vec<Expr>>) -> Result<LogicalPlan> {
        let rows = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|expr| match expr {
                        Expr::Value(_) => self.build_expr(expr),
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
            Some(expr) => self.build_expr(expr)?,
            None => LogicalExpr::Literal(value!(Bool, "true".to_string())),
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
            return Err(anyhow!(
                "Column {} does not exist in table {}",
                assignments.0,
                table_name
            ));
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

        Ok((col, self.build_expr(value)?))
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
            Schema::from_sql(columns),
            if_not_exists,
        );

        Ok(LogicalPlan::CreateTable(Box::new(create)))
    }

    fn build_select(
        &self,
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

                let schema = self
                    .catalog
                    .lock()
                    .get_schema(&name, txn_id)
                    .ok_or(anyhow!("Table {} does not exist", name))?;

                LogicalPlan::Scan(Scan::new(name, schema.clone()))
            }
        };

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
                    let left = match self.build_expr(*left.clone()) {
                        Ok(expr) => expr,
                        Err(e) => return vec![Err(e)],
                    };
                    let right = match self.build_expr(*right.clone()) {
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
                    let expr = match self.build_expr(expr.clone()) {
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
        LogicalExpr::Literal(value!(Bool, b.to_string()))
    }
}
