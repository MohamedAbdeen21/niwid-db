pub mod result_set;

use crate::context::Context;
use crate::errors::Error;
use crate::lit;
use crate::sql::logical_plan::expr::BinaryExpr;
use crate::sql::logical_plan::expr::{BooleanBinaryExpr, LogicalExpr};
use crate::sql::logical_plan::plan::{
    CreateTable, Delete, DropTables, Filter, Identity, IndexScan, Insert, Join, Limit, LogicalPlan,
    Scan, Truncate, Update, Values,
};
use crate::sql::logical_plan::plan::{Explain, Projection};
use crate::tuple::constraints::Constraints;
use crate::tuple::schema::{Field, Schema};
use crate::tuple::{Tuple, TupleId};
use crate::types::Types;
use crate::types::Value;
use crate::types::ValueFactory;
use anyhow::{anyhow, bail, Result};
use result_set::ResultSet;
use sqlparser::ast::BinaryOperator;

pub trait Executable {
    /// Context is passed for client controls like
    /// start/commit/rollback transactions, most other
    /// plans only need access to the active_txn id field or catalog, not the whole context
    /// I'm aware that I can pass an Option<Context> and Option<TxnId> to each plan
    /// and None to other plans, but that would make the API too ugly
    fn execute(self, ctx: &mut Context) -> Result<ResultSet>;
}

impl LogicalPlan {
    pub fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        match self {
            LogicalPlan::Projection(plan) => plan.execute(ctx),
            LogicalPlan::Scan(scan) => scan.execute(ctx),
            LogicalPlan::Filter(filter) => filter.execute(ctx),
            LogicalPlan::CreateTable(create) => create.execute(ctx),
            LogicalPlan::Explain(explain) => explain.execute(ctx),
            LogicalPlan::Insert(i) => i.execute(ctx),
            LogicalPlan::Values(v) => v.execute(ctx),
            LogicalPlan::DropTables(d) => d.execute(ctx),
            LogicalPlan::Truncate(t) => t.execute(ctx),
            LogicalPlan::Update(u) => u.execute(ctx),
            LogicalPlan::Delete(d) => d.execute(ctx),
            LogicalPlan::Empty => Ok(ResultSet::default()),
            LogicalPlan::Join(j) => j.execute(ctx),
            LogicalPlan::Limit(l) => l.execute(ctx),
            LogicalPlan::IndexScan(i) => i.execute(ctx),
            LogicalPlan::StartTxn => {
                ctx.start_txn()?;
                Ok(ResultSet::default())
            }
            LogicalPlan::CommitTxn => {
                ctx.commit_txn()?;
                Ok(ResultSet::default())
            }
            LogicalPlan::RollbackTxn => {
                ctx.rollback_txn()?;
                Ok(ResultSet::default())
            }
            #[cfg(test)]
            LogicalPlan::Identity(i) => i.execute(ctx),
        }
    }
}

impl Executable for Limit {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let input = self.input.execute(ctx)?;
        Ok(input.take(self.limit))
    }
}

impl Executable for Delete {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let txn_id = ctx.get_active_txn();

        let input = self.input.execute(ctx)?;

        let c = ctx.get_catalog();
        let mut catalog = c.write();

        let table = catalog
            .get_table_mut(&self.table_name, txn_id)
            .ok_or_else(|| anyhow!("Table {} does not exist", self.table_name))??;

        let (_, mask) = self.selection.evaluate(&input)?;

        let selected_rows = input
            .rows()
            .into_iter()
            .enumerate()
            .filter_map(|(i, row)| mask[i].is_truthy().then_some(row))
            .collect::<Vec<_>>();

        let (txn_id, is_temp) = match txn_id {
            Some(id) => (id, false),
            None => (ctx.start_txn()?, true),
        };

        table.start_txn(txn_id)?;

        for row in selected_rows {
            let tuple_id = (row[0].u32(), row[1].u32() as u16);

            if let Err(err) = table.delete(tuple_id) {
                table.rollback_txn()?;
                drop(catalog);
                if is_temp {
                    ctx.rollback_txn()?;
                }
                return Err(err);
            }
        }

        table.commit_txn()?;
        drop(catalog);
        if is_temp {
            ctx.commit_txn()?;
        }

        Ok(ResultSet::default())
    }
}

impl Executable for IndexScan {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let txn_id = ctx.get_active_txn();
        let arc_catalog = ctx.get_catalog();
        let catalog = arc_catalog.read();
        let table = catalog.get_table(&self.table_name, txn_id).unwrap();

        let schema = table.get_schema();

        let mut tuple_ids = vec![];

        let scanner = |(key, tuple_id): &(u32, TupleId)| {
            if let Some(from) = self.from {
                if *key == from && !self.include_from {
                    return Ok(());
                }
            }

            if let Some(to) = self.to {
                if *key > to || (*key == to && !self.include_to) {
                    return Err(anyhow!("End of loop"));
                };
            }
            tuple_ids.push(*tuple_id);
            Ok(())
        };

        let index = table.get_index().as_ref().unwrap();

        let _ = if let Some(from) = self.from {
            index.scan_from(txn_id, from, scanner)
        } else {
            index.scan(txn_id, scanner)
        };

        let tuples = tuple_ids
            .into_iter()
            .map(|tuple_id| {
                (
                    tuple_id,
                    table
                        .get_tuple(tuple_id)
                        .expect("Index returned a deleted record"),
                )
            })
            .collect::<Vec<_>>();

        let mut cols: Vec<Vec<Value>> = vec![vec![]; schema.fields.len() + 2];

        // TODO: pass the tuple_id as tuple for udpate to use
        // need to define a tuple type first though
        tuples
            .into_iter()
            .try_for_each(|((page_id, slot_id), tuple)| -> Result<()> {
                let mut values = vec![
                    lit!(UInt, page_id.to_string()),
                    lit!(UInt, slot_id.to_string()),
                ];

                values.extend(tuple.get_values(&schema)?);

                let values = values.into_iter().map(|v| {
                    if matches!(v, Value::StrAddr(_)) {
                        Value::Str(table.fetch_string(v.str_addr()))
                    } else {
                        v
                    }
                });

                values.enumerate().for_each(|(i, v)| {
                    cols[i].push(v);
                });

                Ok(())
            })?;

        let mut fields = vec![
            Field::new("page_id", Types::UInt, Constraints::nullable(false)),
            Field::new("slot_id", Types::UInt, Constraints::nullable(false)),
        ];

        fields.extend(schema.fields.clone());

        Ok(ResultSet::new(fields, cols))
    }
}

impl Executable for Join {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let left_name = match self.left {
            LogicalPlan::Scan(Scan { ref table_name, .. }) => table_name.clone(),
            _ => unreachable!(),
        };
        let right_name = match self.right {
            LogicalPlan::Scan(Scan { ref table_name, .. }) => table_name.clone(),
            _ => unreachable!(),
        };
        let left = self.left.execute(ctx)?;
        let right = self.right.execute(ctx)?;

        // drop the first page_id and slot_id cols from each table
        let left_fields = left.fields().len();
        let right_fields = right.fields().len();
        let mut left = left.select((2..left_fields).collect());
        let mut right = right.select((2..right_fields).collect());

        let join_schema = match left.schema.join(right.schema.clone()) {
            Ok(schema) => schema,
            Err(_) => {
                left.schema = left.schema.add_qualifier(&left_name);
                right.schema = right.schema.add_qualifier(&right_name);
                left.schema.join(right.schema.clone()).unwrap()
            }
        };

        let mut output_rows: Vec<Vec<Value>> = vec![];

        for left_row in left.rows() {
            let ll = ResultSet::from_tuple(left.fields().clone(), left_row.to_vec(), right.len());
            let input = ll.concat(right.clone());
            let mask = self.on.evaluate(&input)?;
            for (i, row) in input.rows().into_iter().enumerate() {
                if mask[i].is_truthy() {
                    output_rows.push(row);
                }
            }
        }

        Ok(ResultSet::from_rows(join_schema.fields, output_rows))
    }
}

impl Executable for Update {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let txn_id = ctx.get_active_txn();

        let input = self.input.execute(ctx)?;

        let c = ctx.get_catalog();
        let mut catalog = c.write();

        let table = catalog
            .get_table_mut(&self.table_name, txn_id)
            .ok_or_else(|| anyhow!("Table {} does not exist", self.table_name))??;

        let (_, mask) = self.selection.evaluate(&input)?;

        let (selected_col, expr) = self.assignments;

        let schema = table.get_schema();

        let updated_col_id = schema
            .fields
            .iter()
            .position(|f| f.name == selected_col)
            .ok_or_else(|| anyhow!("Column {} does not exist", selected_col))?;

        let selected_rows = input
            .rows()
            .into_iter()
            .enumerate()
            .filter_map(|(i, row)| mask[i].is_truthy().then_some(row))
            .collect::<Vec<_>>();

        let (txn_id, is_temp) = match txn_id {
            Some(id) => (id, false),
            None => (ctx.start_txn()?, true),
        };

        table.start_txn(txn_id)?;

        for row in selected_rows {
            let tuple_id = (row[0].u32(), row[1].u32() as u16);

            let mut new_tuple = row[2..].to_vec();

            for value in expr.evaluate(&input)?.1 {
                new_tuple[updated_col_id] = value;
            }

            let new_tuple = Tuple::new(new_tuple, &schema);

            if let Err(err) = table.update(Some(tuple_id), new_tuple) {
                table.rollback_txn()?;
                drop(catalog);
                if is_temp {
                    ctx.rollback_txn()?;
                }
                return Err(err);
            }
        }

        table.commit_txn()?;
        drop(catalog);
        if is_temp {
            ctx.commit_txn()?;
        }

        Ok(ResultSet::default())
    }
}

impl Executable for Truncate {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let txn_id = ctx.get_active_txn();
        ctx.get_catalog()
            .write()
            .truncate_table(self.table_name, txn_id)?;

        Ok(ResultSet::default())
    }
}

impl Executable for DropTables {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let txn_id = ctx.get_active_txn();
        for table_name in self.table_names {
            if ctx
                .get_catalog()
                .write()
                .drop_table(table_name.clone(), self.if_exists, txn_id)
                .is_none()
            {
                bail!(Error::TableNotFound(table_name));
            }
        }

        Ok(ResultSet::default())
    }
}

impl Executable for Values {
    fn execute(self, _: &mut Context) -> Result<ResultSet> {
        let input = ResultSet::with_capacity(1);
        let output = self
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|expr| expr.evaluate(&input))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(|(field, data)| ResultSet::new(vec![field], vec![data]))
                    .reduce(|a, b| a.concat(b))
                    .ok_or(anyhow!("Empty row"))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(|a, b| a.union(b))
            .unwrap_or_default();

        Ok(output)
    }
}

impl Executable for Insert {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let txn_id = ctx.get_active_txn();
        let input = self.input.execute(ctx)?;

        if input.fields().len() != self.table_schema.fields.len() {
            return Err(anyhow!("Column count mismatch"));
        }

        for row in input.rows() {
            let tuple = Tuple::new(row, &self.table_schema);

            let _tuple_id = ctx
                .get_catalog()
                .write()
                .get_table_mut(&self.table_name, txn_id)
                .ok_or_else(|| anyhow!("Table {} does not exist", self.table_name))??
                .insert(tuple)?;
        }

        Ok(ResultSet::default())
    }
}

impl Executable for Explain {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let plan = self.input.print();
        // println!("Logical plan:\n{}", self.input.print());
        // time the execution time
        if self.analyze {
            let start = std::time::Instant::now();
            let mut result = self.input.execute(ctx)?;
            let info = format!(
                "Execution time: {:?}\nLogical Plan:\n{}",
                start.elapsed(),
                plan
            );
            result.set_info(info);
            Ok(result)
        } else {
            let mut empty = ResultSet::default();
            empty.set_info(plan);
            Ok(empty)
        }
    }
}

impl Executable for CreateTable {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let txn_id = ctx.get_active_txn();
        let catalog = ctx.get_catalog();
        catalog
            .write()
            .add_table(self.table_name, &self.schema, self.if_not_exists, txn_id)?;
        Ok(ResultSet::default())
    }
}

impl Executable for Filter {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let input = self.input.execute(ctx)?;
        let mask = self.expr.evaluate(&input);

        let output = input
            .cols
            .into_iter()
            .map(|col| {
                col.into_iter()
                    .zip(&mask)
                    .filter_map(|(value, &mask)| if mask { Some(value) } else { None })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Ok(ResultSet::new(input.schema.fields, output))
    }
}

impl LogicalExpr {
    fn evaluate(&self, input: &ResultSet) -> Result<(Field, Vec<Value>)> {
        let size = input.len();
        match self {
            LogicalExpr::Literal(ref c) => {
                let input_schema = Schema::new(input.fields().clone());
                let field = self.to_field(&input_schema);
                let data = (0..size).map(|_| c.clone()).collect::<Vec<_>>();
                Ok((field, data))
            }
            LogicalExpr::Column(c) => {
                let fields = input.fields();
                let index = fields.iter().position(|col| col.name == *c).unwrap();
                Ok((input.fields()[index].clone(), input.cols()[index].clone()))
            }
            LogicalExpr::BinaryExpr(ref expr) => {
                let schema = Schema::new(input.fields().clone());
                let field = self.to_field(&schema);
                Ok((field, expr.evaluate(input)?))
            }
            LogicalExpr::AliasedExpr(ref expr, _) => {
                let result = expr.clone().evaluate(input)?;

                let schema = Schema::new(input.fields().clone());
                let field = self.to_field(&schema);

                Ok((field, result.1))
            }
        }
    }
}

impl BinaryExpr {
    fn eval_op(&self, left: &Value, right: &Value) -> Result<Value> {
        match &self.op {
            BinaryOperator::Plus => Ok(left.add(right)?),
            BinaryOperator::Minus => Ok(left.sub(right)?),
            BinaryOperator::Multiply => Ok(left.mul(right)?),
            BinaryOperator::Divide => Ok(left.div(right)?),
            BinaryOperator::Eq => Ok(lit!(Bool, left.equ(right)?.to_string())),
            BinaryOperator::And => Ok(lit!(Bool, left.and(right)?.to_string())),
            BinaryOperator::Or => Ok(lit!(Bool, left.or(right)?.to_string())),
            e => bail!(Error::Unsupported(format!("Operator evaluation {}", e))),
        }
    }

    pub(super) fn evaluate(&self, input: &ResultSet) -> Result<Vec<Value>> {
        match (&self.left, &self.right) {
            (LogicalExpr::Column(c1), LogicalExpr::Column(c2)) => {
                let fields = input.fields();
                let index1 = fields
                    .iter()
                    .position(|col| &col.name == c1)
                    .ok_or(anyhow!("Column {} does not exist", c1))?;
                let index2 = fields
                    .iter()
                    .position(|col| &col.name == c2)
                    .ok_or(anyhow!("Column {} does not exist", c2))?;
                let col1 = &input.cols()[index1];
                let col2 = &input.cols()[index2];
                Ok(col1
                    .iter()
                    .zip(col2)
                    .map(|(l, r)| self.eval_op(l, r))
                    .collect::<Result<_>>()?)
            }
            (LogicalExpr::Literal(lit), LogicalExpr::Column(c2)) => {
                let index = input
                    .fields()
                    .iter()
                    .position(|col| &col.name == c2)
                    .ok_or(anyhow!("Column {} does not exist", c2))?;
                let col = &input.cols()[index];
                Ok(col
                    .iter()
                    .map(|val| self.eval_op(lit, val))
                    .collect::<Result<_>>()?)
            }
            (LogicalExpr::Column(c1), LogicalExpr::Literal(lit)) => {
                let index = input
                    .fields()
                    .iter()
                    .position(|col| &col.name == c1)
                    .ok_or(anyhow!("Column {} does not exist", c1))?;
                let col = &input.cols()[index];
                Ok(col
                    .iter()
                    .map(|val| self.eval_op(val, lit))
                    .collect::<Result<_>>()?)
            }
            (LogicalExpr::Literal(v1), LogicalExpr::Literal(v2)) => {
                let rows = input.len();
                Ok((0..rows)
                    .map(|_| self.eval_op(v1, v2))
                    .collect::<Result<_>>()?)
            }
            (LogicalExpr::BinaryExpr(l), LogicalExpr::BinaryExpr(r)) => {
                let left = l.evaluate(input)?;
                let right = r.evaluate(input)?;
                Ok(left
                    .iter()
                    .zip(right.iter())
                    .map(|(l, r)| self.eval_op(l, r))
                    .collect::<Result<_>>()?)
            }
            (LogicalExpr::Literal(value), LogicalExpr::BinaryExpr(binary_expr)) => {
                let right = binary_expr.evaluate(input)?;
                Ok(right
                    .iter()
                    .map(|r| self.eval_op(value, r))
                    .collect::<Result<_>>()?)
            }
            (LogicalExpr::Column(c), LogicalExpr::BinaryExpr(binary_expr)) => {
                let fields = input.fields();
                let index = fields.iter().position(|col| &col.name == c).unwrap();
                let left = &input.cols()[index];

                let right = binary_expr.evaluate(input)?;

                Ok(left
                    .iter()
                    .zip(right)
                    .map(|(l, r)| self.eval_op(l, &r))
                    .collect::<Result<_>>()?)
            }
            (LogicalExpr::BinaryExpr(binary_expr), LogicalExpr::Literal(lit)) => {
                let left = binary_expr.evaluate(input)?;
                Ok(left
                    .iter()
                    .map(|l| self.eval_op(l, lit))
                    .collect::<Result<_>>()?)
            }
            (LogicalExpr::BinaryExpr(binary_expr), LogicalExpr::Column(c)) => {
                let fields = input.fields();
                let index = fields.iter().position(|col| &col.name == c).unwrap();
                let right = &input.cols()[index];

                let left = binary_expr.evaluate(input)?;

                Ok(left
                    .iter()
                    .zip(right)
                    .map(|(l, r)| self.eval_op(l, r))
                    .collect::<Result<_>>()?)
            }
            (LogicalExpr::AliasedExpr(expr, _), expr2)
            | (expr2, LogicalExpr::AliasedExpr(expr, _)) => {
                let (_, left) = expr.clone().evaluate(input)?;
                let (_, right) = expr2.clone().evaluate(input)?;
                Ok(left
                    .iter()
                    .zip(right.iter())
                    .map(|(l, r)| self.eval_op(l, r))
                    .collect::<Result<_>>()?)
            }
        }
    }
}

impl BooleanBinaryExpr {
    fn eval_op(&self, left: &Value, right: &Value) -> bool {
        match &self.op {
            BinaryOperator::Eq => left == right,
            BinaryOperator::NotEq => left != right,
            BinaryOperator::Gt => left > right,
            BinaryOperator::Lt => left < right,
            BinaryOperator::GtEq => left >= right,
            BinaryOperator::LtEq => left <= right,
            e => todo!("{}", e),
        }
    }

    fn evaluate(self, input: &ResultSet) -> Vec<bool> {
        match (&self.left, &self.right) {
            (LogicalExpr::Column(c1), LogicalExpr::Column(c2)) => {
                let fields = input.fields();
                let index1 = fields.iter().position(|col| &col.name == c1).unwrap();
                let index2 = fields.iter().position(|col| &col.name == c2).unwrap();

                let left = &input.cols()[index1];
                let right = &input.cols()[index2];

                left.iter()
                    .zip(right)
                    .map(|(l, r)| self.eval_op(l, r))
                    .collect()
            }
            (LogicalExpr::Literal(v1), LogicalExpr::Column(c2)) => {
                let index2 = input
                    .fields()
                    .iter()
                    .position(|col| &col.name == c2)
                    .unwrap();
                let right = &input.cols()[index2];

                right.iter().map(|r| self.eval_op(v1, r)).collect()
            }
            (LogicalExpr::Column(c1), LogicalExpr::Literal(v2)) => {
                let index1 = input
                    .fields()
                    .iter()
                    .position(|col| &col.name == c1)
                    .unwrap();
                let left = &input.cols()[index1];
                left.iter().map(|l| self.eval_op(l, v2)).collect()
            }
            (LogicalExpr::Literal(v1), LogicalExpr::Literal(v2)) => {
                [self.eval_op(v1, v2)].repeat(input.len())
            }
            e => todo!("{:?}", e),
        }
    }
}

impl Executable for Projection {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let input = if matches!(self.input, LogicalPlan::Empty) {
            ResultSet::with_capacity(1)
        } else {
            self.input.execute(ctx)?
        };

        let output = self
            .projections
            .iter()
            .map(|p| p.evaluate(&input))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|(field, data)| ResultSet::new(vec![field], vec![data]))
            .reduce(|a, b| a.concat(b))
            .unwrap_or_default();

        Ok(output)
    }
}

impl Executable for Scan {
    fn execute(self, ctx: &mut Context) -> Result<ResultSet> {
        let txn_id = ctx.get_active_txn();
        let arc_catalog = ctx.get_catalog();
        let catalog = arc_catalog.read();
        let table = catalog.get_table(&self.table_name, txn_id).unwrap();

        let schema = table.get_schema();

        let mut cols: Vec<Vec<Value>> = vec![vec![]; schema.fields.len() + 2];

        // TODO: pass the tuple_id as tuple for udpate to use
        // need to define a tuple type first though
        table.scan(txn_id, |((page_id, slot_id), (_, tuple))| {
            let mut values = vec![
                lit!(UInt, page_id.to_string()),
                lit!(UInt, slot_id.to_string()),
            ];

            values.extend(tuple.get_values(&schema)?);

            let values = values.into_iter().map(|v| {
                if matches!(v, Value::StrAddr(_)) {
                    Value::Str(table.fetch_string(v.str_addr()))
                } else {
                    v
                }
            });

            values.enumerate().for_each(|(i, v)| {
                cols[i].push(v);
            });

            Ok(())
        })?;

        let mut fields = vec![
            Field::new("page_id", Types::UInt, Constraints::nullable(false)),
            Field::new("slot_id", Types::UInt, Constraints::nullable(false)),
        ];

        fields.extend(schema.fields.clone());

        Ok(ResultSet::new(fields, cols))
    }
}

impl Executable for Identity {
    fn execute(self, _ctx: &mut Context) -> Result<ResultSet> {
        Ok(self.input)
    }
}

#[cfg(test)]
mod tests {
    use crate::context::tests::test_context;

    use super::*;
    use anyhow::Result;

    fn identity_plan(values: &[Vec<Value>], fields: &[Field]) -> LogicalPlan {
        let set = ResultSet::from_rows(fields.to_owned(), values.to_owned());
        LogicalPlan::Identity(Identity::new(set))
    }

    fn values_to_exprs(values: &[Vec<Value>]) -> Vec<Vec<LogicalExpr>> {
        values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|value| LogicalExpr::Literal(value.clone()))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn test_values() -> Result<()> {
        let mut ctx = test_context();
        let values = vec![
            vec![lit!(UInt, "1"), lit!(Str, "hello")],
            vec![lit!(UInt, "2"), lit!(Str, "world")],
        ];

        let input = values_to_exprs(&values);

        let schema = Schema::new(vec![
            Field::new("col_1", Types::UInt, Constraints::unique(true)),
            Field::new("col_2", Types::Str, Constraints::nullable(false)),
        ]);
        let expected = ResultSet::from_rows(schema.fields.clone(), values);
        let plan = Values::new(input, Schema::default());

        let output = plan.execute(&mut ctx).unwrap();

        // don't check the schema
        assert_eq!(output.cols(), expected.cols());

        Ok(())
    }

    #[test]
    fn test_filter() -> Result<()> {
        let mut ctx = test_context();

        let values = vec![
            vec![lit!(UInt, "1"), lit!(Str, "hello")],
            vec![lit!(UInt, "2"), lit!(Str, "world")],
            vec![lit!(UInt, "3"), lit!(Str, "hello")],
            vec![lit!(UInt, "4"), lit!(Str, "world")],
        ];

        let schema = Schema::new(vec![
            Field::new("col_1", Types::UInt, Constraints::unique(true)),
            Field::new("col_2", Types::Str, Constraints::nullable(false)),
        ]);

        let root = identity_plan(&values, &schema.fields);

        let filter = BooleanBinaryExpr::new(
            LogicalExpr::Column("col_1".to_string()),
            BinaryOperator::Gt,
            LogicalExpr::Literal(lit!(UInt, "2")),
        );

        let plan = Filter::new(root, filter);

        let expected = ResultSet::from_rows(schema.fields.clone(), values[2..].to_vec());
        let output = plan.execute(&mut ctx)?;

        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_projection() -> Result<()> {
        let mut ctx = test_context();

        let values = vec![
            vec![lit!(UInt, "1"), lit!(Str, "hello"), lit!(Char, "a")],
            vec![lit!(UInt, "2"), lit!(Str, "world"), lit!(Char, "b")],
            vec![lit!(UInt, "3"), lit!(Str, "hello"), lit!(Char, "c")],
            vec![lit!(UInt, "4"), lit!(Str, "world"), lit!(Char, "d")],
        ];

        let schema = Schema::new(vec![
            Field::new("col_1", Types::UInt, Constraints::unique(true)),
            Field::new("col_2", Types::Str, Constraints::nullable(false)),
            Field::new("col_3", Types::Char, Constraints::nullable(false)),
        ]);

        let root = identity_plan(&values, &schema.fields);

        let projections = vec![
            LogicalExpr::Column("col_1".to_string()),
            LogicalExpr::Column("col_3".to_string()),
        ];

        let plan = Projection::new(root, projections);

        let expected =
            ResultSet::from_rows(schema.fields.clone(), values.to_vec()).select(vec![0, 2]);
        let output = plan.execute(&mut ctx)?;

        assert_eq!(output, expected);
        Ok(())
    }
}
