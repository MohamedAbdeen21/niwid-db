pub mod result_set;

use crate::catalog::Catalog;
use crate::sql::logical_plan::expr::BinaryExpr;
use crate::sql::logical_plan::expr::{BooleanBinaryExpr, LogicalExpr};
use crate::sql::logical_plan::plan::{
    CreateTable, DropTables, Filter, Insert, LogicalPlan, Scan, Truncate, Update, Values,
};
use crate::sql::logical_plan::plan::{Explain, Projection};
use crate::tuple::schema::{Field, Schema};
use crate::tuple::Tuple;
use crate::txn_manager::TxnId;
use crate::types::Value;
use crate::types::ValueFactory;
use crate::types::{Types, UInt};
use crate::value;
use anyhow::{anyhow, Result};
use result_set::ResultSet;
use sqlparser::ast::BinaryOperator;

pub trait Executable {
    fn execute(self, txn: Option<TxnId>) -> Result<ResultSet>;
}

impl LogicalPlan {
    pub fn execute(self, txn: Option<TxnId>) -> Result<ResultSet> {
        match self {
            LogicalPlan::Projection(plan) => plan.execute(txn),
            LogicalPlan::Scan(scan) => scan.execute(txn),
            LogicalPlan::Filter(filter) => filter.execute(txn),
            LogicalPlan::CreateTable(create) => create.execute(txn),
            LogicalPlan::Explain(explain) => explain.execute(txn),
            LogicalPlan::Insert(i) => i.execute(txn),
            LogicalPlan::Values(v) => v.execute(txn),
            LogicalPlan::DropTables(d) => d.execute(txn),
            LogicalPlan::Truncate(t) => t.execute(txn),
            LogicalPlan::Update(u) => u.execute(txn),
            LogicalPlan::Empty => Ok(ResultSet::default()),
        }
    }
}

impl Executable for Update {
    fn execute(self, txn_id: Option<TxnId>) -> Result<ResultSet> {
        let input = self.input.execute(txn_id)?;

        let c = Catalog::get();
        let mut catalog = c.lock();

        let table = catalog
            .get_table(&self.table_name, txn_id)
            .ok_or_else(|| anyhow!("Table {} does not exist", self.table_name))?;

        let (_, mask) = self.selection.evaluate(&input);

        let (selected_col, expr) = self.assignments;

        let schema = table.get_schema();

        let updated_col_id = schema
            .fields
            .iter()
            .position(|f| f.name == selected_col)
            .ok_or_else(|| anyhow!("Column {} does not exist", selected_col))?;

        let selected_rows = input
            .data
            .iter()
            .enumerate()
            .filter(|(i, _)| mask[*i].is_truthy())
            .map(|(_, r)| r)
            .collect::<Vec<_>>();

        for row in selected_rows {
            let tuple_id = match (&row[0], &row[1]) {
                (Value::UInt(UInt(v)), Value::UInt(UInt(u))) => Some((*v, *u as usize)),
                _ => unreachable!(),
            };

            let mut new_tuple = row[2..].to_vec();

            for value in expr.evaluate(&input).1 {
                new_tuple[updated_col_id] = value;
            }

            let new_tuple = Tuple::new(new_tuple, &schema);

            table.update(tuple_id, new_tuple)?;
        }

        Ok(ResultSet::default())
    }
}

impl Executable for Truncate {
    fn execute(self, txn_id: Option<TxnId>) -> Result<ResultSet> {
        Catalog::get()
            .lock()
            .truncate_table(self.table_name, txn_id)?;

        Ok(ResultSet::default())
    }
}

impl Executable for DropTables {
    fn execute(self, txn_id: Option<TxnId>) -> Result<ResultSet> {
        for table_name in self.table_names {
            if Catalog::get()
                .lock()
                .drop_table(table_name.clone(), self.if_exists, txn_id)
                .is_none()
            {
                return Err(anyhow!("Table {} does not exist", table_name));
            }
        }

        Ok(ResultSet::default())
    }
}

impl Executable for Values {
    fn execute(self, _: Option<TxnId>) -> Result<ResultSet> {
        let input = ResultSet::with_capacity(1);
        let output = self
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|expr| expr.evaluate(&input))
                    .map(|(field, data)| ResultSet::from_col(field, data))
                    .reduce(|a, b| a.concat(b))
                    .unwrap() // TODO: ?
            })
            .reduce(|a, b| a.union(b))
            .unwrap();

        Ok(output)
    }
}

impl Executable for Insert {
    fn execute(self, txn_id: Option<TxnId>) -> Result<ResultSet> {
        let input = self.input.execute(txn_id)?;

        if input.fields.len() != self.table_schema.fields.len() {
            return Err(anyhow!("Column count mismatch"));
        }

        for row in input.data {
            let tuple = Tuple::new(row, &self.schema);

            let _tuple_id = Catalog::get()
                .lock()
                .get_table(&self.table_name, txn_id)
                .unwrap()
                .insert(tuple);
        }

        Ok(ResultSet::default())
    }
}

impl Executable for Explain {
    fn execute(self, _txn_id: Option<TxnId>) -> Result<ResultSet> {
        println!("Logical plan:\n{}", self.input.print());
        // time the execution time
        if self.analyze {
            let start = std::time::Instant::now();
            let result = self.input.execute(_txn_id)?;
            println!("Execution time: {:?}", start.elapsed());
            Ok(result)
        } else {
            Ok(ResultSet::default())
        }
    }
}

impl Executable for CreateTable {
    fn execute(self, txn_id: Option<TxnId>) -> Result<ResultSet> {
        let catalog = Catalog::get();
        catalog
            .lock()
            .add_table(self.table_name, &self.schema, self.if_not_exists, txn_id)
            .unwrap();
        Ok(ResultSet::default())
    }
}

impl Executable for Filter {
    fn execute(self, _txn_id: Option<TxnId>) -> Result<ResultSet> {
        let input = self.input.execute(_txn_id)?;
        let mask = self.expr.evaluate(&input);

        let output = input
            .data
            .into_iter()
            .enumerate()
            .filter(|(i, _)| mask[*i])
            .map(|(_, r)| r)
            .collect::<Vec<_>>();

        Ok(ResultSet::new(input.fields, output))
    }
}

impl LogicalExpr {
    fn evaluate(&self, input: &ResultSet) -> (Field, Vec<Value>) {
        let size = input.size();
        match self {
            LogicalExpr::Literal(ref c) => {
                let input_schema = Schema::new(input.fields.clone());
                let field = self.to_field(&input_schema);
                let data = (0..size).map(|_| c.clone()).collect::<Vec<_>>();
                (field, data)
            }
            LogicalExpr::Column(c) => {
                let index = input.fields.iter().position(|col| col.name == *c).unwrap();
                let data = (0..size)
                    .map(|i| input.data[i][index].clone())
                    .collect::<Vec<_>>();
                (input.fields[index].clone(), data)
            }
            LogicalExpr::BinaryExpr(ref expr) => {
                let schema = Schema::new(input.fields.clone());
                let field = self.to_field(&schema);
                (field, expr.evaluate(input))
            }
            LogicalExpr::AliasedExpr(ref expr, _) => {
                let result = expr.clone().evaluate(input);

                let schema = Schema::new(input.fields.clone());
                let field = self.to_field(&schema);

                (field, result.1)
            }
        }
    }
}

impl BinaryExpr {
    fn eval_op(&self, left: &Value, right: &Value) -> Value {
        match &self.op {
            BinaryOperator::Plus => left.add(right),
            BinaryOperator::Minus => left.sub(right),
            BinaryOperator::Multiply => left.mul(right),
            BinaryOperator::Divide => left.div(right),
            BinaryOperator::Eq => value!(Bool, left.eq(right).to_string()),
            e => todo!("{:?}", e),
        }
    }

    pub(super) fn evaluate(&self, input: &ResultSet) -> Vec<Value> {
        match (&self.left, &self.right) {
            (LogicalExpr::Column(c1), LogicalExpr::Column(c2)) => {
                let index1 = input.fields.iter().position(|col| &col.name == c1).unwrap();
                let index2 = input.fields.iter().position(|col| &col.name == c2).unwrap();
                input
                    .data
                    .iter()
                    .map(|row| self.eval_op(&row[index1], &row[index2]))
                    .collect()
            }
            (LogicalExpr::Literal(v1), LogicalExpr::Column(c2)) => {
                let index2 = input.fields.iter().position(|col| &col.name == c2).unwrap();
                input
                    .data
                    .iter()
                    .map(|row| self.eval_op(v1, &row[index2]))
                    .collect()
            }
            (LogicalExpr::Column(c1), LogicalExpr::Literal(v2)) => {
                let index1 = input.fields.iter().position(|col| &col.name == c1).unwrap();
                input
                    .data
                    .iter()
                    .map(|row| self.eval_op(&row[index1], v2))
                    .collect()
            }
            (LogicalExpr::Literal(v1), LogicalExpr::Literal(v2)) => {
                input.data.iter().map(|_| self.eval_op(v1, v2)).collect()
            }
            (LogicalExpr::BinaryExpr(l), LogicalExpr::BinaryExpr(r)) => {
                let left = l.evaluate(input);
                let right = r.evaluate(input);
                left.iter()
                    .zip(right.iter())
                    .map(|(l, r)| self.eval_op(l, r))
                    .collect()
            }
            (LogicalExpr::Literal(value), LogicalExpr::BinaryExpr(binary_expr)) => {
                let right = binary_expr.evaluate(input);
                right.iter().map(|r| self.eval_op(value, r)).collect()
            }
            (LogicalExpr::Column(c), LogicalExpr::BinaryExpr(binary_expr)) => {
                let index = input.fields.iter().position(|col| &col.name == c).unwrap();
                let right = binary_expr.evaluate(input);
                right
                    .iter()
                    .map(|r| self.eval_op(&input.data[0][index], r))
                    .collect()
            }
            (LogicalExpr::BinaryExpr(binary_expr), LogicalExpr::Literal(value)) => {
                let left = binary_expr.evaluate(input);
                left.iter().map(|l| self.eval_op(l, value)).collect()
            }
            (LogicalExpr::BinaryExpr(binary_expr), LogicalExpr::Column(_)) => {
                let left = binary_expr.evaluate(input);
                left.iter()
                    .map(|l| self.eval_op(l, &input.data[0][0]))
                    .collect()
            }
            (LogicalExpr::AliasedExpr(expr, _), expr2)
            | (expr2, LogicalExpr::AliasedExpr(expr, _)) => {
                let (_, left) = expr.clone().evaluate(input);
                let (_, right) = expr2.clone().evaluate(input);
                left.iter()
                    .zip(right.iter())
                    .map(|(l, r)| self.eval_op(l, r))
                    .collect()
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
                let index1 = input.fields.iter().position(|col| &col.name == c1).unwrap();
                let index2 = input.fields.iter().position(|col| &col.name == c2).unwrap();
                input
                    .data
                    .iter()
                    .map(|row| self.eval_op(&row[index1], &row[index2]))
                    .collect()
            }
            (LogicalExpr::Literal(v1), LogicalExpr::Column(c2)) => {
                let index2 = input.fields.iter().position(|col| &col.name == c2).unwrap();
                input
                    .data
                    .iter()
                    .map(|row| self.eval_op(v1, &row[index2]))
                    .collect()
            }
            (LogicalExpr::Column(c1), LogicalExpr::Literal(v2)) => {
                let index1 = input.fields.iter().position(|col| &col.name == c1).unwrap();
                input
                    .data
                    .iter()
                    .map(|row| self.eval_op(&row[index1], v2))
                    .collect()
            }
            (LogicalExpr::Literal(v1), LogicalExpr::Literal(v2)) => {
                [self.eval_op(v1, v2)].repeat(input.size())
            }
            e => todo!("{:?}", e),
        }
    }
}

impl Executable for Projection {
    fn execute(self, _txn_id: Option<TxnId>) -> Result<ResultSet> {
        let input = if matches!(self.input, LogicalPlan::Empty) {
            ResultSet::with_capacity(1)
        } else {
            self.input.execute(_txn_id)?
        };

        let output = self
            .projections
            .iter()
            .cloned()
            .map(|p| p.evaluate(&input))
            .map(|(field, data)| ResultSet::from_col(field, data))
            .reduce(|a, b| a.concat(b))
            .unwrap();

        Ok(output)
    }
}

impl Executable for Scan {
    fn execute(self, txn_id: Option<TxnId>) -> Result<ResultSet> {
        let arc_catalog = Catalog::get();
        let mut catalog = arc_catalog.lock();
        let table = catalog.get_table(&self.table_name, txn_id).unwrap();

        let schema = table.get_schema();

        let mut output = vec![];
        // TODO: pass the tuple_id as tuple for udpate to use
        // need to define a tuple type first though
        table.scan(|((page_id, slot_id), (_, tuple))| {
            let mut values = vec![
                value!(UInt, page_id.to_string()),
                value!(UInt, slot_id.to_string()),
            ];

            values.extend(tuple.get_values(&schema)?);

            let values = values
                .into_iter()
                .map(|v| {
                    if matches!(v, Value::StrAddr(_)) {
                        Value::Str(table.fetch_string(v.str_addr()))
                    } else {
                        v
                    }
                })
                .collect();

            output.push(values);
            Ok(())
        })?;

        let mut fields = vec![
            Field::new("page_id", Types::UInt, false),
            Field::new("slot_id", Types::UInt, false),
        ];

        fields.extend(schema.fields.clone());

        Ok(ResultSet::new(fields, output))
    }
}
