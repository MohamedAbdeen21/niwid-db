pub mod result_set;

use crate::catalog::Catalog;
use crate::sql::logical_plan::expr::{BooleanBinaryExpr, LogicalExpr};
use crate::sql::logical_plan::plan::{
    CreateTable, DropTables, Filter, Insert, LogicalPlan, Scan, Truncate, Values,
};
use crate::sql::logical_plan::plan::{Explain, Projection};
use crate::tuple::schema::Schema;
use crate::tuple::Tuple;
use crate::types::Value;
use anyhow::{anyhow, Result};
use result_set::ResultSet;
use sqlparser::ast::BinaryOperator;

pub trait Executable {
    fn execute(self) -> Result<ResultSet>;
}

impl LogicalPlan {
    pub fn execute(self) -> Result<ResultSet> {
        match self {
            LogicalPlan::Projection(plan) => plan.execute(),
            LogicalPlan::Scan(scan) => scan.execute(),
            LogicalPlan::Filter(filter) => filter.execute(),
            LogicalPlan::CreateTable(create) => create.execute(),
            LogicalPlan::Explain(explain) => explain.execute(),
            LogicalPlan::Insert(i) => i.execute(),
            LogicalPlan::Values(v) => v.execute(),
            LogicalPlan::DropTables(d) => d.execute(),
            LogicalPlan::Truncate(t) => t.execute(),
            LogicalPlan::Empty => Ok(ResultSet::default()),
        }
    }
}

impl Executable for Truncate {
    fn execute(self) -> Result<ResultSet> {
        Catalog::get().lock().truncate_table(&self.table_name)?;

        Ok(ResultSet::default())
    }
}

impl Executable for DropTables {
    fn execute(self) -> Result<ResultSet> {
        for table_name in self.table_names {
            if Catalog::get()
                .lock()
                .drop_table(&table_name, self.if_exists)
                .is_none()
            {
                return Err(anyhow!("Table {} does not exist", table_name));
            }
        }

        Ok(ResultSet::default())
    }
}

impl Executable for Values {
    fn execute(self) -> Result<ResultSet> {
        let input = ResultSet::with_capacity(1);
        let output = self
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|expr| expr.evaluate(&input))
                    .reduce(|a, b| a.concat(b))
                    .unwrap() // TODO: ?
            })
            .reduce(|a, b| a.union(b))
            .unwrap();

        Ok(output)
    }
}

impl Executable for Insert {
    fn execute(self) -> Result<ResultSet> {
        let input = self.input.execute()?;

        if input.fields.len() != self.table_schema.fields.len() {
            return Err(anyhow!("Column count mismatch"));
        }

        for row in input.data {
            let tuple = Tuple::new(row, &self.schema);

            let _tuple_id = Catalog::get()
                .lock()
                .get_table(&self.table_name)
                .unwrap()
                .insert(tuple);
        }

        Ok(ResultSet::default())
    }
}

impl Executable for Explain {
    fn execute(self) -> Result<ResultSet> {
        println!("Logical plan:\n{}", self.input.print());
        // time the execution time
        if self.analyze {
            let start = std::time::Instant::now();
            let result = self.input.execute()?;
            println!("Execution time: {:?}", start.elapsed());
            Ok(result)
        } else {
            Ok(ResultSet::default())
        }
    }
}

impl Executable for CreateTable {
    fn execute(self) -> Result<ResultSet> {
        let catalog = Catalog::get();
        catalog
            .lock()
            .add_table(&self.table_name, &self.schema, self.if_not_exists)
            .unwrap();
        Ok(ResultSet::default())
    }
}

impl Executable for Filter {
    fn execute(self) -> Result<ResultSet> {
        let input = self.input.execute()?;
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
    fn evaluate(self, input: &ResultSet) -> ResultSet {
        let size = input.size();
        match self {
            LogicalExpr::Literal(ref c) => {
                let input_schema = Schema::new(input.fields.clone());
                let field = self.to_field(&input_schema);
                let data = (0..size).map(|_| vec![c.clone()]).collect::<Vec<_>>();
                ResultSet::new(vec![field], data)
            }
            LogicalExpr::Column(c) => {
                let index = input.fields.iter().position(|col| col.name == *c).unwrap();
                let data = (0..size)
                    .map(|i| vec![input.data[i][index].clone()])
                    .collect::<Vec<_>>();
                ResultSet::new(vec![input.fields[index].clone()], data)
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
        }
    }
}

impl Executable for Projection {
    fn execute(self) -> Result<ResultSet> {
        let input = if matches!(self.input, LogicalPlan::Empty) {
            ResultSet::with_capacity(1)
        } else {
            self.input.execute()?
        };

        let output = self
            .projections
            .iter()
            .cloned()
            .map(|p| p.evaluate(&input))
            .reduce(|a, b| a.concat(b))
            .unwrap();

        Ok(output)
    }
}

impl Executable for Scan {
    fn execute(self) -> Result<ResultSet> {
        let arc_catalog = Catalog::get();
        let mut catalog = arc_catalog.lock();
        let table = catalog.get_table(&self.table_name).unwrap();

        let schema = table.get_schema();

        let mut output = vec![];
        table.scan(|(_, (_, tuple))| {
            let values = tuple.get_values(&schema)?;

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

        Ok(ResultSet::new(schema.fields.clone(), output))
    }
}
