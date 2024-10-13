pub mod result_set;

use crate::catalog::Catalog;
use crate::sql::logical_plan::expr::LogicalExpr;
use crate::sql::logical_plan::plan::{CreateTable, Filter, Insert, LogicalPlan, Scan};
use crate::sql::logical_plan::plan::{Explain, Projection};
use crate::tuple::schema::Schema;
use crate::tuple::Tuple;
use crate::types::Value;
use anyhow::Result;
use result_set::ResultSet;

pub trait Executable {
    fn execute(&self) -> Result<ResultSet>;
}

impl LogicalPlan {
    pub fn execute(&self) -> Result<ResultSet> {
        match self {
            LogicalPlan::Projection(plan) => plan.execute(),
            LogicalPlan::Scan(scan) => scan.execute(),
            LogicalPlan::Filter(filter) => filter.execute(),
            LogicalPlan::CreateTable(create) => create.execute(),
            LogicalPlan::Explain(explain) => explain.execute(),
            LogicalPlan::Insert(i) => i.execute(),
            LogicalPlan::Empty => Ok(ResultSet::default()),
        }
    }
}

impl Executable for Insert {
    fn execute(&self) -> Result<ResultSet> {
        let input = self.input.execute()?;

        for row in input.data {
            println!("{:?}", row);
            let tuple = Tuple::new(row, &self.schema);
            println!("{:?}", tuple);

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
    fn execute(&self) -> Result<ResultSet> {
        println!("{}", self.input.print());
        Ok(ResultSet::default())
    }
}

impl Executable for CreateTable {
    fn execute(&self) -> Result<ResultSet> {
        let catalog = Catalog::get();
        catalog
            .lock()
            .add_table(&self.table_name, &self.schema, self.if_not_exists)
            .unwrap();
        Ok(ResultSet::default())
    }
}

impl LogicalExpr {
    fn evaluate(&self, input: &ResultSet) -> ResultSet {
        let size = input.size();
        match self {
            LogicalExpr::Literal(c) => {
                let input_schema = Schema::new(input.cols.clone());
                let field = self.to_field(&input_schema);
                let data = (0..size).map(|_| vec![c.clone()]).collect::<Vec<_>>();
                ResultSet::new(vec![field], data)
            }
            LogicalExpr::Column(c) => {
                let index = input.cols.iter().position(|col| col.name == *c).unwrap();
                let data = (0..size)
                    .map(|i| vec![input.data[i][index].clone()])
                    .collect::<Vec<_>>();
                ResultSet::new(vec![input.cols[index].clone()], data)
            }
        }
    }
}

impl Executable for Projection {
    fn execute(&self) -> Result<ResultSet> {
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
    fn execute(&self) -> Result<ResultSet> {
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

impl Executable for Filter {
    fn execute(&self) -> Result<ResultSet> {
        let input = self.input.execute()?;

        let mut output = vec![];
        for row in input.data {
            // TODO: evaluate the expression
            output.push(row);
        }

        Ok(ResultSet::new(input.cols, output))
    }
}
